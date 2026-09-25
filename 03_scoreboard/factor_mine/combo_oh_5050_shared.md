# Factor mine action — `combo_oh_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared overnight_mega_h1/union_hot_n4_holdup w=0.5,0.5 net=priority

Cash book **+26.36%** ($12,636) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 138 · skips 62 · realized $+1377.50.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: overnight_mega_h1 50%, union_hot_n4_holdup 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: overnight_mega_h1 50%, union_hot_n4_holdup 50%.
- Member: overnight_mega_h1 (50% · long · hold 1).
- Member: union_hot_n4_holdup (50% · long · hold 1).
- Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day.

### When it buys

- At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- One ticker, one side. Claim order: fresh-E, then heat, then the other longs, then shorts. A name already held cannot be opened on the other side.
- Shared pile: leftover cash is offered in claim order (fresh-E, then heat, then other longs, then shorts). Each kid splits their room equally across *their* new names (leftover, whole shares, fees out of cash). Unused room spills to the next kid. A short fill adds cash; that cash can later fund a long, still capped by the cover rule (equity ≥ 2× notional).
- Skip a name if the slice cannot buy 1 share after fees.
- Skip a name if there is no official 09:30 open.
- Long lots buy shares (want the price up). Short lots borrow (want the price down) and are marked as a liability.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is the owner kid’s hold — the buy morning counts as 1.
- No extra panic button unless that owner recipe has one (🚨 / last-red / news🔴).
- List-drop: after the owner’s min-hold, sell at the 09:30 open if the name is no longer on *that owner’s* list today. The heat kid falling off does not sell a fresh-E lot.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `combo` — each member keeps its own 09:30 list (not a mashed shopping list).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **owner mix**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $47.14.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $2500.00; owner union_hot_n4_holdup | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $2500.00; owner union_hot_n4_holdup | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $2500.00; owner union_hot_n4_holdup | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_hot_n4_holdup | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $2591.73; owner union_hot_n4_holdup | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $2591.73; owner union_hot_n4_holdup | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $2591.73; owner union_hot_n4_holdup | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $2591.73; owner union_hot_n4_holdup | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▼ close $10,066.79 vs 09:30 $10,412.10 (session -277.26) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $9,866.28 vs yday $10,066.79 (-200.51) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | ▲ +11.10 after sell → book $9,863.94; vs 09:30 mark -2.34 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | ▼ -4.81 after sell → book $9,861.51; vs 09:30 mark -2.43 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | ▼ -166.08 after sell → book $9,846.10; vs 09:30 mark -15.41 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | ▼ -364.07 after sell → book $9,843.06; vs 09:30 mark -3.04 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $2460.77; owner union_hot_n4_holdup | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $2460.77; owner union_hot_n4_holdup | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $2460.77; owner union_hot_n4_holdup | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $2460.77; owner union_hot_n4_holdup | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.42 | ▲ close $9,851.95 vs 09:30 $9,866.28 (session +25.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.42 | ▲ 09:30 equity $9,861.50 vs yday $9,851.95 (+9.55) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | ▼ -162.01 after sell → book $9,853.81; vs 09:30 mark -7.69 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | ▼ -64.51 after sell → book $9,851.23; vs 09:30 mark -2.58 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | ▲ +11.57 after sell → book $9,849.03; vs 09:30 mark -2.20 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,164.03 | ▼ close $9,698.67 vs 09:30 $9,861.50 (session -150.36) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,164.03 | ▲ 09:30 equity $9,738.05 vs yday $9,698.67 (+39.38) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | ▲ +105.24 after sell → book $9,733.36; vs 09:30 mark -4.69 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,733.36 | ▲ close $9,733.36 vs 09:30 $9,738.05 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.36 | ▲ 09:30 equity $9,733.36 vs yday $9,733.36 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,530.22 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1216.67; owner union_hot_n4_holdup | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1057 | $1.15 | $13.64 | — | $7,301.04 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1216.67; owner union_hot_n4_holdup | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 102 | $11.81 | $2.30 | — | $6,093.61 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1216.67; owner union_hot_n4_holdup | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 888 | $1.37 | $11.46 | — | $4,865.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1216.67; owner union_hot_n4_holdup | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 21 | $229.55 | $2.05 | — | $42.99 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-5.5; combo leftover $4865.59; owner overnight_mega_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.99 | ▼ close $9,635.03 vs 09:30 $9,733.36 (session -66.87) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.99 | ▲ 09:30 equity $10,100.58 vs yday $9,635.03 (+465.55) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 102 | $11.57 | $2.32 | $-29.61 | $1,220.81 | ▼ -29.61 after sell → book $10,098.26; vs 09:30 mark -2.32 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 888 | $1.46 | $11.61 | $+56.85 | $2,505.68 | ▲ +56.85 after sell → book $10,086.65; vs 09:30 mark -11.61 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 21 | $243.85 | $2.10 | $+296.14 | $7,624.42 | ▲ +296.14 after sell → book $10,084.54; vs 09:30 mark -2.11 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 424 | $4.49 | $5.47 | — | $5,715.19 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1906.11; owner union_hot_n4_holdup | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 279 | $6.81 | $3.60 | — | $3,811.60 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1906.11; owner union_hot_n4_holdup | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 42 | $90.03 | $2.12 | — | $28.23 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+6.4; combo leftover $3811.60; owner overnight_mega_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.23 | ▼ close $10,026.92 vs 09:30 $10,100.58 (session -46.44) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.23 | ▲ 09:30 equity $10,996.09 vs yday $10,026.92 (+969.17) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,167.79 | ▼ -63.57 after sell → book $10,994.05; vs 09:30 mark -2.04 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1057 | $1.83 | $13.83 | $+691.30 | $3,088.28 | ▲ +691.30 after sell → book $10,980.23; vs 09:30 mark -13.82 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 424 | $4.32 | $5.55 | $-83.10 | $4,914.40 | ▼ -83.10 after sell → book $10,974.67; vs 09:30 mark -5.56 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 279 | $8.03 | $3.66 | $+333.12 | $7,151.11 | ▲ +333.12 after sell → book $10,971.01; vs 09:30 mark -3.66 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 42 | $90.95 | $2.16 | $+34.37 | $10,968.85 | ▲ +34.37 after sell → book $10,968.85; vs 09:30 mark -2.16 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,968.85 | ▲ close $10,968.85 vs 09:30 $10,996.09 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,968.85 | ▲ 09:30 equity $10,968.85 vs yday $10,968.85 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 56 | $24.11 | $2.16 | — | $9,616.53 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1371.11; owner union_hot_n4_holdup | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 878 | $1.56 | $11.33 | — | $8,235.53 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1371.11; owner union_hot_n4_holdup | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 336 | $4.07 | $4.33 | — | $6,863.67 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1371.11; owner union_hot_n4_holdup | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 72 | $19.04 | $2.21 | — | $5,490.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1371.11; owner union_hot_n4_holdup | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 15 | $364.35 | $2.04 | — | $23.30 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $5490.59; owner overnight_mega_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.30 | ▲ close $11,308.00 vs 09:30 $10,968.85 (session +361.21) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.30 | ▼ 09:30 equity $10,542.31 vs yday $11,308.00 (-765.69) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 56 | $26.61 | $2.18 | $+135.66 | $1,511.28 | ▲ +135.66 after sell → book $10,540.13; vs 09:30 mark -2.18 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 878 | $1.60 | $11.48 | $+12.31 | $2,904.60 | ▲ +12.31 after sell → book $10,528.65; vs 09:30 mark -11.48 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 72 | $20.72 | $2.23 | $+116.52 | $4,394.21 | ▲ +116.52 after sell → book $10,526.42; vs 09:30 mark -2.23 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 15 | $323.47 | $2.08 | $-617.32 | $9,244.18 | ▼ -617.32 after sell → book $10,524.34; vs 09:30 mark -2.08 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 109 | $14.11 | $2.32 | — | $7,703.87 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1540.70; owner union_hot_n4_holdup | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 265 | $5.81 | $3.42 | — | $6,160.80 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1540.70; owner union_hot_n4_holdup | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 132 | $11.59 | $2.39 | — | $4,629.19 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1540.70; owner union_hot_n4_holdup | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 5 | $118.50 | $2.00 | — | $4,034.69 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 3 | $199.94 | $2.00 | — | $3,432.87 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; ret5=+2.1; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 3 | $182.75 | $2.00 | — | $2,882.62 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-12.9; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 3 | $212.64 | $2.00 | — | $2,242.70 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-3.0; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `RY` | 3 | $206.95 | $2.00 | — | $1,619.85 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.8; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `SNPS` | 1 | $405.10 | $1.99 | — | $1,212.76 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+1.1; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TD` | 5 | $119.11 | $2.00 | — | $615.21 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.4; combo leftover $661.31; owner overnight_mega_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $615.21 | ▲ close $10,676.40 vs 09:30 $10,542.31 (session +174.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $615.21 | ▲ 09:30 equity $11,075.73 vs yday $10,676.40 (+399.33) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 109 | $14.20 | $2.35 | $+5.15 | $2,160.66 | ▲ +5.15 after sell → book $11,073.38; vs 09:30 mark -2.35 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 265 | $6.50 | $3.48 | $+175.96 | $3,879.68 | ▲ +175.96 after sell → book $11,069.90; vs 09:30 mark -3.48 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 132 | $12.18 | $2.42 | $+73.73 | $5,485.02 | ▲ +73.73 after sell → book $11,067.48; vs 09:30 mark -2.42 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 5 | $118.77 | $2.02 | $-2.68 | $6,076.85 | ▼ -2.68 after sell → book $11,065.46; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 3 | $230.05 | $2.02 | $+86.31 | $6,764.98 | ▲ +86.31 after sell → book $11,063.44; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 3 | $208.25 | $2.02 | $+72.48 | $7,387.71 | ▲ +72.48 after sell → book $11,061.42; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 3 | $222.86 | $2.02 | $+26.64 | $8,054.27 | ▲ +26.64 after sell → book $11,059.40; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RY` | 3 | $206.82 | $2.02 | $-4.41 | $8,672.71 | ▼ -4.41 after sell → book $11,057.38; vs 09:30 mark -2.02 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SNPS` | 1 | $419.66 | $2.01 | $+10.55 | $9,090.36 | ▲ +10.55 after sell → book $11,055.37; vs 09:30 mark -2.01 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TD` | 5 | $120.17 | $2.02 | $+1.27 | $9,689.18 | ▲ +1.27 after sell → book $11,053.34; vs 09:30 mark -2.03 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 175 | $9.19 | $2.52 | — | $8,078.42 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1614.86; owner union_hot_n4_holdup | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 11 | $144.18 | $2.02 | — | $6,490.41 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1614.86; owner union_hot_n4_holdup | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 87 | $18.50 | $2.25 | — | $4,878.66 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $1614.86; owner union_hot_n4_holdup | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 9 | $261.47 | $2.02 | — | $2,523.42 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+1.4; combo leftover $2439.33; owner overnight_mega_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 9 | $253.44 | $2.02 | — | $240.44 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; combo leftover $2439.33; owner overnight_mega_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.44 | ▼ close $11,022.48 vs 09:30 $11,075.73 (session -20.04) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.44 | ▼ 09:30 equity $10,648.95 vs yday $11,022.48 (-373.53) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 336 | $3.69 | $4.40 | $-136.41 | $1,475.88 | ▼ -136.41 after sell → book $10,644.55; vs 09:30 mark -4.40 | union_hot_n4_holdup: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 87 | $18.15 | $2.28 | $-34.98 | $3,052.65 | ▼ -34.98 after sell → book $10,642.27; vs 09:30 mark -2.28 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 9 | $261.16 | $2.05 | $-6.85 | $5,401.04 | ▼ -6.85 after sell → book $10,640.22; vs 09:30 mark -2.05 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 9 | $225.26 | $2.04 | $-257.68 | $7,426.34 | ▼ -257.68 after sell → book $10,638.18; vs 09:30 mark -2.04 | overnight_mega_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 265 | $14.00 | $3.42 | — | $3,712.92 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $3713.17; owner union_hot_n4_holdup | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 25 | $146.07 | $2.06 | — | $59.11 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $3713.17; owner union_hot_n4_holdup | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.11 | ▲ close $10,638.65 vs 09:30 $10,648.95 (session +5.95) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.11 | ▼ 09:30 equity $10,557.11 vs yday $10,638.65 (-81.54) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 175 | $9.50 | $2.56 | $+49.18 | $1,719.05 | ▲ +49.18 after sell → book $10,554.55; vs 09:30 mark -2.56 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 11 | $134.10 | $2.04 | $-114.95 | $3,192.11 | ▼ -114.95 after sell → book $10,552.51; vs 09:30 mark -2.04 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 25 | $148.03 | $2.10 | $+44.83 | $6,890.75 | ▲ +44.83 after sell → book $10,550.40; vs 09:30 mark -2.11 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,890.75 | ▼ close $10,415.25 vs 09:30 $10,557.11 (session -135.15) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,890.75 | ▼ 09:30 equity $10,346.35 vs yday $10,415.25 (-68.90) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 265 | $13.04 | $3.49 | $-261.31 | $10,342.86 | ▼ -261.31 after sell → book $10,342.86; vs 09:30 mark -3.49 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,342.86 | ▲ close $10,342.86 vs 09:30 $10,346.35 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,342.86 | ▲ 09:30 equity $10,342.86 vs yday $10,342.86 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,342.86 | ▲ close $10,342.86 vs 09:30 $10,342.86 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,342.86 | ▲ 09:30 equity $10,342.86 vs yday $10,342.86 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1452 | $1.78 | $18.73 | — | $7,739.57 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $2585.72; owner union_hot_n4_holdup | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 140 | $18.40 | $2.41 | — | $5,161.16 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $2585.72; owner union_hot_n4_holdup | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 188 | $13.71 | $2.55 | — | $2,581.13 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $2585.72; owner union_hot_n4_holdup | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 107 | $23.88 | $2.31 | — | $23.66 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $2585.72; owner union_hot_n4_holdup | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.66 | ▼ close $9,770.74 vs 09:30 $10,342.86 (session -546.12) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.66 | ▲ 09:30 equity $9,875.82 vs yday $9,770.74 (+105.08) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 140 | $18.15 | $2.45 | $-39.86 | $2,562.20 | ▼ -39.86 after sell → book $9,873.36; vs 09:30 mark -2.46 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 188 | $13.89 | $2.61 | $+28.68 | $5,170.92 | ▲ +28.68 after sell → book $9,870.76; vs 09:30 mark -2.60 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 107 | $23.84 | $2.35 | $-8.94 | $7,719.45 | ▼ -8.94 after sell → book $9,868.41; vs 09:30 mark -2.35 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 102 | $25.18 | $2.30 | — | $5,148.79 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $2573.15; owner union_hot_n4_holdup | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 326 | $7.87 | $4.21 | — | $2,578.97 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $2573.15; owner union_hot_n4_holdup | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 444 | $5.79 | $5.73 | — | $2.48 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $2573.15; owner union_hot_n4_holdup | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.48 | ▲ close $10,430.62 vs 09:30 $9,875.82 (session +574.44) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.48 | ▼ 09:30 equity $10,081.14 vs yday $10,430.62 (-349.48) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1452 | $1.56 | $18.99 | $-349.90 | $2,255.87 | ▼ -349.90 after sell → book $10,062.15; vs 09:30 mark -18.99 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 102 | $26.44 | $2.33 | $+123.89 | $4,950.41 | ▲ +123.89 after sell → book $10,059.81; vs 09:30 mark -2.34 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 326 | $7.76 | $4.28 | $-44.35 | $7,475.89 | ▼ -44.35 after sell → book $10,055.53; vs 09:30 mark -4.28 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 444 | $5.81 | $5.82 | $-2.67 | $10,049.71 | ▼ -2.67 after sell → book $10,049.71; vs 09:30 mark -5.82 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.71 | ▲ close $10,049.71 vs 09:30 $10,081.14 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.71 | ▲ 09:30 equity $10,049.71 vs yday $10,049.71 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.71 | ▲ close $10,049.71 vs 09:30 $10,049.71 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.71 | ▲ 09:30 equity $10,049.71 vs yday $10,049.71 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.71 | ▲ close $10,049.71 vs 09:30 $10,049.71 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.71 | ▲ 09:30 equity $10,049.71 vs yday $10,049.71 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 930 | $2.70 | $12.00 | — | $7,526.71 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $2512.43; owner union_hot_n4_holdup | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 511 | $4.91 | $6.59 | — | $5,011.11 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $2512.43; owner union_hot_n4_holdup | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 407 | $6.16 | $5.25 | — | $2,498.74 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $2512.43; owner union_hot_n4_holdup | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 795 | $3.13 | $10.26 | — | $0.14 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $2512.43; owner union_hot_n4_holdup | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.14 | ▲ close $10,273.79 vs 09:30 $10,049.71 (session +258.17) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.14 | ▲ 09:30 equity $10,415.06 vs yday $10,273.79 (+141.27) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 407 | $6.02 | $5.34 | $-67.57 | $2,444.94 | ▼ -67.57 after sell → book $10,409.72; vs 09:30 mark -5.34 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,444.94 | ▲ close $10,951.91 vs 09:30 $10,415.06 (session +542.19) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,444.94 | ▲ 09:30 equity $11,111.95 vs yday $10,951.91 (+160.04) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 511 | $5.11 | $6.70 | $+88.91 | $5,049.45 | ▲ +88.91 after sell → book $11,105.25; vs 09:30 mark -6.70 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 795 | $3.64 | $10.41 | $+384.78 | $7,932.84 | ▲ +384.78 after sell → book $11,094.84; vs 09:30 mark -10.41 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,932.84 | ▲ close $11,318.04 vs 09:30 $11,111.95 (session +223.20) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,932.84 | ▲ 09:30 equity $11,336.64 vs yday $11,318.04 (+18.60) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1469 | $1.80 | $18.95 | — | $5,269.69 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $2644.28; owner union_hot_n4_holdup | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 113 | $23.29 | $2.33 | — | $2,635.59 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $2644.28; owner union_hot_n4_holdup | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 180 | $14.62 | $2.53 | — | $1.46 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $2644.28; owner union_hot_n4_holdup | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.46 | ▼ close $11,303.88 vs 09:30 $11,336.64 (session -8.95) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.46 | ▲ 09:30 equity $11,356.13 vs yday $11,303.88 (+52.25) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 113 | $24.09 | $2.37 | $+85.70 | $2,721.26 | ▲ +85.70 after sell → book $11,353.76; vs 09:30 mark -2.37 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 180 | $13.77 | $2.58 | $-158.11 | $5,197.28 | ▼ -158.11 after sell → book $11,351.18; vs 09:30 mark -2.58 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 115 | $22.46 | $2.33 | — | $2,612.05 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $2598.64; owner union_hot_n4_holdup | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 70 | $36.76 | $2.20 | — | $36.65 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $2598.64; owner union_hot_n4_holdup | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.65 | ▲ close $11,787.58 vs 09:30 $11,356.13 (session +440.93) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.65 | ▼ 09:30 equity $11,710.89 vs yday $11,787.58 (-76.69) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1469 | $1.96 | $19.22 | $+196.87 | $2,896.67 | ▲ +196.87 after sell → book $11,691.67; vs 09:30 mark -19.22 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 115 | $21.30 | $2.37 | $-138.11 | $5,343.80 | ▼ -138.11 after sell → book $11,689.30; vs 09:30 mark -2.37 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 70 | $39.50 | $2.23 | $+187.37 | $8,106.56 | ▲ +187.37 after sell → book $11,687.06; vs 09:30 mark -2.24 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 92 | $29.32 | $2.27 | — | $5,406.86 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $2702.19; owner union_hot_n4_holdup | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 890 | $3.04 | $11.48 | — | $2,694.23 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $2702.19; owner union_hot_n4_holdup | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 33 | $81.40 | $2.09 | — | $5.94 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $2702.19; owner union_hot_n4_holdup | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.94 | ▲ close $11,750.00 vs 09:30 $11,710.89 (session +78.77) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.94 | ▲ 09:30 equity $12,184.64 vs yday $11,750.00 (+434.64) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 930 | $3.55 | $12.18 | $+766.33 | $3,295.26 | ▲ +766.33 after sell → book $12,172.46; vs 09:30 mark -12.18 | union_hot_n4_holdup: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 92 | $29.43 | $2.30 | $+5.55 | $6,000.52 | ▲ +5.55 after sell → book $12,170.16; vs 09:30 mark -2.30 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 890 | $4.00 | $11.66 | $+835.71 | $9,548.86 | ▲ +835.71 after sell → book $12,158.50; vs 09:30 mark -11.66 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 33 | $79.08 | $2.12 | $-80.77 | $12,156.38 | ▼ -80.77 after sell → book $12,156.38; vs 09:30 mark -2.12 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1230 | $2.47 | $15.87 | — | $9,102.41 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $3039.09; owner union_hot_n4_holdup | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 179 | $16.91 | $2.53 | — | $6,073.00 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $3039.09; owner union_hot_n4_holdup | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1841 | $1.65 | $23.75 | — | $3,011.60 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $3039.09; owner union_hot_n4_holdup | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 257 | $11.67 | $3.32 | — | $9.09 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $3039.09; owner union_hot_n4_holdup | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▲ close $12,492.54 vs 09:30 $12,184.64 (session +381.62) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $12,353.76 vs yday $12,492.54 (-138.78) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▲ close $12,364.04 vs 09:30 $12,353.76 (session +10.28) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▲ 09:30 equity $12,514.78 vs yday $12,364.04 (+150.74) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 179 | $16.92 | $2.58 | $-3.32 | $3,035.19 | ▼ -3.32 after sell → book $12,512.20; vs 09:30 mark -2.58 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1841 | $1.41 | $24.08 | $-489.66 | $5,606.93 | ▼ -489.66 after sell → book $12,488.13; vs 09:30 mark -24.07 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 257 | $12.80 | $3.38 | $+283.71 | $8,893.14 | ▲ +283.71 after sell → book $12,484.74; vs 09:30 mark -3.39 | union_hot_n4_holdup: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1097 | $2.70 | $14.15 | — | $5,917.09 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2964.38; owner union_hot_n4_holdup | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 70 | $41.76 | $2.20 | — | $2,991.69 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $2964.38; owner union_hot_n4_holdup | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 660 | $4.49 | $8.51 | — | $19.78 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $2964.38; owner union_hot_n4_holdup | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.78 | ▼ close $12,034.55 vs 09:30 $12,514.78 (session -425.33) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.78 | ▼ 09:30 equity $11,958.57 vs yday $12,034.55 (-75.98) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1230 | $2.68 | $16.10 | $+226.34 | $3,300.08 | ▲ +226.34 after sell → book $11,942.47; vs 09:30 mark -16.10 | union_hot_n4_holdup: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 70 | $36.02 | $2.23 | $-405.88 | $5,819.60 | ▼ -405.88 after sell → book $11,940.24; vs 09:30 mark -2.23 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 660 | $3.92 | $8.64 | $-390.06 | $8,401.45 | ▼ -390.06 after sell → book $11,931.60; vs 09:30 mark -8.64 | union_hot_n4_holdup: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,401.45 | ▲ close $14,270.40 vs 09:30 $11,958.57 (session +2,338.80) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,809.00 | ▲ 09:30 equity $13,189.92 vs yday $12,499.09 (+690.83) | 09:30 open · cash $4,809.00 (unchanged overnight, no fees) · equity $13,189.92 vs prior close $12,499.09 (+690.83) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 53 | $29.76 | $2.15 | — | $3,229.57 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $1603.00; owner union_hot_n4_holdup | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 98 | $16.21 | $2.28 | — | $1,638.71 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1603.00; owner union_hot_n4_holdup | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 102 | $15.58 | $2.30 | — | $47.14 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $1603.00; owner union_hot_n4_holdup | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.14 | ▼ close $12,636.40 vs 09:30 $13,189.92 (session -546.79) | 16:00 close · cash $47.14 · equity $12,636.40 vs 09:30 $13,189.92 (-553.52; session marks -546.79) · 5 name(s) marked open→close (per-name table). GLND×973 09:30 $6.06 → close $5.54 -505.96; VICR×9 09:30 $276.06 → close $276.06 -0.00; TJGC×53 09:30 $29.76 → close $26.24 -186.56; SECZ×98 09:30 $16.21 → close $15.96 -24.50; USDE×102 09:30 $15.58 → close $17.25 +170.23 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `TJX` | hard_red | hard-red S=-6.20 sit; no new long overnight_mega_h1 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_holdup |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_holdup |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new long overnight_mega_h1 |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new long overnight_mega_h1 |
| 2026-08-19 | `WMT` | hard_red | hard-red S=-7.20 sit; no new long overnight_mega_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new long overnight_mega_h1 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_holdup |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new long overnight_mega_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_holdup |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_holdup |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new long overnight_mega_h1 |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new long overnight_mega_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long overnight_mega_h1 |
| 2026-09-02 | `SNOW` | hard_red | hard-red S=-3.83 sit; no new long overnight_mega_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new long overnight_mega_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_holdup |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_holdup |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_holdup |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_holdup |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_holdup |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.03 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 3.03 < 1 share @ 7.23 |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new long overnight_mega_h1 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_holdup |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_holdup |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_holdup |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 1097 | 2026-09-23 @ $2.70 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2964.38; owner union_hot_n4_holdup |
