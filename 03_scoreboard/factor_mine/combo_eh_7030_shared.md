# Factor mine action — `combo_eh_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/union_hot_n4_h1 w=0.7,0.3 net=priority

Cash book **-3.65%** ($9,635) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 269 · skips 270 · realized $+3272.06.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 70%, union_hot_n4_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 70%, union_hot_n4_h1 30%.
- Member: union_e_fresh_h3 (70% · long · hold 3).
- Member: union_hot_n4_h1 (30% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $37.40.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 4320 | $0.81 | $47.95 | — | $6,452.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $3500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 159 | $22.01 | $2.47 | — | $2,950.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $3500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 21 | $45.98 | $2.05 | — | $1,983.16 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $983.60; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 42 | $23.33 | $2.12 | — | $1,001.18 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $983.60; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 19 | $50.62 | $2.05 | — | $37.29 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $983.60; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.29 | ▲ close $10,577.60 vs 09:30 $10,000.00 (session +634.24) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.29 | ▲ 09:30 equity $10,703.40 vs yday $10,577.60 (+125.80) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 21 | $44.09 | $2.07 | $-43.82 | $961.11 | ▼ -43.82 after sell → book $10,701.33; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 42 | $22.92 | $2.14 | $-21.47 | $1,921.62 | ▼ -21.47 after sell → book $10,699.20; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 19 | $55.29 | $2.07 | $+84.56 | $2,970.06 | ▲ +84.56 after sell → book $10,697.13; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 173 | $1.50 | $2.51 | — | $2,708.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 13 | $19.57 | $2.03 | — | $2,451.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 23 | $11.12 | $2.06 | — | $2,193.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 19 | $13.55 | $2.05 | — | $1,934.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 23 | $10.83 | $2.06 | — | $1,683.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 220 | $1.18 | $2.84 | — | $1,420.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 13 | $19.17 | $2.03 | — | $1,169.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 26 | $9.89 | $2.07 | — | $910.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $259.88; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 18 | $24.68 | $2.04 | — | $463.85 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $455.07; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 206 | $2.20 | $2.66 | — | $7.99 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $455.07; owner union_hot_n4_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.99 | ▲ close $11,336.71 vs 09:30 $10,703.40 (session +661.92) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.99 | ▼ 09:30 equity $11,209.09 vs yday $11,336.71 (-127.62) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 18 | $24.83 | $2.06 | $-1.41 | $452.86 | ▼ -1.41 after sell → book $11,207.02; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 206 | $2.08 | $2.70 | $-29.05 | $879.67 | ▼ -29.05 after sell → book $11,204.32; vs 09:30 mark -2.70 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 52 | $4.19 | $2.15 | — | $659.65 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $219.92; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 32 | $6.87 | $2.09 | — | $437.72 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $219.92; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 16 | $13.64 | $2.04 | — | $217.44 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $219.92; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 5 | $41.23 | $2.00 | — | $9.29 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $219.92; owner union_hot_n4_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.29 | ▲ close $11,513.16 vs 09:30 $11,209.09 (session +317.11) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.29 | ▼ 09:30 equity $11,410.68 vs yday $11,513.16 (-102.48) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 4320 | $1.14 | $56.48 | $+1321.16 | $4,877.60 | ▲ +1,321.16 after sell → book $11,354.19; vs 09:30 mark -56.49 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 159 | $22.82 | $2.52 | $+123.80 | $8,503.46 | ▲ +123.80 after sell → book $11,351.67; vs 09:30 mark -2.52 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 52 | $3.94 | $2.17 | $-17.31 | $8,706.18 | ▼ -17.31 after sell → book $11,349.51; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 16 | $13.31 | $2.06 | $-9.38 | $8,917.08 | ▼ -9.38 after sell → book $11,347.45; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 5 | $41.50 | $2.02 | $-2.68 | $9,122.55 | ▼ -2.68 after sell → book $11,345.42; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,122.55 | ▲ close $11,346.67 vs 09:30 $11,410.68 (session +1.25) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,122.55 | ▼ 09:30 equity $11,340.01 vs yday $11,346.67 (-6.66) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 173 | $1.42 | $2.55 | $-18.90 | $9,365.66 | ▼ -18.90 after sell → book $11,337.46; vs 09:30 mark -2.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 13 | $19.58 | $2.05 | $-3.95 | $9,618.16 | ▼ -3.95 after sell → book $11,335.42; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 23 | $9.10 | $2.08 | $-50.60 | $9,825.38 | ▼ -50.60 after sell → book $11,333.34; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 19 | $13.01 | $2.07 | $-14.37 | $10,070.50 | ▼ -14.37 after sell → book $11,331.27; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 23 | $10.85 | $2.08 | $-3.68 | $10,317.97 | ▼ -3.68 after sell → book $11,329.19; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 220 | $1.07 | $2.88 | $-29.92 | $10,550.49 | ▼ -29.92 after sell → book $11,326.31; vs 09:30 mark -2.88 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 13 | $18.98 | $2.05 | $-6.55 | $10,795.18 | ▼ -6.55 after sell → book $11,324.26; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 26 | $11.50 | $2.09 | $+37.57 | $11,092.09 | ▲ +37.57 after sell → book $11,322.17; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 32 | $7.19 | $2.11 | $+6.05 | $11,320.06 | ▲ +6.05 after sell → book $11,320.06; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,320.06 | ▲ close $11,320.06 vs 09:30 $11,340.01 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,320.06 | ▲ 09:30 equity $11,320.06 vs yday $11,320.06 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 10 | $97.43 | $2.02 | — | $10,343.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 223 | $4.43 | $2.88 | — | $9,352.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 3301 | $0.30 | $19.81 | — | $8,342.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 21 | $46.85 | $2.05 | — | $7,356.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 109 | $9.01 | $2.32 | — | $6,372.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 254 | $3.89 | $3.28 | — | $5,381.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 29 | $34.05 | $2.08 | — | $4,391.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 44 | $22.44 | $2.12 | — | $3,402.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $990.51; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $2,649.51 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $850.55; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 739 | $1.15 | $9.53 | — | $1,790.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $850.55; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 71 | $11.81 | $2.20 | — | $949.06 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $850.55; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 620 | $1.37 | $8.00 | — | $91.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $850.55; owner union_hot_n4_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▲ close $11,300.00 vs 09:30 $11,320.06 (session +38.22) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▲ 09:30 equity $11,438.85 vs yday $11,300.00 (+138.85) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 71 | $11.57 | $2.22 | $-21.82 | $910.91 | ▼ -21.82 after sell → book $11,436.63; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 620 | $1.46 | $8.11 | $+39.69 | $1,807.99 | ▲ +39.69 after sell → book $11,428.51; vs 09:30 mark -8.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 1 | $115.18 | $1.15 | — | $1,691.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $180.80; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 1 | $103.69 | $1.04 | — | $1,586.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $180.80; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 10 | $17.93 | $1.82 | — | $1,405.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $180.80; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 1 | $93.98 | $0.94 | — | $1,310.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $180.80; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 4 | $43.08 | $1.74 | — | $1,136.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $180.80; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 78 | $2.30 | $2.03 | — | $955.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $180.80; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 106 | $4.49 | $2.31 | — | $477.10 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $477.68; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 69 | $6.81 | $2.20 | — | $5.02 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $477.68; owner union_hot_n4_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.02 | ▲ close $11,675.08 vs 09:30 $11,438.85 (session +259.79) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.02 | ▲ 09:30 equity $12,101.20 vs yday $11,675.08 (+426.12) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $716.49 | ▼ -41.23 after sell → book $12,099.17; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 739 | $1.83 | $9.67 | $+483.32 | $2,059.19 | ▲ +483.32 after sell → book $12,089.50; vs 09:30 mark -9.67 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 106 | $4.32 | $2.34 | $-22.66 | $2,514.78 | ▼ -22.66 after sell → book $12,087.17; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 69 | $8.03 | $2.22 | $+79.76 | $3,066.63 | ▲ +79.76 after sell → book $12,084.95; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,066.63 | ▲ close $12,185.06 vs 09:30 $12,101.20 (session +100.11) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,066.63 | ▲ 09:30 equity $12,193.61 vs yday $12,185.06 (+8.55) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 10 | $104.00 | $2.04 | $+61.64 | $4,104.59 | ▲ +61.64 after sell → book $12,191.57; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 223 | $4.42 | $2.92 | $-8.03 | $5,087.33 | ▼ -8.03 after sell → book $12,188.65; vs 09:30 mark -2.92 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 3301 | $0.31 | $20.69 | $-7.49 | $6,089.94 | ▼ -7.49 after sell → book $12,167.95; vs 09:30 mark -20.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 21 | $43.63 | $2.07 | $-71.75 | $7,004.10 | ▼ -71.75 after sell → book $12,165.88; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 109 | $9.23 | $2.35 | $+19.32 | $8,007.82 | ▲ +19.32 after sell → book $12,163.53; vs 09:30 mark -2.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 254 | $5.24 | $3.33 | $+336.29 | $9,335.45 | ▲ +336.29 after sell → book $12,160.20; vs 09:30 mark -3.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 29 | $34.72 | $2.10 | $+15.26 | $10,340.24 | ▲ +15.26 after sell → book $12,158.11; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 44 | $21.85 | $2.14 | $-30.22 | $11,299.50 | ▼ -30.22 after sell → book $12,155.97; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 5 | $175.01 | $2.00 | — | $10,422.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 11 | $88.94 | $2.02 | — | $9,442.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 64 | $15.28 | $2.18 | — | $8,461.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 6 | $142.36 | $2.01 | — | $7,605.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 193 | $5.10 | $2.57 | — | $6,618.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 20 | $47.89 | $2.05 | — | $5,659.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 71 | $13.92 | $2.20 | — | $4,668.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 217 | $4.54 | $2.80 | — | $3,679.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $988.71; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 38 | $24.11 | $2.10 | — | $2,761.22 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $919.88; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 589 | $1.56 | $7.60 | — | $1,834.78 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $919.88; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 226 | $4.07 | $2.92 | — | $912.04 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $919.88; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 47 | $19.04 | $2.13 | — | $15.03 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $919.88; owner union_hot_n4_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.03 | ▲ close $12,167.27 vs 09:30 $12,193.61 (session +43.90) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.03 | ▼ 09:30 equity $11,975.30 vs yday $12,167.27 (-191.97) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 1 | $124.67 | $1.27 | $+7.07 | $138.43 | ▲ +7.07 after sell → book $11,974.03; vs 09:30 mark -1.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `WMT` | 1 | $105.51 | $1.08 | $-0.30 | $242.87 | ▼ -0.30 after sell → book $11,972.96; vs 09:30 mark -1.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 10 | $18.14 | $1.86 | $-1.64 | $422.40 | ▼ -1.64 after sell → book $11,971.09; vs 09:30 mark -1.87 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 1 | $94.60 | $0.97 | $-1.29 | $516.03 | ▼ -1.29 after sell → book $11,970.12; vs 09:30 mark -0.97 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 4 | $44.39 | $1.81 | $+1.70 | $691.78 | ▲ +1.70 after sell → book $11,968.31; vs 09:30 mark -1.81 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 78 | $2.35 | $2.09 | $-0.22 | $872.99 | ▼ -0.22 after sell → book $11,966.22; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 38 | $26.61 | $2.12 | $+90.77 | $1,882.05 | ▲ +90.77 after sell → book $11,964.10; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 589 | $1.60 | $7.71 | $+8.26 | $2,816.74 | ▲ +8.26 after sell → book $11,956.39; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 47 | $20.72 | $2.15 | $+74.68 | $3,788.43 | ▲ +74.68 after sell → book $11,954.24; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 758 | $0.58 | $6.69 | — | $3,339.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $441.98; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 84 | $5.21 | $2.24 | — | $2,899.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $441.98; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 3 | $131.37 | $2.00 | — | $2,503.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $441.98; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 24 | $18.26 | $2.06 | — | $2,063.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $441.98; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 12 | $34.30 | $2.03 | — | $1,649.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $441.98; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $1,321.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $441.98; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 31 | $14.11 | $2.08 | — | $881.51 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $440.33; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 75 | $5.81 | $2.21 | — | $443.55 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $440.33; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 38 | $11.59 | $2.10 | — | $1.21 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $440.33; owner union_hot_n4_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.21 | ▲ close $12,288.25 vs 09:30 $11,975.30 (session +357.43) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.21 | ▲ 09:30 equity $12,346.36 vs yday $12,288.25 (+58.11) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 31 | $14.20 | $2.10 | $-1.40 | $439.31 | ▼ -1.40 after sell → book $12,344.26; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 75 | $6.50 | $2.24 | $+47.30 | $924.57 | ▲ +47.30 after sell → book $12,342.02; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 38 | $12.18 | $2.12 | $+18.38 | $1,385.29 | ▲ +18.38 after sell → book $12,339.90; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 1 | $80.60 | $0.81 | — | $1,303.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 7 | $16.18 | $1.15 | — | $1,189.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 1 | $118.77 | $1.19 | — | $1,069.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 6 | $17.78 | $1.08 | — | $961.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 9 | $13.41 | $1.23 | — | $839.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 1 | $97.16 | $0.97 | — | $741.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 1 | $120.17 | $1.20 | — | $620.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $121.21; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 33 | $9.19 | $2.09 | — | $314.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $310.15; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 2 | $144.18 | $2.00 | — | $24.59 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $310.15; owner union_hot_n4_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.59 | ▼ close $12,245.29 vs 09:30 $12,346.36 (session -82.87) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.59 | ▼ 09:30 equity $12,206.52 vs yday $12,245.29 (-38.77) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 5 | $172.76 | $2.02 | $-15.28 | $886.37 | ▼ -15.28 after sell → book $12,204.49; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 11 | $93.30 | $2.04 | $+43.89 | $1,910.62 | ▲ +43.89 after sell → book $12,202.45; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 64 | $18.15 | $2.20 | $+179.30 | $3,070.02 | ▲ +179.30 after sell → book $12,200.25; vs 09:30 mark -2.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 6 | $132.80 | $2.03 | $-61.40 | $3,864.79 | ▼ -61.40 after sell → book $12,198.22; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 193 | $4.58 | $2.61 | $-105.54 | $4,746.12 | ▼ -105.54 after sell → book $12,195.61; vs 09:30 mark -2.61 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 20 | $48.42 | $2.07 | $+6.48 | $5,712.45 | ▲ +6.48 after sell → book $12,193.54; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 71 | $15.66 | $2.22 | $+119.11 | $6,822.09 | ▲ +119.11 after sell → book $12,191.32; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 217 | $3.38 | $2.85 | $-258.45 | $7,552.70 | ▼ -258.45 after sell → book $12,188.47; vs 09:30 mark -2.85 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 226 | $3.69 | $2.96 | $-91.76 | $8,383.68 | ▼ -91.76 after sell → book $12,185.51; vs 09:30 mark -2.96 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $7,859.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 48 | $15.01 | $2.13 | — | $7,136.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 7 | $103.89 | $2.01 | — | $6,407.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 189 | $3.88 | $2.56 | — | $5,671.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $4,959.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 29 | $24.69 | $2.08 | — | $4,241.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 87 | $8.35 | $2.25 | — | $3,512.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 19 | $37.65 | $2.05 | — | $2,795.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $733.57; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 199 | $14.00 | $2.59 | — | $6.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2795.10; owner union_hot_n4_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.52 | ▼ close $11,910.89 vs 09:30 $12,206.52 (session -254.93) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.52 | ▼ 09:30 equity $11,886.61 vs yday $11,910.89 (-24.28) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 758 | $0.51 | $6.28 | $-68.30 | $386.82 | ▼ -68.30 after sell → book $11,880.33; vs 09:30 mark -6.28 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 84 | $5.00 | $2.27 | $-22.15 | $804.55 | ▼ -22.15 after sell → book $11,878.06; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 3 | $148.03 | $2.02 | $+45.96 | $1,246.63 | ▲ +45.96 after sell → book $11,876.05; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 24 | $19.25 | $2.08 | $+19.62 | $1,706.54 | ▲ +19.62 after sell → book $11,873.96; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 12 | $34.72 | $2.05 | $+0.97 | $2,121.14 | ▲ +0.97 after sell → book $11,871.92; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 1 | $298.01 | $2.01 | $-32.91 | $2,417.13 | ▼ -32.91 after sell → book $11,869.90; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 33 | $9.50 | $2.11 | $+6.03 | $2,728.53 | ▲ +6.03 after sell → book $11,867.80; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 2 | $134.10 | $2.02 | $-24.17 | $2,994.71 | ▼ -24.17 after sell → book $11,865.78; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,994.71 | ▼ close $11,793.49 vs 09:30 $11,886.61 (session -72.29) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,994.71 | ▼ 09:30 equity $11,676.63 vs yday $11,793.49 (-116.86) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 1 | $79.83 | $0.82 | $-2.40 | $3,073.72 | ▼ -2.40 after sell → book $11,675.81; vs 09:30 mark -0.82 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 7 | $15.97 | $1.16 | $-3.78 | $3,184.35 | ▼ -3.78 after sell → book $11,674.65; vs 09:30 mark -1.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 1 | $113.66 | $1.16 | $-7.46 | $3,296.85 | ▼ -7.46 after sell → book $11,673.49; vs 09:30 mark -1.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 6 | $18.28 | $1.13 | $+0.78 | $3,405.40 | ▲ +0.78 after sell → book $11,672.36; vs 09:30 mark -1.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 9 | $12.18 | $1.14 | $-13.45 | $3,513.87 | ▼ -13.45 after sell → book $11,671.21; vs 09:30 mark -1.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 1 | $96.65 | $0.99 | $-2.47 | $3,609.53 | ▼ -2.47 after sell → book $11,670.22; vs 09:30 mark -0.99 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 1 | $120.54 | $1.23 | $-2.06 | $3,728.84 | ▼ -2.06 after sell → book $11,668.99; vs 09:30 mark -1.23 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 199 | $13.04 | $2.64 | $-196.27 | $6,321.16 | ▼ -196.27 after sell → book $11,666.35; vs 09:30 mark -2.64 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,321.16 | ▼ close $11,610.06 vs 09:30 $11,676.63 (session -56.29) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,321.16 | ▼ 09:30 equity $11,576.94 vs yday $11,610.06 (-33.12) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $6,812.55 | ▼ -32.93 after sell → book $11,574.92; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 48 | $15.01 | $2.15 | $-4.29 | $7,530.87 | ▼ -4.29 after sell → book $11,572.77; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 7 | $92.00 | $2.03 | $-87.27 | $8,172.84 | ▼ -87.27 after sell → book $11,570.74; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 189 | $3.32 | $2.60 | $-111.00 | $8,797.72 | ▼ -111.00 after sell → book $11,568.14; vs 09:30 mark -2.60 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 16 | $44.17 | $2.06 | $-7.78 | $9,502.39 | ▼ -7.78 after sell → book $11,566.08; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 29 | $21.97 | $2.10 | $-83.05 | $10,137.42 | ▼ -83.05 after sell → book $11,563.98; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 87 | $8.58 | $2.28 | $+15.48 | $10,881.60 | ▲ +15.48 after sell → book $11,561.71; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 19 | $35.80 | $2.07 | $-39.26 | $11,559.64 | ▼ -39.26 after sell → book $11,559.64; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,559.64 | ▲ close $11,559.64 vs 09:30 $11,576.94 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,559.64 | ▲ 09:30 equity $11,559.64 vs yday $11,559.64 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 94 | $10.74 | $2.27 | — | $10,547.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $9,841.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 146 | $6.90 | $2.43 | — | $8,832.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $8,121.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 45 | $22.32 | $2.12 | — | $7,114.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 3 | $257.00 | $2.00 | — | $6,341.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 21 | $47.60 | $2.05 | — | $5,339.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 67 | $15.09 | $2.19 | — | $4,326.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1011.47; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 607 | $1.78 | $7.83 | — | $3,238.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1081.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 58 | $18.40 | $2.16 | — | $2,169.01 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1081.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 78 | $13.71 | $2.22 | — | $1,097.40 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1081.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 45 | $23.88 | $2.12 | — | $20.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1081.67; owner union_hot_n4_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.68 | ▲ close $11,668.86 vs 09:30 $11,559.64 (session +140.62) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.68 | ▲ 09:30 equity $11,740.89 vs yday $11,668.86 (+72.03) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 58 | $18.15 | $2.18 | $-18.85 | $1,071.19 | ▼ -18.85 after sell → book $11,738.70; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 78 | $13.89 | $2.25 | $+9.57 | $2,152.37 | ▲ +9.57 after sell → book $11,736.46; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 45 | $23.84 | $2.15 | $-6.07 | $3,223.02 | ▼ -6.07 after sell → book $11,734.31; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 4 | $63.18 | $2.00 | — | $2,968.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 32 | $8.74 | $2.09 | — | $2,686.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 4 | $68.52 | $2.00 | — | $2,410.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 78 | $3.62 | $2.22 | — | $2,126.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 1 | $167.55 | $1.68 | — | $1,957.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 6 | $44.90 | $2.01 | — | $1,685.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 2 | $98.15 | $1.97 | — | $1,487.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 17 | $15.70 | $2.04 | — | $1,218.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $282.01; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 16 | $25.18 | $2.04 | — | $813.49 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $406.14; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 51 | $7.87 | $2.14 | — | $409.98 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $406.14; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 70 | $5.79 | $2.20 | — | $2.48 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $406.14; owner union_hot_n4_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.48 | ▲ close $11,903.18 vs 09:30 $11,740.89 (session +191.26) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.48 | ▼ 09:30 equity $11,794.49 vs yday $11,903.18 (-108.69) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 607 | $1.56 | $7.94 | $-146.28 | $944.49 | ▼ -146.28 after sell → book $11,786.54; vs 09:30 mark -7.95 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 16 | $26.44 | $2.06 | $+16.06 | $1,365.48 | ▲ +16.06 after sell → book $11,784.49; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 51 | $7.76 | $2.16 | $-9.92 | $1,759.07 | ▼ -9.92 after sell → book $11,782.32; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 70 | $5.81 | $2.22 | $-3.02 | $2,163.55 | ▼ -3.02 after sell → book $11,780.10; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,163.55 | ▼ close $11,754.02 vs 09:30 $11,794.49 (session -26.08) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,163.55 | ▼ 09:30 equity $11,733.54 vs yday $11,754.02 (-20.48) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 94 | $10.51 | $2.30 | $-26.66 | $3,149.19 | ▼ -26.66 after sell → book $11,731.24; vs 09:30 mark -2.30 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $3,879.64 | ▲ +24.97 after sell → book $11,729.23; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 146 | $9.39 | $2.46 | $+358.65 | $5,248.12 | ▲ +358.65 after sell → book $11,726.77; vs 09:30 mark -2.46 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $5,929.90 | ▼ -29.19 after sell → book $11,724.75; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 45 | $21.67 | $2.15 | $-33.52 | $6,902.90 | ▼ -33.52 after sell → book $11,722.60; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 3 | $252.92 | $2.02 | $-16.26 | $7,659.65 | ▼ -16.26 after sell → book $11,720.59; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 21 | $56.94 | $2.07 | $+192.01 | $8,853.31 | ▲ +192.01 after sell → book $11,718.51; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 67 | $13.84 | $2.21 | $-88.15 | $9,778.38 | ▼ -88.15 after sell → book $11,716.30; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,778.38 | ▼ close $11,702.80 vs 09:30 $11,733.54 (session -13.51) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,778.38 | ▼ 09:30 equity $11,692.76 vs yday $11,702.80 (-10.04) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 4 | $67.44 | $2.02 | $+13.02 | $10,046.12 | ▲ +13.02 after sell → book $11,690.74; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 32 | $8.26 | $2.11 | $-19.55 | $10,308.33 | ▼ -19.55 after sell → book $11,688.63; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 4 | $64.60 | $2.02 | $-19.70 | $10,564.71 | ▼ -19.70 after sell → book $11,686.61; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 78 | $3.76 | $2.25 | $+6.84 | $10,855.74 | ▲ +6.84 after sell → book $11,684.36; vs 09:30 mark -2.25 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 1 | $142.43 | $1.45 | $-28.25 | $10,996.73 | ▼ -28.25 after sell → book $11,682.92; vs 09:30 mark -1.44 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 6 | $38.23 | $2.03 | $-44.09 | $11,224.05 | ▼ -44.09 after sell → book $11,680.89; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 2 | $98.71 | $2.00 | $-2.85 | $11,419.47 | ▼ -2.85 after sell → book $11,678.89; vs 09:30 mark -2.00 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 17 | $15.26 | $2.06 | $-11.58 | $11,676.83 | ▼ -11.58 after sell → book $11,676.83; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,676.83 | ▲ close $11,676.83 vs 09:30 $11,692.76 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,676.83 | ▲ 09:30 equity $11,676.83 vs yday $11,676.83 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $10,688.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 172 | $5.91 | $2.51 | — | $9,669.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $8,698.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 31 | $32.01 | $2.08 | — | $7,704.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 14 | $71.71 | $2.03 | — | $6,698.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 18 | $56.02 | $2.04 | — | $5,687.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 109 | $9.37 | $2.32 | — | $4,664.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 77 | $13.10 | $2.22 | — | $3,653.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $1021.72; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 338 | $2.70 | $4.36 | — | $2,736.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $913.30; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 186 | $4.91 | $2.55 | — | $1,820.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $913.30; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 148 | $6.16 | $2.43 | — | $906.31 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $913.30; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 288 | $3.13 | $3.72 | — | $1.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $913.30; owner union_hot_n4_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.16 | ▲ close $11,784.48 vs 09:30 $11,676.83 (session +137.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.16 | ▲ 09:30 equity $11,847.06 vs yday $11,784.48 (+62.58) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 148 | $6.02 | $2.47 | $-25.62 | $889.65 | ▼ -25.62 after sell → book $11,844.59; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $889.65 | ▲ close $12,344.36 vs 09:30 $11,847.06 (session +499.77) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $889.65 | ▲ 09:30 equity $12,373.60 vs yday $12,344.36 (+29.24) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 186 | $5.11 | $2.59 | $+32.06 | $1,837.52 | ▲ +32.06 after sell → book $12,371.01; vs 09:30 mark -2.59 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 288 | $3.64 | $3.77 | $+139.39 | $2,882.07 | ▲ +139.39 after sell → book $12,367.24; vs 09:30 mark -3.77 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,882.07 | ▲ close $12,497.10 vs 09:30 $12,373.60 (session +129.86) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,882.07 | ▼ 09:30 equity $12,441.13 vs yday $12,497.10 (-55.97) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 6 | $140.03 | $2.03 | $-150.44 | $3,720.22 | ▼ -150.44 after sell → book $12,439.10; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 172 | $6.25 | $2.54 | $+53.43 | $4,792.67 | ▲ +53.43 after sell → book $12,436.55; vs 09:30 mark -2.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 4 | $253.34 | $2.02 | $+40.66 | $5,804.01 | ▲ +40.66 after sell → book $12,434.53; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 31 | $30.57 | $2.10 | $-48.83 | $6,749.58 | ▼ -48.83 after sell → book $12,432.43; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 14 | $78.12 | $2.05 | $+85.66 | $7,841.21 | ▲ +85.66 after sell → book $12,430.38; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 18 | $61.93 | $2.06 | $+102.27 | $8,953.88 | ▲ +102.27 after sell → book $12,428.31; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 109 | $9.40 | $2.35 | $-1.39 | $9,976.14 | ▼ -1.39 after sell → book $12,425.97; vs 09:30 mark -2.34 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 77 | $15.75 | $2.24 | $+199.59 | $11,186.64 | ▲ +199.59 after sell → book $12,423.72; vs 09:30 mark -2.25 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 118 | $33.14 | $2.34 | — | $7,273.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $3915.33; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 95 | $40.93 | $2.27 | — | $3,383.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $3915.33; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 626 | $1.80 | $8.08 | — | $2,248.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1127.72; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 48 | $23.29 | $2.13 | — | $1,128.23 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1127.72; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 77 | $14.62 | $2.22 | — | $0.26 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1127.72; owner union_hot_n4_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.26 | ▲ close $12,582.00 vs 09:30 $12,441.13 (session +175.33) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.26 | ▲ 09:30 equity $12,859.60 vs yday $12,582.00 (+277.60) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 48 | $24.09 | $2.15 | $+34.11 | $1,154.43 | ▲ +34.11 after sell → book $12,857.45; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 77 | $13.77 | $2.24 | $-69.91 | $2,212.48 | ▼ -69.91 after sell → book $12,855.21; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 69 | $11.21 | $2.20 | — | $1,436.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $774.37; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 9 | $81.00 | $2.02 | — | $705.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $774.37; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 31 | $22.46 | $2.08 | — | $7.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $705.77; owner union_hot_n4_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.43 | ▲ close $13,103.80 vs 09:30 $12,859.60 (session +254.89) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.43 | ▲ 09:30 equity $13,222.35 vs yday $13,103.80 (+118.55) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 626 | $1.96 | $8.19 | $+83.90 | $1,226.20 | ▲ +83.90 after sell → book $13,214.16; vs 09:30 mark -8.19 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 31 | $21.30 | $2.10 | $-40.15 | $1,884.40 | ▼ -40.15 after sell → book $13,212.06; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 21 | $29.32 | $2.05 | — | $1,266.62 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $628.13; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 206 | $3.04 | $2.66 | — | $638.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $628.13; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 7 | $81.40 | $2.01 | — | $66.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $628.13; owner union_hot_n4_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.95 | ▲ close $13,247.16 vs 09:30 $13,222.35 (session +41.82) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.95 | ▲ 09:30 equity $13,479.42 vs yday $13,247.16 (+232.26) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 338 | $3.55 | $4.43 | $+278.51 | $1,262.42 | ▲ +278.51 after sell → book $13,474.99; vs 09:30 mark -4.43 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 118 | $40.03 | $2.40 | $+808.27 | $5,983.56 | ▲ +808.27 after sell → book $13,472.59; vs 09:30 mark -2.40 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 95 | $41.00 | $2.32 | $+2.05 | $9,876.24 | ▲ +2.05 after sell → book $13,470.27; vs 09:30 mark -2.32 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 21 | $29.43 | $2.07 | $-1.82 | $10,492.19 | ▼ -1.82 after sell → book $13,468.20; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 206 | $4.00 | $2.70 | $+193.43 | $11,313.49 | ▲ +193.43 after sell → book $13,465.50; vs 09:30 mark -2.70 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 7 | $79.08 | $2.03 | $-20.28 | $11,865.02 | ▼ -20.28 after sell → book $13,463.47; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1200 | $2.47 | $15.48 | — | $8,885.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $2966.26; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 175 | $16.91 | $2.52 | — | $5,923.78 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $2966.26; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1797 | $1.65 | $23.18 | — | $2,935.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $2966.26; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 251 | $11.67 | $3.24 | — | $3.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $2966.26; owner union_hot_n4_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.14 | ▲ close $13,835.36 vs 09:30 $13,479.42 (session +416.30) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.14 | ▼ 09:30 equity $13,699.82 vs yday $13,835.36 (-135.54) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.14 | ▲ close $13,709.86 vs 09:30 $13,699.82 (session +10.04) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.14 | ▲ 09:30 equity $13,906.29 vs yday $13,709.86 (+196.43) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 69 | $13.82 | $2.22 | $+175.67 | $954.50 | ▲ +175.67 after sell → book $13,904.07; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 9 | $82.00 | $2.04 | $+4.95 | $1,690.46 | ▲ +4.95 after sell → book $13,902.03; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 175 | $16.92 | $2.57 | $-3.33 | $4,648.89 | ▼ -3.33 after sell → book $13,899.46; vs 09:30 mark -2.57 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1797 | $1.41 | $23.50 | $-477.96 | $7,159.16 | ▼ -477.96 after sell → book $13,875.96; vs 09:30 mark -23.50 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 251 | $12.80 | $3.31 | $+277.09 | $10,368.66 | ▲ +277.09 after sell → book $13,872.66; vs 09:30 mark -3.30 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 30 | $47.57 | $2.08 | — | $8,939.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1451.61; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 7 | $196.78 | $2.01 | — | $7,560.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1451.61; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 40 | $35.74 | $2.11 | — | $6,128.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1451.61; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 30 | $47.15 | $2.08 | — | $4,711.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1451.61; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 13 | $109.67 | $2.03 | — | $3,283.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1451.61; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 405 | $2.70 | $5.22 | — | $2,185.25 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1094.66; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 26 | $41.76 | $2.07 | — | $1,097.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1094.66; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 243 | $4.49 | $3.13 | — | $3.22 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1094.66; owner union_hot_n4_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.22 | ▼ close $13,387.02 vs 09:30 $13,906.29 (session -464.90) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.22 | ▲ 09:30 equity $13,389.15 vs yday $13,387.02 (+2.13) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1200 | $2.68 | $15.70 | $+220.82 | $3,203.52 | ▲ +220.82 after sell → book $13,373.45; vs 09:30 mark -15.70 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 26 | $36.02 | $2.09 | $-153.27 | $4,138.08 | ▼ -153.27 after sell → book $13,371.36; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 243 | $3.92 | $3.19 | $-143.61 | $5,088.67 | ▼ -143.61 after sell → book $13,368.17; vs 09:30 mark -3.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,088.67 | ▲ close $14,337.75 vs 09:30 $13,389.15 (session +969.57) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $696.87 | ▲ 09:30 equity $9,699.57 vs yday $9,627.88 (+71.69) | 09:30 open · cash $696.87 (unchanged overnight, no fees) · equity $9,699.57 vs prior close $9,627.88 (+71.69) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 7 | $29.76 | $2.01 | — | $486.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $232.29; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 14 | $16.21 | $2.03 | — | $257.57 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $232.29; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 14 | $15.58 | $2.03 | — | $37.40 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $232.29; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.40 | ▼ close $9,634.62 vs 09:30 $9,699.57 (session -58.88) | 16:00 close · cash $37.40 · equity $9,634.62 vs 09:30 $9,699.57 (-64.95; session marks -58.88) · 13 name(s) marked open→close (per-name table). ABVX×16 09:30 $94.87 → close $94.87 +0.00; ANAB×30 09:30 $51.70 → close $51.70 +0.00; CBRL×9 09:30 $52.39 → close $51.81 -5.22; CTAS×2 09:30 $197.68 → close $197.68 -0.00; GIS×12 09:30 $34.83 → close $34.83 +0.00; GLND×94 09:30 $6.06 → close $5.54 -48.88; KBH×9 09:30 $47.65 → close $47.65 +0.00; MLKN×81 09:30 $19.91 → close $19.91 -0.00; PAYX×4 09:30 $101.59 → close $101.59 -0.00; THO×23 09:30 $70.93 → close $70.93 +0.00; TJGC×7 09:30 $29.76 → close $26.24 -24.64; SECZ×14 09:30 $16.21 → close $15.96 -3.50; USDE×14 09:30 $15.58 → close $17.25 +23.36 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DE` | cash | leftover split 180.80 < 1 share @ 623.26 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `WMT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `WMT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `RY` | cash | leftover split 121.21 < 1 share @ 206.82 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h3 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h3 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-04 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GWRE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `GWRE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-14 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 1.05 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 1.05 < 1 share @ 7.23 |
| 2026-09-24 | `CBRL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CBRL` | 30 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1451.61; owner union_e_fresh_h3 |
| `CTAS` | 7 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1451.61; owner union_e_fresh_h3 |
| `GIS` | 40 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1451.61; owner union_e_fresh_h3 |
| `KBH` | 30 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1451.61; owner union_e_fresh_h3 |
| `PAYX` | 13 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1451.61; owner union_e_fresh_h3 |
| `GLND` | 405 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1094.66; owner union_hot_n4_h1 |
