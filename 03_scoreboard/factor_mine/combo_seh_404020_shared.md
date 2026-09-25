# Factor mine action — `combo_seh_404020_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_e_fresh_h3/union_hot_n4_h1 w=0.4,0.4,0.2 net=priority

Cash book **+0.02%** ($10,002) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 361 · skips 370 · realized $+2809.98.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 40%, union_e_fresh_h3 40%, union_hot_n4_h1 20%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 40%, union_e_fresh_h3 40%, union_hot_n4_h1 20%.
- Member: short_news_r_h3 (40% · short · hold 3).
- Member: union_e_fresh_h3 (40% · long · hold 3).
- Member: union_hot_n4_h1 (20% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,679.25.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 4115 | $0.81 | $45.68 | — | $6,621.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $3333.33; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 151 | $22.01 | $2.44 | — | $3,295.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $3333.33; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 23 | $45.98 | $2.06 | — | $2,235.62 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $1098.41; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,136.98 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $1098.41; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $71.84 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $1098.41; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.84 | ▲ close $10,555.74 vs 09:30 $10,000.00 (session +610.10) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.84 | ▲ 09:30 equity $10,674.02 vs yday $10,555.74 (+118.28) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 23 | $44.09 | $2.08 | $-47.61 | $1,083.83 | ▼ -47.61 after sell → book $10,671.94; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $2,158.92 | ▼ -23.55 after sell → book $10,669.79; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,317.94 | ▲ +93.88 after sell → book $10,667.72; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 140 | $1.18 | $2.07 | — | $3,150.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 8 | $19.17 | $1.56 | — | $2,995.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 110 | $1.50 | $1.98 | — | $2,828.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 8 | $19.57 | $1.59 | — | $2,670.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 14 | $11.12 | $1.60 | — | $2,513.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 12 | $13.55 | $1.66 | — | $2,349.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 15 | $10.83 | $1.67 | — | $2,184.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 16 | $9.89 | $1.63 | — | $2,025.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $165.90; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 13 | $24.68 | $2.03 | — | $1,702.14 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $337.50; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 153 | $2.20 | $2.45 | — | $1,363.09 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $337.50; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 107 | $12.70 | $2.38 | — | $2,719.08 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1363.09; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,719.08 | ▲ close $11,324.29 vs 09:30 $10,674.02 (session +677.18) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,719.08 | ▼ 09:30 equity $11,216.92 vs yday $11,324.29 (-107.37) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 13 | $24.83 | $2.05 | $-2.13 | $3,039.82 | ▼ -2.13 after sell → book $11,214.87; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 153 | $2.08 | $2.48 | $-22.53 | $3,356.34 | ▼ -22.53 after sell → book $11,212.39; vs 09:30 mark -2.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 66 | $4.19 | $2.19 | — | $3,077.61 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $279.69; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 40 | $6.87 | $2.11 | — | $2,800.70 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $279.69; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 20 | $13.64 | $2.05 | — | $2,525.85 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $279.69; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 6 | $41.23 | $2.01 | — | $2,276.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $279.69; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 395 | $1.15 | $5.18 | — | $2,725.53 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $455.29; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 127 | $3.56 | $2.41 | — | $3,175.23 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $455.29; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 14 | $31.70 | $2.06 | — | $3,616.97 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $455.29; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 151 | $3.01 | $2.49 | — | $4,068.99 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $455.29; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 66 | $6.80 | $2.22 | — | $4,515.57 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $455.29; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,515.57 | ▲ close $11,541.01 vs 09:30 $11,216.92 (session +351.34) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,515.57 | ▼ 09:30 equity $11,486.65 vs yday $11,541.01 (-54.36) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 4115 | $1.14 | $53.80 | $+1258.47 | $9,152.86 | ▲ +1,258.47 after sell → book $11,432.84; vs 09:30 mark -53.81 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 151 | $22.82 | $2.50 | $+117.37 | $12,596.19 | ▲ +117.37 after sell → book $11,430.35; vs 09:30 mark -2.49 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 66 | $3.94 | $2.21 | $-20.90 | $12,854.02 | ▼ -20.90 after sell → book $11,428.14; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 20 | $13.31 | $2.07 | $-10.72 | $13,118.15 | ▼ -10.72 after sell → book $11,426.07; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 6 | $41.50 | $2.03 | $-2.42 | $13,365.12 | ▼ -2.42 after sell → book $11,424.04; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,365.12 | ▲ close $11,483.87 vs 09:30 $11,486.65 (session +59.83) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,365.12 | ▼ 09:30 equity $11,446.66 vs yday $11,483.87 (-37.21) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 140 | $1.07 | $1.95 | $-19.42 | $13,512.97 | ▼ -19.42 after sell → book $11,444.71; vs 09:30 mark -1.95 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 8 | $18.98 | $1.56 | $-4.64 | $13,663.25 | ▼ -4.64 after sell → book $11,443.15; vs 09:30 mark -1.56 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 110 | $1.42 | $1.92 | $-12.70 | $13,817.53 | ▼ -12.70 after sell → book $11,441.23; vs 09:30 mark -1.92 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 8 | $19.58 | $1.61 | $-3.12 | $13,972.56 | ▼ -3.12 after sell → book $11,439.62; vs 09:30 mark -1.61 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 14 | $9.10 | $1.34 | $-31.21 | $14,098.62 | ▼ -31.21 after sell → book $11,438.28; vs 09:30 mark -1.34 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 12 | $13.01 | $1.62 | $-9.76 | $14,253.12 | ▼ -9.76 after sell → book $11,436.66; vs 09:30 mark -1.62 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 15 | $10.85 | $1.69 | $-3.06 | $14,414.18 | ▼ -3.06 after sell → book $11,434.97; vs 09:30 mark -1.69 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 16 | $11.50 | $1.91 | $+22.14 | $14,596.27 | ▲ +22.14 after sell → book $11,433.06; vs 09:30 mark -1.91 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 107 | $11.75 | $2.31 | $+96.43 | $13,336.71 | ▲ +96.43 after sell → book $11,430.75; vs 09:30 mark -2.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 40 | $7.19 | $2.13 | $+8.56 | $13,622.18 | ▲ +8.56 after sell → book $11,428.62; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,622.18 | ▲ close $11,432.43 vs 09:30 $11,446.66 (session +3.81) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,622.18 | ▼ 09:30 equity $11,390.80 vs yday $11,432.43 (-41.63) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 395 | $0.96 | $4.99 | $+63.69 | $13,236.81 | ▲ +63.69 after sell → book $11,385.81; vs 09:30 mark -4.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 127 | $4.01 | $2.37 | $-62.57 | $12,724.53 | ▼ -62.57 after sell → book $11,383.44; vs 09:30 mark -2.37 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 14 | $31.87 | $2.03 | $-6.48 | $12,276.32 | ▼ -6.48 after sell → book $11,381.41; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 151 | $2.95 | $2.44 | $+4.13 | $11,828.43 | ▲ +4.13 after sell → book $11,378.97; vs 09:30 mark -2.44 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 66 | $6.81 | $2.19 | $-5.07 | $11,376.78 | ▼ -5.07 after sell → book $11,376.78; vs 09:30 mark -2.19 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 128 | $4.43 | $2.37 | — | $10,807.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 12 | $46.85 | $2.03 | — | $10,243.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 5 | $97.43 | $2.00 | — | $9,753.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1896 | $0.30 | $11.38 | — | $9,173.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 63 | $9.01 | $2.18 | — | $8,604.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 146 | $3.89 | $2.43 | — | $8,033.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 16 | $34.05 | $2.04 | — | $7,486.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 25 | $22.44 | $2.06 | — | $6,923.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $568.84; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 48 | $11.81 | $2.13 | — | $6,354.47 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $576.98; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 3 | $150.14 | $2.00 | — | $5,902.06 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $576.98; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 501 | $1.15 | $6.46 | — | $5,319.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $576.98; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 421 | $1.37 | $5.43 | — | $4,737.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $576.98; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 4 | $204.45 | $2.04 | — | $5,553.00 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $947.45; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 44 | $21.40 | $2.17 | — | $6,492.43 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $947.45; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 5 | $173.90 | $2.05 | — | $7,359.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $947.45; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 8 | $106.38 | $2.06 | — | $8,208.86 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $947.45; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 205 | $4.61 | $2.71 | — | $9,151.20 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $947.45; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,151.20 | ▲ close $11,369.12 vs 09:30 $11,390.80 (session +45.90) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,151.20 | ▲ 09:30 equity $11,421.26 vs yday $11,369.12 (+52.14) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 48 | $11.57 | $2.15 | $-16.05 | $9,704.41 | ▼ -16.05 after sell → book $11,419.11; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 421 | $1.46 | $5.51 | $+26.95 | $10,313.55 | ▲ +26.95 after sell → book $11,413.59; vs 09:30 mark -5.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 5 | $115.18 | $2.00 | — | $9,735.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $687.57; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $9,110.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $687.57; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 38 | $17.93 | $2.10 | — | $8,426.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $687.57; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 7 | $93.98 | $2.01 | — | $7,766.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $687.57; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 15 | $43.08 | $2.04 | — | $7,118.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $687.57; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 298 | $2.30 | $3.84 | — | $6,429.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $687.57; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 238 | $4.49 | $3.07 | — | $5,357.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1071.57; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 157 | $6.81 | $2.46 | — | $4,286.09 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1071.57; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 275 | $3.11 | $3.63 | — | $5,137.71 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $857.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $5,937.56 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $857.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $6,780.26 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $857.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $7,635.00 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $857.22; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $8,469.87 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $857.22; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,469.87 | ▲ close $11,628.18 vs 09:30 $11,421.26 (session +246.18) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,469.87 | ▲ 09:30 equity $12,119.63 vs yday $11,628.18 (+491.45) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 3 | $142.70 | $2.02 | $-26.34 | $8,895.95 | ▼ -26.34 after sell → book $12,117.61; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 501 | $1.83 | $6.56 | $+327.66 | $9,806.23 | ▲ +327.66 after sell → book $12,111.06; vs 09:30 mark -6.55 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 238 | $4.32 | $3.12 | $-46.65 | $10,831.27 | ▼ -46.65 after sell → book $12,107.94; vs 09:30 mark -3.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 157 | $8.03 | $2.50 | $+186.58 | $12,089.48 | ▲ +186.58 after sell → book $12,105.44; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,089.48 | ▲ close $12,107.97 vs 09:30 $12,119.63 (session +2.53) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,089.48 | ▲ 09:30 equity $12,227.02 vs yday $12,107.97 (+119.05) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 128 | $4.42 | $2.41 | $-6.06 | $12,652.84 | ▼ -6.06 after sell → book $12,224.62; vs 09:30 mark -2.40 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 12 | $43.63 | $2.05 | $-42.71 | $13,174.35 | ▼ -42.71 after sell → book $12,222.57; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 5 | $104.00 | $2.02 | $+28.82 | $13,692.33 | ▲ +28.82 after sell → book $12,220.55; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1896 | $0.31 | $11.89 | $-4.31 | $14,268.20 | ▼ -4.31 after sell → book $12,208.66; vs 09:30 mark -11.89 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 63 | $9.23 | $2.20 | $+9.48 | $14,847.49 | ▲ +9.48 after sell → book $12,206.46; vs 09:30 mark -2.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 146 | $5.24 | $2.46 | $+192.21 | $15,610.06 | ▲ +192.21 after sell → book $12,203.99; vs 09:30 mark -2.47 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 16 | $34.72 | $2.06 | $+6.62 | $16,163.53 | ▲ +6.62 after sell → book $12,201.94; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 25 | $21.85 | $2.08 | $-18.90 | $16,707.69 | ▼ -18.90 after sell → book $12,199.85; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 4 | $212.00 | $2.00 | $-34.25 | $15,857.69 | ▼ -34.25 after sell → book $12,197.85; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 44 | $20.90 | $2.12 | $+17.71 | $14,935.97 | ▲ +17.71 after sell → book $12,195.73; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 5 | $170.64 | $2.00 | $+12.25 | $14,080.76 | ▲ +12.25 after sell → book $12,193.72; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 8 | $105.58 | $2.01 | $+2.33 | $13,234.11 | ▲ +2.33 after sell → book $12,191.71; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 205 | $4.77 | $2.64 | $-38.16 | $12,253.61 | ▼ -38.16 after sell → book $12,189.06; vs 09:30 mark -2.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $11,726.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 6 | $88.94 | $2.01 | — | $11,190.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 40 | $15.28 | $2.11 | — | $10,577.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $10,006.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 120 | $5.10 | $2.35 | — | $9,391.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 12 | $47.89 | $2.03 | — | $8,815.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 44 | $13.92 | $2.12 | — | $8,200.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 134 | $4.54 | $2.39 | — | $7,589.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $612.68; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 26 | $24.11 | $2.07 | — | $6,960.18 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $632.43; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 405 | $1.56 | $5.22 | — | $6,323.15 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $632.43; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 155 | $4.07 | $2.46 | — | $5,689.85 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $632.43; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 33 | $19.04 | $2.09 | — | $5,059.44 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $632.43; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 123 | $13.62 | $2.44 | — | $6,732.87 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1686.48; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 30 | $54.51 | $2.15 | — | $8,366.03 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1686.48; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $9,821.36 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1686.48; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,821.36 | ▲ close $12,163.73 vs 09:30 $12,227.02 (session +10.17) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,821.36 | ▲ 09:30 equity $12,244.20 vs yday $12,163.73 (+80.47) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 5 | $124.67 | $2.02 | $+43.42 | $10,442.69 | ▲ +43.42 after sell → book $12,242.18; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `DE` | 1 | $632.15 | $2.01 | $+4.88 | $11,072.82 | ▲ +4.88 after sell → book $12,240.16; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 38 | $18.14 | $2.12 | $+3.56 | $11,760.02 | ▲ +3.56 after sell → book $12,238.04; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 7 | $94.60 | $2.03 | $+0.30 | $12,420.19 | ▲ +0.30 after sell → book $12,236.01; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 15 | $44.39 | $2.06 | $+15.56 | $13,083.98 | ▲ +15.56 after sell → book $12,233.95; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 298 | $2.35 | $3.90 | $+7.15 | $13,780.38 | ▲ +7.15 after sell → book $12,230.05; vs 09:30 mark -3.90 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 275 | $2.83 | $3.55 | $+69.83 | $12,998.58 | ▲ +69.83 after sell → book $12,226.50; vs 09:30 mark -3.55 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $12,202.41 | ▲ +3.66 after sell → book $12,224.49; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $11,355.33 | ▼ -4.38 after sell → book $12,222.43; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $10,512.72 | ▲ +12.13 after sell → book $12,220.32; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $9,704.63 | ▲ +26.79 after sell → book $12,218.23; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 26 | $26.61 | $2.09 | $+60.84 | $10,394.41 | ▲ +60.84 after sell → book $12,216.15; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 405 | $1.60 | $5.30 | $+5.67 | $11,037.10 | ▲ +5.67 after sell → book $12,210.84; vs 09:30 mark -5.31 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 33 | $20.72 | $2.11 | $+51.24 | $11,718.76 | ▲ +51.24 after sell → book $12,208.74; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1340 | $0.58 | $11.83 | — | $10,925.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $781.25; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 149 | $5.21 | $2.44 | — | $10,146.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $781.25; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 5 | $131.37 | $2.00 | — | $9,488.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $781.25; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 42 | $18.26 | $2.12 | — | $8,719.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $781.25; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 22 | $34.30 | $2.06 | — | $7,962.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $781.25; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $7,306.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $781.25; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 57 | $14.11 | $2.16 | — | $6,500.18 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $811.85; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 139 | $5.81 | $2.41 | — | $5,690.19 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $811.85; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 70 | $11.59 | $2.20 | — | $4,877.04 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $811.85; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $5,730.75 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $975.41; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 79 | $12.22 | $2.28 | — | $6,693.85 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $975.41; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 192 | $5.08 | $2.63 | — | $7,666.58 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $975.41; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $8,593.00 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $975.41; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $9,390.72 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $975.41; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,390.72 | ▲ close $12,259.44 vs 09:30 $12,244.20 (session +90.97) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,390.72 | ▼ 09:30 equity $12,206.65 vs yday $12,259.44 (-52.79) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 57 | $14.20 | $2.18 | $+0.79 | $10,197.94 | ▲ +0.79 after sell → book $12,204.47; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 139 | $6.50 | $2.44 | $+91.06 | $11,099.00 | ▲ +91.06 after sell → book $12,202.03; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 70 | $12.18 | $2.22 | $+37.23 | $11,949.38 | ▲ +37.23 after sell → book $12,199.81; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 7 | $80.60 | $2.01 | — | $11,383.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 36 | $16.18 | $2.10 | — | $10,798.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 5 | $118.77 | $2.00 | — | $10,202.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 33 | $17.78 | $2.09 | — | $9,613.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 44 | $13.41 | $2.12 | — | $9,021.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 6 | $97.16 | $2.01 | — | $8,436.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 2 | $206.82 | $2.00 | — | $8,021.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 4 | $120.17 | $2.00 | — | $7,538.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $597.47; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 136 | $9.19 | $2.40 | — | $6,286.22 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1256.41; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 8 | $144.18 | $2.01 | — | $5,130.76 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1256.41; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 34 | $74.54 | $2.19 | — | $7,662.93 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2565.38; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 46 | $55.25 | $2.23 | — | $10,202.20 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2565.38; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,202.20 | ▼ close $12,165.37 vs 09:30 $12,206.65 (session -9.27) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,202.20 | ▼ 09:30 equity $12,053.76 vs yday $12,165.37 (-111.61) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 3 | $172.76 | $2.02 | $-10.77 | $10,718.46 | ▼ -10.77 after sell → book $12,051.74; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 6 | $93.30 | $2.03 | $+22.12 | $11,276.24 | ▲ +22.12 after sell → book $12,049.71; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 40 | $18.15 | $2.13 | $+110.56 | $12,000.11 | ▲ +110.56 after sell → book $12,047.58; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 4 | $132.80 | $2.02 | $-42.26 | $12,529.28 | ▼ -42.26 after sell → book $12,045.56; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 120 | $4.58 | $2.38 | $-67.13 | $13,076.50 | ▼ -67.13 after sell → book $12,043.18; vs 09:30 mark -2.38 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 12 | $48.42 | $2.05 | $+2.29 | $13,655.50 | ▲ +2.29 after sell → book $12,041.13; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 44 | $15.66 | $2.14 | $+72.30 | $14,342.40 | ▲ +72.30 after sell → book $12,038.99; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 134 | $3.38 | $2.42 | $-160.93 | $14,792.89 | ▼ -160.93 after sell → book $12,036.57; vs 09:30 mark -2.42 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 155 | $3.69 | $2.49 | $-63.85 | $15,362.35 | ▼ -63.85 after sell → book $12,034.08; vs 09:30 mark -2.49 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 123 | $13.90 | $2.36 | $-38.62 | $13,650.29 | ▼ -38.62 after sell → book $12,031.72; vs 09:30 mark -2.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 30 | $52.49 | $2.08 | $+56.37 | $12,073.51 | ▲ +56.37 after sell → book $12,029.64; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $10,680.23 | ▲ +62.05 after sell → book $12,027.64; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $10,155.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 35 | $15.01 | $2.10 | — | $9,628.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 5 | $103.89 | $2.00 | — | $9,107.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 137 | $3.88 | $2.40 | — | $8,573.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 12 | $44.40 | $2.03 | — | $8,038.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 21 | $24.69 | $2.05 | — | $7,517.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 63 | $8.35 | $2.18 | — | $6,989.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 14 | $37.65 | $2.03 | — | $6,460.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $534.01; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 153 | $14.00 | $2.45 | — | $4,315.94 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2153.46; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 8 | $252.24 | $2.10 | — | $6,331.77 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2157.97; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 71 | $30.18 | $2.29 | — | $8,472.26 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2157.97; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,472.26 | ▲ close $12,117.16 vs 09:30 $12,053.76 (session +113.16) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,472.26 | ▲ 09:30 equity $12,132.71 vs yday $12,117.16 (+15.55) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 1340 | $0.51 | $11.09 | $-120.74 | $9,144.57 | ▼ -120.74 after sell → book $12,121.62; vs 09:30 mark -11.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 149 | $5.00 | $2.47 | $-36.20 | $9,887.10 | ▼ -36.20 after sell → book $12,119.15; vs 09:30 mark -2.47 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 5 | $148.03 | $2.02 | $+79.27 | $10,625.23 | ▲ +79.27 after sell → book $12,117.13; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 42 | $19.25 | $2.14 | $+37.33 | $11,431.59 | ▲ +37.33 after sell → book $12,114.99; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 22 | $34.72 | $2.08 | $+5.11 | $12,193.35 | ▲ +5.11 after sell → book $12,112.91; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 2 | $298.01 | $2.02 | $-61.81 | $12,787.36 | ▼ -61.81 after sell → book $12,110.90; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $11,949.84 | ▲ +16.19 after sell → book $12,108.90; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 79 | $11.10 | $2.23 | $+83.98 | $11,070.71 | ▲ +83.98 after sell → book $12,106.67; vs 09:30 mark -2.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 192 | $4.97 | $2.57 | $+14.96 | $10,112.94 | ▲ +14.96 after sell → book $12,104.10; vs 09:30 mark -2.57 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $9,218.78 | ▲ +32.26 after sell → book $12,102.09; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $8,199.22 | ▼ -221.85 after sell → book $12,100.09; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 136 | $9.50 | $2.43 | $+37.33 | $9,488.79 | ▲ +37.33 after sell → book $12,097.66; vs 09:30 mark -2.43 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 8 | $134.10 | $2.03 | $-84.69 | $10,559.55 | ▼ -84.69 after sell → book $12,095.62; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,559.55 | ▼ close $12,019.87 vs 09:30 $12,132.71 (session -75.75) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,559.55 | ▲ 09:30 equity $12,050.11 vs yday $12,019.87 (+30.24) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 7 | $79.83 | $2.03 | $-9.43 | $11,116.33 | ▼ -9.43 after sell → book $12,048.08; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 36 | $15.97 | $2.12 | $-11.78 | $11,689.14 | ▼ -11.78 after sell → book $12,045.97; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 5 | $113.66 | $2.02 | $-29.58 | $12,255.41 | ▼ -29.58 after sell → book $12,043.94; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 33 | $18.28 | $2.11 | $+12.30 | $12,856.54 | ▲ +12.30 after sell → book $12,041.83; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 44 | $12.18 | $2.14 | $-58.38 | $13,390.32 | ▼ -58.38 after sell → book $12,039.69; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 6 | $96.65 | $2.03 | $-7.10 | $13,968.19 | ▼ -7.10 after sell → book $12,037.66; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 2 | $203.78 | $2.02 | $-10.09 | $14,373.74 | ▼ -10.09 after sell → book $12,035.65; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 4 | $120.54 | $2.02 | $-2.54 | $14,853.87 | ▼ -2.54 after sell → book $12,033.62; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 34 | $73.22 | $2.09 | $+40.60 | $12,362.30 | ▲ +40.60 after sell → book $12,031.53; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 46 | $54.76 | $2.13 | $+18.18 | $9,841.21 | ▲ +18.18 after sell → book $12,029.40; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 153 | $13.04 | $2.49 | $-151.82 | $11,833.84 | ▼ -151.82 after sell → book $12,026.91; vs 09:30 mark -2.49 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,833.84 | ▼ close $11,995.35 vs 09:30 $12,050.11 (session -31.56) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,833.84 | ▲ 09:30 equity $12,013.45 vs yday $11,995.35 (+18.10) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $12,325.23 | ▼ -32.93 after sell → book $12,011.44; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 35 | $15.01 | $2.12 | $-4.21 | $12,848.46 | ▼ -4.21 after sell → book $12,009.32; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 5 | $92.00 | $2.02 | $-63.48 | $13,306.44 | ▼ -63.48 after sell → book $12,007.30; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 137 | $3.32 | $2.43 | $-81.55 | $13,758.84 | ▼ -81.55 after sell → book $12,004.86; vs 09:30 mark -2.44 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 12 | $44.17 | $2.05 | $-6.83 | $14,286.84 | ▼ -6.83 after sell → book $12,002.82; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 21 | $21.97 | $2.07 | $-61.25 | $14,746.13 | ▼ -61.25 after sell → book $12,000.74; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 63 | $8.58 | $2.20 | $+10.11 | $15,284.47 | ▲ +10.11 after sell → book $11,998.54; vs 09:30 mark -2.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 14 | $35.80 | $2.05 | $-29.98 | $15,783.55 | ▼ -29.98 after sell → book $11,996.49; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 8 | $235.71 | $2.01 | $+128.13 | $13,895.86 | ▲ +128.13 after sell → book $11,994.48; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 71 | $26.78 | $2.20 | $+236.91 | $11,992.28 | ▲ +236.91 after sell → book $11,992.28; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,992.28 | ▲ close $11,992.28 vs 09:30 $12,013.45 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,992.28 | ▲ 09:30 equity $11,992.28 vs yday $11,992.28 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 55 | $10.74 | $2.15 | — | $11,399.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $11,045.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 86 | $6.90 | $2.25 | — | $10,449.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $10,093.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 26 | $22.32 | $2.07 | — | $9,510.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $8,994.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 12 | $47.60 | $2.03 | — | $8,421.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 39 | $15.09 | $2.11 | — | $7,831.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $599.61; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 366 | $1.78 | $4.72 | — | $7,174.85 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $652.59; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 35 | $18.40 | $2.10 | — | $6,528.76 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $652.59; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 47 | $13.71 | $2.13 | — | $5,882.26 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $652.59; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 27 | $23.88 | $2.07 | — | $5,235.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $652.59; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 176 | $14.85 | $2.64 | — | $7,846.39 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2617.71; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1530 | $1.71 | $20.08 | — | $10,442.60 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2617.71; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.60 | ▲ close $12,184.22 vs 09:30 $11,992.28 (session +242.28) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.60 | ▲ 09:30 equity $12,284.78 vs yday $12,184.22 (+100.56) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 35 | $18.15 | $2.12 | $-12.96 | $11,075.74 | ▼ -12.96 after sell → book $12,282.67; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 47 | $13.89 | $2.15 | $+4.18 | $11,726.42 | ▲ +4.18 after sell → book $12,280.52; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 27 | $23.84 | $2.09 | $-5.24 | $12,368.01 | ▼ -5.24 after sell → book $12,278.43; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 9 | $63.18 | $2.02 | — | $11,797.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 70 | $8.74 | $2.20 | — | $11,183.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 9 | $68.52 | $2.02 | — | $10,564.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 171 | $3.62 | $2.50 | — | $9,944.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 3 | $167.55 | $2.00 | — | $9,439.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 13 | $44.90 | $2.03 | — | $8,853.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 6 | $98.15 | $2.01 | — | $8,262.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 39 | $15.70 | $2.11 | — | $7,648.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $618.40; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 33 | $25.18 | $2.09 | — | $6,815.28 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $849.81; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 107 | $7.87 | $2.31 | — | $5,970.88 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $849.81; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 146 | $5.79 | $2.43 | — | $5,123.11 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $849.81; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 548 | $4.67 | $7.25 | — | $7,675.02 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2561.56; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 33 | $76.55 | $2.19 | — | $10,198.98 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2561.56; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,198.98 | ▲ close $12,311.38 vs 09:30 $12,284.78 (session +66.10) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,198.98 | ▼ 09:30 equity $12,232.67 vs yday $12,311.38 (-78.71) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 366 | $1.56 | $4.79 | $-88.20 | $10,766.98 | ▼ -88.20 after sell → book $12,227.88; vs 09:30 mark -4.79 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 33 | $26.44 | $2.11 | $+37.38 | $11,637.39 | ▲ +37.38 after sell → book $12,225.77; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 107 | $7.76 | $2.34 | $-16.42 | $12,465.37 | ▼ -16.42 after sell → book $12,223.43; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 146 | $5.81 | $2.46 | $-1.97 | $13,311.17 | ▼ -1.97 after sell → book $12,220.97; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,311.17 | ▲ close $12,450.16 vs 09:30 $12,232.67 (session +229.19) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,311.17 | ▼ 09:30 equity $12,441.40 vs yday $12,450.16 (-8.76) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 55 | $10.51 | $2.17 | $-17.25 | $13,887.05 | ▼ -17.25 after sell → book $12,439.23; vs 09:30 mark -2.17 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $14,251.26 | ▲ +10.48 after sell → book $12,437.21; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 86 | $9.39 | $2.27 | $+209.62 | $15,056.53 | ▲ +209.62 after sell → book $12,434.94; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $15,396.42 | ▼ -16.60 after sell → book $12,432.93; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 26 | $21.67 | $2.09 | $-21.06 | $15,957.75 | ▼ -21.06 after sell → book $12,430.84; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $16,461.57 | ▼ -12.17 after sell → book $12,428.82; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 12 | $56.94 | $2.05 | $+108.01 | $17,142.81 | ▲ +108.01 after sell → book $12,426.78; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 39 | $13.84 | $2.13 | $-52.98 | $17,680.44 | ▼ -52.98 after sell → book $12,424.65; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 176 | $13.60 | $2.52 | $+214.84 | $15,284.32 | ▲ +214.84 after sell → book $12,422.13; vs 09:30 mark -2.52 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1530 | $1.58 | $19.74 | $+159.08 | $12,847.19 | ▲ +159.08 after sell → book $12,402.40; vs 09:30 mark -19.73 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,847.19 | ▼ close $12,391.77 vs 09:30 $12,441.40 (session -10.63) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,847.19 | ▲ 09:30 equity $12,444.97 vs yday $12,391.77 (+53.20) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 9 | $67.44 | $2.04 | $+34.29 | $13,452.11 | ▲ +34.29 after sell → book $12,442.93; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 70 | $8.26 | $2.22 | $-38.02 | $14,028.09 | ▼ -38.02 after sell → book $12,440.71; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 9 | $64.60 | $2.04 | $-39.33 | $14,607.45 | ▼ -39.33 after sell → book $12,438.68; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 171 | $3.76 | $2.54 | $+19.75 | $15,247.87 | ▲ +19.75 after sell → book $12,436.13; vs 09:30 mark -2.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 3 | $142.43 | $2.02 | $-79.38 | $15,673.14 | ▼ -79.38 after sell → book $12,434.12; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 13 | $38.23 | $2.05 | $-90.85 | $16,168.02 | ▼ -90.85 after sell → book $12,432.07; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 6 | $98.71 | $2.03 | $-0.68 | $16,758.25 | ▼ -0.68 after sell → book $12,430.04; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 39 | $15.26 | $2.13 | $-21.39 | $17,351.26 | ▼ -21.39 after sell → book $12,427.91; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 548 | $4.36 | $7.07 | $+155.56 | $14,954.91 | ▲ +155.56 after sell → book $12,420.84; vs 09:30 mark -7.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 33 | $76.79 | $2.09 | $-12.20 | $12,418.75 | ▼ -12.20 after sell → book $12,418.75; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,418.75 | ▲ close $12,418.75 vs 09:30 $12,444.97 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,418.75 | ▲ 09:30 equity $12,418.75 vs yday $12,418.75 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $11,923.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 105 | $5.91 | $2.31 | — | $11,300.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $10,814.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 19 | $32.01 | $2.05 | — | $10,204.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 8 | $71.71 | $2.01 | — | $9,628.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 11 | $56.02 | $2.02 | — | $9,010.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 66 | $9.37 | $2.19 | — | $8,389.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 47 | $13.10 | $2.13 | — | $7,771.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $620.94; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 239 | $2.70 | $3.08 | — | $7,123.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $647.64; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 131 | $4.91 | $2.38 | — | $6,477.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $647.64; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 105 | $6.16 | $2.31 | — | $5,828.58 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $647.64; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 206 | $3.13 | $2.66 | — | $5,181.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $647.64; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 9 | $112.83 | $2.06 | — | $6,194.59 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1036.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 294 | $3.52 | $3.88 | — | $7,225.59 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1036.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 510 | $2.03 | $6.70 | — | $8,254.19 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1036.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 41 | $24.97 | $2.16 | — | $9,275.80 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1036.23; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 307 | $3.37 | $4.05 | — | $10,306.34 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1036.23; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,306.34 | ▲ close $12,459.10 vs 09:30 $12,418.75 (session +86.33) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,306.34 | ▲ 09:30 equity $12,535.37 vs yday $12,459.10 (+76.27) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 105 | $6.02 | $2.33 | $-19.34 | $10,936.11 | ▼ -19.34 after sell → book $12,533.04; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,936.11 | ▲ close $12,721.38 vs 09:30 $12,535.37 (session +188.34) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,936.11 | ▲ 09:30 equity $12,747.77 vs yday $12,721.38 (+26.39) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 131 | $5.11 | $2.41 | $+21.40 | $11,603.10 | ▲ +21.40 after sell → book $12,745.35; vs 09:30 mark -2.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 206 | $3.64 | $2.70 | $+99.70 | $12,350.24 | ▲ +99.70 after sell → book $12,742.65; vs 09:30 mark -2.70 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,350.24 | ▼ close $12,741.12 vs 09:30 $12,747.77 (session -1.53) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,350.24 | ▼ 09:30 equity $12,725.96 vs yday $12,741.12 (-15.16) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 3 | $140.03 | $2.02 | $-77.22 | $12,768.31 | ▼ -77.22 after sell → book $12,723.94; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 105 | $6.25 | $2.33 | $+31.06 | $13,422.23 | ▲ +31.06 after sell → book $12,721.61; vs 09:30 mark -2.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 2 | $253.34 | $2.02 | $+18.33 | $13,926.89 | ▲ +18.33 after sell → book $12,719.59; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 19 | $30.57 | $2.07 | $-31.47 | $14,505.66 | ▼ -31.47 after sell → book $12,717.53; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 8 | $78.12 | $2.03 | $+47.23 | $15,128.58 | ▲ +47.23 after sell → book $12,715.49; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 11 | $61.93 | $2.04 | $+60.94 | $15,807.77 | ▲ +60.94 after sell → book $12,713.45; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 66 | $9.40 | $2.21 | $-2.42 | $16,425.96 | ▼ -2.42 after sell → book $12,711.24; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 47 | $15.75 | $2.15 | $+120.27 | $17,164.06 | ▲ +120.27 after sell → book $12,709.09; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 9 | $118.18 | $2.02 | $-52.19 | $16,098.42 | ▼ -52.19 after sell → book $12,707.07; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 294 | $3.98 | $3.79 | $-142.91 | $14,924.51 | ▼ -142.91 after sell → book $12,703.28; vs 09:30 mark -3.79 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 510 | $1.85 | $6.58 | $+78.52 | $13,974.43 | ▲ +78.52 after sell → book $12,696.70; vs 09:30 mark -6.58 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 41 | $24.42 | $2.11 | $+18.28 | $12,971.10 | ▲ +18.28 after sell → book $12,694.59; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 307 | $3.75 | $3.96 | $-124.67 | $11,815.89 | ▼ -124.67 after sell → book $12,690.63; vs 09:30 mark -3.96 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 71 | $33.14 | $2.20 | — | $9,460.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $2363.18; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 57 | $40.93 | $2.16 | — | $7,125.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2363.18; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 439 | $1.80 | $5.66 | — | $6,329.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $791.73; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 33 | $23.29 | $2.09 | — | $5,559.05 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $791.73; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 54 | $14.62 | $2.15 | — | $4,767.42 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $791.73; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 128 | $18.61 | $2.48 | — | $7,147.02 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2383.71; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 349 | $6.83 | $4.64 | — | $9,526.05 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2383.71; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,526.05 | ▼ close $12,437.42 vs 09:30 $12,725.96 (session -231.82) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,526.05 | ▲ 09:30 equity $12,573.79 vs yday $12,437.42 (+136.37) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 33 | $24.09 | $2.11 | $+22.20 | $10,318.91 | ▲ +22.20 after sell → book $12,571.68; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 54 | $13.77 | $2.17 | $-50.22 | $11,060.31 | ▼ -50.22 after sell → book $12,569.50; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 34 | $81.00 | $2.09 | — | $8,304.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $2765.08; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 246 | $11.21 | $3.17 | — | $5,543.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $2765.08; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 697 | $7.95 | $9.30 | — | $11,075.24 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $5543.39; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,075.24 | ▲ close $13,012.69 vs 09:30 $12,573.79 (session +457.75) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,075.24 | ▼ 09:30 equity $12,983.83 vs yday $13,012.69 (-28.86) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 439 | $1.96 | $5.75 | $+58.83 | $11,929.93 | ▲ +58.83 after sell → book $12,978.08; vs 09:30 mark -5.75 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 45 | $29.32 | $2.12 | — | $10,608.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1325.55; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 436 | $3.04 | $5.62 | — | $9,279.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1325.55; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 16 | $81.40 | $2.04 | — | $7,975.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1325.55; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 188 | $34.44 | $2.81 | — | $14,446.99 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6484.15; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,446.99 | ▲ close $13,283.48 vs 09:30 $12,983.83 (session +318.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,446.99 | ▲ 09:30 equity $13,360.95 vs yday $13,283.48 (+77.47) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 239 | $3.55 | $3.13 | $+196.93 | $15,292.31 | ▲ +196.93 after sell → book $13,357.82; vs 09:30 mark -3.13 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 71 | $40.03 | $2.24 | $+484.75 | $18,132.20 | ▲ +484.75 after sell → book $13,355.58; vs 09:30 mark -2.24 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 57 | $41.00 | $2.19 | $-0.36 | $20,467.01 | ▼ -0.36 after sell → book $13,353.39; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 128 | $22.11 | $2.37 | $-452.85 | $17,634.55 | ▼ -452.85 after sell → book $13,351.01; vs 09:30 mark -2.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 349 | $6.55 | $4.50 | $+88.57 | $15,344.10 | ▲ +88.57 after sell → book $13,346.51; vs 09:30 mark -4.50 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 45 | $29.43 | $2.15 | $+0.68 | $16,666.31 | ▲ +0.68 after sell → book $13,344.37; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 436 | $4.00 | $5.71 | $+409.40 | $18,404.60 | ▲ +409.40 after sell → book $13,338.66; vs 09:30 mark -5.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $-41.22 | $19,667.82 | ▼ -41.22 after sell → book $13,336.60; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 663 | $2.47 | $8.55 | — | $18,021.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $1638.98; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 96 | $16.91 | $2.28 | — | $16,396.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1638.98; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 993 | $1.65 | $12.81 | — | $14,744.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $1638.98; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 140 | $11.67 | $2.41 | — | $13,108.55 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $1638.98; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 402 | $8.26 | $5.37 | — | $16,423.70 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3327.64; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $19,340.98 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3327.64; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,340.98 | ▲ close $13,517.40 vs 09:30 $13,360.95 (session +214.34) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,340.98 | ▼ 09:30 equity $13,469.12 vs yday $13,517.40 (-48.28) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 697 | $8.28 | $8.99 | $-244.82 | $13,564.31 | ▼ -244.82 after sell → book $13,460.13; vs 09:30 mark -8.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 165 | $9.11 | $2.48 | — | $12,058.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $1507.15; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 208 | $7.23 | $2.68 | — | $10,552.15 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $1507.15; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 35 | $93.97 | $2.22 | — | $13,838.88 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3363.74; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,838.88 | ▼ close $13,226.65 vs 09:30 $13,469.12 (session -226.09) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,838.88 | ▼ 09:30 equity $12,810.17 vs yday $13,226.65 (-416.48) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 34 | $82.00 | $2.12 | $+29.78 | $16,624.76 | ▲ +29.78 after sell → book $12,808.05; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 246 | $13.82 | $3.24 | $+635.65 | $20,021.24 | ▲ +635.65 after sell → book $12,804.81; vs 09:30 mark -3.24 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 96 | $16.92 | $2.31 | $-3.62 | $21,643.25 | ▼ -3.62 after sell → book $12,802.50; vs 09:30 mark -2.31 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 993 | $1.41 | $12.99 | $-264.12 | $23,030.39 | ▼ -264.12 after sell → book $12,789.51; vs 09:30 mark -12.99 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 140 | $12.80 | $2.45 | $+153.34 | $24,819.95 | ▲ +153.34 after sell → book $12,787.07; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 165 | $8.39 | $2.52 | $-123.81 | $26,201.77 | ▼ -123.81 after sell → book $12,784.54; vs 09:30 mark -2.53 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 208 | $6.83 | $2.73 | $-88.61 | $27,619.68 | ▼ -88.61 after sell → book $12,781.81; vs 09:30 mark -2.73 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 46 | $47.57 | $2.13 | — | $25,429.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2209.57; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 11 | $196.78 | $2.02 | — | $23,262.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2209.57; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 61 | $35.74 | $2.17 | — | $21,080.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2209.57; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 46 | $47.15 | $2.13 | — | $18,909.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2209.57; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 20 | $109.67 | $2.05 | — | $16,713.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2209.57; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 687 | $2.70 | $8.86 | — | $14,850.18 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1857.10; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 44 | $41.76 | $2.12 | — | $13,010.62 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1857.10; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 413 | $4.49 | $5.33 | — | $11,150.92 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1857.10; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 54 | $116.85 | $2.39 | — | $17,458.43 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6377.50; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,458.43 | ▼ close $12,621.56 vs 09:30 $12,810.17 (session -131.05) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,458.43 | ▲ 09:30 equity $12,684.47 vs yday $12,621.56 (+62.91) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 663 | $2.68 | $8.68 | $+122.00 | $19,226.60 | ▲ +122.00 after sell → book $12,675.79; vs 09:30 mark -8.68 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $16,223.24 | ▼ -86.07 after sell → book $12,673.78; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 44 | $36.02 | $2.14 | $-256.61 | $17,806.20 | ▼ -256.61 after sell → book $12,671.64; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 413 | $3.92 | $5.41 | $-244.08 | $19,421.81 | ▼ -244.08 after sell → book $12,666.23; vs 09:30 mark -5.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,421.81 | ▲ close $14,005.13 vs 09:30 $12,684.47 (session +1,338.90) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,259.73 | ▲ 09:30 equity $10,182.47 vs yday $9,969.82 (+212.65) | 09:30 open · cash $5,259.73 (unchanged overnight, no fees) · equity $10,182.47 vs prior close $9,969.82 (+212.65) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $3,483.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $2103.89; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 13 | $29.76 | $2.03 | — | $3,094.82 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $387.08; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 23 | $16.21 | $2.06 | — | $2,719.94 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $387.08; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 24 | $15.58 | $2.06 | — | $2,343.93 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $387.08; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 298 | $7.85 | $3.98 | — | $4,679.25 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $2343.93; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,679.25 | ▼ close $10,001.83 vs 09:30 $10,182.47 (session -168.52) | 16:00 close · cash $4,679.25 · equity $10,001.83 vs 09:30 $10,182.47 (-180.64; session marks -168.52) · 20 name(s) marked open→close (per-name table). ABVX×19 09:30 $94.87 → close $94.87 +0.00; AEHL×305 09:30 $9.05 → close $9.36 -94.55; ANAB×35 09:30 $51.70 → close $51.70 +0.00; BAND×40 09:30 $61.83 → close $61.83 -0.00; CBRL×35 09:30 $52.39 → close $51.81 -20.30; CTAS×8 09:30 $197.68 → close $197.68 -0.00; GIS×46 09:30 $34.83 → close $34.83 +0.00; GLND×315 09:30 $6.06 → close $5.54 -163.80; HALO×20 09:30 $115.36 → close $113.90 +29.20; KBH×35 09:30 $47.65 → close $47.65 +0.00; MLKN×95 09:30 $19.91 → close $19.91 -0.00; PAYX×20 09:30 $101.59 → close $101.59 +0.00; THO×27 09:30 $70.93 → close $70.93 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; VICR×3 09:30 $276.06 → close $276.06 -0.00; COST×2 09:30 $887.00 → close $922.76 +71.53; TJGC×13 09:30 $29.76 → close $26.24 -45.76; SECZ×23 09:30 $16.21 → close $15.96 -5.75; USDE×24 09:30 $15.58 → close $17.25 +40.05; RSKD×298 09:30 $7.85 → close $7.78 +20.86 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
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
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
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
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `DE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `DE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
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
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h3 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
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
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GWRE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
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
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
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
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
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
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `CBRL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
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
| `FIVN` | 188 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6484.15; owner short_news_r_h3 |
| `AEHL` | 402 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3327.64; owner short_news_r_h3 |
| `USFD` | 35 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3363.74; owner short_news_r_h3 |
| `CBRL` | 46 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2209.57; owner union_e_fresh_h3 |
| `CTAS` | 11 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2209.57; owner union_e_fresh_h3 |
| `GIS` | 61 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2209.57; owner union_e_fresh_h3 |
| `KBH` | 46 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2209.57; owner union_e_fresh_h3 |
| `PAYX` | 20 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2209.57; owner union_e_fresh_h3 |
| `GLND` | 687 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1857.10; owner union_hot_n4_h1 |
| `HALO` | 54 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6377.50; owner short_news_r_h3 |
