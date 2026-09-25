# Factor mine action — `combo_he1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **+7.01%** ($10,701) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 283 · skips 104 · realized $+2529.81.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_e_fresh_h1 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: union_e_fresh_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $22.84.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 36 | $45.98 | $2.10 | — | $3,319.25 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 71 | $23.33 | $2.20 | — | $1,660.62 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $38.59 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.59 | ▲ close $10,449.19 vs 09:30 $10,000.00 (session +492.16) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.59 | ▲ 09:30 equity $10,528.70 vs yday $10,449.19 (+79.51) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3086 | $0.93 | $38.49 | $+297.57 | $2,870.07 | ▲ +297.57 after sell → book $10,490.20; vs 09:30 mark -38.50 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $5,504.00 | ▲ +144.46 after sell → book $10,487.84; vs 09:30 mark -2.36 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 36 | $44.09 | $2.12 | $-72.26 | $7,089.11 | ▼ -72.26 after sell → book $10,485.71; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 71 | $22.92 | $2.23 | $-33.54 | $8,714.21 | ▼ -33.54 after sell → book $10,483.49; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $10,481.38 | ▲ +145.14 after sell → book $10,481.38; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 33 | $19.57 | $2.09 | — | $9,833.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 58 | $11.12 | $2.16 | — | $9,186.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 436 | $1.50 | $5.62 | — | $8,526.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 48 | $13.55 | $2.13 | — | $7,874.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 60 | $10.83 | $2.17 | — | $7,222.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 555 | $1.18 | $7.16 | — | $6,560.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 34 | $19.17 | $2.09 | — | $5,906.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 66 | $9.89 | $2.19 | — | $5,251.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $655.09; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 106 | $24.68 | $2.31 | — | $2,632.65 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $2625.52; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1189 | $2.20 | $15.34 | — | $1.51 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $2625.52; owner union_hot_n4_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.51 | ▲ close $10,531.86 vs 09:30 $10,528.70 (session +93.75) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.51 | ▼ 09:30 equity $10,363.29 vs yday $10,531.86 (-168.57) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 33 | $19.57 | $2.11 | $-4.20 | $645.21 | ▼ -4.20 after sell → book $10,361.19; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 58 | $9.57 | $2.18 | $-94.25 | $1,198.09 | ▼ -94.25 after sell → book $10,359.00; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 436 | $1.52 | $5.71 | $-2.61 | $1,855.10 | ▼ -2.61 after sell → book $10,353.29; vs 09:30 mark -5.71 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 48 | $13.16 | $2.15 | $-23.01 | $2,484.63 | ▼ -23.01 after sell → book $10,351.14; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 60 | $11.19 | $2.19 | $+17.24 | $3,153.84 | ▲ +17.24 after sell → book $10,348.95; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 555 | $1.21 | $7.26 | $+2.23 | $3,818.12 | ▲ +2.23 after sell → book $10,341.69; vs 09:30 mark -7.26 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 34 | $20.25 | $2.11 | $+32.52 | $4,504.51 | ▲ +32.52 after sell → book $10,339.58; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 66 | $10.97 | $2.21 | $+66.55 | $5,226.32 | ▲ +66.55 after sell → book $10,337.37; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 106 | $24.83 | $2.35 | $+11.25 | $7,855.96 | ▲ +11.25 after sell → book $10,335.02; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1189 | $2.08 | $15.56 | $-167.63 | $10,319.47 | ▼ -167.63 after sell → book $10,319.47; vs 09:30 mark -15.55 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 615 | $4.19 | $7.93 | — | $7,734.68 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $2579.87; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 375 | $6.87 | $4.84 | — | $5,153.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $2579.87; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 189 | $13.64 | $2.56 | — | $2,573.08 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $2579.87; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 62 | $41.23 | $2.18 | — | $14.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $2579.87; owner union_hot_n4_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.64 | ▲ close $10,328.91 vs 09:30 $10,363.29 (session +26.95) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.64 | ▲ 09:30 equity $10,338.83 vs yday $10,328.91 (+9.92) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 615 | $3.94 | $8.05 | $-169.74 | $2,429.69 | ▼ -169.74 after sell → book $10,330.78; vs 09:30 mark -8.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 189 | $13.31 | $2.61 | $-67.54 | $4,942.67 | ▼ -67.54 after sell → book $10,328.17; vs 09:30 mark -2.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 62 | $41.50 | $2.21 | $+12.36 | $7,513.46 | ▲ +12.36 after sell → book $10,325.96; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,513.46 | ▼ close $10,168.46 vs 09:30 $10,338.83 (session -157.50) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,513.46 | ▲ 09:30 equity $10,209.71 vs yday $10,168.46 (+41.25) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 375 | $7.19 | $4.92 | $+110.24 | $10,204.79 | ▲ +110.24 after sell → book $10,204.79; vs 09:30 mark -4.92 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,204.79 | ▲ close $10,204.79 vs 09:30 $10,209.71 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,204.79 | ▲ 09:30 equity $10,204.79 vs yday $10,204.79 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $9,618.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 143 | $4.43 | $2.42 | — | $8,982.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2125 | $0.30 | $12.75 | — | $8,332.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 13 | $46.85 | $2.03 | — | $7,720.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 70 | $9.01 | $2.20 | — | $7,088.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 163 | $3.89 | $2.48 | — | $6,451.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $5,836.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 28 | $22.44 | $2.07 | — | $5,206.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $637.80; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $4,003.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1301.54; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $2,687.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1301.54; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $1,385.83 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1301.54; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 950 | $1.37 | $12.26 | — | $72.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1301.54; owner union_hot_n4_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.08 | ▼ close $10,141.41 vs 09:30 $10,204.79 (session -4.20) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.08 | ▲ 09:30 equity $10,325.54 vs yday $10,141.41 (+184.13) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 6 | $96.75 | $2.03 | $-8.12 | $650.55 | ▼ -8.12 after sell → book $10,323.51; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 143 | $4.68 | $2.45 | $+30.88 | $1,317.34 | ▲ +30.88 after sell → book $10,321.06; vs 09:30 mark -2.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2125 | $0.31 | $13.33 | $-4.83 | $1,962.76 | ▼ -4.83 after sell → book $10,307.73; vs 09:30 mark -13.33 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 70 | $9.04 | $2.22 | $-2.32 | $2,593.34 | ▼ -2.32 after sell → book $10,305.51; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 163 | $4.32 | $2.52 | $+65.09 | $3,294.98 | ▲ +65.09 after sell → book $10,302.99; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 18 | $34.31 | $2.06 | $+0.57 | $3,910.50 | ▲ +0.57 after sell → book $10,300.93; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 28 | $22.20 | $2.09 | $-10.89 | $4,530.01 | ▼ -10.89 after sell → book $10,298.84; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 110 | $11.57 | $2.35 | $-31.62 | $5,800.36 | ▼ -31.62 after sell → book $10,296.49; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 950 | $1.46 | $12.42 | $+60.82 | $7,174.93 | ▲ +60.82 after sell → book $10,284.06; vs 09:30 mark -12.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 4 | $115.18 | $2.00 | — | $6,712.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $512.50; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 4 | $103.69 | $2.00 | — | $6,295.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $512.50; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 28 | $17.93 | $2.07 | — | $5,791.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $512.50; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 5 | $93.98 | $2.00 | — | $5,319.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $512.50; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 11 | $43.08 | $2.02 | — | $4,843.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $512.50; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 222 | $2.30 | $2.86 | — | $4,329.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $512.50; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 482 | $4.49 | $6.22 | — | $2,159.53 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $2164.96; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 316 | $6.81 | $4.08 | — | $3.49 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $2164.96; owner union_hot_n4_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.49 | ▲ close $10,324.98 vs 09:30 $10,325.54 (session +64.18) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.49 | ▲ 09:30 equity $11,291.29 vs yday $10,324.98 (+966.31) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 13 | $43.05 | $2.05 | $-53.48 | $561.09 | ▼ -53.48 after sell → book $11,289.24; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,700.66 | ▼ -63.57 after sell → book $11,287.21; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1131 | $1.83 | $14.79 | $+739.70 | $3,755.59 | ▲ +739.70 after sell → book $11,272.41; vs 09:30 mark -14.80 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 4 | $121.00 | $2.02 | $+19.26 | $4,237.57 | ▲ +19.26 after sell → book $11,270.39; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 4 | $104.14 | $2.02 | $-2.22 | $4,652.11 | ▼ -2.22 after sell → book $11,268.37; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 28 | $18.05 | $2.09 | $-0.81 | $5,155.56 | ▼ -0.81 after sell → book $11,266.28; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 5 | $97.02 | $2.02 | $+11.17 | $5,638.63 | ▲ +11.17 after sell → book $11,264.25; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 11 | $44.22 | $2.04 | $+8.47 | $6,123.01 | ▲ +8.47 after sell → book $11,262.21; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 222 | $2.34 | $2.91 | $+3.11 | $6,639.58 | ▲ +3.11 after sell → book $11,259.30; vs 09:30 mark -2.91 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 482 | $4.32 | $6.31 | $-94.47 | $8,715.50 | ▼ -94.47 after sell → book $11,252.98; vs 09:30 mark -6.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 316 | $8.03 | $4.15 | $+377.29 | $11,248.83 | ▲ +377.29 after sell → book $11,248.83; vs 09:30 mark -4.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,248.83 | ▲ close $11,248.83 vs 09:30 $11,291.29 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,248.83 | ▲ 09:30 equity $11,248.83 vs yday $11,248.83 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 4 | $175.01 | $2.00 | — | $10,546.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 7 | $88.94 | $2.01 | — | $9,922.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 46 | $15.28 | $2.13 | — | $9,217.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $8,645.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 137 | $5.10 | $2.40 | — | $7,944.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 14 | $47.89 | $2.03 | — | $7,272.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 50 | $13.92 | $2.14 | — | $6,574.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 154 | $4.54 | $2.45 | — | $5,871.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $703.05; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 60 | $24.11 | $2.17 | — | $4,422.87 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1467.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 940 | $1.56 | $12.13 | — | $2,944.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1467.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 360 | $4.07 | $4.64 | — | $1,474.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1467.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 77 | $19.04 | $2.22 | — | $6.19 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1467.91; owner union_hot_n4_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.19 | ▲ close $11,514.51 vs 09:30 $11,248.83 (session +304.01) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.19 | ▼ 09:30 equity $11,225.03 vs yday $11,514.51 (-289.48) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 4 | $173.22 | $2.02 | $-11.18 | $697.05 | ▼ -11.18 after sell → book $11,223.01; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 7 | $92.65 | $2.03 | $+21.93 | $1,343.57 | ▲ +21.93 after sell → book $11,220.98; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 137 | $4.77 | $2.43 | $-50.04 | $1,994.63 | ▼ -50.04 after sell → book $11,218.55; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 14 | $48.24 | $2.05 | $+0.82 | $2,667.94 | ▲ +0.82 after sell → book $11,216.50; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 50 | $14.03 | $2.16 | $+1.20 | $3,367.28 | ▲ +1.20 after sell → book $11,214.34; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 154 | $3.38 | $2.49 | $-184.35 | $3,885.31 | ▼ -184.35 after sell → book $11,211.85; vs 09:30 mark -2.49 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 60 | $26.61 | $2.19 | $+145.64 | $5,479.71 | ▲ +145.64 after sell → book $11,209.65; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 940 | $1.60 | $12.29 | $+13.18 | $6,971.42 | ▲ +13.18 after sell → book $11,197.36; vs 09:30 mark -12.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 77 | $20.72 | $2.25 | $+124.89 | $8,564.61 | ▲ +124.89 after sell → book $11,195.11; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1224 | $0.58 | $10.81 | — | $7,840.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $713.72; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 136 | $5.21 | $2.40 | — | $7,129.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $713.72; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 5 | $131.37 | $2.00 | — | $6,470.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $713.72; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 39 | $18.26 | $2.11 | — | $5,756.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $713.72; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 20 | $34.30 | $2.05 | — | $5,068.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $713.72; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $4,412.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $713.72; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 104 | $14.11 | $2.30 | — | $2,942.55 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1470.76; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 253 | $5.81 | $3.26 | — | $1,469.35 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1470.76; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 126 | $11.59 | $2.37 | — | $7.27 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1470.76; owner union_hot_n4_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.27 | ▲ close $11,486.88 vs 09:30 $11,225.03 (session +321.07) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.27 | ▲ 09:30 equity $11,644.14 vs yday $11,486.88 (+157.26) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 46 | $18.50 | $2.15 | $+143.84 | $856.13 | ▲ +143.84 after sell → book $11,642.00; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 4 | $128.73 | $2.02 | $-58.54 | $1,369.02 | ▼ -58.54 after sell → book $11,639.97; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1224 | $0.53 | $10.37 | $-86.05 | $2,007.37 | ▼ -86.05 after sell → book $11,629.60; vs 09:30 mark -10.37 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 136 | $5.49 | $2.43 | $+33.25 | $2,751.58 | ▲ +33.25 after sell → book $11,627.17; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 5 | $144.70 | $2.02 | $+62.62 | $3,473.06 | ▲ +62.62 after sell → book $11,625.15; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 39 | $18.69 | $2.13 | $+12.54 | $4,199.84 | ▲ +12.54 after sell → book $11,623.02; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 20 | $33.79 | $2.07 | $-14.32 | $4,873.57 | ▼ -14.32 after sell → book $11,620.95; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 2 | $314.90 | $2.02 | $-28.03 | $5,501.35 | ▼ -28.03 after sell → book $11,618.93; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 104 | $14.20 | $2.33 | $+4.73 | $6,975.82 | ▲ +4.73 after sell → book $11,616.60; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 253 | $6.50 | $3.32 | $+167.99 | $8,617.00 | ▲ +167.99 after sell → book $11,613.28; vs 09:30 mark -3.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 126 | $12.18 | $2.40 | $+70.20 | $10,149.28 | ▲ +70.20 after sell → book $11,610.88; vs 09:30 mark -2.40 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 7 | $80.60 | $2.01 | — | $9,583.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 39 | $16.18 | $2.11 | — | $8,949.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 5 | $118.77 | $2.00 | — | $8,354.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 35 | $17.78 | $2.10 | — | $7,729.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 47 | $13.41 | $2.13 | — | $7,097.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 6 | $97.16 | $2.01 | — | $6,512.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 3 | $206.82 | $2.00 | — | $5,889.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 5 | $120.17 | $2.00 | — | $5,287.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $634.33; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 191 | $9.19 | $2.56 | — | $3,529.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1762.34; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 12 | $144.18 | $2.03 | — | $1,796.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1762.34; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 95 | $18.50 | $2.27 | — | $37.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $1762.34; owner union_hot_n4_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.20 | ▲ close $11,639.12 vs 09:30 $11,644.14 (session +51.47) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.20 | ▼ 09:30 equity $11,500.89 vs yday $11,639.12 (-138.23) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 360 | $3.69 | $4.71 | $-146.16 | $1,360.88 | ▼ -146.16 after sell → book $11,496.17; vs 09:30 mark -4.72 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 7 | $83.85 | $2.03 | $+18.71 | $1,945.80 | ▲ +18.71 after sell → book $11,494.14; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 39 | $16.94 | $2.13 | $+25.41 | $2,604.34 | ▲ +25.41 after sell → book $11,492.02; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 5 | $115.66 | $2.02 | $-19.58 | $3,180.61 | ▼ -19.58 after sell → book $11,489.99; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 35 | $18.58 | $2.12 | $+23.79 | $3,828.80 | ▲ +23.79 after sell → book $11,487.88; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 47 | $13.65 | $2.15 | $+7.00 | $4,468.19 | ▲ +7.00 after sell → book $11,485.72; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 6 | $93.62 | $2.03 | $-25.28 | $5,027.89 | ▼ -25.28 after sell → book $11,483.70; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 3 | $205.50 | $2.02 | $-7.98 | $5,642.37 | ▼ -7.98 after sell → book $11,481.68; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 5 | $122.07 | $2.02 | $+5.47 | $6,250.69 | ▲ +5.47 after sell → book $11,479.65; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 95 | $18.15 | $2.30 | $-37.83 | $7,972.64 | ▼ -37.83 after sell → book $11,477.35; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $7,709.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 33 | $15.01 | $2.09 | — | $7,212.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 4 | $103.89 | $2.00 | — | $6,794.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 128 | $3.88 | $2.37 | — | $6,295.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 11 | $44.40 | $2.02 | — | $5,805.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 20 | $24.69 | $2.05 | — | $5,309.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 59 | $8.35 | $2.17 | — | $4,814.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 13 | $37.65 | $2.03 | — | $4,322.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $498.29; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 154 | $14.00 | $2.45 | — | $2,164.53 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2161.49; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 14 | $146.07 | $2.03 | — | $117.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2161.49; owner union_hot_n4_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.52 | ▼ close $11,306.69 vs 09:30 $11,500.89 (session -149.45) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.52 | ▼ 09:30 equity $11,236.80 vs yday $11,306.69 (-69.89) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 191 | $9.50 | $2.61 | $+54.04 | $1,929.41 | ▲ +54.04 after sell → book $11,234.19; vs 09:30 mark -2.61 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 12 | $134.10 | $2.05 | $-125.03 | $3,536.56 | ▼ -125.03 after sell → book $11,232.14; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 1 | $257.71 | $2.01 | $-7.46 | $3,792.26 | ▼ -7.46 after sell → book $11,230.13; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 33 | $14.88 | $2.11 | $-8.49 | $4,281.19 | ▼ -8.49 after sell → book $11,228.02; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 4 | $98.00 | $2.02 | $-27.58 | $4,671.17 | ▼ -27.58 after sell → book $11,226.00; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 128 | $3.39 | $2.41 | $-67.50 | $5,102.68 | ▼ -67.50 after sell → book $11,223.59; vs 09:30 mark -2.41 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 11 | $44.85 | $2.04 | $+0.88 | $5,593.99 | ▲ +0.88 after sell → book $11,221.55; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 20 | $22.98 | $2.07 | $-38.32 | $6,051.52 | ▼ -38.32 after sell → book $11,219.48; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 59 | $8.53 | $2.19 | $+6.27 | $6,552.60 | ▲ +6.27 after sell → book $11,217.29; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 13 | $35.81 | $2.05 | $-27.93 | $7,016.09 | ▼ -27.93 after sell → book $11,215.25; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 14 | $148.03 | $2.06 | $+23.35 | $9,086.45 | ▲ +23.35 after sell → book $11,213.19; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,086.45 | ▼ close $11,134.65 vs 09:30 $11,236.80 (session -78.54) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,086.45 | ▼ 09:30 equity $11,094.61 vs yday $11,134.65 (-40.04) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 154 | $13.04 | $2.49 | $-152.79 | $11,092.11 | ▼ -152.79 after sell → book $11,092.11; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,092.11 | ▲ close $11,092.11 vs 09:30 $11,094.61 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,092.11 | ▲ 09:30 equity $11,092.11 vs yday $11,092.11 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,092.11 | ▲ close $11,092.11 vs 09:30 $11,092.11 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,092.11 | ▲ 09:30 equity $11,092.11 vs yday $11,092.11 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 64 | $10.74 | $2.18 | — | $10,402.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,048.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 100 | $6.90 | $2.29 | — | $9,356.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $8,999.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 31 | $22.32 | $2.08 | — | $8,305.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,789.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $7,121.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 45 | $15.09 | $2.12 | — | $6,440.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $693.26; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 904 | $1.78 | $11.66 | — | $4,819.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1610.03; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 87 | $18.40 | $2.25 | — | $3,216.31 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1610.03; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 117 | $13.71 | $2.34 | — | $1,609.90 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1610.03; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 67 | $23.88 | $2.19 | — | $7.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1610.03; owner union_hot_n4_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.74 | ▼ close $10,979.39 vs 09:30 $11,092.11 (session -77.58) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.74 | ▲ 09:30 equity $11,061.79 vs yday $10,979.39 (+82.40) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 64 | $10.91 | $2.20 | $+6.18 | $703.78 | ▲ +6.18 after sell → book $11,059.59; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $1,061.47 | ▲ +3.95 after sell → book $11,057.58; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 100 | $9.28 | $2.32 | $+233.39 | $1,987.15 | ▲ +233.39 after sell → book $11,055.26; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $2,306.81 | ▼ -36.83 after sell → book $11,053.25; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 31 | $22.10 | $2.10 | $-11.01 | $2,989.81 | ▼ -11.01 after sell → book $11,051.15; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $3,465.55 | ▼ -40.25 after sell → book $11,049.13; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 14 | $53.85 | $2.05 | $+83.42 | $4,217.40 | ▲ +83.42 after sell → book $11,047.08; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 45 | $15.34 | $2.15 | $+6.98 | $4,905.55 | ▲ +6.98 after sell → book $11,044.93; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 87 | $18.15 | $2.28 | $-26.28 | $6,482.33 | ▼ -26.28 after sell → book $11,042.66; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 117 | $13.89 | $2.37 | $+16.35 | $8,105.08 | ▲ +16.35 after sell → book $11,040.28; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 67 | $23.84 | $2.21 | $-7.09 | $9,700.15 | ▼ -7.09 after sell → book $11,038.07; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 9 | $63.18 | $2.02 | — | $9,129.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 69 | $8.74 | $2.20 | — | $8,524.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 8 | $68.52 | $2.01 | — | $7,974.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 167 | $3.62 | $2.49 | — | $7,367.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 3 | $167.55 | $2.00 | — | $6,863.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 13 | $44.90 | $2.03 | — | $6,277.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 6 | $98.15 | $2.01 | — | $5,686.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 38 | $15.70 | $2.10 | — | $5,087.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $606.26; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 67 | $25.18 | $2.19 | — | $3,398.64 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1695.96; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 215 | $7.87 | $2.77 | — | $1,703.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1695.96; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 292 | $5.79 | $3.77 | — | $9.37 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1695.96; owner union_hot_n4_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.37 | ▲ close $11,342.28 vs 09:30 $11,061.79 (session +329.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.37 | ▼ 09:30 equity $11,091.97 vs yday $11,342.28 (-250.31) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 904 | $1.56 | $11.82 | $-217.84 | $1,412.31 | ▼ -217.84 after sell → book $11,080.15; vs 09:30 mark -11.82 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 9 | $63.83 | $2.04 | $+1.80 | $1,984.74 | ▲ +1.80 after sell → book $11,078.11; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 69 | $8.73 | $2.22 | $-5.11 | $2,584.89 | ▼ -5.11 after sell → book $11,075.89; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 8 | $67.05 | $2.03 | $-15.81 | $3,119.26 | ▼ -15.81 after sell → book $11,073.86; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 167 | $3.84 | $2.53 | $+32.56 | $3,758.01 | ▲ +32.56 after sell → book $11,071.33; vs 09:30 mark -2.53 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 3 | $160.52 | $2.02 | $-25.11 | $4,237.55 | ▼ -25.11 after sell → book $11,069.31; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 13 | $39.56 | $2.05 | $-73.50 | $4,749.78 | ▼ -73.50 after sell → book $11,067.26; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 6 | $100.58 | $2.03 | $+10.54 | $5,351.23 | ▲ +10.54 after sell → book $11,065.23; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 38 | $15.20 | $2.12 | $-23.23 | $5,926.71 | ▼ -23.23 after sell → book $11,063.11; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 67 | $26.44 | $2.22 | $+80.01 | $7,695.97 | ▲ +80.01 after sell → book $11,060.89; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 215 | $7.76 | $2.82 | $-29.25 | $9,361.55 | ▼ -29.25 after sell → book $11,058.07; vs 09:30 mark -2.82 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 292 | $5.81 | $3.83 | $-1.76 | $11,054.24 | ▼ -1.76 after sell → book $11,054.24; vs 09:30 mark -3.83 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,054.24 | ▲ close $11,054.24 vs 09:30 $11,091.97 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,054.24 | ▲ 09:30 equity $11,054.24 vs yday $11,054.24 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,054.24 | ▲ close $11,054.24 vs 09:30 $11,054.24 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,054.24 | ▲ 09:30 equity $11,054.24 vs yday $11,054.24 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,054.24 | ▲ close $11,054.24 vs 09:30 $11,054.24 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,054.24 | ▲ 09:30 equity $11,054.24 vs yday $11,054.24 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $10,394.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 116 | $5.91 | $2.34 | — | $9,706.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $9,220.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 21 | $32.01 | $2.05 | — | $8,546.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 9 | $71.71 | $2.02 | — | $7,898.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 12 | $56.02 | $2.03 | — | $7,224.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 73 | $9.37 | $2.21 | — | $6,538.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 52 | $13.10 | $2.15 | — | $5,854.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $690.89; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 542 | $2.70 | $6.99 | — | $4,384.39 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1463.70; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 298 | $4.91 | $3.84 | — | $2,917.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1463.70; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 237 | $6.16 | $3.06 | — | $1,454.39 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1463.70; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 462 | $3.13 | $5.96 | — | $2.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1463.70; owner union_hot_n4_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.37 | ▲ close $11,188.91 vs 09:30 $11,054.24 (session +171.31) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.37 | ▲ 09:30 equity $11,272.17 vs yday $11,188.91 (+83.26) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 4 | $141.42 | $2.02 | $-96.06 | $566.03 | ▼ -96.06 after sell → book $11,270.15; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 116 | $5.86 | $2.37 | $-10.51 | $1,243.42 | ▼ -10.51 after sell → book $11,267.78; vs 09:30 mark -2.37 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $1,764.43 | ▲ +34.67 after sell → book $11,265.77; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 21 | $30.63 | $2.07 | $-33.11 | $2,405.58 | ▼ -33.11 after sell → book $11,263.69; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 9 | $77.68 | $2.04 | $+49.68 | $3,102.67 | ▲ +49.68 after sell → book $11,261.66; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 12 | $59.31 | $2.05 | $+35.41 | $3,812.34 | ▲ +35.41 after sell → book $11,259.61; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 73 | $8.85 | $2.23 | $-42.40 | $4,456.16 | ▼ -42.40 after sell → book $11,257.38; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 52 | $14.16 | $2.17 | $+50.81 | $5,190.31 | ▲ +50.81 after sell → book $11,255.21; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 237 | $6.02 | $3.11 | $-39.35 | $6,613.95 | ▼ -39.35 after sell → book $11,252.11; vs 09:30 mark -3.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,613.95 | ▲ close $11,567.97 vs 09:30 $11,272.17 (session +315.86) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,613.95 | ▲ 09:30 equity $11,661.21 vs yday $11,567.97 (+93.24) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 298 | $5.11 | $3.91 | $+51.85 | $8,132.82 | ▲ +51.85 after sell → book $11,657.30; vs 09:30 mark -3.91 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 462 | $3.64 | $6.05 | $+223.61 | $9,808.45 | ▲ +223.61 after sell → book $11,651.25; vs 09:30 mark -6.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,808.45 | ▲ close $11,781.33 vs 09:30 $11,661.21 (session +130.08) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,808.45 | ▲ 09:30 equity $11,792.17 vs yday $11,781.33 (+10.84) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 73 | $33.14 | $2.21 | — | $7,387.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $2452.11; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 59 | $40.93 | $2.17 | — | $4,969.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2452.11; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 920 | $1.80 | $11.87 | — | $3,302.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1656.66; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 71 | $23.29 | $2.20 | — | $1,646.32 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1656.66; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 112 | $14.62 | $2.33 | — | $6.56 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1656.66; owner union_hot_n4_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.56 | ▲ close $11,878.98 vs 09:30 $11,792.17 (session +107.58) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.56 | ▲ 09:30 equity $12,069.88 vs yday $11,878.98 (+190.90) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 73 | $36.76 | $2.24 | $+259.81 | $2,687.79 | ▲ +259.81 after sell → book $12,067.63; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 59 | $40.79 | $2.20 | $-12.62 | $5,092.21 | ▼ -12.62 after sell → book $12,065.44; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 71 | $24.09 | $2.23 | $+52.37 | $6,800.37 | ▲ +52.37 after sell → book $12,063.21; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 112 | $13.77 | $2.36 | $-99.88 | $8,340.25 | ▼ -99.88 after sell → book $12,060.85; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 186 | $11.21 | $2.55 | — | $6,252.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $2085.06; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 25 | $81.00 | $2.06 | — | $4,225.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $2085.06; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 94 | $22.46 | $2.27 | — | $2,112.07 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $2112.79; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 57 | $36.76 | $2.16 | — | $14.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $2112.79; owner union_hot_n4_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.59 | ▲ close $12,326.76 vs 09:30 $12,069.88 (session +274.95) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.59 | ▼ 09:30 equity $12,279.48 vs yday $12,326.76 (-47.28) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 920 | $1.96 | $12.04 | $+123.30 | $1,805.75 | ▲ +123.30 after sell → book $12,267.44; vs 09:30 mark -12.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 186 | $11.64 | $2.60 | $+74.84 | $3,968.20 | ▲ +74.84 after sell → book $12,264.85; vs 09:30 mark -2.59 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 25 | $78.25 | $2.09 | $-72.91 | $5,922.35 | ▼ -72.91 after sell → book $12,262.75; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 94 | $21.30 | $2.30 | $-113.62 | $7,922.25 | ▼ -113.62 after sell → book $12,260.45; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 57 | $39.50 | $2.19 | $+151.83 | $10,171.56 | ▲ +151.83 after sell → book $12,258.26; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 115 | $29.32 | $2.33 | — | $6,797.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $3390.52; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1117 | $3.04 | $14.41 | — | $3,392.92 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $3390.52; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 41 | $81.40 | $2.11 | — | $53.41 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $3390.52; owner union_hot_n4_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.41 | ▲ close $12,527.45 vs 09:30 $12,279.48 (session +288.04) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.41 | ▲ 09:30 equity $13,072.24 vs yday $12,527.45 (+544.79) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 542 | $3.55 | $7.10 | $+446.61 | $1,970.41 | ▲ +446.61 after sell → book $13,065.14; vs 09:30 mark -7.10 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 115 | $29.43 | $2.38 | $+7.93 | $5,352.48 | ▲ +7.93 after sell → book $13,062.76; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 1117 | $4.00 | $14.63 | $+1048.87 | $9,805.85 | ▲ +1,048.87 after sell → book $13,048.13; vs 09:30 mark -14.63 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 41 | $79.08 | $2.15 | $-99.38 | $13,045.98 | ▼ -99.38 after sell → book $13,045.98; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1320 | $2.47 | $17.03 | — | $9,768.55 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $3261.50; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 192 | $16.91 | $2.57 | — | $6,519.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $3261.50; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1976 | $1.65 | $25.49 | — | $3,233.38 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $3261.50; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 276 | $11.67 | $3.56 | — | $8.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $3261.50; owner union_hot_n4_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.90 | ▲ close $13,407.14 vs 09:30 $13,072.24 (session +409.80) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.90 | ▼ 09:30 equity $13,258.10 vs yday $13,407.14 (-149.04) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.90 | ▲ close $13,269.14 vs 09:30 $13,258.10 (session +11.04) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.90 | ▲ 09:30 equity $13,430.90 vs yday $13,269.14 (+161.76) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 192 | $16.92 | $2.62 | $-3.27 | $3,254.91 | ▼ -3.27 after sell → book $13,428.27; vs 09:30 mark -2.63 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1976 | $1.41 | $25.84 | $-525.57 | $6,015.23 | ▼ -525.57 after sell → book $13,402.43; vs 09:30 mark -25.84 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 276 | $12.80 | $3.63 | $+304.69 | $9,544.40 | ▲ +304.69 after sell → book $13,398.80; vs 09:30 mark -3.63 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 20 | $47.57 | $2.05 | — | $8,590.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $954.44; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $7,801.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $954.44; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 26 | $35.74 | $2.07 | — | $6,870.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $954.44; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 20 | $47.15 | $2.05 | — | $5,925.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $954.44; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 8 | $109.67 | $2.01 | — | $5,046.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $954.44; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 622 | $2.70 | $8.02 | — | $3,358.67 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1682.03; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 40 | $41.76 | $2.11 | — | $1,686.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1682.03; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 374 | $4.49 | $4.82 | — | $2.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1682.03; owner union_hot_n4_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.08 | ▼ close $12,898.64 vs 09:30 $13,430.90 (session -475.02) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.08 | ▼ 09:30 equity $12,878.56 vs yday $12,898.64 (-20.08) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1320 | $2.68 | $17.28 | $+242.90 | $3,522.40 | ▲ +242.90 after sell → book $12,861.28; vs 09:30 mark -17.28 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 20 | $46.88 | $2.07 | $-17.92 | $4,457.93 | ▼ -17.92 after sell → book $12,859.21; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 4 | $192.26 | $2.02 | $-22.10 | $5,224.95 | ▼ -22.10 after sell → book $12,857.19; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 26 | $35.96 | $2.09 | $+1.56 | $6,157.82 | ▲ +1.56 after sell → book $12,855.10; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 20 | $47.14 | $2.07 | $-4.32 | $7,098.55 | ▼ -4.32 after sell → book $12,853.03; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 8 | $105.49 | $2.03 | $-37.47 | $7,940.45 | ▼ -37.47 after sell → book $12,851.00; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 40 | $36.02 | $2.13 | $-233.64 | $9,379.32 | ▼ -233.64 after sell → book $12,848.87; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 374 | $3.92 | $4.90 | $-221.03 | $10,842.37 | ▼ -221.03 after sell → book $12,843.97; vs 09:30 mark -4.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,842.37 | ▲ close $14,170.07 vs 09:30 $12,878.56 (session +1,326.10) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,903.77 | ▲ 09:30 equity $10,886.51 vs yday $10,549.26 (+337.25) | 09:30 open · cash $6,903.77 (unchanged overnight, no fees) · equity $10,886.51 vs prior close $10,549.26 (+337.25) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 3 | $887.00 | $2.00 | — | $4,240.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $3451.89; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 47 | $29.76 | $2.13 | — | $2,839.92 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $1413.59; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 87 | $16.21 | $2.25 | — | $1,427.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1413.59; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 90 | $15.58 | $2.26 | — | $22.84 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $1413.59; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.84 | ▼ close $10,701.17 vs 09:30 $10,886.51 (session -176.70) | 16:00 close · cash $22.84 · equity $10,701.17 vs 09:30 $10,886.51 (-185.34; session marks -176.70) · 6 name(s) marked open→close (per-name table). GLND×475 09:30 $6.06 → close $5.54 -247.00; VICR×4 09:30 $276.06 → close $276.06 -0.00; COST×3 09:30 $887.00 → close $922.76 +107.29; TJGC×47 09:30 $29.76 → close $26.24 -165.44; SECZ×87 09:30 $16.21 → close $15.96 -21.75; USDE×90 09:30 $15.58 → close $17.25 +150.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-21 | `DE` | cash | leftover split 512.50 < 1 share @ 623.26 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 2.97 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 2.97 < 1 share @ 7.23 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 622 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1682.03; owner union_hot_n4_h1 |
