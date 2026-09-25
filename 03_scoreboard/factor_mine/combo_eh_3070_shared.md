# Factor mine action — `combo_eh_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/union_hot_n4_h1 w=0.3,0.7 net=priority

Cash book **+11.34%** ($11,134) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 267 · skips 269 · realized $+2681.43.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 30%, union_hot_n4_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 30%, union_hot_n4_h1 70%.
- Member: union_e_fresh_h3 (30% · long · hold 3).
- Member: union_hot_n4_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $27.81.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1851 | $0.81 | $20.55 | — | $8,480.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $1500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 68 | $22.01 | $2.19 | — | $6,981.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $1500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 50 | $45.98 | $2.14 | — | $4,680.13 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $2327.09; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 99 | $23.33 | $2.29 | — | $2,368.17 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $2327.09; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 45 | $50.62 | $2.12 | — | $88.00 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $2327.09; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.00 | ▲ close $10,323.39 vs 09:30 $10,000.00 (session +352.69) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.00 | ▲ 09:30 equity $10,357.50 vs yday $10,323.39 (+34.11) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 50 | $44.09 | $2.17 | $-98.81 | $2,290.34 | ▼ -98.81 after sell → book $10,355.34; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 99 | $22.92 | $2.32 | $-45.20 | $4,557.09 | ▼ -45.20 after sell → book $10,353.01; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 45 | $55.29 | $2.15 | $+205.73 | $7,042.99 | ▲ +205.73 after sell → book $10,350.86; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 176 | $1.50 | $2.52 | — | $6,776.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 13 | $19.57 | $2.03 | — | $6,520.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 23 | $11.12 | $2.06 | — | $6,262.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 19 | $13.55 | $2.05 | — | $6,002.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 24 | $10.83 | $2.06 | — | $5,740.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 223 | $1.18 | $2.88 | — | $5,474.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 13 | $19.17 | $2.03 | — | $5,223.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 26 | $9.89 | $2.07 | — | $4,964.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $264.11; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 100 | $24.68 | $2.29 | — | $2,493.85 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $2482.07; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1126 | $2.20 | $14.53 | — | $2.13 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $2482.07; owner union_hot_n4_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.13 | ▲ close $10,673.21 vs 09:30 $10,357.50 (session +356.85) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.13 | ▼ 09:30 equity $10,450.23 vs yday $10,673.21 (-222.98) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 100 | $24.83 | $2.33 | $+10.38 | $2,482.80 | ▲ +10.38 after sell → book $10,447.90; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1126 | $2.08 | $14.73 | $-158.75 | $4,815.78 | ▼ -158.75 after sell → book $10,433.17; vs 09:30 mark -14.73 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 287 | $4.19 | $3.70 | — | $3,609.55 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1203.94; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 175 | $6.87 | $2.52 | — | $2,404.78 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1203.94; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 88 | $13.64 | $2.25 | — | $1,202.21 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1203.94; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1203.94; owner union_hot_n4_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.46 | ▲ close $10,542.78 vs 09:30 $10,450.23 (session +120.16) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.46 | ▼ 09:30 equity $10,485.74 vs yday $10,542.78 (-57.04) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1851 | $1.14 | $24.20 | $+566.08 | $2,090.40 | ▲ +566.08 after sell → book $10,461.54; vs 09:30 mark -24.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 68 | $22.82 | $2.22 | $+50.67 | $3,639.94 | ▲ +50.67 after sell → book $10,459.32; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 287 | $3.94 | $3.76 | $-79.21 | $4,766.96 | ▼ -79.21 after sell → book $10,455.56; vs 09:30 mark -3.76 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 88 | $13.31 | $2.28 | $-33.57 | $5,935.96 | ▼ -33.57 after sell → book $10,453.28; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,137.36 | ▲ +3.66 after sell → book $10,451.18; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,137.36 | ▼ close $10,392.48 vs 09:30 $10,485.74 (session -58.70) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,137.36 | ▲ 09:30 equity $10,401.31 vs yday $10,392.48 (+8.83) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 176 | $1.42 | $2.56 | $-19.16 | $7,384.73 | ▼ -19.16 after sell → book $10,398.76; vs 09:30 mark -2.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 13 | $19.58 | $2.05 | $-3.95 | $7,637.22 | ▼ -3.95 after sell → book $10,396.71; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 23 | $9.10 | $2.08 | $-50.60 | $7,844.44 | ▼ -50.60 after sell → book $10,394.63; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 19 | $13.01 | $2.07 | $-14.37 | $8,089.56 | ▼ -14.37 after sell → book $10,392.56; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 24 | $10.85 | $2.08 | $-3.66 | $8,347.88 | ▼ -3.66 after sell → book $10,390.48; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 223 | $1.07 | $2.92 | $-30.33 | $8,583.57 | ▼ -30.33 after sell → book $10,387.56; vs 09:30 mark -2.92 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 13 | $18.98 | $2.05 | $-6.55 | $8,828.26 | ▼ -6.55 after sell → book $10,385.51; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 26 | $11.50 | $2.09 | $+37.57 | $9,125.17 | ▲ +37.57 after sell → book $10,383.42; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 175 | $7.19 | $2.55 | $+50.93 | $10,380.87 | ▲ +50.93 after sell → book $10,380.87; vs 09:30 mark -2.55 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,380.87 | ▲ close $10,380.87 vs 09:30 $10,401.31 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,380.87 | ▲ 09:30 equity $10,380.87 vs yday $10,380.87 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 3 | $97.43 | $2.00 | — | $10,086.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 87 | $4.43 | $2.25 | — | $9,698.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1297 | $0.30 | $7.78 | — | $9,302.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 8 | $46.85 | $2.01 | — | $8,925.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 43 | $9.01 | $2.12 | — | $8,535.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 100 | $3.89 | $2.29 | — | $8,144.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 11 | $34.05 | $2.02 | — | $7,767.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 17 | $22.44 | $2.04 | — | $7,384.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $389.28; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 12 | $150.14 | $2.03 | — | $5,580.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1846.07; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1605 | $1.15 | $20.70 | — | $3,714.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1846.07; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 156 | $11.81 | $2.46 | — | $1,868.53 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1846.07; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1347 | $1.37 | $17.38 | — | $5.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1846.07; owner union_hot_n4_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.76 | ▼ close $10,262.79 vs 09:30 $10,380.87 (session -53.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.76 | ▲ 09:30 equity $10,507.51 vs yday $10,262.79 (+244.72) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 156 | $11.57 | $2.50 | $-43.18 | $1,808.18 | ▼ -43.18 after sell → book $10,505.01; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1347 | $1.46 | $17.62 | $+86.24 | $3,757.19 | ▲ +86.24 after sell → book $10,487.40; vs 09:30 mark -17.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 1 | $115.18 | $1.15 | — | $3,640.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $161.02; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 1 | $103.69 | $1.04 | — | $3,536.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $161.02; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 8 | $17.93 | $1.46 | — | $3,391.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $161.02; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 1 | $93.98 | $0.94 | — | $3,296.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $161.02; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 3 | $43.08 | $1.30 | — | $3,165.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $161.02; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 70 | $2.30 | $1.82 | — | $3,002.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $161.02; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 334 | $4.49 | $4.31 | — | $1,498.93 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1501.45; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 219 | $6.81 | $2.83 | — | $4.72 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1501.45; owner union_hot_n4_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.72 | ▲ close $10,706.52 vs 09:30 $10,507.51 (session +233.97) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.72 | ▲ 09:30 equity $11,697.62 vs yday $10,706.52 (+991.10) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 12 | $142.70 | $2.05 | $-93.36 | $1,715.07 | ▼ -93.36 after sell → book $11,695.57; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1605 | $1.83 | $20.99 | $+1049.70 | $4,631.22 | ▲ +1,049.70 after sell → book $11,674.57; vs 09:30 mark -21.00 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 334 | $4.32 | $4.38 | $-65.46 | $6,069.73 | ▼ -65.46 after sell → book $11,670.20; vs 09:30 mark -4.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 219 | $8.03 | $2.88 | $+261.48 | $7,825.42 | ▲ +261.48 after sell → book $11,667.32; vs 09:30 mark -2.88 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,825.42 | ▲ close $11,701.95 vs 09:30 $11,697.62 (session +34.63) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,825.42 | ▲ 09:30 equity $11,705.48 vs yday $11,701.95 (+3.53) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 3 | $104.00 | $2.02 | $+15.69 | $8,135.40 | ▲ +15.69 after sell → book $11,703.46; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 87 | $4.42 | $2.28 | $-5.40 | $8,517.67 | ▼ -5.40 after sell → book $11,701.19; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1297 | $0.31 | $8.14 | $-2.95 | $8,911.60 | ▼ -2.95 after sell → book $11,693.05; vs 09:30 mark -8.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 8 | $43.63 | $2.03 | $-29.81 | $9,258.61 | ▼ -29.81 after sell → book $11,691.02; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 43 | $9.23 | $2.14 | $+5.20 | $9,653.36 | ▲ +5.20 after sell → book $11,688.88; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 100 | $5.24 | $2.32 | $+130.39 | $10,175.04 | ▲ +130.39 after sell → book $11,686.56; vs 09:30 mark -2.32 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 11 | $34.72 | $2.04 | $+3.30 | $10,554.92 | ▲ +3.30 after sell → book $11,684.52; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 17 | $21.85 | $2.06 | $-14.13 | $10,924.31 | ▼ -14.13 after sell → book $11,682.46; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 2 | $175.01 | $2.00 | — | $10,572.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 4 | $88.94 | $2.00 | — | $10,214.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 26 | $15.28 | $2.07 | — | $9,815.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 2 | $142.36 | $2.00 | — | $9,528.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 80 | $5.10 | $2.23 | — | $9,118.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 8 | $47.89 | $2.01 | — | $8,733.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 29 | $13.92 | $2.08 | — | $8,327.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 90 | $4.54 | $2.26 | — | $7,916.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $409.66; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 82 | $24.11 | $2.24 | — | $5,936.78 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1979.01; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1268 | $1.56 | $16.36 | — | $3,942.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1979.01; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 486 | $4.07 | $6.27 | — | $1,958.05 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1979.01; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 102 | $19.04 | $2.30 | — | $13.67 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1979.01; owner union_hot_n4_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.67 | ▲ close $12,208.90 vs 09:30 $11,705.48 (session +570.25) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.67 | ▼ 09:30 equity $11,832.55 vs yday $12,208.90 (-376.35) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 1 | $124.67 | $1.27 | $+7.07 | $137.07 | ▲ +7.07 after sell → book $11,831.28; vs 09:30 mark -1.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `WMT` | 1 | $105.51 | $1.08 | $-0.30 | $241.51 | ▼ -0.30 after sell → book $11,830.21; vs 09:30 mark -1.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 8 | $18.14 | $1.50 | $-1.31 | $385.13 | ▼ -1.31 after sell → book $11,828.71; vs 09:30 mark -1.50 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 1 | $94.60 | $0.97 | $-1.29 | $478.76 | ▼ -1.29 after sell → book $11,827.74; vs 09:30 mark -0.97 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 3 | $44.39 | $1.36 | $+1.27 | $610.57 | ▲ +1.27 after sell → book $11,826.38; vs 09:30 mark -1.36 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 70 | $2.35 | $1.88 | $-0.20 | $773.20 | ▼ -0.20 after sell → book $11,824.51; vs 09:30 mark -1.87 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 82 | $26.61 | $2.27 | $+200.50 | $2,952.95 | ▲ +200.50 after sell → book $11,822.24; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1268 | $1.60 | $16.58 | $+17.78 | $4,965.16 | ▲ +17.78 after sell → book $11,805.65; vs 09:30 mark -16.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 102 | $20.72 | $2.33 | $+166.73 | $7,076.27 | ▲ +166.73 after sell → book $11,803.32; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 606 | $0.58 | $5.35 | — | $6,717.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $353.81; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 67 | $5.21 | $2.19 | — | $6,366.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $353.81; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 2 | $131.37 | $2.00 | — | $6,101.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $353.81; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 19 | $18.26 | $2.05 | — | $5,752.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $353.81; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 10 | $34.30 | $2.02 | — | $5,407.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $353.81; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $5,078.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $353.81; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 119 | $14.11 | $2.35 | — | $3,397.28 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1692.91; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 291 | $5.81 | $3.75 | — | $1,702.82 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1692.91; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 146 | $11.59 | $2.43 | — | $8.98 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1692.91; owner union_hot_n4_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.98 | ▲ close $12,077.99 vs 09:30 $11,832.55 (session +298.79) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.98 | ▲ 09:30 equity $12,308.38 vs yday $12,077.99 (+230.39) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 119 | $14.20 | $2.38 | $+5.98 | $1,696.40 | ▲ +5.98 after sell → book $12,306.00; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 291 | $6.50 | $3.82 | $+193.22 | $3,584.08 | ▲ +193.22 after sell → book $12,302.18; vs 09:30 mark -3.82 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 146 | $12.18 | $2.47 | $+81.98 | $5,359.90 | ▲ +81.98 after sell → book $12,299.72; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 2 | $80.60 | $1.62 | — | $5,197.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 12 | $16.18 | $1.98 | — | $5,000.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 1 | $118.77 | $1.19 | — | $4,880.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 11 | $17.78 | $1.99 | — | $4,683.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 14 | $13.41 | $1.92 | — | $4,493.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 2 | $97.16 | $1.95 | — | $4,297.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 1 | $120.17 | $1.20 | — | $4,176.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $201.00; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 227 | $9.19 | $2.93 | — | $2,087.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $2088.05; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 14 | $144.18 | $2.03 | — | $66.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $2088.05; owner union_hot_n4_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.50 | ▲ close $12,315.49 vs 09:30 $12,308.38 (session +32.58) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.50 | ▼ 09:30 equity $12,111.32 vs yday $12,315.49 (-204.17) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 2 | $172.76 | $2.02 | $-8.51 | $410.00 | ▼ -8.51 after sell → book $12,109.31; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 4 | $93.30 | $2.02 | $+13.42 | $781.18 | ▲ +13.42 after sell → book $12,107.28; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 26 | $18.15 | $2.09 | $+70.46 | $1,250.99 | ▲ +70.46 after sell → book $12,105.20; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 2 | $132.80 | $2.02 | $-23.13 | $1,514.57 | ▼ -23.13 after sell → book $12,103.18; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 80 | $4.58 | $2.25 | $-46.08 | $1,878.72 | ▼ -46.08 after sell → book $12,100.93; vs 09:30 mark -2.25 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 8 | $48.42 | $2.03 | $+0.19 | $2,264.05 | ▲ +0.19 after sell → book $12,098.89; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 29 | $15.66 | $2.10 | $+46.29 | $2,716.09 | ▲ +46.29 after sell → book $12,096.80; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 90 | $3.38 | $2.28 | $-109.39 | $3,018.01 | ▼ -109.39 after sell → book $12,094.51; vs 09:30 mark -2.29 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 486 | $3.69 | $6.36 | $-197.31 | $4,804.98 | ▼ -197.31 after sell → book $12,088.15; vs 09:30 mark -6.36 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 12 | $15.01 | $1.84 | — | $4,623.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 1 | $103.89 | $1.04 | — | $4,518.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 46 | $3.88 | $1.92 | — | $4,337.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 4 | $44.40 | $1.79 | — | $4,158.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 7 | $24.69 | $1.75 | — | $3,983.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 21 | $8.35 | $1.82 | — | $3,806.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 4 | $37.65 | $1.52 | — | $3,654.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $180.19; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 260 | $14.00 | $3.35 | — | $11.10 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $3654.46; owner union_hot_n4_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.10 | ▼ close $11,951.74 vs 09:30 $12,111.32 (session -121.38) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.10 | ▼ 09:30 equity $11,861.54 vs yday $11,951.74 (-90.20) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 606 | $0.51 | $5.02 | $-54.61 | $315.14 | ▼ -54.61 after sell → book $11,856.52; vs 09:30 mark -5.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 67 | $5.00 | $2.21 | $-18.47 | $647.93 | ▼ -18.47 after sell → book $11,854.31; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 2 | $148.03 | $2.02 | $+29.31 | $941.98 | ▲ +29.31 after sell → book $11,852.30; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 19 | $19.25 | $2.07 | $+14.70 | $1,305.66 | ▲ +14.70 after sell → book $11,850.23; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 10 | $34.72 | $2.04 | $+0.14 | $1,650.82 | ▲ +0.14 after sell → book $11,848.19; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 1 | $298.01 | $2.01 | $-32.91 | $1,946.82 | ▼ -32.91 after sell → book $11,846.18; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 227 | $9.50 | $2.98 | $+64.46 | $4,100.33 | ▲ +64.46 after sell → book $11,843.19; vs 09:30 mark -2.99 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 14 | $134.10 | $2.06 | $-145.21 | $5,975.68 | ▼ -145.21 after sell → book $11,841.14; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,975.68 | ▼ close $11,694.15 vs 09:30 $11,861.54 (session -146.99) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,975.68 | ▼ 09:30 equity $11,610.75 vs yday $11,694.15 (-83.40) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 2 | $79.83 | $1.62 | $-4.78 | $6,133.71 | ▼ -4.78 after sell → book $11,609.12; vs 09:30 mark -1.63 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 12 | $15.97 | $1.97 | $-6.47 | $6,323.38 | ▼ -6.47 after sell → book $11,607.15; vs 09:30 mark -1.97 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 1 | $113.66 | $1.16 | $-7.46 | $6,435.88 | ▼ -7.46 after sell → book $11,605.99; vs 09:30 mark -1.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 11 | $18.28 | $2.04 | $+1.47 | $6,634.92 | ▲ +1.47 after sell → book $11,603.95; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 14 | $12.18 | $1.77 | $-20.91 | $6,803.67 | ▼ -20.91 after sell → book $11,602.18; vs 09:30 mark -1.77 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 2 | $96.65 | $1.96 | $-4.93 | $6,995.01 | ▼ -4.93 after sell → book $11,600.22; vs 09:30 mark -1.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 1 | $120.54 | $1.23 | $-2.06 | $7,114.32 | ▼ -2.06 after sell → book $11,598.99; vs 09:30 mark -1.23 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 260 | $13.04 | $3.42 | $-256.38 | $10,501.30 | ▼ -256.38 after sell → book $11,595.57; vs 09:30 mark -3.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,501.30 | ▼ close $11,586.68 vs 09:30 $11,610.75 (session -8.89) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,501.30 | ▼ 09:30 equity $11,579.97 vs yday $11,586.68 (-6.71) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 12 | $15.01 | $1.86 | $-3.69 | $10,679.56 | ▼ -3.69 after sell → book $11,578.11; vs 09:30 mark -1.86 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 1 | $92.00 | $0.94 | $-13.87 | $10,770.62 | ▼ -13.87 after sell → book $11,577.17; vs 09:30 mark -0.94 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 46 | $3.32 | $1.69 | $-29.37 | $10,921.65 | ▼ -29.37 after sell → book $11,575.48; vs 09:30 mark -1.69 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 4 | $44.17 | $1.80 | $-4.51 | $11,096.54 | ▼ -4.51 after sell → book $11,573.69; vs 09:30 mark -1.79 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 7 | $21.97 | $1.58 | $-22.37 | $11,248.75 | ▼ -22.37 after sell → book $11,572.11; vs 09:30 mark -1.58 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 21 | $8.58 | $1.88 | $+1.13 | $11,427.04 | ▲ +1.13 after sell → book $11,570.22; vs 09:30 mark -1.89 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 4 | $35.80 | $1.46 | $-10.38 | $11,568.76 | ▼ -10.38 after sell → book $11,568.76; vs 09:30 mark -1.46 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,568.76 | ▲ close $11,568.76 vs 09:30 $11,579.97 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,568.76 | ▲ 09:30 equity $11,568.76 vs yday $11,568.76 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 40 | $10.74 | $2.11 | — | $11,136.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,783.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 62 | $6.90 | $2.18 | — | $10,353.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,996.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 19 | $22.32 | $2.05 | — | $9,570.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 1 | $257.00 | $1.99 | — | $9,311.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 9 | $47.60 | $2.02 | — | $8,881.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 28 | $15.09 | $2.07 | — | $8,456.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $433.83; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1187 | $1.78 | $15.31 | — | $6,328.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $2114.13; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 114 | $18.40 | $2.33 | — | $4,228.42 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $2114.13; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 154 | $13.71 | $2.45 | — | $2,114.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $2114.13; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 88 | $23.88 | $2.25 | — | $10.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $2114.13; owner union_hot_n4_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.93 | ▼ close $11,240.67 vs 09:30 $11,568.76 (session -289.33) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.93 | ▲ 09:30 equity $11,339.85 vs yday $11,240.67 (+99.18) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 114 | $18.15 | $2.37 | $-33.20 | $2,077.67 | ▼ -33.20 after sell → book $11,337.49; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 154 | $13.89 | $2.49 | $+22.77 | $4,214.23 | ▲ +22.77 after sell → book $11,334.99; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 88 | $23.84 | $2.29 | $-8.06 | $6,309.87 | ▼ -8.06 after sell → book $11,332.71; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 3 | $63.18 | $1.90 | — | $6,118.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 27 | $8.74 | $2.07 | — | $5,880.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 3 | $68.52 | $2.00 | — | $5,672.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 65 | $3.62 | $2.19 | — | $5,435.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 1 | $167.55 | $1.68 | — | $5,266.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 5 | $44.90 | $2.00 | — | $5,039.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 2 | $98.15 | $1.97 | — | $4,841.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 15 | $15.70 | $2.04 | — | $4,604.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $236.62; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 60 | $25.18 | $2.17 | — | $3,091.15 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1534.71; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 195 | $7.87 | $2.58 | — | $1,553.92 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1534.71; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 265 | $5.79 | $3.42 | — | $16.15 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1534.71; owner union_hot_n4_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.15 | ▲ close $11,716.58 vs 09:30 $11,339.85 (session +407.88) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.15 | ▼ 09:30 equity $11,460.94 vs yday $11,716.58 (-255.64) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1187 | $1.56 | $15.52 | $-286.04 | $1,858.28 | ▼ -286.04 after sell → book $11,445.41; vs 09:30 mark -15.53 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 60 | $26.44 | $2.19 | $+71.24 | $3,442.49 | ▲ +71.24 after sell → book $11,443.22; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 195 | $7.76 | $2.62 | $-26.64 | $4,953.07 | ▼ -26.64 after sell → book $11,440.60; vs 09:30 mark -2.62 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 265 | $5.81 | $3.47 | $-1.59 | $6,489.25 | ▼ -1.59 after sell → book $11,437.13; vs 09:30 mark -3.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,489.25 | ▼ close $11,427.20 vs 09:30 $11,460.94 (session -9.93) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,489.25 | ▼ 09:30 equity $11,411.20 vs yday $11,427.20 (-16.00) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 40 | $10.51 | $2.13 | $-13.64 | $6,907.52 | ▼ -13.64 after sell → book $11,409.07; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $7,271.73 | ▲ +10.48 after sell → book $11,407.05; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 62 | $9.39 | $2.20 | $+150.01 | $7,851.72 | ▲ +150.01 after sell → book $11,404.86; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $8,191.60 | ▼ -16.60 after sell → book $11,402.84; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 19 | $21.67 | $2.07 | $-16.46 | $8,601.27 | ▼ -16.46 after sell → book $11,400.78; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 1 | $252.92 | $2.01 | $-8.09 | $8,852.17 | ▼ -8.09 after sell → book $11,398.76; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 9 | $56.94 | $2.04 | $+80.01 | $9,362.60 | ▲ +80.01 after sell → book $11,396.73; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 28 | $13.84 | $2.09 | $-39.17 | $9,748.02 | ▼ -39.17 after sell → book $11,394.63; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,748.02 | ▼ close $11,379.68 vs 09:30 $11,411.20 (session -14.96) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,748.02 | ▼ 09:30 equity $11,371.44 vs yday $11,379.68 (-8.24) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 3 | $67.44 | $2.02 | $+8.86 | $9,948.32 | ▲ +8.86 after sell → book $11,369.42; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 27 | $8.26 | $2.09 | $-17.12 | $10,169.25 | ▼ -17.12 after sell → book $11,367.33; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 3 | $64.60 | $1.97 | $-15.73 | $10,361.09 | ▼ -15.73 after sell → book $11,365.36; vs 09:30 mark -1.97 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 65 | $3.76 | $2.21 | $+5.03 | $10,603.28 | ▲ +5.03 after sell → book $11,363.15; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 1 | $142.43 | $1.45 | $-28.25 | $10,744.26 | ▼ -28.25 after sell → book $11,361.71; vs 09:30 mark -1.44 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 5 | $38.23 | $1.95 | $-37.33 | $10,933.44 | ▼ -37.33 after sell → book $11,359.76; vs 09:30 mark -1.95 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 2 | $98.71 | $2.00 | $-2.85 | $11,128.86 | ▼ -2.85 after sell → book $11,357.76; vs 09:30 mark -2.00 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 15 | $15.26 | $2.06 | $-10.69 | $11,355.71 | ▼ -10.69 after sell → book $11,355.71; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,355.71 | ▲ close $11,355.71 vs 09:30 $11,371.44 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,355.71 | ▲ 09:30 equity $11,355.71 vs yday $11,355.71 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 2 | $164.43 | $2.00 | — | $11,024.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 72 | $5.91 | $2.21 | — | $10,597.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 1 | $242.17 | $1.99 | — | $10,352.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 13 | $32.01 | $2.03 | — | $9,934.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 5 | $71.71 | $2.00 | — | $9,574.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 7 | $56.02 | $2.01 | — | $9,180.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 45 | $9.37 | $2.12 | — | $8,756.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 32 | $13.10 | $2.09 | — | $8,335.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $425.84; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 771 | $2.70 | $9.95 | — | $6,243.39 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $2083.76; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 424 | $4.91 | $5.47 | — | $4,156.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $2083.76; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 338 | $6.16 | $4.36 | — | $2,069.64 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $2083.76; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 658 | $3.13 | $8.49 | — | $1.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $2083.76; owner union_hot_n4_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.61 | ▲ close $11,538.38 vs 09:30 $11,355.71 (session +227.39) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.61 | ▲ 09:30 equity $11,656.87 vs yday $11,538.38 (+118.49) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 338 | $6.02 | $4.43 | $-56.11 | $2,031.94 | ▼ -56.11 after sell → book $11,652.44; vs 09:30 mark -4.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,031.94 | ▲ close $12,221.05 vs 09:30 $11,656.87 (session +568.61) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,031.94 | ▲ 09:30 equity $12,345.87 vs yday $12,221.05 (+124.82) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 424 | $5.11 | $5.56 | $+73.77 | $4,193.02 | ▲ +73.77 after sell → book $12,340.31; vs 09:30 mark -5.56 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 658 | $3.64 | $8.62 | $+318.48 | $6,579.52 | ▲ +318.48 after sell → book $12,331.69; vs 09:30 mark -8.62 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,579.52 | ▲ close $12,539.44 vs 09:30 $12,345.87 (session +207.75) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,579.52 | ▼ 09:30 equity $12,533.30 vs yday $12,539.44 (-6.14) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 2 | $140.03 | $2.02 | $-52.81 | $6,857.57 | ▼ -52.81 after sell → book $12,531.29; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 72 | $6.25 | $2.23 | $+20.05 | $7,305.34 | ▲ +20.05 after sell → book $12,529.06; vs 09:30 mark -2.23 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 1 | $253.34 | $2.01 | $+7.16 | $7,556.67 | ▲ +7.16 after sell → book $12,527.05; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 13 | $30.57 | $2.05 | $-22.80 | $7,952.03 | ▼ -22.80 after sell → book $12,525.00; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 5 | $78.12 | $2.02 | $+28.02 | $8,340.60 | ▲ +28.02 after sell → book $12,522.97; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 7 | $61.93 | $2.03 | $+37.33 | $8,772.08 | ▲ +37.33 after sell → book $12,520.94; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 45 | $9.40 | $2.15 | $-2.92 | $9,192.94 | ▼ -2.92 after sell → book $12,518.80; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 32 | $15.75 | $2.11 | $+80.61 | $9,694.83 | ▲ +80.61 after sell → book $12,516.69; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 43 | $33.14 | $2.12 | — | $8,267.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $1454.22; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 35 | $40.93 | $2.10 | — | $6,833.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1454.22; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1265 | $1.80 | $16.32 | — | $4,539.73 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $2277.68; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 97 | $23.29 | $2.28 | — | $2,278.32 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $2277.68; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 155 | $14.62 | $2.46 | — | $9.76 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $2277.68; owner union_hot_n4_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.76 | ▲ close $12,552.55 vs 09:30 $12,533.30 (session +61.13) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.76 | ▲ 09:30 equity $12,689.97 vs yday $12,552.55 (+137.42) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 97 | $24.09 | $2.32 | $+73.00 | $2,344.18 | ▲ +73.00 after sell → book $12,687.66; vs 09:30 mark -2.31 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 155 | $13.77 | $2.50 | $-136.70 | $4,476.03 | ▼ -136.70 after sell → book $12,685.16; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 59 | $11.21 | $2.17 | — | $3,812.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $671.40; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 8 | $81.00 | $2.01 | — | $3,162.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $671.40; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 140 | $22.46 | $2.41 | — | $15.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $3162.46; owner union_hot_n4_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.65 | ▲ close $12,969.11 vs 09:30 $12,689.97 (session +290.54) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.65 | ▼ 09:30 equity $12,878.01 vs yday $12,969.11 (-91.10) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1265 | $1.96 | $16.55 | $+169.53 | $2,478.50 | ▲ +169.53 after sell → book $12,861.46; vs 09:30 mark -16.55 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 140 | $21.30 | $2.46 | $-167.27 | $5,458.04 | ▼ -167.27 after sell → book $12,859.00; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 62 | $29.32 | $2.18 | — | $3,638.03 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1819.35; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 599 | $3.04 | $7.73 | — | $1,812.33 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1819.35; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 22 | $81.40 | $2.06 | — | $19.48 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1819.35; owner union_hot_n4_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.48 | ▲ close $12,906.58 vs 09:30 $12,878.01 (session +59.53) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.48 | ▲ 09:30 equity $13,263.45 vs yday $12,906.58 (+356.87) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 771 | $3.55 | $10.10 | $+635.31 | $2,746.43 | ▲ +635.31 after sell → book $13,253.36; vs 09:30 mark -10.09 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 43 | $40.03 | $2.14 | $+292.01 | $4,465.58 | ▲ +292.01 after sell → book $13,251.22; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 35 | $41.00 | $2.12 | $-1.76 | $5,898.46 | ▼ -1.76 after sell → book $13,249.10; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 62 | $29.43 | $2.20 | $+2.44 | $7,720.92 | ▲ +2.44 after sell → book $13,246.90; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 599 | $4.00 | $7.85 | $+562.46 | $10,109.08 | ▲ +562.46 after sell → book $13,239.05; vs 09:30 mark -7.85 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 22 | $79.08 | $2.08 | $-55.18 | $11,846.76 | ▼ -55.18 after sell → book $13,236.97; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1199 | $2.47 | $15.47 | — | $8,869.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $2961.69; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 175 | $16.91 | $2.52 | — | $5,907.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $2961.69; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1794 | $1.65 | $23.14 | — | $2,924.75 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $2961.69; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 250 | $11.67 | $3.23 | — | $4.03 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $2961.69; owner union_hot_n4_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.03 | ▲ close $13,601.50 vs 09:30 $13,263.45 (session +408.87) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.03 | ▼ 09:30 equity $13,466.50 vs yday $13,601.50 (-135.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.03 | ▲ close $13,476.50 vs 09:30 $13,466.50 (session +10.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.03 | ▲ 09:30 equity $13,667.03 vs yday $13,476.50 (+190.53) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 59 | $13.82 | $2.19 | $+149.64 | $817.22 | ▲ +149.64 after sell → book $13,664.84; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 8 | $82.00 | $2.03 | $+3.95 | $1,471.19 | ▲ +3.95 after sell → book $13,662.81; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 175 | $16.92 | $2.57 | $-3.33 | $4,429.62 | ▼ -3.33 after sell → book $13,660.24; vs 09:30 mark -2.57 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1794 | $1.41 | $23.46 | $-477.16 | $6,935.70 | ▼ -477.16 after sell → book $13,636.78; vs 09:30 mark -23.46 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 250 | $12.80 | $3.29 | $+275.98 | $10,132.41 | ▲ +275.98 after sell → book $13,633.49; vs 09:30 mark -3.29 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 12 | $47.57 | $2.03 | — | $9,559.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $607.94; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 3 | $196.78 | $2.00 | — | $8,967.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $607.94; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 17 | $35.74 | $2.04 | — | $8,357.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $607.94; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 12 | $47.15 | $2.03 | — | $7,789.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $607.94; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 5 | $109.67 | $2.00 | — | $7,239.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $607.94; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 893 | $2.70 | $11.52 | — | $4,816.78 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2413.13; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 57 | $41.76 | $2.16 | — | $2,434.30 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $2413.13; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 537 | $4.49 | $6.93 | — | $16.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $2413.13; owner union_hot_n4_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.24 | ▼ close $13,161.73 vs 09:30 $13,667.03 (session -441.05) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.24 | ▼ 09:30 equity $13,108.18 vs yday $13,161.73 (-53.55) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1199 | $2.68 | $15.69 | $+220.63 | $3,213.87 | ▲ +220.63 after sell → book $13,092.49; vs 09:30 mark -15.69 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 57 | $36.02 | $2.19 | $-331.24 | $5,265.11 | ▼ -331.24 after sell → book $13,090.31; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 537 | $3.92 | $7.03 | $-317.37 | $7,365.80 | ▼ -317.37 after sell → book $13,083.27; vs 09:30 mark -7.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,365.80 | ▲ close $15,030.33 vs 09:30 $13,108.18 (session +1,947.06) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,519.96 | ▲ 09:30 equity $11,419.62 vs yday $11,071.03 (+348.59) | 09:30 open · cash $2,519.96 (unchanged overnight, no fees) · equity $11,419.62 vs prior close $11,071.03 (+348.59) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 28 | $29.76 | $2.07 | — | $1,684.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $839.99; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 51 | $16.21 | $2.14 | — | $855.75 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $839.99; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 53 | $15.58 | $2.15 | — | $27.81 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $839.99; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.81 | ▼ close $11,133.50 vs 09:30 $11,419.62 (session -279.76) | 16:00 close · cash $27.81 · equity $11,133.50 vs 09:30 $11,419.62 (-286.12; session marks -279.76) · 14 name(s) marked open→close (per-name table). ABVX×7 09:30 $94.87 → close $94.87 +0.00; ANAB×13 09:30 $51.70 → close $51.70 +0.00; CBRL×9 09:30 $52.39 → close $51.81 -5.22; CTAS×2 09:30 $197.68 → close $197.68 -0.00; GIS×12 09:30 $34.83 → close $34.83 +0.00; GLND×484 09:30 $6.06 → close $5.54 -251.68; KBH×9 09:30 $47.65 → close $47.65 +0.00; MLKN×35 09:30 $19.91 → close $19.91 -0.00; PAYX×4 09:30 $101.59 → close $101.59 -0.00; THO×10 09:30 $70.93 → close $70.93 +0.00; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×28 09:30 $29.76 → close $26.24 -98.56; SECZ×51 09:30 $16.21 → close $15.96 -12.75; USDE×53 09:30 $15.58 → close $17.25 +88.45 | — |

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
| 2026-08-21 | `DE` | cash | leftover split 161.02 < 1 share @ 623.26 |
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
| 2026-08-27 | `RY` | cash | leftover split 201.00 < 1 share @ 206.82 |
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
| 2026-08-28 | `ADSK` | cash | leftover split 180.19 < 1 share @ 261.16 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-09-22 | `CRML` | cash | leftover split 1.34 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 1.34 < 1 share @ 7.23 |
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
| `CBRL` | 12 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $607.94; owner union_e_fresh_h3 |
| `CTAS` | 3 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $607.94; owner union_e_fresh_h3 |
| `GIS` | 17 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $607.94; owner union_e_fresh_h3 |
| `KBH` | 12 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $607.94; owner union_e_fresh_h3 |
| `PAYX` | 5 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $607.94; owner union_e_fresh_h3 |
| `GLND` | 893 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2413.13; owner union_hot_n4_h1 |
