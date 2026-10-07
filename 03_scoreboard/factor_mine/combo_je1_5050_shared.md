# Factor mine action — `combo_je1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_join_vol_green_h1/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **-3.67%** ($9,634) · signal-only (no cash/fees) was —. Starts YES **25/30**. Fills 494 · skips 110 · realized $+1669.53.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_join_vol_green_h1 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_join_vol_green_h1 50%, union_e_fresh_h1 50%.
- Member: union_join_vol_green_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,633.52.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 453 | $1.50 | $5.84 | — | $10,198.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 61 | $11.12 | $2.17 | — | $9,517.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 34 | $19.57 | $2.09 | — | $8,850.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 50 | $13.55 | $2.14 | — | $8,170.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 62 | $10.83 | $2.18 | — | $7,497.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 576 | $1.18 | $7.43 | — | $6,809.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 35 | $19.17 | $2.10 | — | $6,136.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 68 | $9.89 | $2.19 | — | $5,461.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 61 | $14.80 | $2.17 | — | $4,556.91 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $910.31; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 211 | $4.31 | $2.72 | — | $3,644.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $910.31; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 217 | $4.18 | $2.80 | — | $2,734.91 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $910.31; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 55 | $16.50 | $2.15 | — | $1,825.26 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $910.31; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 338 | $2.69 | $4.36 | — | $911.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $910.31; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 124 | $7.29 | $2.36 | — | $5.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $910.31; owner union_join_vol_green_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.36 | ▼ close $10,817.54 vs 09:30 $10,963.61 (session -23.42) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.36 | ▲ 09:30 equity $10,850.00 vs yday $10,817.54 (+32.46) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 453 | $1.52 | $5.93 | $-2.71 | $687.99 | ▼ -2.71 after sell → book $10,844.07; vs 09:30 mark -5.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 61 | $9.57 | $2.19 | $-98.92 | $1,269.57 | ▼ -98.92 after sell → book $10,841.88; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 34 | $19.57 | $2.11 | $-4.20 | $1,932.83 | ▼ -4.20 after sell → book $10,839.76; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 50 | $13.16 | $2.16 | $-23.80 | $2,588.67 | ▼ -23.80 after sell → book $10,837.60; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 62 | $11.19 | $2.20 | $+17.95 | $3,280.26 | ▲ +17.95 after sell → book $10,835.41; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 576 | $1.21 | $7.54 | $+2.31 | $3,969.68 | ▲ +2.31 after sell → book $10,827.87; vs 09:30 mark -7.54 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 35 | $20.25 | $2.12 | $+33.59 | $4,676.32 | ▲ +33.59 after sell → book $10,825.76; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 68 | $10.97 | $2.22 | $+68.69 | $5,420.06 | ▲ +68.69 after sell → book $10,823.54; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 61 | $13.67 | $2.19 | $-73.30 | $6,251.74 | ▼ -73.30 after sell → book $10,821.35; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 211 | $4.60 | $2.77 | $+55.70 | $7,219.57 | ▲ +55.70 after sell → book $10,818.58; vs 09:30 mark -2.77 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 217 | $4.10 | $2.85 | $-23.00 | $8,106.43 | ▼ -23.00 after sell → book $10,815.74; vs 09:30 mark -2.84 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 55 | $15.73 | $2.17 | $-46.68 | $8,969.40 | ▼ -46.68 after sell → book $10,813.56; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 338 | $2.80 | $4.43 | $+28.39 | $9,911.37 | ▲ +28.39 after sell → book $10,809.13; vs 09:30 mark -4.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 124 | $7.24 | $2.39 | $-10.95 | $10,806.74 | ▼ -10.95 after sell → book $10,806.74; vs 09:30 mark -2.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 236 | $9.12 | $3.04 | — | $8,651.38 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $2161.35; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 147 | $14.66 | $2.43 | — | $6,493.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $2161.35; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 470 | $4.59 | $6.06 | — | $4,330.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $2161.35; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 515 | $4.19 | $6.64 | — | $2,166.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $2161.35; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 37 | $58.01 | $2.10 | — | $17.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $2161.35; owner union_join_vol_green_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.60 | ▼ close $10,500.12 vs 09:30 $10,850.00 (session -286.33) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.60 | ▼ 09:30 equity $10,344.86 vs yday $10,500.12 (-155.26) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 236 | $9.03 | $3.10 | $-27.39 | $2,145.58 | ▼ -27.39 after sell → book $10,341.76; vs 09:30 mark -3.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 147 | $13.19 | $2.47 | $-220.99 | $4,082.04 | ▼ -220.99 after sell → book $10,339.29; vs 09:30 mark -2.47 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 470 | $4.56 | $6.16 | $-26.32 | $6,219.08 | ▼ -26.32 after sell → book $10,333.13; vs 09:30 mark -6.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 515 | $3.94 | $6.75 | $-142.14 | $8,241.43 | ▼ -142.14 after sell → book $10,326.38; vs 09:30 mark -6.75 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 37 | $56.35 | $2.13 | $-65.65 | $10,324.26 | ▼ -65.65 after sell → book $10,324.26; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.26 | ▲ close $10,324.26 vs 09:30 $10,344.86 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.26 | ▲ 09:30 equity $10,324.26 vs yday $10,324.26 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.26 | ▲ close $10,324.26 vs 09:30 $10,324.26 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.26 | ▲ 09:30 equity $10,324.26 vs yday $10,324.26 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $9,737.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 145 | $4.43 | $2.42 | — | $9,092.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2150 | $0.30 | $12.90 | — | $8,434.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 13 | $46.85 | $2.03 | — | $7,823.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 71 | $9.01 | $2.20 | — | $7,182.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 165 | $3.89 | $2.48 | — | $6,537.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $5,922.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 28 | $22.44 | $2.07 | — | $5,292.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $645.27; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 32 | $20.55 | $2.09 | — | $4,632.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 32 | $20.65 | $2.09 | — | $3,969.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 114 | $5.77 | $2.33 | — | $3,309.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 33 | $19.63 | $2.09 | — | $2,659.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 22 | $29.63 | $2.06 | — | $2,005.85 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 378 | $1.75 | $4.88 | — | $1,339.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $759.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 134 | $4.92 | $2.39 | — | $97.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $661.54; owner union_join_vol_green_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.64 | ▲ close $10,406.62 vs 09:30 $10,324.26 (session +130.45) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.64 | ▲ 09:30 equity $10,610.14 vs yday $10,406.62 (+203.52) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 6 | $96.75 | $2.03 | $-8.12 | $676.11 | ▼ -8.12 after sell → book $10,608.11; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 145 | $4.68 | $2.46 | $+31.37 | $1,352.25 | ▲ +31.37 after sell → book $10,605.65; vs 09:30 mark -2.46 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2150 | $0.31 | $13.48 | $-4.88 | $2,005.27 | ▼ -4.88 after sell → book $10,592.17; vs 09:30 mark -13.48 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 71 | $9.04 | $2.22 | $-2.30 | $2,644.88 | ▼ -2.30 after sell → book $10,589.94; vs 09:30 mark -2.23 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 165 | $4.32 | $2.52 | $+65.94 | $3,355.16 | ▲ +65.94 after sell → book $10,587.42; vs 09:30 mark -2.52 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 18 | $34.31 | $2.06 | $+0.57 | $3,970.68 | ▲ +0.57 after sell → book $10,585.36; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 28 | $22.20 | $2.09 | $-10.89 | $4,590.18 | ▼ -10.89 after sell → book $10,583.26; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 32 | $21.90 | $2.11 | $+39.01 | $5,288.88 | ▲ +39.01 after sell → book $10,581.16; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 32 | $21.75 | $2.11 | $+31.01 | $5,982.77 | ▲ +31.01 after sell → book $10,579.05; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 114 | $5.67 | $2.36 | $-16.09 | $6,626.79 | ▼ -16.09 after sell → book $10,576.69; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 33 | $21.17 | $2.11 | $+46.62 | $7,323.29 | ▲ +46.62 after sell → book $10,574.58; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 22 | $32.17 | $2.08 | $+51.75 | $8,028.96 | ▲ +51.75 after sell → book $10,572.51; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 378 | $1.79 | $4.95 | $+5.29 | $8,700.63 | ▲ +5.29 after sell → book $10,567.56; vs 09:30 mark -4.95 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $9,317.41 | ▲ +36.62 after sell → book $10,565.54; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 134 | $5.20 | $2.42 | $+32.70 | $10,011.78 | ▲ +32.70 after sell → book $10,563.11; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 6 | $115.18 | $2.01 | — | $9,318.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $8,693.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 6 | $103.69 | $2.01 | — | $8,069.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 39 | $17.93 | $2.11 | — | $7,367.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 7 | $93.98 | $2.01 | — | $6,707.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 16 | $43.08 | $2.04 | — | $6,016.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 310 | $2.30 | $4.00 | — | $5,299.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $715.13; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $4,700.38 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 38 | $17.20 | $2.10 | — | $4,044.67 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 3 | $216.30 | $2.00 | — | $3,393.77 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 59 | $11.13 | $2.17 | — | $2,734.94 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 501 | $1.32 | $6.46 | — | $2,067.15 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 399 | $1.66 | $5.15 | — | $1,399.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 476 | $1.39 | $6.14 | — | $731.89 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 80 | $8.28 | $2.23 | — | $67.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $662.44; owner union_join_vol_green_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.26 | ▲ close $10,730.42 vs 09:30 $10,610.14 (session +211.73) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.26 | ▲ 09:30 equity $10,929.48 vs yday $10,730.42 (+199.06) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 13 | $43.05 | $2.05 | $-53.48 | $624.86 | ▼ -53.48 after sell → book $10,927.43; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 6 | $121.00 | $2.03 | $+30.88 | $1,348.83 | ▲ +30.88 after sell → book $10,925.41; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $1,999.86 | ▲ +25.77 after sell → book $10,923.39; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 6 | $104.14 | $2.03 | $-1.34 | $2,622.67 | ▼ -1.34 after sell → book $10,921.36; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 39 | $18.05 | $2.13 | $+0.45 | $3,324.69 | ▲ +0.45 after sell → book $10,919.24; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 7 | $97.02 | $2.03 | $+17.24 | $4,001.80 | ▲ +17.24 after sell → book $10,917.21; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 16 | $44.22 | $2.06 | $+14.14 | $4,707.26 | ▲ +14.14 after sell → book $10,915.15; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 310 | $2.34 | $4.06 | $+4.34 | $5,428.60 | ▲ +4.34 after sell → book $10,911.09; vs 09:30 mark -4.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $6,029.12 | ▲ +1.37 after sell → book $10,909.06; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 38 | $16.57 | $2.12 | $-28.17 | $6,656.66 | ▼ -28.17 after sell → book $10,906.94; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 3 | $217.03 | $2.02 | $-1.83 | $7,305.73 | ▼ -1.83 after sell → book $10,904.92; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 59 | $13.33 | $2.19 | $+125.45 | $8,090.01 | ▲ +125.45 after sell → book $10,902.73; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 501 | $1.83 | $6.56 | $+242.49 | $9,000.29 | ▲ +242.49 after sell → book $10,896.18; vs 09:30 mark -6.55 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 399 | $1.55 | $5.22 | $-54.26 | $9,613.51 | ▼ -54.26 after sell → book $10,890.95; vs 09:30 mark -5.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 476 | $1.24 | $6.23 | $-83.77 | $10,197.52 | ▼ -83.77 after sell → book $10,884.72; vs 09:30 mark -6.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 80 | $8.59 | $2.25 | $+20.32 | $10,882.47 | ▲ +20.32 after sell → book $10,882.47; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,882.47 | ▲ close $10,882.47 vs 09:30 $10,929.48 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,882.47 | ▲ 09:30 equity $10,882.47 vs yday $10,882.47 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $10,355.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 7 | $88.94 | $2.01 | — | $9,730.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 44 | $15.28 | $2.12 | — | $9,056.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $8,484.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 133 | $5.10 | $2.39 | — | $7,804.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 14 | $47.89 | $2.03 | — | $7,131.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 48 | $13.92 | $2.13 | — | $6,461.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 149 | $4.54 | $2.44 | — | $5,781.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $680.15; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 443 | $1.63 | $5.71 | — | $5,054.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 203 | $3.55 | $2.62 | — | $4,330.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 113 | $6.37 | $2.33 | — | $3,608.64 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 20 | $35.05 | $2.05 | — | $2,905.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 11 | $64.55 | $2.02 | — | $2,193.51 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 4 | $156.51 | $2.00 | — | $1,565.47 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 80 | $8.98 | $2.23 | — | $844.84 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 380 | $1.90 | $4.90 | — | $117.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $722.73; owner union_join_vol_green_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.94 | ▼ close $10,785.28 vs 09:30 $10,882.47 (session -56.19) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.94 | ▼ 09:30 equity $10,724.13 vs yday $10,785.28 (-61.15) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 3 | $173.22 | $2.02 | $-9.39 | $635.58 | ▼ -9.39 after sell → book $10,722.11; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 7 | $92.65 | $2.03 | $+21.93 | $1,282.10 | ▲ +21.93 after sell → book $10,720.08; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 133 | $4.77 | $2.42 | $-48.70 | $1,914.09 | ▼ -48.70 after sell → book $10,717.65; vs 09:30 mark -2.43 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 14 | $48.24 | $2.05 | $+0.82 | $2,587.40 | ▲ +0.82 after sell → book $10,715.60; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 48 | $14.03 | $2.15 | $+0.99 | $3,258.68 | ▲ +0.99 after sell → book $10,713.45; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 149 | $3.38 | $2.47 | $-178.49 | $3,759.83 | ▼ -178.49 after sell → book $10,710.98; vs 09:30 mark -2.47 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 443 | $1.75 | $5.80 | $+43.86 | $4,531.50 | ▲ +43.86 after sell → book $10,705.18; vs 09:30 mark -5.80 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 203 | $3.77 | $2.66 | $+39.38 | $5,294.15 | ▲ +39.38 after sell → book $10,702.52; vs 09:30 mark -2.66 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 113 | $6.13 | $2.36 | $-31.81 | $5,984.48 | ▼ -31.81 after sell → book $10,700.16; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 20 | $35.70 | $2.07 | $+8.88 | $6,696.41 | ▲ +8.88 after sell → book $10,698.09; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 11 | $63.60 | $2.04 | $-14.52 | $7,393.96 | ▼ -14.52 after sell → book $10,696.04; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 4 | $160.93 | $2.02 | $+13.66 | $8,035.66 | ▲ +13.66 after sell → book $10,694.02; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 80 | $9.03 | $2.25 | $-0.48 | $8,755.81 | ▼ -0.48 after sell → book $10,691.77; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 380 | $1.87 | $4.98 | $-21.28 | $9,461.43 | ▼ -21.28 after sell → book $10,686.79; vs 09:30 mark -4.98 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1352 | $0.58 | $11.94 | — | $8,661.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $788.45; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 151 | $5.21 | $2.44 | — | $7,872.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $788.45; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 6 | $131.37 | $2.01 | — | $7,081.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $788.45; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 43 | $18.26 | $2.12 | — | $6,294.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $788.45; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 22 | $34.30 | $2.06 | — | $5,537.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $788.45; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $4,882.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $788.45; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 838 | $5.81 | $10.81 | — | $2.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $4882.13; owner union_join_vol_green_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.54 | ▲ close $10,985.04 vs 09:30 $10,724.13 (session +331.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.54 | ▲ 09:30 equity $11,369.06 vs yday $10,985.04 (+384.02) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 44 | $18.50 | $2.14 | $+137.42 | $814.40 | ▲ +137.42 after sell → book $11,366.92; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 4 | $128.73 | $2.02 | $-58.54 | $1,327.29 | ▼ -58.54 after sell → book $11,364.89; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 1352 | $0.53 | $11.46 | $-95.05 | $2,032.40 | ▼ -95.05 after sell → book $11,353.44; vs 09:30 mark -11.45 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 151 | $5.49 | $2.48 | $+37.36 | $2,858.91 | ▲ +37.36 after sell → book $11,350.96; vs 09:30 mark -2.48 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 6 | $144.70 | $2.03 | $+75.94 | $3,725.08 | ▲ +75.94 after sell → book $11,348.93; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 43 | $18.69 | $2.14 | $+14.23 | $4,526.61 | ▲ +14.23 after sell → book $11,346.79; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 22 | $33.79 | $2.08 | $-15.35 | $5,267.92 | ▼ -15.35 after sell → book $11,344.72; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 2 | $314.90 | $2.02 | $-28.03 | $5,895.70 | ▼ -28.03 after sell → book $11,342.70; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 838 | $6.50 | $10.99 | $+556.42 | $11,331.71 | ▲ +556.42 after sell → book $11,331.71; vs 09:30 mark -10.99 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 8 | $80.60 | $2.01 | — | $10,684.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 43 | $16.18 | $2.12 | — | $9,987.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 5 | $118.77 | $2.00 | — | $9,391.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 39 | $17.78 | $2.11 | — | $8,695.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 52 | $13.41 | $2.15 | — | $7,996.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 7 | $97.16 | $2.01 | — | $7,314.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 3 | $206.82 | $2.00 | — | $6,691.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 5 | $120.17 | $2.00 | — | $6,088.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $708.23; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 47 | $128.73 | $2.13 | — | $36.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $6088.74; owner union_join_vol_green_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.30 | ▲ close $11,505.75 vs 09:30 $11,369.06 (session +192.58) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.30 | ▲ 09:30 equity $11,572.03 vs yday $11,505.75 (+66.28) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 8 | $83.85 | $2.03 | $+21.95 | $705.07 | ▲ +21.95 after sell → book $11,570.00; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 43 | $16.94 | $2.14 | $+28.42 | $1,431.35 | ▲ +28.42 after sell → book $11,567.86; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 5 | $115.66 | $2.02 | $-19.58 | $2,007.62 | ▼ -19.58 after sell → book $11,565.83; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 39 | $18.58 | $2.13 | $+26.97 | $2,730.12 | ▲ +26.97 after sell → book $11,563.71; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 52 | $13.65 | $2.17 | $+8.17 | $3,437.75 | ▲ +8.17 after sell → book $11,561.54; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 7 | $93.62 | $2.03 | $-28.82 | $4,091.06 | ▼ -28.82 after sell → book $11,559.51; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 3 | $205.50 | $2.02 | $-7.98 | $4,705.54 | ▼ -7.98 after sell → book $11,557.49; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 5 | $122.07 | $2.02 | $+5.47 | $5,313.87 | ▲ +5.47 after sell → book $11,555.47; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 47 | $132.80 | $2.19 | $+186.97 | $11,553.27 | ▲ +186.97 after sell → book $11,553.27; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 29 | $24.69 | $2.08 | — | $10,835.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $10,310.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 48 | $15.01 | $2.13 | — | $9,588.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 6 | $103.89 | $2.01 | — | $8,962.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 186 | $3.88 | $2.55 | — | $8,238.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 16 | $44.40 | $2.04 | — | $7,526.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 86 | $8.35 | $2.25 | — | $6,805.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 19 | $37.65 | $2.05 | — | $6,088.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $722.08; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,187.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2029.53; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 87 | $23.30 | $2.25 | — | $2,158.30 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $2029.53; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 106 | $19.00 | $2.31 | — | $142.00 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $2029.53; owner union_join_vol_green_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.00 | ▼ close $11,275.56 vs 09:30 $11,572.03 (session -254.04) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.00 | ▼ 09:30 equity $11,205.25 vs yday $11,275.56 (-70.31) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 29 | $22.98 | $2.10 | $-53.76 | $806.32 | ▼ -53.76 after sell → book $11,203.15; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-10.91 | $1,319.72 | ▼ -10.91 after sell → book $11,201.13; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 48 | $14.88 | $2.15 | $-10.53 | $2,031.81 | ▼ -10.53 after sell → book $11,198.98; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 6 | $98.00 | $2.03 | $-39.38 | $2,617.78 | ▼ -39.38 after sell → book $11,196.95; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 186 | $3.39 | $2.59 | $-96.28 | $3,245.73 | ▼ -96.28 after sell → book $11,194.36; vs 09:30 mark -2.59 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 16 | $44.85 | $2.06 | $+3.10 | $3,961.27 | ▲ +3.10 after sell → book $11,192.30; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 86 | $8.53 | $2.27 | $+10.96 | $4,692.58 | ▲ +10.96 after sell → book $11,190.03; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 19 | $35.81 | $2.07 | $-38.98 | $5,370.90 | ▼ -38.98 after sell → book $11,187.96; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.03 | $2.05 | $+21.40 | $7,293.24 | ▲ +21.40 after sell → book $11,185.91; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 87 | $22.66 | $2.28 | $-60.21 | $9,262.38 | ▼ -60.21 after sell → book $11,183.63; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 106 | $18.12 | $2.34 | $-97.40 | $11,181.29 | ▼ -97.40 after sell → book $11,181.29; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,181.29 | ▲ close $11,181.29 vs 09:30 $11,205.25 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,181.29 | ▲ 09:30 equity $11,181.29 vs yday $11,181.29 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,181.29 | ▲ close $11,181.29 vs 09:30 $11,181.29 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,181.29 | ▲ 09:30 equity $11,181.29 vs yday $11,181.29 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,181.29 | ▲ close $11,181.29 vs 09:30 $11,181.29 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,181.29 | ▲ 09:30 equity $11,181.29 vs yday $11,181.29 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 65 | $10.74 | $2.19 | — | $10,480.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,126.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 101 | $6.90 | $2.29 | — | $9,427.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,071.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 31 | $22.32 | $2.08 | — | $8,377.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,861.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $7,192.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 46 | $15.09 | $2.13 | — | $6,496.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $698.83; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $5,699.86 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 48 | $16.77 | $2.13 | — | $4,892.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 372 | $2.18 | $4.80 | — | $4,077.01 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $3,263.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 77 | $10.42 | $2.22 | — | $2,458.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 420 | $1.93 | $5.42 | — | $1,642.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 5 | $161.54 | $2.00 | — | $832.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 78 | $10.38 | $2.22 | — | $21.24 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $812.07; owner union_join_vol_green_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.24 | ▲ close $11,377.57 vs 09:30 $11,181.29 (session +235.89) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.24 | ▲ 09:30 equity $11,384.72 vs yday $11,377.57 (+7.15) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 65 | $10.91 | $2.21 | $+6.33 | $728.18 | ▲ +6.33 after sell → book $11,382.51; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $1,085.87 | ▲ +3.95 after sell → book $11,380.50; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 101 | $9.28 | $2.32 | $+235.77 | $2,020.83 | ▲ +235.77 after sell → book $11,378.18; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $2,340.49 | ▼ -36.83 after sell → book $11,376.17; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 31 | $22.10 | $2.10 | $-11.01 | $3,023.48 | ▼ -11.01 after sell → book $11,374.06; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $3,499.23 | ▼ -40.25 after sell → book $11,372.05; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 14 | $53.85 | $2.05 | $+83.42 | $4,251.08 | ▲ +83.42 after sell → book $11,370.00; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 46 | $15.34 | $2.15 | $+7.22 | $4,954.57 | ▲ +7.22 after sell → book $11,367.85; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 6 | $130.03 | $2.03 | $-18.56 | $5,732.72 | ▼ -18.56 after sell → book $11,365.82; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 48 | $15.61 | $2.15 | $-59.97 | $6,479.85 | ▼ -59.97 after sell → book $11,363.67; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 372 | $2.16 | $4.87 | $-17.11 | $7,278.50 | ▼ -17.11 after sell → book $11,358.80; vs 09:30 mark -4.87 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $8,086.94 | ▼ -5.56 after sell → book $11,356.68; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 77 | $10.50 | $2.24 | $+1.70 | $8,893.20 | ▲ +1.70 after sell → book $11,354.44; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 420 | $1.90 | $5.50 | $-23.52 | $9,685.70 | ▼ -23.52 after sell → book $11,348.94; vs 09:30 mark -5.50 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 5 | $157.46 | $2.02 | $-24.43 | $10,470.98 | ▼ -24.43 after sell → book $11,346.92; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 78 | $11.23 | $2.25 | $+62.22 | $11,344.67 | ▲ +62.22 after sell → book $11,344.67; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 11 | $63.18 | $2.02 | — | $10,647.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 81 | $8.74 | $2.23 | — | $9,937.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 10 | $68.52 | $2.02 | — | $9,250.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 196 | $3.62 | $2.58 | — | $8,539.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 4 | $167.55 | $2.00 | — | $7,866.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 15 | $44.90 | $2.04 | — | $7,191.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 7 | $98.15 | $2.01 | — | $6,502.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 45 | $15.70 | $2.12 | — | $5,793.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $709.04; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $5,277.96 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 8 | $82.70 | $2.01 | — | $4,614.35 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 288 | $2.51 | $3.72 | — | $3,887.75 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $3,507.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 28 | $25.18 | $2.07 | — | $2,800.30 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 125 | $5.79 | $2.37 | — | $2,074.19 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 19 | $37.44 | $2.05 | — | $1,360.78 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 114 | $6.32 | $2.33 | — | $637.97 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $724.22; owner union_join_vol_green_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $637.97 | ▲ close $11,464.60 vs 09:30 $11,384.72 (session +155.49) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $637.97 | ▼ 09:30 equity $11,381.79 vs yday $11,464.60 (-82.81) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 11 | $63.83 | $2.04 | $+3.08 | $1,338.06 | ▲ +3.08 after sell → book $11,379.75; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 81 | $8.73 | $2.26 | $-5.30 | $2,042.93 | ▼ -5.30 after sell → book $11,377.49; vs 09:30 mark -2.26 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 10 | $67.05 | $2.04 | $-18.76 | $2,711.39 | ▼ -18.76 after sell → book $11,375.45; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 196 | $3.84 | $2.62 | $+38.90 | $3,461.41 | ▲ +38.90 after sell → book $11,372.83; vs 09:30 mark -2.62 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 4 | $160.52 | $2.02 | $-32.14 | $4,101.47 | ▼ -32.14 after sell → book $11,370.81; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 15 | $39.56 | $2.06 | $-84.19 | $4,692.81 | ▼ -84.19 after sell → book $11,368.75; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 7 | $100.58 | $2.03 | $+12.97 | $5,394.84 | ▲ +12.97 after sell → book $11,366.72; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 45 | $15.20 | $2.15 | $-26.77 | $6,076.70 | ▼ -26.77 after sell → book $11,364.58; vs 09:30 mark -2.14 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $6,595.83 | ▲ +3.36 after sell → book $11,362.56; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 8 | $89.67 | $2.03 | $+51.71 | $7,311.16 | ▲ +51.71 after sell → book $11,360.53; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 288 | $2.66 | $3.77 | $+35.71 | $8,073.47 | ▲ +35.71 after sell → book $11,356.76; vs 09:30 mark -3.77 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $8,432.20 | ▼ -21.60 after sell → book $11,354.74; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 28 | $26.44 | $2.09 | $+31.11 | $9,170.43 | ▲ +31.11 after sell → book $11,352.65; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 125 | $5.81 | $2.40 | $-2.26 | $9,894.28 | ▼ -2.26 after sell → book $11,350.25; vs 09:30 mark -2.40 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 19 | $37.75 | $2.07 | $+1.78 | $10,609.47 | ▲ +1.78 after sell → book $11,348.19; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 114 | $6.48 | $2.36 | $+13.55 | $11,345.83 | ▲ +13.55 after sell → book $11,345.83; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,345.83 | ▲ close $11,345.83 vs 09:30 $11,381.79 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,345.83 | ▲ 09:30 equity $11,345.83 vs yday $11,345.83 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,345.83 | ▲ close $11,345.83 vs 09:30 $11,345.83 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,345.83 | ▲ 09:30 equity $11,345.83 vs yday $11,345.83 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,345.83 | ▲ close $11,345.83 vs 09:30 $11,345.83 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,345.83 | ▲ 09:30 equity $11,345.83 vs yday $11,345.83 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $10,686.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 119 | $5.91 | $2.35 | — | $9,980.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $9,494.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 22 | $32.01 | $2.06 | — | $8,787.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 9 | $71.71 | $2.02 | — | $8,140.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 12 | $56.02 | $2.03 | — | $7,466.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 75 | $9.37 | $2.21 | — | $6,761.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 54 | $13.10 | $2.15 | — | $6,051.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $709.11; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 32 | $23.63 | $2.09 | — | $5,293.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 280 | $2.70 | $3.61 | — | $4,533.81 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 69 | $10.95 | $2.20 | — | $3,776.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 154 | $4.91 | $2.45 | — | $3,017.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 8 | $84.27 | $2.01 | — | $2,341.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 13 | $54.91 | $2.03 | — | $1,625.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 122 | $6.16 | $2.36 | — | $871.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 13 | $54.66 | $2.03 | — | $158.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $756.46; owner union_join_vol_green_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.95 | ▼ close $11,257.18 vs 09:30 $11,345.83 (session -53.06) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.95 | ▲ 09:30 equity $11,315.92 vs yday $11,257.18 (+58.74) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 4 | $141.42 | $2.02 | $-96.06 | $722.61 | ▼ -96.06 after sell → book $11,313.90; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 119 | $5.86 | $2.38 | $-10.67 | $1,417.57 | ▼ -10.67 after sell → book $11,311.52; vs 09:30 mark -2.38 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $1,938.58 | ▲ +34.67 after sell → book $11,309.51; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 22 | $30.63 | $2.08 | $-34.49 | $2,610.36 | ▼ -34.49 after sell → book $11,307.43; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 9 | $77.68 | $2.04 | $+49.68 | $3,307.44 | ▲ +49.68 after sell → book $11,305.39; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 12 | $59.31 | $2.05 | $+35.41 | $4,017.12 | ▲ +35.41 after sell → book $11,303.35; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 75 | $8.85 | $2.24 | $-43.45 | $4,678.63 | ▼ -43.45 after sell → book $11,301.11; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 54 | $14.16 | $2.17 | $+52.92 | $5,441.10 | ▲ +52.92 after sell → book $11,298.94; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 32 | $23.20 | $2.11 | $-17.95 | $6,181.39 | ▼ -17.95 after sell → book $11,296.83; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 280 | $2.80 | $3.67 | $+20.72 | $6,961.72 | ▲ +20.72 after sell → book $11,293.16; vs 09:30 mark -3.67 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 69 | $10.29 | $2.22 | $-49.96 | $7,669.51 | ▼ -49.96 after sell → book $11,290.94; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 154 | $5.03 | $2.49 | $+13.54 | $8,441.65 | ▲ +13.54 after sell → book $11,288.46; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 8 | $86.06 | $2.03 | $+10.27 | $9,128.09 | ▲ +10.27 after sell → book $11,286.42; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 13 | $54.75 | $2.05 | $-6.16 | $9,837.79 | ▼ -6.16 after sell → book $11,284.37; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 122 | $6.02 | $2.39 | $-21.82 | $10,569.85 | ▼ -21.82 after sell → book $11,281.99; vs 09:30 mark -2.38 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 13 | $54.78 | $2.05 | $-2.52 | $11,279.94 | ▼ -2.52 after sell → book $11,279.94; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,279.94 | ▲ close $11,279.94 vs 09:30 $11,315.92 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,279.94 | ▲ 09:30 equity $11,279.94 vs yday $11,279.94 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,279.94 | ▲ close $11,279.94 vs 09:30 $11,279.94 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,279.94 | ▲ 09:30 equity $11,279.94 vs yday $11,279.94 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 85 | $33.14 | $2.25 | — | $8,460.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $2819.98; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 68 | $40.93 | $2.19 | — | $5,675.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2819.98; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 10 | $77.12 | $2.02 | — | $4,902.14 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 138 | $5.87 | $2.40 | — | $4,089.68 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 9 | $87.40 | $2.02 | — | $3,301.06 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 29 | $27.09 | $2.08 | — | $2,513.37 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 9 | $89.38 | $2.02 | — | $1,706.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 34 | $23.29 | $2.09 | — | $912.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 28 | $28.16 | $2.07 | — | $122.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $810.77; owner union_join_vol_green_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.43 | ▲ close $11,274.31 vs 09:30 $11,279.94 (session +13.51) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.43 | ▲ 09:30 equity $11,523.22 vs yday $11,274.31 (+248.91) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 85 | $36.76 | $2.28 | $+303.17 | $3,244.74 | ▲ +303.17 after sell → book $11,520.93; vs 09:30 mark -2.29 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 68 | $40.79 | $2.23 | $-13.94 | $6,016.24 | ▼ -13.94 after sell → book $11,518.71; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 10 | $76.44 | $2.04 | $-10.86 | $6,778.60 | ▼ -10.86 after sell → book $11,516.67; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 138 | $5.58 | $2.44 | $-44.86 | $7,546.20 | ▼ -44.86 after sell → book $11,514.23; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 9 | $83.20 | $2.04 | $-41.85 | $8,292.96 | ▼ -41.85 after sell → book $11,512.19; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 29 | $28.23 | $2.10 | $+28.89 | $9,109.54 | ▲ +28.89 after sell → book $11,510.10; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 9 | $86.76 | $2.04 | $-27.63 | $9,888.34 | ▼ -27.63 after sell → book $11,508.06; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 34 | $24.09 | $2.11 | $+23.00 | $10,705.29 | ▲ +23.00 after sell → book $11,505.95; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 28 | $28.59 | $2.09 | $+8.01 | $11,503.85 | ▲ +8.01 after sell → book $11,503.85; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 256 | $11.21 | $3.30 | — | $8,630.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $2875.96; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 35 | $81.00 | $2.10 | — | $5,793.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $2875.96; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 3 | $233.85 | $2.00 | — | $5,090.15 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $4,497.70 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 95 | $7.59 | $2.27 | — | $3,774.38 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 27 | $25.95 | $2.07 | — | $3,071.66 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 4 | $170.85 | $2.00 | — | $2,386.26 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 40 | $18.04 | $2.11 | — | $1,662.75 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 19 | $36.76 | $2.05 | — | $962.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 11 | $61.90 | $2.02 | — | $279.34 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $724.21; owner union_join_vol_green_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.34 | ▲ close $11,615.94 vs 09:30 $11,523.22 (session +134.01) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.34 | ▲ 09:30 equity $11,684.09 vs yday $11,615.94 (+68.15) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 256 | $11.64 | $3.37 | $+103.41 | $3,255.81 | ▲ +103.41 after sell → book $11,680.72; vs 09:30 mark -3.37 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 35 | $78.25 | $2.13 | $-100.47 | $5,992.43 | ▼ -100.47 after sell → book $11,678.59; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 3 | $249.13 | $2.02 | $+41.82 | $6,737.80 | ▲ +41.82 after sell → book $11,676.57; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 4 | $146.50 | $2.02 | $-8.46 | $7,321.78 | ▼ -8.46 after sell → book $11,674.55; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 95 | $7.98 | $2.30 | $+32.47 | $8,077.58 | ▲ +32.47 after sell → book $11,672.25; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 27 | $26.14 | $2.09 | $+0.97 | $8,781.27 | ▲ +0.97 after sell → book $11,670.16; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 4 | $182.33 | $2.02 | $+41.90 | $9,508.57 | ▲ +41.90 after sell → book $11,668.14; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 40 | $17.80 | $2.13 | $-13.64 | $10,218.44 | ▼ -13.64 after sell → book $11,666.01; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 19 | $39.50 | $2.07 | $+47.95 | $10,966.87 | ▲ +47.95 after sell → book $11,663.94; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 11 | $63.37 | $2.04 | $+12.10 | $11,661.90 | ▲ +12.10 after sell → book $11,661.90; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $10,342.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 17 | $85.00 | $2.04 | — | $8,895.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 369 | $3.95 | $4.76 | — | $7,432.82 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 103 | $14.07 | $2.30 | — | $5,981.31 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 98 | $14.79 | $2.28 | — | $4,529.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 49 | $29.32 | $2.14 | — | $3,090.79 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 480 | $3.04 | $6.19 | — | $1,627.79 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 128 | $11.38 | $2.37 | — | $168.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $1457.74; owner union_join_vol_green_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.78 | ▲ close $11,879.44 vs 09:30 $11,684.09 (session +241.64) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.78 | ▲ 09:30 equity $12,151.43 vs yday $11,879.44 (+271.99) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $1,548.25 | ▲ +59.74 after sell → book $12,149.40; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 17 | $82.83 | $2.06 | $-40.99 | $2,954.30 | ▼ -40.99 after sell → book $12,147.34; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 369 | $3.87 | $4.83 | $-39.11 | $4,377.50 | ▼ -39.11 after sell → book $12,142.51; vs 09:30 mark -4.83 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 103 | $13.90 | $2.33 | $-22.14 | $5,806.87 | ▼ -22.14 after sell → book $12,140.18; vs 09:30 mark -2.33 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 98 | $14.58 | $2.31 | $-25.18 | $7,233.40 | ▼ -25.18 after sell → book $12,137.87; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 49 | $29.43 | $2.16 | $+1.09 | $8,673.31 | ▲ +1.09 after sell → book $12,135.71; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 480 | $4.00 | $6.29 | $+450.72 | $10,587.02 | ▲ +450.72 after sell → book $12,129.42; vs 09:30 mark -6.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 128 | $12.05 | $2.41 | $+80.98 | $12,127.01 | ▲ +80.98 after sell → book $12,127.01; vs 09:30 mark -2.41 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 9 | $157.87 | $2.02 | — | $10,704.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 17 | $88.83 | $2.04 | — | $9,192.02 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 162 | $9.31 | $2.48 | — | $7,681.32 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 112 | $13.47 | $2.33 | — | $6,169.79 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 151 | $9.99 | $2.44 | — | $4,658.86 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 89 | $16.91 | $2.26 | — | $3,151.61 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 116 | $13.05 | $2.34 | — | $1,635.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 263 | $5.75 | $3.39 | — | $118.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $1515.88; owner union_join_vol_green_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.52 | ▲ close $12,152.80 vs 09:30 $12,151.43 (session +45.08) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.52 | ▲ 09:30 equity $12,204.56 vs yday $12,152.80 (+51.76) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 151 | $9.91 | $2.48 | $-17.00 | $1,612.45 | ▼ -17.00 after sell → book $12,202.08; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 116 | $12.99 | $2.37 | $-11.67 | $3,116.92 | ▼ -11.67 after sell → book $12,199.71; vs 09:30 mark -2.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 263 | $6.05 | $3.45 | $+72.06 | $4,705.93 | ▲ +72.06 after sell → book $12,196.26; vs 09:30 mark -3.45 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 2 | $319.41 | $2.00 | — | $4,065.12 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $941.19; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 33 | $28.02 | $2.09 | — | $3,138.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $941.19; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 1 | $731.40 | $1.99 | — | $2,404.98 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; combo leftover $941.19; owner union_join_vol_green_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,404.98 | ▼ close $12,158.95 vs 09:30 $12,204.56 (session -31.24) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,404.98 | ▲ 09:30 equity $12,174.16 vs yday $12,158.95 (+15.21) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 162 | $9.50 | $2.52 | $+25.79 | $3,941.46 | ▲ +25.79 after sell → book $12,171.64; vs 09:30 mark -2.52 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 112 | $12.84 | $2.36 | $-75.80 | $5,377.19 | ▼ -75.80 after sell → book $12,169.29; vs 09:30 mark -2.35 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 89 | $16.92 | $2.28 | $-3.65 | $6,880.78 | ▼ -3.65 after sell → book $12,167.00; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 2 | $331.78 | $2.02 | $+20.73 | $7,542.33 | ▲ +20.73 after sell → book $12,164.99; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 33 | $25.90 | $2.11 | $-74.16 | $8,394.92 | ▼ -74.16 after sell → book $12,162.88; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 1 | $747.60 | $2.01 | $+12.19 | $9,140.50 | ▲ +12.19 after sell → book $12,160.86; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 19 | $47.57 | $2.05 | — | $8,234.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $914.05; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $7,445.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $914.05; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 25 | $35.74 | $2.06 | — | $6,549.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $914.05; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 19 | $47.15 | $2.05 | — | $5,652.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $914.05; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 8 | $109.67 | $2.01 | — | $4,772.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $914.05; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 38 | $20.65 | $2.10 | — | $3,985.86 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $795.44; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 50 | $15.72 | $2.14 | — | $3,197.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $795.44; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 31 | $25.40 | $2.08 | — | $2,408.24 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $795.44; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1035 | $0.77 | $11.05 | — | $1,602.31 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $795.44; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 19 | $41.76 | $2.05 | — | $806.82 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $795.44; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 80 | $9.90 | $2.23 | — | $12.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $795.44; owner union_join_vol_green_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.59 | ▼ close $11,847.92 vs 09:30 $12,174.16 (session -281.12) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.59 | ▼ 09:30 equity $11,705.61 vs yday $11,847.92 (-142.31) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 9 | $163.95 | $2.04 | $+50.66 | $1,486.10 | ▲ +50.66 after sell → book $11,703.57; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 17 | $87.67 | $2.06 | $-23.74 | $2,974.51 | ▼ -23.74 after sell → book $11,701.51; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 19 | $46.88 | $2.07 | $-17.22 | $3,863.17 | ▼ -17.22 after sell → book $11,699.44; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 4 | $192.26 | $2.02 | $-22.10 | $4,630.19 | ▼ -22.10 after sell → book $11,697.42; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 25 | $35.96 | $2.08 | $+1.35 | $5,527.10 | ▲ +1.35 after sell → book $11,695.33; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 19 | $47.14 | $2.07 | $-4.30 | $6,420.69 | ▼ -4.30 after sell → book $11,693.26; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 8 | $105.49 | $2.03 | $-37.47 | $7,262.60 | ▼ -37.47 after sell → book $11,691.23; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 38 | $20.52 | $2.12 | $-9.17 | $8,040.23 | ▼ -9.17 after sell → book $11,689.11; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 50 | $14.38 | $2.16 | $-71.30 | $8,757.07 | ▼ -71.30 after sell → book $11,686.95; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 31 | $23.99 | $2.10 | $-47.90 | $9,498.66 | ▼ -47.90 after sell → book $11,684.84; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1035 | $0.75 | $11.01 | $-44.83 | $10,259.76 | ▼ -44.83 after sell → book $11,673.84; vs 09:30 mark -11.00 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 19 | $36.02 | $2.07 | $-113.08 | $10,942.17 | ▼ -113.08 after sell → book $11,671.77; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 80 | $9.12 | $2.25 | $-66.88 | $11,669.52 | ▼ -66.88 after sell → book $11,669.52; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,669.52 | ▲ close $11,669.52 vs 09:30 $11,705.61 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,373.03 | ▲ 09:30 equity $9,373.03 vs yday $9,373.03 (+0.00) | 09:30 open · cash $9,373.03 (unchanged overnight, no fees) · equity $9,373.03 vs prior close $9,373.03 (+0.00) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 5 | $887.00 | $2.00 | — | $4,936.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $4686.52; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 23 | $26.27 | $2.06 | — | $4,329.76 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 159 | $3.86 | $2.47 | — | $3,713.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 3 | $184.00 | $2.00 | — | $3,159.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 38 | $16.21 | $2.10 | — | $2,541.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 4 | $123.50 | $2.00 | — | $2,045.46 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 20 | $29.80 | $2.05 | — | $1,447.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 154 | $4.00 | $2.45 | — | $828.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 10 | $61.33 | $2.02 | — | $212.87 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $617.00; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.87 | ▲ close $9,609.46 vs 09:30 $9,373.03 (session +255.58) | 16:00 close · cash $212.87 · equity $9,609.46 vs 09:30 $9,373.03 (+236.43; session marks +255.58) · 9 name(s) marked open→close (per-name table). COST×5 09:30 $887.00 → close $922.76 +178.82; WRBY×23 09:30 $26.27 → close $26.71 +10.12; ZSQR×159 09:30 $3.86 → close $3.78 -12.72; TWST×3 09:30 $184.00 → close $182.83 -3.51; SECZ×38 09:30 $16.21 → close $15.96 -9.50; GRAL×4 09:30 $123.50 → close $126.89 +13.56; QMCO×20 09:30 $29.80 → close $31.68 +37.60; CYPH×154 09:30 $4.00 → close $4.12 +17.71; CDNA×10 09:30 $61.33 → close $63.68 +23.50 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.87 | ▼ 09:30 equity $9,571.35 vs yday $9,609.46 (-38.11) | 09:30 open · cash $212.87 (unchanged overnight, no fees) · equity $9,571.35 vs prior close $9,609.46 (-38.11) | — |
| 2026-09-28 09:30 ET | **SELL** | `CDNA` | 10 | $62.30 | $2.04 | $+5.64 | $833.83 | ▲ +5.64 after sell → book $9,569.31; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `COST` | 5 | $924.88 | $2.05 | $+185.32 | $5,456.15 | ▲ +185.32 after sell → book $9,567.26; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 154 | $4.03 | $2.49 | $-0.70 | $6,074.67 | ▼ -0.70 after sell → book $9,564.77; vs 09:30 mark -2.49 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `GRAL` | 4 | $128.90 | $2.02 | $+17.58 | $6,588.25 | ▲ +17.58 after sell → book $9,562.75; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `QMCO` | 20 | $31.65 | $2.07 | $+32.88 | $7,219.18 | ▲ +32.88 after sell → book $9,560.68; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `SECZ` | 38 | $16.00 | $2.12 | $-12.21 | $7,825.05 | ▼ -12.21 after sell → book $9,558.55; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TWST` | 3 | $181.87 | $2.02 | $-10.41 | $8,368.65 | ▼ -10.41 after sell → book $9,556.54; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `WRBY` | 23 | $26.00 | $2.08 | $-10.35 | $8,964.57 | ▼ -10.35 after sell → book $9,554.46; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 159 | $3.71 | $2.50 | $-28.82 | $9,551.95 | ▼ -28.82 after sell → book $9,551.95; vs 09:30 mark -2.51 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,551.95 | ▲ close $9,551.95 vs 09:30 $9,571.35 (session +0.00) | 16:00 close · cash $9,551.95 · no lots left · equity $9,551.95. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,551.95 | ▲ 09:30 equity $9,551.95 vs yday $9,551.95 (+0.00) | 09:30 open · cash $9,551.95 (unchanged overnight, no fees) · equity $9,551.95 vs prior close $9,551.95 (+0.00) | — |
| 2026-09-29 09:30 ET | **BUY** | `CCL` | 39 | $24.39 | $2.11 | — | $8,598.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $955.20; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `JEF` | 20 | $46.08 | $2.05 | — | $7,674.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.5; combo leftover $955.20; owner union_e_fresh_h1 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `KMX` | 15 | $60.41 | $2.04 | — | $6,766.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.3; combo leftover $955.20; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MTN` | 6 | $138.42 | $2.01 | — | $5,934.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.7; combo leftover $955.20; owner union_e_fresh_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `UEC` | 96 | $9.91 | $2.28 | — | $4,980.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $955.20; owner union_e_fresh_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `QNC` | 345 | $2.06 | $4.45 | — | $4,265.56 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+24.3; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BB` | 80 | $8.86 | $2.23 | — | $3,554.53 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer; ⚪; ret5=+3.2; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MDB` | 2 | $335.14 | $2.00 | — | $2,882.24 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_mover; ⚪; ret5=-17.8; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `LTRX` | 96 | $7.34 | $2.28 | — | $2,175.32 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; ret5=+16.8; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `VFC` | 49 | $14.51 | $2.14 | — | $1,462.20 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.5; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AEO` | 40 | $17.36 | $2.11 | — | $765.69 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; 🔵; ret5=+12.0; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BURL` | 2 | $268.37 | $2.00 | — | $226.95 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; 🔵; ret5=+5.8; combo leftover $711.53; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.95 | ▼ close $9,409.72 vs 09:30 $9,551.95 (session -114.56) | 16:00 close · cash $226.95 · equity $9,409.72 vs 09:30 $9,551.95 (-142.23; session marks -114.56) · 12 name(s) marked open→close (per-name table). CCL×39 09:30 $24.39 → close $25.11 +28.08; JEF×20 09:30 $46.08 → close $46.52 +8.80; KMX×15 09:30 $60.41 → close $59.23 -17.63; MTN×6 09:30 $138.42 → close $141.29 +17.22; UEC×96 09:30 $9.91 → close $9.29 -59.52; QNC×345 09:30 $2.06 → close $1.75 -106.95; BB×80 09:30 $8.86 → close $8.72 -11.20; MDB×2 09:30 $335.14 → close $337.19 +4.09; LTRX×96 09:30 $7.34 → close $7.22 -11.52; VFC×49 09:30 $14.51 → close $14.50 -0.49; AEO×40 09:30 $17.36 → close $18.14 +31.20; BURL×2 09:30 $268.37 → close $270.05 +3.36 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.95 | ▲ 09:30 equity $9,409.72 vs yday $9,409.72 (+0.00) | 09:30 open · cash $226.95 (unchanged overnight, no fees) · equity $9,409.72 vs prior close $9,409.72 (+0.00) | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.95 | ▲ close $9,409.72 vs 09:30 $9,409.72 (session +0.00) | 16:00 close · cash $226.95 · equity $9,409.72 vs 09:30 $9,409.72 (+0.00; session marks +0.00) · 12 name(s) marked open→close (per-name table). AEO×40 09:30 $18.14 → close $18.14 +0.00; BB×80 09:30 $8.72 → close $8.72 +0.00; BURL×2 09:30 $270.05 → close $270.05 +0.00; CCL×39 09:30 $25.11 → close $25.11 +0.00; JEF×20 09:30 $46.52 → close $46.52 +0.00; KMX×15 09:30 $59.23 → close $59.23 +0.00; LTRX×96 09:30 $7.22 → close $7.22 +0.00; MDB×2 09:30 $337.19 → close $337.19 +0.00; MTN×6 09:30 $141.29 → close $141.29 +0.00; QNC×345 09:30 $1.75 → close $1.75 +0.00; UEC×96 09:30 $9.29 → close $9.29 +0.00; VFC×49 09:30 $14.50 → close $14.50 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.95 | ▼ 09:30 equity $9,262.50 vs yday $9,409.72 (-147.22) | 09:30 open · cash $226.95 (unchanged overnight, no fees) · equity $9,262.50 vs prior close $9,409.72 (-147.22) | — |
| 2026-10-01 09:30 ET | **SELL** | `AEO` | 40 | $17.82 | $2.13 | $+14.16 | $937.62 | ▲ +14.16 after sell → book $9,260.37; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `BB` | 80 | $8.97 | $2.25 | $+4.32 | $1,652.97 | ▲ +4.32 after sell → book $9,258.12; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `BURL` | 2 | $269.84 | $2.02 | $-1.07 | $2,190.63 | ▼ -1.07 after sell → book $9,256.10; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `CCL` | 39 | $24.66 | $2.13 | $+6.30 | $3,150.24 | ▲ +6.30 after sell → book $9,253.97; vs 09:30 mark -2.13 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `JEF` | 20 | $45.41 | $2.07 | $-17.52 | $4,056.37 | ▼ -17.52 after sell → book $9,251.90; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `KMX` | 15 | $55.27 | $2.06 | $-81.11 | $4,883.37 | ▼ -81.11 after sell → book $9,249.85; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `LTRX` | 96 | $7.25 | $2.30 | $-13.22 | $5,577.06 | ▼ -13.22 after sell → book $9,247.54; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `MDB` | 2 | $352.47 | $2.02 | $+30.64 | $6,279.99 | ▲ +30.64 after sell → book $9,245.53; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `MTN` | 6 | $138.51 | $2.03 | $-3.50 | $7,109.02 | ▼ -3.50 after sell → book $9,243.50; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `QNC` | 345 | $1.57 | $4.52 | $-178.02 | $7,646.15 | ▼ -178.02 after sell → book $9,238.98; vs 09:30 mark -4.52 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `UEC` | 96 | $9.39 | $2.30 | $-54.50 | $8,545.29 | ▼ -54.50 after sell → book $9,236.68; vs 09:30 mark -2.30 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **SELL** | `VFC` | 49 | $14.11 | $2.16 | $-23.89 | $9,234.52 | ▼ -23.89 after sell → book $9,234.52; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-01 09:30 ET | **BUY** | `ACN` | 5 | $215.98 | $2.00 | — | $8,152.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.1; combo leftover $1154.32; owner union_e_fresh_h1 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MKC` | 24 | $46.80 | $2.06 | — | $7,027.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-5.5; combo leftover $1154.32; owner union_e_fresh_h1 | join🟢 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MU` | 1 | $1054.08 | $1.99 | — | $5,971.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.6; combo leftover $1154.32; owner union_e_fresh_h1 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PRGS` | 28 | $40.52 | $2.07 | — | $4,834.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.6; combo leftover $1154.32; owner union_e_fresh_h1 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 42 | $14.27 | $2.12 | — | $4,233.19 | — | combo gate; gate join=good,last_green=True,vol=good; list flatten,ohlc_hot; 🔵; ret5=+7.3; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 544 | $1.11 | $7.02 | — | $3,622.33 | — | combo gate; gate join=good,last_green=True,vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IVA` | 179 | $3.38 | $2.53 | — | $3,015.68 | — | combo gate; gate join=good,last_green=True,vol=good; list probable,yday_gainer; ret5=+7.0; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `SWMR` | 34 | $17.50 | $2.09 | — | $2,418.59 | — | combo gate; gate join=good,last_green=True,vol=good; list probable,yday_gainer; 🔵; ret5=-21.6; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PACB` | 258 | $2.34 | $3.33 | — | $1,811.54 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+68.1; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PMVP` | 359 | $1.68 | $4.63 | — | $1,203.79 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+21.9; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MNKD` | 157 | $3.84 | $2.46 | — | $598.45 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+16.8; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `UTHR` | 1 | $557.53 | $1.99 | — | $38.93 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+10.7; combo leftover $604.33; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.93 | ▼ close $9,136.56 vs 09:30 $9,262.50 (session -63.67) | 16:00 close · cash $38.93 · equity $9,136.56 vs 09:30 $9,262.50 (-125.94; session marks -63.67) · 12 name(s) marked open→close (per-name table). ACN×5 09:30 $215.98 → close $212.30 -18.40; MKC×24 09:30 $46.80 → close $44.14 -63.84; MU×1 09:30 $1054.08 → close $1097.39 +43.31; PRGS×28 09:30 $40.52 → close $36.58 -110.32; AVPT×42 09:30 $14.27 → close $14.08 -7.98; TLSA×544 09:30 $1.11 → close $1.14 +16.32; IVA×179 09:30 $3.38 → close $3.46 +15.21; SWMR×34 09:30 $17.50 → close $16.38 -38.08; PACB×258 09:30 $2.34 → close $2.51 +43.86; PMVP×359 09:30 $1.68 → close $1.75 +25.13; MNKD×157 09:30 $3.84 → close $3.95 +17.27; UTHR×1 09:30 $557.53 → close $571.38 +13.85 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.93 | ▲ 09:30 equity $9,166.13 vs yday $9,136.56 (+29.57) | 09:30 open · cash $38.93 (unchanged overnight, no fees) · equity $9,166.13 vs prior close $9,136.56 (+29.57) | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 42 | $14.22 | $2.14 | $-6.35 | $634.03 | ▼ -6.35 after sell → book $9,163.99; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `IVA` | 179 | $3.57 | $2.57 | $+29.81 | $1,270.50 | ▲ +29.81 after sell → book $9,161.43; vs 09:30 mark -2.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `MKC` | 24 | $43.52 | $2.08 | $-82.86 | $2,312.90 | ▼ -82.86 after sell → book $9,159.35; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `MNKD` | 157 | $4.01 | $2.50 | $+21.73 | $2,939.97 | ▲ +21.73 after sell → book $9,156.85; vs 09:30 mark -2.50 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `MU` | 1 | $1107.45 | $2.01 | $+49.36 | $4,045.41 | ▲ +49.36 after sell → book $9,154.84; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PACB` | 258 | $2.50 | $3.38 | $+34.57 | $4,687.02 | ▲ +34.57 after sell → book $9,151.45; vs 09:30 mark -3.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PMVP` | 359 | $1.75 | $4.70 | $+15.80 | $5,310.57 | ▲ +15.80 after sell → book $9,146.75; vs 09:30 mark -4.70 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PRGS` | 28 | $36.88 | $2.09 | $-106.09 | $6,341.12 | ▼ -106.09 after sell → book $9,144.66; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `SWMR` | 34 | $16.10 | $2.11 | $-51.80 | $6,886.41 | ▼ -51.80 after sell → book $9,142.55; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `TLSA` | 544 | $1.16 | $7.12 | $+13.06 | $7,510.33 | ▲ +13.06 after sell → book $9,135.43; vs 09:30 mark -7.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `UTHR` | 1 | $570.00 | $2.01 | $+8.46 | $8,078.32 | ▲ +8.46 after sell → book $9,133.42; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `NKE` | 124 | $32.55 | $2.36 | — | $4,039.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.3; combo leftover $4039.16; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 7 | $66.33 | $2.01 | — | $3,573.06 | — | combo gate; gate join=good,last_green=True,vol=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 9 | $52.42 | $2.02 | — | $3,099.26 | — | combo gate; gate join=good,last_green=True,vol=good; list flatten; 🔵; ⚪; ret5=-12.6; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `COHR` | 1 | $316.56 | $1.99 | — | $2,780.71 | — | combo gate; gate join=good,last_green=True,vol=good; list probable,yday_gainer; 🔵; ret5=+9.8; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `INOD` | 6 | $73.05 | $2.01 | — | $2,340.40 | — | combo gate; gate join=good,last_green=True,vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=-0.0; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SDEV` | 99 | $5.06 | $2.29 | — | $1,837.17 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+183.7; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SES` | 587 | $0.86 | $6.81 | — | $1,325.54 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+54.3; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `QSI` | 365 | $1.38 | $4.71 | — | $817.13 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+82.2; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SNPS` | 1 | $497.86 | $1.99 | — | $317.29 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; combo leftover $504.92; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.29 | ▲ close $9,552.90 vs 09:30 $9,166.13 (session +445.68) | 16:00 close · cash $317.29 · equity $9,552.90 vs 09:30 $9,166.13 (+386.77; session marks +445.68) · 10 name(s) marked open→close (per-name table). ACN×5 09:30 $211.02 → close $198.90 -60.60; NKE×124 09:30 $32.55 → close $33.87 +163.31; CDNA×7 09:30 $66.33 → close $67.15 +5.74; ETON×9 09:30 $52.42 → close $55.36 +26.46; COHR×1 09:30 $316.56 → close $337.04 +20.48; INOD×6 09:30 $73.05 → close $70.07 -17.88; SDEV×99 09:30 $5.06 → close $7.48 +239.58; SES×587 09:30 $0.86 → close $0.88 +14.50; QSI×365 09:30 $1.38 → close $1.55 +62.05; SNPS×1 09:30 $497.86 → close $489.90 -7.96 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.29 | ▲ 09:30 equity $9,772.69 vs yday $9,552.90 (+219.79) | 09:30 open · cash $317.29 (unchanged overnight, no fees) · equity $9,772.69 vs prior close $9,552.90 (+219.79) | — |
| 2026-10-05 09:30 ET | **SELL** | `ACN` | 5 | $196.30 | $2.02 | $-102.43 | $1,296.77 | ▼ -102.43 after sell → book $9,770.67; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 7 | $66.90 | $2.03 | $-0.05 | $1,763.03 | ▼ -0.05 after sell → book $9,768.63; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `COHR` | 1 | $340.93 | $2.01 | $+20.35 | $2,101.95 | ▲ +20.35 after sell → book $9,766.62; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ETON` | 9 | $55.85 | $2.04 | $+26.82 | $2,602.56 | ▲ +26.82 after sell → book $9,764.58; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `INOD` | 6 | $70.98 | $2.03 | $-16.46 | $3,026.41 | ▼ -16.46 after sell → book $9,762.56; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `NKE` | 124 | $33.82 | $2.42 | $+152.33 | $7,217.67 | ▲ +152.33 after sell → book $9,760.14; vs 09:30 mark -2.42 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `QSI` | 365 | $1.52 | $4.78 | $+43.44 | $7,769.52 | ▲ +43.44 after sell → book $9,755.36; vs 09:30 mark -4.78 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SDEV` | 99 | $9.71 | $2.31 | $+455.75 | $8,728.50 | ▲ +455.75 after sell → book $9,753.05; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `SES` | 587 | $0.90 | $7.15 | $+9.52 | $9,249.65 | ▲ +9.52 after sell → book $9,745.90; vs 09:30 mark -7.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SNPS` | 1 | $496.25 | $2.01 | $-5.61 | $9,743.88 | ▼ -5.61 after sell → book $9,743.88; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `EVGO` | 1765 | $1.38 | $22.77 | — | $7,285.41 | — | combo gate; gate join=good,last_green=True,vol=good; list probable,yday_gainer; ret5=+0.0; combo leftover $2435.97; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VECO` | 42 | $56.94 | $2.12 | — | $4,891.82 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; ret5=+15.5; combo leftover $2435.97; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `NTAP` | 10 | $225.47 | $2.02 | — | $2,635.10 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; ⚪; ret5=+12.5; combo leftover $2435.97; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `STM` | 43 | $56.60 | $2.12 | — | $199.18 | — | combo gate; gate join=good,last_green=True,vol=good; list ohlc_hot; ret5=+10.3; combo leftover $2435.97; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.18 | ▼ close $9,664.89 vs 09:30 $9,772.69 (session -49.97) | 16:00 close · cash $199.18 · equity $9,664.89 vs 09:30 $9,772.69 (-107.80; session marks -49.97) · 4 name(s) marked open→close (per-name table). EVGO×1765 09:30 $1.38 → close $1.35 -52.95; VECO×42 09:30 $56.94 → close $56.31 -26.46; NTAP×10 09:30 $225.47 → close $223.77 -17.00; STM×43 09:30 $56.60 → close $57.68 +46.44 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $199.18 | ▲ 09:30 equity $9,742.47 vs yday $9,664.89 (+77.58) | 09:30 open · cash $199.18 (unchanged overnight, no fees) · equity $9,742.47 vs prior close $9,664.89 (+77.58) | — |
| 2026-10-06 09:30 ET | **SELL** | `EVGO` | 1765 | $1.36 | $23.08 | $-82.74 | $2,574.91 | ▼ -82.74 after sell → book $9,719.39; vs 09:30 mark -23.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `NTAP` | 10 | $224.80 | $2.05 | $-10.77 | $4,820.86 | ▼ -10.77 after sell → book $9,717.34; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `STM` | 43 | $57.96 | $2.15 | $+54.21 | $7,310.99 | ▲ +54.21 after sell → book $9,715.20; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `VECO` | 42 | $57.24 | $2.15 | $+8.46 | $9,713.05 | ▲ +8.46 after sell → book $9,713.05; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **BUY** | `RPM` | 50 | $96.25 | $2.14 | — | $4,898.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $4856.53; owner union_e_fresh_h1 | join🔴 sector🟢 gen🟡 news🟡 digest🔴 ab🟢 peer🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `AVPT` | 41 | $14.77 | $2.11 | — | $4,290.73 | — | combo gate; gate join=good,last_green=True,vol=good; list flatten; ⚪; ret5=+7.2; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `QTEX` | 359 | $1.71 | $4.63 | — | $3,674.00 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+123.8; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XP` | 20 | $29.20 | $2.05 | — | $3,087.95 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+39.1; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PAGS` | 55 | $10.96 | $2.15 | — | $2,483.00 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+23.0; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `STNE` | 52 | $11.67 | $2.15 | — | $1,874.01 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+24.5; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `INTR` | 89 | $6.81 | $2.26 | — | $1,265.66 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+30.2; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `BBD` | 135 | $4.52 | $2.40 | — | $653.07 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+29.4; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `NU` | 39 | $15.45 | $2.11 | — | $48.41 | — | combo gate; gate join=good,last_green=True,vol=good; list yday_gainer; 🔵; ret5=+24.1; combo leftover $612.30; owner union_join_vol_green_h1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.41 | ▲ close $9,766.61 vs 09:30 $9,742.47 (session +75.56) | 16:00 close · cash $48.41 · equity $9,766.61 vs 09:30 $9,742.47 (+24.14; session marks +75.56) · 9 name(s) marked open→close (per-name table). RPM×50 09:30 $96.25 → close $98.31 +103.00; AVPT×41 09:30 $14.77 → close $14.59 -7.38; QTEX×359 09:30 $1.71 → close $1.59 -41.28; XP×20 09:30 $29.20 → close $29.73 +10.60; PAGS×55 09:30 $10.96 → close $10.72 -13.20; STNE×52 09:30 $11.67 → close $11.62 -2.60; INTR×89 09:30 $6.81 → close $7.03 +19.58; BBD×135 09:30 $4.52 → close $4.51 -1.35; NU×39 09:30 $15.45 → close $15.66 +8.19 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.41 | ▼ 09:30 equity $9,655.80 vs yday $9,766.61 (-110.82) | 09:30 open · cash $48.41 (unchanged overnight, no fees) · equity $9,655.80 vs prior close $9,766.61 (-110.82) | — |
| 2026-10-07 09:30 ET | **SELL** | `AVPT` | 41 | $14.51 | $2.13 | $-14.91 | $641.19 | ▼ -14.91 after sell → book $9,653.66; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `BBD` | 135 | $4.50 | $2.43 | $-7.52 | $1,246.26 | ▼ -7.52 after sell → book $9,651.23; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `INTR` | 89 | $7.02 | $2.28 | $+14.15 | $1,868.76 | ▲ +14.15 after sell → book $9,648.95; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `NU` | 39 | $15.55 | $2.13 | $-0.33 | $2,473.08 | ▼ -0.33 after sell → book $9,646.83; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `PAGS` | 55 | $10.64 | $2.17 | $-21.93 | $3,056.11 | ▼ -21.93 after sell → book $9,644.65; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `QTEX` | 359 | $1.38 | $4.70 | $-127.80 | $3,545.03 | ▼ -127.80 after sell → book $9,639.95; vs 09:30 mark -4.70 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `RPM` | 50 | $98.10 | $2.19 | $+88.17 | $8,447.84 | ▲ +88.17 after sell → book $9,637.76; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `STNE` | 52 | $11.56 | $2.17 | $-10.03 | $9,046.79 | ▼ -10.03 after sell → book $9,635.59; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `XP` | 20 | $29.44 | $2.07 | $+0.68 | $9,633.52 | ▲ +0.68 after sell → book $9,633.52; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,633.52 | ▲ close $9,633.52 vs 09:30 $9,655.80 (session +0.00) | 16:00 close · cash $9,633.52 · no lots left · equity $9,633.52. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
