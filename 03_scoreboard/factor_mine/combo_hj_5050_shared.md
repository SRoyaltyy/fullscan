# Factor mine action — `combo_hj_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_join_vol_green_h1 w=0.5,0.5 net=priority

Cash book **+9.44%** ($10,944) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 301 · skips 100 · realized $+1412.04.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_join_vol_green_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_join_vol_green_h1 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: union_join_vol_green_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $235.97.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $2500.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $2500.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $2500.00; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $9,081.41 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $1295.87; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 66 | $19.57 | $2.19 | — | $7,787.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $1295.87; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 589 | $2.20 | $7.60 | — | $6,484.21 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $1295.87; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 116 | $11.12 | $2.34 | — | $5,191.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $1295.87; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 494 | $1.50 | $6.37 | — | $4,444.58 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 50 | $14.80 | $2.14 | — | $3,702.44 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 172 | $4.31 | $2.51 | — | $2,958.61 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 177 | $4.18 | $2.52 | — | $2,216.23 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 44 | $16.50 | $2.12 | — | $1,488.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 275 | $2.69 | $3.55 | — | $744.81 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 101 | $7.29 | $2.29 | — | $6.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $741.71; owner union_join_vol_green_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.23 | ▼ close $10,196.45 vs 09:30 $10,412.10 (session -134.70) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.23 | ▼ 09:30 equity $10,071.83 vs yday $10,196.45 (-124.62) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,295.22 | ▲ +3.49 after sell → book $10,069.67; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 66 | $19.57 | $2.21 | $-4.40 | $2,584.63 | ▼ -4.40 after sell → book $10,067.46; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 589 | $2.08 | $7.71 | $-83.04 | $3,804.99 | ▼ -83.04 after sell → book $10,059.75; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 116 | $9.57 | $2.37 | $-184.51 | $4,912.74 | ▼ -184.51 after sell → book $10,057.38; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 494 | $1.52 | $6.46 | $-2.96 | $5,657.16 | ▼ -2.96 after sell → book $10,050.92; vs 09:30 mark -6.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 50 | $13.67 | $2.16 | $-60.80 | $6,338.50 | ▼ -60.80 after sell → book $10,048.76; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 172 | $4.60 | $2.54 | $+44.83 | $7,127.16 | ▲ +44.83 after sell → book $10,046.22; vs 09:30 mark -2.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 177 | $4.10 | $2.56 | $-19.24 | $7,850.30 | ▼ -19.24 after sell → book $10,043.66; vs 09:30 mark -2.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 44 | $15.73 | $2.14 | $-38.14 | $8,540.27 | ▼ -38.14 after sell → book $10,041.51; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 275 | $2.80 | $3.60 | $+23.10 | $9,306.67 | ▲ +23.10 after sell → book $10,037.91; vs 09:30 mark -3.60 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 101 | $7.24 | $2.32 | $-9.66 | $10,035.59 | ▼ -9.66 after sell → book $10,035.59; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 299 | $4.19 | $3.86 | — | $8,778.92 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1254.45; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 182 | $6.87 | $2.54 | — | $7,526.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1254.45; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 91 | $13.64 | $2.26 | — | $6,282.54 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1254.45; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,043.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1254.45; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $3,782.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $1260.89; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 86 | $14.66 | $2.25 | — | $2,519.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $1260.89; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 274 | $4.59 | $3.53 | — | $1,258.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $1260.89; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 21 | $58.01 | $2.05 | — | $38.13 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $1260.89; owner union_join_vol_green_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.13 | ▼ close $9,944.33 vs 09:30 $10,071.83 (session -70.28) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.13 | ▼ 09:30 equity $9,850.67 vs yday $9,944.33 (-93.66) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 299 | $3.94 | $3.92 | $-82.52 | $1,212.28 | ▼ -82.52 after sell → book $9,846.76; vs 09:30 mark -3.91 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 91 | $13.31 | $2.29 | $-34.58 | $2,421.20 | ▼ -34.58 after sell → book $9,844.47; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,664.10 | ▲ +3.92 after sell → book $9,842.37; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $4,907.80 | ▼ -17.26 after sell → book $9,839.93; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 86 | $13.19 | $2.27 | $-130.94 | $6,039.87 | ▼ -130.94 after sell → book $9,837.66; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 274 | $4.56 | $3.59 | $-15.34 | $7,285.72 | ▼ -15.34 after sell → book $9,834.07; vs 09:30 mark -3.59 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 21 | $56.35 | $2.07 | $-38.99 | $8,467.00 | ▼ -38.99 after sell → book $9,832.00; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,467.00 | ▼ close $9,755.56 vs 09:30 $9,850.67 (session -76.44) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,467.00 | ▲ 09:30 equity $9,775.58 vs yday $9,755.56 (+20.02) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 182 | $7.19 | $2.58 | $+53.13 | $9,773.00 | ▲ +53.13 after sell → book $9,773.00; vs 09:30 mark -2.58 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,773.00 | ▲ close $9,773.00 vs 09:30 $9,775.58 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,773.00 | ▲ 09:30 equity $9,773.00 vs yday $9,773.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,569.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1221.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1062 | $1.15 | $13.70 | — | $7,334.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1221.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 103 | $11.81 | $2.30 | — | $6,115.62 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1221.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 891 | $1.37 | $11.49 | — | $4,883.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1221.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 29 | $20.55 | $2.08 | — | $4,285.43 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 29 | $20.65 | $2.08 | — | $3,684.51 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 105 | $5.77 | $2.31 | — | $3,076.35 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $2,465.74 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 20 | $29.63 | $2.05 | — | $1,871.09 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 348 | $1.75 | $4.49 | — | $1,257.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $677.44 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 124 | $4.92 | $2.36 | — | $64.99 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $610.43; owner union_join_vol_green_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.99 | ▲ close $9,747.21 vs 09:30 $9,773.00 (session +23.17) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.99 | ▲ 09:30 equity $10,071.67 vs yday $9,747.21 (+324.46) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 103 | $11.57 | $2.33 | $-29.86 | $1,254.38 | ▼ -29.86 after sell → book $10,069.35; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 891 | $1.46 | $11.65 | $+57.04 | $2,543.59 | ▲ +57.04 after sell → book $10,057.70; vs 09:30 mark -11.65 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 29 | $21.90 | $2.10 | $+34.98 | $3,176.59 | ▲ +34.98 after sell → book $10,055.60; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 29 | $21.75 | $2.10 | $+27.73 | $3,805.24 | ▲ +27.73 after sell → book $10,053.50; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 105 | $5.67 | $2.33 | $-15.14 | $4,398.26 | ▼ -15.14 after sell → book $10,051.17; vs 09:30 mark -2.33 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $5,052.43 | ▲ +43.55 after sell → book $10,049.07; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 20 | $32.17 | $2.07 | $+46.68 | $5,693.76 | ▲ +46.68 after sell → book $10,047.00; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 348 | $1.79 | $4.56 | $+4.87 | $6,312.12 | ▲ +4.87 after sell → book $10,042.44; vs 09:30 mark -4.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $6,928.90 | ▲ +36.62 after sell → book $10,040.42; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 124 | $5.20 | $2.39 | $+29.97 | $7,571.30 | ▲ +29.97 after sell → book $10,038.02; vs 09:30 mark -2.40 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 421 | $4.49 | $5.43 | — | $5,675.58 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1892.83; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 277 | $6.81 | $3.57 | — | $3,785.64 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1892.83; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 4 | $119.43 | $2.00 | — | $3,305.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 31 | $17.20 | $2.08 | — | $2,770.64 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $2,336.04 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 48 | $11.13 | $2.13 | — | $1,799.67 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 325 | $1.66 | $4.19 | — | $1,255.97 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 389 | $1.39 | $5.02 | — | $710.24 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 65 | $8.28 | $2.19 | — | $169.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $540.81; owner union_join_vol_green_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.86 | ▲ close $10,077.29 vs 09:30 $10,071.67 (session +67.88) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.86 | ▲ 09:30 equity $10,912.02 vs yday $10,077.29 (+834.73) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,309.43 | ▼ -63.57 after sell → book $10,909.99; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1062 | $1.83 | $13.89 | $+694.57 | $3,238.99 | ▲ +694.57 after sell → book $10,896.09; vs 09:30 mark -13.90 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 421 | $4.32 | $5.52 | $-82.52 | $5,052.20 | ▼ -82.52 after sell → book $10,890.58; vs 09:30 mark -5.51 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 277 | $8.03 | $3.64 | $+330.73 | $7,272.87 | ▲ +330.73 after sell → book $10,886.94; vs 09:30 mark -3.64 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 4 | $120.51 | $2.02 | $+0.30 | $7,752.89 | ▲ +0.30 after sell → book $10,884.92; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 31 | $16.57 | $2.10 | $-23.72 | $8,264.46 | ▼ -23.72 after sell → book $10,882.82; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $8,696.50 | ▼ -2.55 after sell → book $10,880.80; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 48 | $13.33 | $2.15 | $+101.31 | $9,334.19 | ▲ +101.31 after sell → book $10,878.65; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 325 | $1.55 | $4.26 | $-44.20 | $9,833.68 | ▼ -44.20 after sell → book $10,874.39; vs 09:30 mark -4.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 389 | $1.24 | $5.09 | $-68.46 | $10,310.95 | ▼ -68.46 after sell → book $10,869.30; vs 09:30 mark -5.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 65 | $8.59 | $2.21 | $+15.76 | $10,867.09 | ▲ +15.76 after sell → book $10,867.09; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,867.09 | ▲ close $10,867.09 vs 09:30 $10,912.02 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,867.09 | ▲ 09:30 equity $10,867.09 vs yday $10,867.09 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 56 | $24.11 | $2.16 | — | $9,514.77 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1358.39; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 870 | $1.56 | $11.22 | — | $8,146.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1358.39; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 333 | $4.07 | $4.30 | — | $6,786.75 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1358.39; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 71 | $19.04 | $2.20 | — | $5,432.70 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1358.39; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 416 | $1.63 | $5.37 | — | $4,749.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 191 | $3.55 | $2.56 | — | $4,068.64 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 106 | $6.37 | $2.31 | — | $3,391.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 19 | $35.05 | $2.05 | — | $2,723.12 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 10 | $64.55 | $2.02 | — | $2,075.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 4 | $156.51 | $2.00 | — | $1,447.56 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 75 | $8.98 | $2.21 | — | $771.84 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 357 | $1.90 | $4.61 | — | $88.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $679.09; owner union_join_vol_green_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.94 | ▲ close $11,411.38 vs 09:30 $10,867.09 (session +587.29) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.94 | ▼ 09:30 equity $11,113.74 vs yday $11,411.38 (-297.64) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 56 | $26.61 | $2.18 | $+135.66 | $1,576.92 | ▲ +135.66 after sell → book $11,111.56; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 870 | $1.60 | $11.38 | $+12.20 | $2,957.54 | ▲ +12.20 after sell → book $11,100.18; vs 09:30 mark -11.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 71 | $20.72 | $2.23 | $+114.85 | $4,426.43 | ▲ +114.85 after sell → book $11,097.95; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 416 | $1.75 | $5.45 | $+41.19 | $5,151.06 | ▲ +41.19 after sell → book $11,092.50; vs 09:30 mark -5.45 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 191 | $3.77 | $2.60 | $+36.85 | $5,868.53 | ▲ +36.85 after sell → book $11,089.90; vs 09:30 mark -2.60 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 106 | $6.13 | $2.34 | $-30.08 | $6,515.97 | ▼ -30.08 after sell → book $11,087.56; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 19 | $35.70 | $2.07 | $+8.24 | $7,192.21 | ▲ +8.24 after sell → book $11,085.50; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 10 | $63.60 | $2.04 | $-13.56 | $7,826.17 | ▼ -13.56 after sell → book $11,083.46; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 4 | $160.93 | $2.02 | $+13.66 | $8,467.87 | ▲ +13.66 after sell → book $11,081.44; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 75 | $9.03 | $2.24 | $-0.70 | $9,142.88 | ▼ -0.70 after sell → book $11,079.20; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 357 | $1.87 | $4.67 | $-19.99 | $9,805.79 | ▼ -19.99 after sell → book $11,074.52; vs 09:30 mark -4.68 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 231 | $14.11 | $2.98 | — | $6,543.40 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $3268.60; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 562 | $5.81 | $7.25 | — | $3,270.93 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $3268.60; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 282 | $11.59 | $3.64 | — | $0.33 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $3268.60; owner union_hot_n4_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.33 | ▲ close $11,264.74 vs 09:30 $11,113.74 (session +204.08) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.33 | ▲ 09:30 equity $11,720.27 vs yday $11,264.74 (+455.53) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 231 | $14.20 | $3.04 | $+14.77 | $3,277.48 | ▲ +14.77 after sell → book $11,717.22; vs 09:30 mark -3.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 562 | $6.50 | $7.37 | $+373.16 | $6,923.11 | ▲ +373.16 after sell → book $11,709.85; vs 09:30 mark -7.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 282 | $12.18 | $3.71 | $+160.44 | $10,354.16 | ▲ +160.44 after sell → book $11,706.14; vs 09:30 mark -3.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 187 | $9.19 | $2.55 | — | $8,633.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1725.69; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 11 | $144.18 | $2.02 | — | $7,045.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1725.69; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 93 | $18.50 | $2.27 | — | $5,322.30 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $1725.69; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 41 | $128.73 | $2.11 | — | $42.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $5322.30; owner union_join_vol_green_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.26 | ▲ close $11,835.92 vs 09:30 $11,720.27 (session +138.74) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.26 | ▼ 09:30 equity $11,732.38 vs yday $11,835.92 (-103.54) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 333 | $3.69 | $4.36 | $-135.20 | $1,266.67 | ▼ -135.20 after sell → book $11,728.02; vs 09:30 mark -4.36 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 93 | $18.15 | $2.30 | $-37.12 | $2,952.32 | ▼ -37.12 after sell → book $11,725.72; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 41 | $132.80 | $2.17 | $+162.59 | $8,394.96 | ▲ +162.59 after sell → book $11,723.56; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 149 | $14.00 | $2.44 | — | $6,306.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2098.74; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 14 | $146.07 | $2.03 | — | $4,259.51 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2098.74; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 60 | $23.30 | $2.17 | — | $2,859.34 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $1419.84; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 74 | $19.00 | $2.21 | — | $1,451.12 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $1419.84; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 57 | $24.69 | $2.16 | — | $41.63 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $1419.84; owner union_join_vol_green_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.63 | ▼ close $11,586.33 vs 09:30 $11,732.38 (session -126.21) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.63 | ▼ 09:30 equity $11,434.05 vs yday $11,586.33 (-152.28) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 187 | $9.50 | $2.60 | $+52.82 | $1,815.54 | ▲ +52.82 after sell → book $11,431.46; vs 09:30 mark -2.59 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 11 | $134.10 | $2.04 | $-114.95 | $3,288.59 | ▼ -114.95 after sell → book $11,429.41; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 14 | $148.03 | $2.06 | $+23.35 | $5,358.95 | ▲ +23.35 after sell → book $11,427.35; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 60 | $22.66 | $2.19 | $-42.76 | $6,716.36 | ▼ -42.76 after sell → book $11,425.16; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 74 | $18.12 | $2.23 | $-69.20 | $8,055.38 | ▼ -69.20 after sell → book $11,422.93; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 57 | $22.98 | $2.18 | $-101.81 | $9,363.06 | ▼ -101.81 after sell → book $11,420.75; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,363.06 | ▼ close $11,344.76 vs 09:30 $11,434.05 (session -75.99) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,363.06 | ▼ 09:30 equity $11,306.02 vs yday $11,344.76 (-38.74) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 149 | $13.04 | $2.48 | $-147.95 | $11,303.54 | ▼ -147.95 after sell → book $11,303.54; vs 09:30 mark -2.48 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,303.54 | ▲ close $11,303.54 vs 09:30 $11,306.02 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,303.54 | ▲ 09:30 equity $11,303.54 vs yday $11,303.54 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,303.54 | ▲ close $11,303.54 vs 09:30 $11,303.54 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,303.54 | ▲ 09:30 equity $11,303.54 vs yday $11,303.54 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 793 | $1.78 | $10.23 | — | $9,881.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1412.94; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 76 | $18.40 | $2.22 | — | $8,481.15 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1412.94; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 103 | $13.71 | $2.30 | — | $7,066.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1412.94; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $23.88 | $2.17 | — | $5,655.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1412.94; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $4,858.93 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 48 | $16.77 | $2.13 | — | $4,051.83 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 370 | $2.18 | $4.77 | — | $3,240.46 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 77 | $10.42 | $2.22 | — | $2,435.90 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 418 | $1.93 | $5.39 | — | $1,623.77 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 5 | $161.54 | $2.00 | — | $814.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 77 | $10.38 | $2.22 | — | $12.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $807.95; owner union_join_vol_green_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.97 | ▼ close $10,938.96 vs 09:30 $11,303.54 (session -326.92) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.97 | ▲ 09:30 equity $10,986.61 vs yday $10,938.96 (+47.65) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 76 | $18.15 | $2.24 | $-23.46 | $1,390.12 | ▼ -23.46 after sell → book $10,984.36; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 103 | $13.89 | $2.33 | $+13.91 | $2,818.47 | ▲ +13.91 after sell → book $10,982.04; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.84 | $2.19 | $-6.72 | $4,222.84 | ▼ -6.72 after sell → book $10,979.85; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 6 | $130.03 | $2.03 | $-18.56 | $5,000.99 | ▼ -18.56 after sell → book $10,977.82; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 48 | $15.61 | $2.15 | $-59.97 | $5,748.12 | ▼ -59.97 after sell → book $10,975.67; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 370 | $2.16 | $4.84 | $-17.02 | $6,542.47 | ▼ -17.02 after sell → book $10,970.82; vs 09:30 mark -4.85 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 77 | $10.50 | $2.24 | $+1.70 | $7,348.73 | ▲ +1.70 after sell → book $10,968.58; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 418 | $1.90 | $5.47 | $-23.40 | $8,137.46 | ▼ -23.40 after sell → book $10,963.11; vs 09:30 mark -5.47 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 5 | $157.46 | $2.02 | $-24.43 | $8,922.73 | ▼ -24.43 after sell → book $10,961.08; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 77 | $11.23 | $2.24 | $+61.37 | $9,785.20 | ▲ +61.37 after sell → book $10,958.84; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 64 | $25.18 | $2.18 | — | $8,171.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1630.87; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 207 | $7.87 | $2.67 | — | $6,539.74 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1630.87; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 281 | $5.79 | $3.62 | — | $4,909.12 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1630.87; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $4,393.35 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $818.19; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 9 | $82.70 | $2.02 | — | $3,647.03 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $818.19; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 325 | $2.51 | $4.19 | — | $2,827.09 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $818.19; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 2 | $378.34 | $2.00 | — | $2,068.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $818.19; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 21 | $37.44 | $2.05 | — | $1,280.12 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $818.19; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 129 | $6.32 | $2.38 | — | $462.46 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $818.19; owner union_join_vol_green_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $462.46 | ▲ close $11,413.28 vs 09:30 $10,986.61 (session +477.55) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $462.46 | ▼ 09:30 equity $11,177.45 vs yday $11,413.28 (-235.83) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 793 | $1.56 | $10.37 | $-191.10 | $1,693.14 | ▼ -191.10 after sell → book $11,167.08; vs 09:30 mark -10.37 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 64 | $26.44 | $2.21 | $+76.25 | $3,383.09 | ▲ +76.25 after sell → book $11,164.87; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 207 | $7.76 | $2.72 | $-28.16 | $4,986.69 | ▼ -28.16 after sell → book $11,162.15; vs 09:30 mark -2.72 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 281 | $5.81 | $3.68 | $-1.69 | $6,615.62 | ▼ -1.69 after sell → book $11,158.47; vs 09:30 mark -3.68 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $7,134.76 | ▲ +3.36 after sell → book $11,156.46; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 9 | $89.67 | $2.04 | $+58.68 | $7,939.75 | ▲ +58.68 after sell → book $11,154.42; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 325 | $2.66 | $4.26 | $+40.30 | $8,799.99 | ▲ +40.30 after sell → book $11,150.16; vs 09:30 mark -4.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 2 | $360.75 | $2.02 | $-39.19 | $9,519.48 | ▼ -39.19 after sell → book $11,148.15; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 21 | $37.75 | $2.07 | $+2.38 | $10,310.15 | ▲ +2.38 after sell → book $11,146.07; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 129 | $6.48 | $2.41 | $+15.85 | $11,143.66 | ▲ +15.85 after sell → book $11,143.66; vs 09:30 mark -2.41 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,143.66 | ▲ close $11,143.66 vs 09:30 $11,177.45 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,143.66 | ▲ 09:30 equity $11,143.66 vs yday $11,143.66 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,143.66 | ▲ close $11,143.66 vs 09:30 $11,143.66 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,143.66 | ▲ 09:30 equity $11,143.66 vs yday $11,143.66 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,143.66 | ▲ close $11,143.66 vs 09:30 $11,143.66 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,143.66 | ▲ 09:30 equity $11,143.66 vs yday $11,143.66 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 515 | $2.70 | $6.64 | — | $9,746.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1392.96; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 283 | $4.91 | $3.65 | — | $8,353.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1392.96; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 226 | $6.16 | $2.92 | — | $6,958.26 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1392.96; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 445 | $3.13 | $5.74 | — | $5,559.67 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1392.96; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 47 | $23.63 | $2.13 | — | $4,446.93 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $1111.93; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 101 | $10.95 | $2.29 | — | $3,338.69 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $1111.93; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $2,241.15 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $1111.93; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 20 | $54.91 | $2.05 | — | $1,140.90 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $1111.93; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 20 | $54.66 | $2.05 | — | $45.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $1111.93; owner union_join_vol_green_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.65 | ▲ close $11,173.11 vs 09:30 $11,143.66 (session +58.95) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.65 | ▲ 09:30 equity $11,272.68 vs yday $11,173.11 (+99.57) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 226 | $6.02 | $2.96 | $-37.52 | $1,403.21 | ▼ -37.52 after sell → book $11,269.72; vs 09:30 mark -2.96 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 47 | $23.20 | $2.15 | $-24.49 | $2,491.46 | ▼ -24.49 after sell → book $11,267.57; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 101 | $10.29 | $2.32 | $-71.27 | $3,528.43 | ▼ -71.27 after sell → book $11,265.25; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 13 | $86.06 | $2.05 | $+19.19 | $4,645.16 | ▲ +19.19 after sell → book $11,263.20; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 20 | $54.75 | $2.07 | $-7.32 | $5,738.09 | ▼ -7.32 after sell → book $11,261.13; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 20 | $54.78 | $2.07 | $-1.72 | $6,831.62 | ▼ -1.72 after sell → book $11,259.06; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,831.62 | ▲ close $11,559.93 vs 09:30 $11,272.68 (session +300.87) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,831.62 | ▲ 09:30 equity $11,648.55 vs yday $11,559.93 (+88.62) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 283 | $5.11 | $3.71 | $+49.24 | $8,274.04 | ▲ +49.24 after sell → book $11,644.84; vs 09:30 mark -3.71 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 445 | $3.64 | $5.83 | $+215.38 | $9,888.01 | ▲ +215.38 after sell → book $11,639.01; vs 09:30 mark -5.83 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,888.01 | ▲ close $11,762.61 vs 09:30 $11,648.55 (session +123.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,888.01 | ▲ 09:30 equity $11,772.91 vs yday $11,762.61 (+10.30) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 915 | $1.80 | $11.80 | — | $8,229.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1648.00; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 70 | $23.29 | $2.20 | — | $6,596.71 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1648.00; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 112 | $14.62 | $2.33 | — | $4,956.94 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1648.00; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 9 | $77.12 | $2.02 | — | $4,260.84 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 120 | $5.87 | $2.35 | — | $3,554.09 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 8 | $87.40 | $2.01 | — | $2,852.88 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 26 | $27.09 | $2.07 | — | $2,146.47 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 7 | $89.38 | $2.01 | — | $1,518.80 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 21 | $33.14 | $2.05 | — | $820.81 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 25 | $28.16 | $2.06 | — | $114.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $708.13; owner union_join_vol_green_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.74 | ▼ close $11,699.94 vs 09:30 $11,772.91 (session -42.06) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.74 | ▲ 09:30 equity $11,815.58 vs yday $11,699.94 (+115.64) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 70 | $24.09 | $2.23 | $+51.57 | $1,798.82 | ▲ +51.57 after sell → book $11,813.35; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 112 | $13.77 | $2.36 | $-99.88 | $3,338.70 | ▼ -99.88 after sell → book $11,811.00; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 9 | $76.44 | $2.04 | $-10.17 | $4,024.62 | ▼ -10.17 after sell → book $11,808.96; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 120 | $5.58 | $2.38 | $-39.53 | $4,691.84 | ▼ -39.53 after sell → book $11,806.58; vs 09:30 mark -2.38 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 8 | $83.20 | $2.03 | $-37.65 | $5,355.41 | ▼ -37.65 after sell → book $11,804.55; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 26 | $28.23 | $2.09 | $+25.48 | $6,087.30 | ▲ +25.48 after sell → book $11,802.46; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 7 | $86.76 | $2.03 | $-22.38 | $6,692.59 | ▼ -22.38 after sell → book $11,800.43; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 25 | $28.59 | $2.08 | $+6.72 | $7,405.38 | ▲ +6.72 after sell → book $11,798.34; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 164 | $22.46 | $2.48 | — | $3,719.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $3702.69; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 2 | $233.85 | $2.00 | — | $3,249.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 3 | $147.61 | $2.00 | — | $2,804.93 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 70 | $7.59 | $2.20 | — | $2,271.43 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 20 | $25.95 | $2.05 | — | $1,750.38 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $1,235.84 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 29 | $18.04 | $2.08 | — | $710.74 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 8 | $61.90 | $2.01 | — | $213.53 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $531.35; owner union_join_vol_green_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.53 | ▲ close $11,940.44 vs 09:30 $11,815.58 (session +158.92) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.53 | ▼ 09:30 equity $11,901.69 vs yday $11,940.44 (-38.75) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 915 | $1.96 | $11.97 | $+122.63 | $1,994.96 | ▲ +122.63 after sell → book $11,889.72; vs 09:30 mark -11.97 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 21 | $39.50 | $2.07 | $+129.43 | $2,822.39 | ▲ +129.43 after sell → book $11,887.65; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 164 | $21.30 | $2.54 | $-195.26 | $6,313.05 | ▼ -195.26 after sell → book $11,885.11; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 2 | $249.13 | $2.02 | $+26.55 | $6,809.29 | ▲ +26.55 after sell → book $11,883.09; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 3 | $146.50 | $2.02 | $-7.35 | $7,246.77 | ▼ -7.35 after sell → book $11,881.07; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 70 | $7.98 | $2.22 | $+22.88 | $7,803.15 | ▲ +22.88 after sell → book $11,878.85; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 20 | $26.14 | $2.07 | $-0.32 | $8,323.88 | ▼ -0.32 after sell → book $11,876.78; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 3 | $182.33 | $2.02 | $+30.42 | $8,868.85 | ▲ +30.42 after sell → book $11,874.76; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 29 | $17.80 | $2.10 | $-10.99 | $9,382.96 | ▼ -10.99 after sell → book $11,872.67; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 8 | $63.37 | $2.03 | $+7.71 | $9,887.88 | ▲ +7.71 after sell → book $11,870.63; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 56 | $29.32 | $2.16 | — | $8,243.81 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1647.98; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 542 | $3.04 | $6.99 | — | $6,591.84 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1647.98; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 20 | $81.40 | $2.05 | — | $4,961.79 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1647.98; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $4,300.93 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $826.97; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 9 | $85.00 | $2.02 | — | $3,533.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $826.97; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 209 | $3.95 | $2.70 | — | $2,705.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $826.97; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 58 | $14.07 | $2.16 | — | $1,887.45 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $826.97; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 55 | $14.79 | $2.15 | — | $1,071.84 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $826.97; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 72 | $11.38 | $2.21 | — | $250.28 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $826.97; owner union_join_vol_green_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $250.28 | ▲ close $11,900.30 vs 09:30 $11,901.69 (session +54.10) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.28 | ▲ 09:30 equity $12,196.96 vs yday $11,900.30 (+296.66) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 515 | $3.55 | $6.74 | $+424.36 | $2,071.78 | ▲ +424.36 after sell → book $12,190.21; vs 09:30 mark -6.75 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 56 | $29.43 | $2.18 | $+1.82 | $3,717.68 | ▲ +1.82 after sell → book $12,188.03; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 542 | $4.00 | $7.10 | $+508.94 | $5,878.58 | ▲ +508.94 after sell → book $12,180.93; vs 09:30 mark -7.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 20 | $79.08 | $2.07 | $-50.52 | $7,458.11 | ▼ -50.52 after sell → book $12,178.86; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 3 | $230.25 | $2.02 | $+27.87 | $8,146.84 | ▲ +27.87 after sell → book $12,176.84; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 9 | $82.83 | $2.04 | $-23.58 | $8,890.27 | ▼ -23.58 after sell → book $12,174.80; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 209 | $3.87 | $2.74 | $-22.16 | $9,696.36 | ▼ -22.16 after sell → book $12,172.06; vs 09:30 mark -2.74 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 58 | $13.90 | $2.18 | $-14.21 | $10,500.38 | ▼ -14.21 after sell → book $12,169.88; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 55 | $14.58 | $2.17 | $-15.88 | $11,300.10 | ▼ -15.88 after sell → book $12,167.70; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 72 | $12.05 | $2.23 | $+43.81 | $12,165.48 | ▲ +43.81 after sell → book $12,165.48; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 615 | $2.47 | $7.93 | — | $10,638.49 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $1520.68; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 89 | $16.91 | $2.26 | — | $9,131.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1520.68; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 921 | $1.65 | $11.88 | — | $7,599.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $1520.68; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 130 | $11.67 | $2.38 | — | $6,080.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $1520.68; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 5 | $157.87 | $2.00 | — | $5,288.88 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 9 | $88.83 | $2.02 | — | $4,487.39 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 93 | $9.31 | $2.27 | — | $3,619.29 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 64 | $13.47 | $2.18 | — | $2,754.71 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 86 | $9.99 | $2.25 | — | $1,893.32 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 66 | $13.05 | $2.19 | — | $1,029.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 150 | $5.75 | $2.44 | — | $164.15 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $868.60; owner union_join_vol_green_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.15 | ▲ close $12,309.69 vs 09:30 $12,196.96 (session +184.01) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.15 | ▼ 09:30 equity $12,268.96 vs yday $12,309.69 (-40.73) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 86 | $9.91 | $2.27 | $-11.40 | $1,014.13 | ▼ -11.40 after sell → book $12,266.68; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 66 | $12.99 | $2.21 | $-8.36 | $1,869.26 | ▼ -8.36 after sell → book $12,264.47; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 150 | $6.05 | $2.47 | $+40.09 | $2,775.04 | ▲ +40.09 after sell → book $12,262.00; vs 09:30 mark -2.47 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 50 | $9.11 | $2.14 | — | $2,317.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $462.51; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 63 | $7.23 | $2.18 | — | $1,859.73 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $462.51; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 11 | $28.02 | $2.02 | — | $1,549.49 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $309.96; owner union_join_vol_green_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,549.49 | ▼ close $12,194.95 vs 09:30 $12,268.96 (session -60.71) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,549.49 | ▲ 09:30 equity $12,291.93 vs yday $12,194.95 (+96.98) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 89 | $16.92 | $2.28 | $-3.65 | $3,053.08 | ▼ -3.65 after sell → book $12,289.64; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 921 | $1.41 | $12.04 | $-244.97 | $4,339.65 | ▼ -244.97 after sell → book $12,277.60; vs 09:30 mark -12.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 130 | $12.80 | $2.41 | $+142.11 | $6,001.23 | ▲ +142.11 after sell → book $12,275.18; vs 09:30 mark -2.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 93 | $9.50 | $2.29 | $+13.11 | $6,882.44 | ▲ +13.11 after sell → book $12,272.89; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 64 | $12.84 | $2.20 | $-45.02 | $7,702.00 | ▼ -45.02 after sell → book $12,270.69; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 50 | $8.39 | $2.16 | $-40.30 | $8,119.34 | ▼ -40.30 after sell → book $12,268.53; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 63 | $6.83 | $2.20 | $-29.58 | $8,547.43 | ▼ -29.58 after sell → book $12,266.33; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 11 | $25.90 | $2.04 | $-27.39 | $8,830.28 | ▼ -27.39 after sell → book $12,264.28; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 545 | $2.70 | $7.03 | — | $7,351.75 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1471.71; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 35 | $41.76 | $2.10 | — | $5,888.06 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1471.71; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 327 | $4.49 | $4.22 | — | $4,415.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1471.71; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 42 | $20.65 | $2.12 | — | $3,546.19 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $883.12; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 56 | $15.72 | $2.16 | — | $2,663.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $883.12; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 34 | $25.40 | $2.09 | — | $1,798.02 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $883.12; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1149 | $0.77 | $12.27 | — | $903.32 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $883.12; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 89 | $9.90 | $2.26 | — | $19.96 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $883.12; owner union_join_vol_green_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.96 | ▼ close $11,797.70 vs 09:30 $12,291.93 (session -432.34) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.96 | ▼ 09:30 equity $11,726.76 vs yday $11,797.70 (-70.94) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 615 | $2.68 | $8.05 | $+113.17 | $1,660.12 | ▲ +113.17 after sell → book $11,718.71; vs 09:30 mark -8.05 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 5 | $163.95 | $2.02 | $+26.37 | $2,477.84 | ▲ +26.37 after sell → book $11,716.69; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 9 | $87.67 | $2.04 | $-14.45 | $3,264.88 | ▼ -14.45 after sell → book $11,714.65; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 35 | $36.02 | $2.12 | $-204.94 | $4,523.64 | ▼ -204.94 after sell → book $11,712.54; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 327 | $3.92 | $4.28 | $-193.26 | $5,802.83 | ▼ -193.26 after sell → book $11,708.25; vs 09:30 mark -4.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 42 | $20.52 | $2.14 | $-9.71 | $6,662.53 | ▼ -9.71 after sell → book $11,706.12; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 56 | $14.38 | $2.18 | $-79.38 | $7,465.64 | ▼ -79.38 after sell → book $11,703.94; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 34 | $23.99 | $2.11 | $-52.14 | $8,279.18 | ▼ -52.14 after sell → book $11,701.83; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1149 | $0.75 | $12.22 | $-49.77 | $9,124.12 | ▼ -49.77 after sell → book $11,689.61; vs 09:30 mark -12.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 89 | $9.12 | $2.28 | $-73.96 | $9,933.52 | ▼ -73.96 after sell → book $11,687.33; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,933.52 | ▲ close $12,849.27 vs 09:30 $11,726.76 (session +1,161.94) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,247.19 | ▲ 09:30 equity $11,163.27 vs yday $10,833.83 (+329.44) | 09:30 open · cash $7,247.19 (unchanged overnight, no fees) · equity $11,163.27 vs prior close $10,833.83 (+329.44) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 40 | $29.76 | $2.11 | — | $6,054.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $1207.87; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 74 | $16.21 | $2.21 | — | $4,852.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1207.87; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 77 | $15.58 | $2.22 | — | $3,650.96 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $1207.87; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 19 | $26.27 | $2.05 | — | $3,149.79 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 135 | $3.86 | $2.40 | — | $2,626.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 2 | $184.00 | $2.00 | — | $2,256.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 4 | $123.50 | $2.00 | — | $1,760.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 17 | $29.80 | $2.04 | — | $1,251.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 130 | $4.00 | $2.38 | — | $728.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 8 | $61.33 | $2.01 | — | $235.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $521.57; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.97 | ▼ close $10,944.27 vs 09:30 $11,163.27 (session -197.58) | 16:00 close · cash $235.97 · equity $10,944.27 vs 09:30 $11,163.27 (-219.00; session marks -197.58) · 12 name(s) marked open→close (per-name table). GLND×464 09:30 $6.06 → close $5.54 -241.28; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×40 09:30 $29.76 → close $26.24 -140.80; SECZ×74 09:30 $16.21 → close $15.96 -18.50; USDE×77 09:30 $15.58 → close $17.25 +128.51; WRBY×19 09:30 $26.27 → close $26.71 +8.36; ZSQR×135 09:30 $3.86 → close $3.78 -10.80; TWST×2 09:30 $184.00 → close $182.83 -2.34; GRAL×4 09:30 $123.50 → close $126.89 +13.56; QMCO×17 09:30 $29.80 → close $31.68 +31.96; CYPH×130 09:30 $4.00 → close $4.12 +14.95; CDNA×8 09:30 $61.33 → close $63.68 +18.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARM` | cash | leftover split 309.96 < 1 share @ 319.41 |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `META` | cash | leftover split 309.96 < 1 share @ 731.40 |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 545 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1471.71; owner union_hot_n4_h1 |
