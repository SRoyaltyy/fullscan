# Factor mine action — `combo_seh_502525_split`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · split short_news_r_h3/union_e_fresh_h3/union_hot_n4_h1 w=0.5,0.25,0.25 net=priority

Cash book **-1.10%** ($9,890) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 330 · skips 360 · realized $+1618.27.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell, each with their own slice of $10,000: short_news_r_h3 50%, union_e_fresh_h3 25%, union_hot_n4_h1 25%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. Each kid gets their own slice of the $10,000 and keeps it — two (or three) tiny books added together. They do not share leftover cash, so the same name can appear in two slices. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_e_fresh_h3 25%, union_hot_n4_h1 25%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: union_e_fresh_h3 (25% · long · hold 3).
- Member: union_hot_n4_h1 (25% · long · hold 1).
- Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day.

### When it buys

- At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- One ticker, one side. Claim order: fresh-E, then heat, then the other longs, then shorts. A name already held cannot be opened on the other side.
- Split pile: each member is a normal leftover book at its weight × $10k. Unused cash in one slice stays in that slice.
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12,729.19.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,233.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $49.01 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $1,900.23 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $1,291.58 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $682.08 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 55 | $22.01 | $2.15 | — | $20.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,000.00 | ▲ close $5,000.00 vs 09:30 $5,000.00 (session +0.00) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.34 | ▲ close $2,689.99 vs 09:30 $2,500.00 (session +209.27) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.01 | ▲ close $2,581.61 vs 09:30 $2,500.00 (session +96.29) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,000.00 | ▲ 09:30 equity $5,000.00 vs yday $5,000.00 (+0.00) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,500.00 | ▲ 09:30 equity $2,500.00 vs yday $2,500.00 (+0.00) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,500.00 | ▲ 09:30 equity $2,500.00 vs yday $2,500.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 58 | $11.12 | $2.16 | — | $15.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $645.70 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 32 | $19.57 | $2.09 | — | $1,310.73 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $645.70 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $18.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2.54 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 2 | $1.18 | $0.03 | — | $16.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $2.54 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 26 | $24.68 | $2.07 | — | $1,939.06 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $645.70 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 293 | $2.20 | $3.78 | — | $662.35 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $645.70 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,468.92 | ▲ close $5,002.93 vs 09:30 $5,000.00 (session +16.57) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.43 | ▲ close $2,968.94 vs 09:30 $2,738.48 (session +230.51) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.23 | ▼ close $2,502.73 vs 09:30 $2,598.61 (session -69.98) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,000.00 | ▲ 09:30 equity $5,000.00 vs yday $5,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.34 | ▲ 09:30 equity $2,738.48 vs yday $2,689.99 (+48.49) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.01 | ▲ 09:30 equity $2,598.61 vs yday $2,581.61 (+17.00) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 771 | $0.93 | $9.62 | $+74.34 | $2,582.81 | ▲ +74.34 after sell → book $2,582.81; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 13 | $44.09 | $2.05 | $-28.65 | $620.13 | ▼ -28.65 after sell → book $2,596.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 26 | $22.92 | $2.09 | $-14.82 | $1,213.96 | ▼ -14.82 after sell → book $2,594.47; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 12 | $55.29 | $2.05 | $+51.93 | $1,875.40 | ▲ +51.93 after sell → book $2,592.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 706 | $1.18 | $9.26 | — | $5,823.82 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $833.33 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 43 | $19.17 | $2.16 | — | $6,645.97 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $833.33 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 65 | $12.70 | $2.23 | — | $7,468.92 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $833.33 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 88 | $6.87 | $2.25 | — | $1,226.01 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $610.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 14 | $41.23 | $2.03 | — | $44.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $610.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 44 | $13.64 | $2.12 | — | $623.73 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $610.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 145 | $4.19 | $2.42 | — | $1,832.82 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $610.70 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,902.06 | ▲ close $5,005.72 vs 09:30 $4,956.11 (session +64.55) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.43 | ▲ close $3,060.29 vs 09:30 $2,931.43 (session +128.86) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.47 | ▲ close $2,439.82 vs 09:30 $2,453.01 (session +5.86) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,468.92 | ▼ 09:30 equity $4,956.11 vs yday $5,002.93 (-46.82) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.43 | ▼ 09:30 equity $2,931.43 vs yday $2,968.94 (-37.51) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.23 | ▼ 09:30 equity $2,453.01 vs yday $2,502.73 (-49.72) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 58 | $9.57 | $2.18 | $-94.25 | $2,442.80 | ▼ -94.25 after sell → book $2,442.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 32 | $19.57 | $2.11 | $-4.19 | $1,282.85 | ▼ -4.19 after sell → book $2,448.82; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 26 | $24.83 | $2.09 | $-0.26 | $658.72 | ▼ -0.26 after sell → book $2,450.93; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 293 | $2.08 | $3.84 | $-41.31 | $1,889.92 | ▼ -41.31 after sell → book $2,444.98; vs 09:30 mark -3.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 15 | $31.70 | $2.07 | — | $8,923.59 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 164 | $3.01 | $2.53 | — | $9,414.70 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 72 | $6.80 | $2.24 | — | $9,902.06 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 430 | $1.15 | $5.64 | — | $7,957.78 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $495.61 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 139 | $3.56 | $2.45 | — | $8,450.16 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $495.61 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,902.06 | ▲ close $5,199.48 vs 09:30 $5,081.15 (session +118.33) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,008.20 | ▼ close $3,011.79 vs 09:30 $3,034.35 (session -0.21) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,775.76 | ▼ close $2,398.80 vs 09:30 $2,442.41 (session -36.96) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,902.06 | ▲ 09:30 equity $5,081.15 vs yday $5,005.72 (+75.43) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.43 | ▼ 09:30 equity $3,034.35 vs yday $3,060.29 (-25.94) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.47 | ▲ 09:30 equity $2,442.41 vs yday $2,439.82 (+2.59) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 14 | $41.50 | $2.05 | $-0.30 | $1,775.76 | ▼ -0.30 after sell → book $2,435.76; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $1,755.28 | ▲ +471.89 after sell → book $3,014.18; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 44 | $13.31 | $2.14 | $-18.78 | $1,196.81 | ▼ -18.78 after sell → book $2,437.81; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 55 | $22.82 | $2.17 | $+40.22 | $3,008.20 | ▲ +40.22 after sell → book $3,012.00; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 145 | $3.94 | $2.46 | $-41.13 | $613.32 | ▼ -41.13 after sell → book $2,439.96; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,371.60 | ▲ close $5,196.35 vs 09:30 $5,183.76 (session +23.88) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,011.68 | ▲ close $3,011.68 vs 09:30 $3,011.76 (session +0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,406.20 | ▲ close $2,406.20 vs 09:30 $2,408.48 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 706 | $1.07 | $9.11 | $+59.30 | $9,137.53 | ▲ +59.30 after sell → book $5,174.65; vs 09:30 mark -9.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 65 | $11.75 | $2.19 | $+57.01 | $8,371.60 | ▲ +57.01 after sell → book $5,172.47; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,902.06 | ▼ 09:30 equity $5,183.76 vs yday $5,199.48 (-15.72) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,008.20 | ▼ 09:30 equity $3,011.76 vs yday $3,011.79 (-0.03) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,775.76 | ▲ 09:30 equity $2,408.48 vs yday $2,398.80 (+9.68) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 1 | $1.42 | $0.04 | $-0.14 | $3,009.58 | ▼ -0.14 after sell → book $3,011.72; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 88 | $7.19 | $2.28 | $+23.63 | $2,406.20 | ▲ +23.63 after sell → book $2,406.20; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 2 | $1.07 | $0.05 | $-0.30 | $3,011.68 | ▼ -0.30 after sell → book $3,011.68; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 8 | $46.85 | $2.01 | — | $1,582.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 50 | $11.81 | $2.14 | — | $602.55 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $601.55 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 41 | $9.01 | $2.11 | — | $1,210.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 96 | $3.89 | $2.28 | — | $835.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 11 | $34.05 | $2.02 | — | $458.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 16 | $22.44 | $2.04 | — | $97.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 435 | $1.37 | $5.61 | — | $0.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $601.55 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 523 | $1.15 | $6.75 | — | $1,195.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $601.55 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1254 | $0.30 | $7.52 | — | $1,959.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 3 | $97.43 | $2.00 | — | $2,717.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $376.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 4 | $150.14 | $2.00 | — | $1,803.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $601.55 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 84 | $4.43 | $2.24 | — | $2,343.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $376.46 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,390.15 | ▲ close $5,172.32 vs 09:30 $5,167.66 (session +37.81) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.59 | ▲ close $3,015.70 vs 09:30 $3,011.68 (session +26.26) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.99 | ▼ close $2,361.54 vs 09:30 $2,406.20 (session -28.16) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 15 | $31.87 | $2.04 | $-6.65 | $6,129.79 | ▼ -6.65 after sell → book $5,155.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 164 | $2.95 | $2.48 | $+4.83 | $5,643.51 | ▲ +4.83 after sell → book $5,153.19; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 43 | $18.13 | $2.12 | $+40.44 | $7,589.89 | ▲ +40.44 after sell → book $5,165.54; vs 09:30 mark -2.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 72 | $6.81 | $2.21 | $-5.17 | $5,150.98 | ▼ -5.17 after sell → book $5,150.98; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 430 | $0.96 | $5.43 | $+69.34 | $7,170.37 | ▲ +69.34 after sell → book $5,160.11; vs 09:30 mark -5.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 139 | $4.01 | $2.41 | $-68.11 | $6,609.88 | ▼ -68.11 after sell → book $5,157.71; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,371.60 | ▼ 09:30 equity $5,167.66 vs yday $5,196.35 (-28.69) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,011.68 | ▲ 09:30 equity $3,011.68 vs yday $3,011.68 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,406.20 | ▲ 09:30 equity $2,406.20 vs yday $2,406.20 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 6 | $46.85 | $2.04 | — | $6,757.18 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 27 | $11.81 | $2.10 | — | $6,305.98 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $5,353.41 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 69 | $4.61 | $2.23 | — | $7,390.15 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 1 | $173.90 | $1.77 | — | $6,478.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 72 | $4.43 | $2.24 | — | $5,989.07 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 3 | $106.38 | $2.03 | — | $7,074.29 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $321.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 15 | $21.40 | $2.06 | — | $5,672.35 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $321.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 88 | $6.81 | $2.25 | — | $1.15 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $603.37 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 6 | $2.30 | $0.16 | — | $83.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $13.94 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 134 | $4.49 | $2.39 | — | $602.69 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $603.37 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,804.64 | ▼ close $5,128.34 vs 09:30 $5,148.39 (session -7.34) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.64 | ▲ close $3,081.88 vs 09:30 $3,026.95 (session +55.08) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.15 | ▲ close $2,468.79 vs 09:30 $2,437.39 (session +43.90) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,390.15 | ▼ 09:30 equity $5,148.39 vs yday $5,172.32 (-23.93) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.59 | ▲ 09:30 equity $3,026.95 vs yday $3,015.70 (+11.25) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.99 | ▲ 09:30 equity $2,437.39 vs yday $2,361.54 (+75.85) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 50 | $11.57 | $2.16 | $-16.55 | $577.33 | ▼ -16.55 after sell → book $2,435.23; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 435 | $1.46 | $5.69 | $+27.84 | $1,206.74 | ▲ +27.84 after sell → book $2,429.54; vs 09:30 mark -5.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 20 | $20.90 | $2.08 | — | $9,401.71 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 4 | $89.10 | $2.03 | — | $8,565.44 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 3 | $133.11 | $2.03 | — | $8,211.08 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 15 | $27.00 | $2.07 | — | $9,804.64 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 137 | $3.11 | $2.45 | — | $7,813.78 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $429.03 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 11 | $38.40 | $2.05 | — | $8,985.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $429.03 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,804.64 | ▲ close $5,196.18 vs 09:30 $5,159.21 (session +36.97) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.64 | ▲ close $3,128.29 vs 09:30 $3,089.99 (session +38.30) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,800.99 | ▲ close $2,800.99 vs 09:30 $2,814.56 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,804.64 | ▲ 09:30 equity $5,159.21 vs yday $5,128.34 (+30.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.64 | ▲ 09:30 equity $3,089.99 vs yday $3,081.88 (+8.11) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.15 | ▲ 09:30 equity $2,814.56 vs yday $2,468.79 (+345.77) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 88 | $8.03 | $2.28 | $+102.83 | $2,800.99 | ▲ +102.83 after sell → book $2,800.99; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 523 | $1.83 | $6.84 | $+342.05 | $1,520.18 | ▲ +342.05 after sell → book $2,805.70; vs 09:30 mark -6.84 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 4 | $142.70 | $2.02 | $-33.78 | $569.93 | ▼ -33.78 after sell → book $2,812.54; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 134 | $4.32 | $2.42 | $-27.60 | $2,096.63 | ▼ -27.60 after sell → book $2,803.27; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 36 | $19.04 | $2.10 | — | $4.98 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $700.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 2 | $175.01 | $2.00 | — | $2,742.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 4 | $88.94 | $2.00 | — | $2,385.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 25 | $15.28 | $2.06 | — | $2,001.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 448 | $1.56 | $5.78 | — | $1,395.07 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $700.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 2 | $142.36 | $2.00 | — | $1,714.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 75 | $5.10 | $2.21 | — | $1,329.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 8 | $47.89 | $2.01 | — | $944.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 27 | $13.92 | $2.07 | — | $566.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 29 | $24.11 | $2.08 | — | $2,099.73 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $700.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 85 | $4.54 | $2.25 | — | $178.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; leftover $386.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 172 | $4.07 | $2.51 | — | $692.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $700.25 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,690.47 | ▼ close $5,120.19 vs 09:30 $5,213.24 (session -68.54) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.08 | ▼ close $2,990.36 vs 09:30 $3,131.61 (session -101.92) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.98 | ▲ close $3,025.65 vs 09:30 $2,800.99 (session +237.12) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 6 | $43.63 | $2.01 | $+15.28 | $8,219.46 | ▲ +15.28 after sell → book $5,201.22; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 27 | $11.00 | $2.07 | $+17.83 | $8,655.60 | ▲ +17.83 after sell → book $5,204.94; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $9,590.65 | ▼ -11.56 after sell → book $5,211.25; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 69 | $4.77 | $2.20 | $-15.46 | $7,569.39 | ▼ -15.46 after sell → book $5,197.02; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 1 | $170.64 | $1.71 | $-0.22 | $8,483.25 | ▼ -0.22 after sell → book $5,203.23; vs 09:30 mark -1.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 72 | $4.42 | $2.21 | $-3.72 | $8,954.67 | ▼ -3.72 after sell → book $5,207.01; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 3 | $105.58 | $2.00 | $-1.63 | $7,900.72 | ▼ -1.63 after sell → book $5,199.22; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 15 | $20.90 | $2.04 | $+3.40 | $9,275.11 | ▲ +3.40 after sell → book $5,209.21; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,804.64 | ▲ 09:30 equity $5,213.24 vs yday $5,196.18 (+17.06) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.64 | ▲ 09:30 equity $3,131.61 vs yday $3,128.29 (+3.32) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,800.99 | ▲ 09:30 equity $2,800.99 vs yday $2,800.99 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 8 | $43.63 | $2.03 | $-29.81 | $1,490.51 | ▼ -29.81 after sell → book $3,117.42; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 41 | $9.23 | $2.13 | $+4.77 | $1,866.81 | ▲ +4.77 after sell → book $3,115.29; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 96 | $5.24 | $2.30 | $+125.02 | $2,367.55 | ▲ +125.02 after sell → book $3,112.99; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 11 | $34.72 | $2.04 | $+3.30 | $2,747.42 | ▲ +3.30 after sell → book $3,110.94; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 16 | $21.85 | $2.06 | $-13.54 | $3,094.96 | ▼ -13.54 after sell → book $3,108.88; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1254 | $0.31 | $7.87 | $-2.85 | $1,143.51 | ▼ -2.85 after sell → book $3,119.46; vs 09:30 mark -7.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 3 | $104.00 | $2.02 | $+15.69 | $393.62 | ▲ +15.69 after sell → book $3,129.59; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 84 | $4.42 | $2.27 | $-5.35 | $762.63 | ▼ -5.35 after sell → book $3,127.32; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 11 | $54.51 | $2.06 | — | $8,805.15 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $649.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 47 | $13.62 | $2.17 | — | $8,207.60 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $649.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 3 | $175.01 | $2.03 | — | $9,328.15 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $649.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 1 | $364.35 | $2.02 | — | $9,690.47 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $649.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 1 | $18.26 | $0.19 | — | $110.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; leftover $32.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 52 | $14.11 | $2.15 | — | $1,493.45 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $743.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 64 | $11.59 | $2.18 | — | $9.59 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $743.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 54 | $0.58 | $0.48 | — | $160.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; leftover $32.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 6 | $5.21 | $0.33 | — | $128.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $32.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 127 | $5.81 | $2.37 | — | $753.21 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $743.11 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,498.69 | ▼ close $5,104.98 vs 09:30 $5,199.54 (session -71.48) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.00 | ▲ close $3,075.13 vs 09:30 $2,981.99 (session +94.32) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.59 | ▲ close $2,948.21 vs 09:30 $2,894.71 (session +70.27) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 20 | $20.50 | $2.05 | $+3.87 | $7,644.22 | ▲ +3.87 after sell → book $5,189.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 4 | $88.24 | $2.00 | $-0.59 | $8,480.80 | ▼ -0.59 after sell → book $5,193.14; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 3 | $154.20 | $2.00 | $-67.30 | $8,835.76 | ▼ -67.30 after sell → book $5,195.14; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 15 | $26.00 | $2.04 | $+10.90 | $7,252.18 | ▲ +10.90 after sell → book $5,187.03; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 137 | $2.83 | $2.40 | $+33.51 | $9,300.36 | ▲ +33.51 after sell → book $5,197.14; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 11 | $38.41 | $2.02 | $-4.19 | $8,056.27 | ▼ -4.19 after sell → book $5,191.12; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,690.47 | ▲ 09:30 equity $5,199.54 vs yday $5,120.19 (+79.35) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.08 | ▼ 09:30 equity $2,981.99 vs yday $2,990.36 (-8.37) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.98 | ▼ 09:30 equity $2,894.71 vs yday $3,025.65 (-130.94) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 36 | $20.72 | $2.12 | $+56.26 | $2,229.32 | ▲ +56.26 after sell → book $2,884.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 448 | $1.60 | $5.86 | $+6.28 | $1,485.51 | ▲ +6.28 after sell → book $2,886.75; vs 09:30 mark -5.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 6 | $2.35 | $0.18 | $-0.03 | $192.00 | ▼ -0.03 after sell → book $2,981.81; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 29 | $26.61 | $2.10 | $+68.33 | $774.58 | ▲ +68.33 after sell → book $2,892.62; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 42 | $12.22 | $2.15 | — | $8,189.13 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 102 | $5.08 | $2.34 | — | $8,704.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 2 | $213.94 | $2.03 | — | $7,678.04 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 2 | $199.94 | $2.03 | — | $9,498.69 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; leftover $518.70 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 3 | $132.64 | $2.03 | — | $9,100.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $518.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 42 | $18.50 | $2.12 | — | $60.82 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $782.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 85 | $9.19 | $2.25 | — | $1,562.84 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $782.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 1 | $13.41 | $0.14 | — | $96.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; leftover $13.75 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 5 | $144.18 | $2.00 | — | $839.94 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $782.08 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,902.63 | ▼ close $5,007.53 vs 09:30 $5,037.87 (session -26.14) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.45 | ▼ close $3,063.44 vs 09:30 $3,079.91 (session -16.33) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.82 | ▲ close $3,039.37 vs 09:30 $3,051.33 (session +1.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,498.69 | ▼ 09:30 equity $5,037.87 vs yday $5,104.98 (-67.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.00 | ▲ 09:30 equity $3,079.91 vs yday $3,075.13 (+4.78) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.59 | ▲ 09:30 equity $3,051.33 vs yday $2,948.21 (+103.12) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 52 | $14.20 | $2.17 | $+0.37 | $745.82 | ▲ +0.37 after sell → book $3,049.16; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 64 | $12.18 | $2.20 | $+33.70 | $2,346.24 | ▲ +33.70 after sell → book $3,044.56; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 127 | $6.50 | $2.40 | $+82.86 | $1,568.92 | ▲ +82.86 after sell → book $3,046.76; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 16 | $74.54 | $2.09 | — | $10,689.24 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1259.47 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 22 | $55.25 | $2.11 | — | $11,902.63 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1259.47 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $2,695.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 4 | $146.07 | $2.00 | — | $150.69 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $726.56 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 24 | $15.01 | $2.06 | — | $2,333.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 51 | $14.00 | $2.14 | — | $736.98 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $726.56 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 3 | $103.89 | $2.00 | — | $2,019.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 95 | $3.88 | $2.27 | — | $1,648.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 8 | $44.40 | $2.01 | — | $1,291.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 14 | $24.69 | $2.03 | — | $943.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 44 | $8.35 | $2.12 | — | $574.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; leftover $369.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 9 | $37.65 | $2.02 | — | $233.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $369.84 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,039.82 | ▲ close $5,172.02 vs 09:30 $5,000.83 (session +183.55) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.48 | ▼ close $2,928.83 vs 09:30 $3,066.88 (session -104.76) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.69 | ▼ close $2,956.33 vs 09:30 $2,970.80 (session -5.64) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 11 | $52.49 | $2.02 | $+18.14 | $10,667.79 | ▲ +18.14 after sell → book $4,996.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 47 | $13.90 | $2.13 | $-17.22 | $11,247.20 | ▼ -17.22 after sell → book $4,998.70; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 3 | $172.76 | $2.00 | $+2.72 | $10,147.51 | ▲ +2.72 after sell → book $4,994.68; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 1 | $347.82 | $1.99 | $+12.51 | $9,797.70 | ▲ +12.51 after sell → book $4,992.69; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,902.63 | ▼ 09:30 equity $5,000.83 vs yday $5,007.53 (-6.70) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.45 | ▲ 09:30 equity $3,066.88 vs yday $3,063.44 (+3.44) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.82 | ▼ 09:30 equity $2,970.80 vs yday $3,039.37 (-68.57) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 2 | $172.76 | $2.02 | $-8.51 | $439.96 | ▼ -8.51 after sell → book $3,064.86; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 4 | $93.30 | $2.02 | $+13.42 | $811.14 | ▲ +13.42 after sell → book $3,062.84; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 25 | $18.15 | $2.08 | $+67.60 | $1,262.80 | ▲ +67.60 after sell → book $3,060.76; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 42 | $18.15 | $2.14 | $-18.95 | $1,453.12 | ▼ -18.95 after sell → book $2,966.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 2 | $132.80 | $2.02 | $-23.13 | $1,526.39 | ▼ -23.13 after sell → book $3,058.74; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 75 | $4.58 | $2.24 | $-43.45 | $1,867.65 | ▼ -43.45 after sell → book $3,056.50; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 8 | $48.42 | $2.03 | $+0.19 | $2,252.97 | ▲ +0.19 after sell → book $3,054.47; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 27 | $15.66 | $2.09 | $+42.82 | $2,673.70 | ▲ +42.82 after sell → book $3,052.38; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 85 | $3.38 | $2.27 | $-103.54 | $2,958.73 | ▼ -103.54 after sell → book $3,050.11; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 172 | $3.69 | $2.54 | $-70.41 | $692.96 | ▼ -70.41 after sell → book $2,968.26; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 41 | $30.18 | $2.17 | — | $12,039.82 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $1248.17 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 4 | $252.24 | $2.05 | — | $10,804.61 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1248.17 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,746.88 | ▲ close $5,232.79 vs 09:30 $5,218.88 (session +24.31) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $309.26 | ▲ close $2,952.76 vs 09:30 $2,932.38 (session +21.38) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,214.50 | ▼ close $2,892.80 vs 09:30 $2,925.12 (session -26.01) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 42 | $11.10 | $2.12 | $+42.77 | $11,151.75 | ▲ +42.77 after sell → book $5,214.77; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 102 | $4.97 | $2.30 | $+6.08 | $10,642.00 | ▲ +6.08 after sell → book $5,212.47; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 2 | $208.88 | $2.00 | $+6.10 | $11,620.06 | ▲ +6.10 after sell → book $5,216.88; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 2 | $254.39 | $2.00 | $-112.92 | $9,746.88 | ▼ -112.92 after sell → book $5,208.48; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 3 | $127.45 | $2.00 | $+11.54 | $10,257.65 | ▲ +11.54 after sell → book $5,210.47; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,039.82 | ▲ 09:30 equity $5,218.88 vs yday $5,172.02 (+46.86) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.48 | ▲ 09:30 equity $2,932.38 vs yday $2,928.83 (+3.55) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.69 | ▼ 09:30 equity $2,925.12 vs yday $2,956.33 (-31.21) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 4 | $148.03 | $2.02 | $+3.82 | $2,214.50 | ▲ +3.82 after sell → book $2,918.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 1 | $19.25 | $0.22 | $+0.59 | $309.26 | ▲ +0.59 after sell → book $2,931.37; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 85 | $9.50 | $2.27 | $+21.84 | $955.93 | ▲ +21.84 after sell → book $2,922.86; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 5 | $134.10 | $2.02 | $-54.43 | $1,624.40 | ▼ -54.43 after sell → book $2,920.83; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 54 | $0.51 | $0.46 | $-4.88 | $260.57 | ▼ -4.88 after sell → book $2,931.93; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 6 | $5.00 | $0.34 | $-1.93 | $290.23 | ▼ -1.93 after sell → book $2,931.59; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,366.54 | ▲ close $5,301.94 vs 09:30 $5,300.82 (session +5.22) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.30 | ▼ close $2,896.10 vs 09:30 $2,923.16 (session -26.92) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,877.38 | ▲ close $2,877.38 vs 09:30 $2,879.54 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 16 | $73.22 | $2.04 | $+16.99 | $8,573.32 | ▲ +16.99 after sell → book $5,298.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 22 | $54.76 | $2.06 | $+6.61 | $7,366.54 | ▲ +6.61 after sell → book $5,296.72; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,746.88 | ▲ 09:30 equity $5,300.82 vs yday $5,232.79 (+68.03) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $309.26 | ▼ 09:30 equity $2,923.16 vs yday $2,952.76 (-29.60) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,214.50 | ▼ 09:30 equity $2,879.54 vs yday $2,892.80 (-13.26) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 51 | $13.04 | $2.16 | $-53.27 | $2,877.38 | ▼ -53.27 after sell → book $2,877.38; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 1 | $12.18 | $0.14 | $-1.51 | $321.30 | ▼ -1.51 after sell → book $2,923.02; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,321.61 | ▲ close $5,321.61 vs 09:30 $5,325.72 (session +0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,863.57 | ▲ close $2,863.57 vs 09:30 $2,880.25 (session +0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,877.38 | ▲ close $2,877.38 vs 09:30 $2,877.38 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 41 | $26.78 | $2.11 | $+135.12 | $5,321.61 | ▲ +135.12 after sell → book $5,321.61; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 4 | $235.71 | $2.00 | $+62.07 | $6,421.70 | ▲ +62.07 after sell → book $5,323.72; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,366.54 | ▲ 09:30 equity $5,325.72 vs yday $5,301.94 (+23.78) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.30 | ▼ 09:30 equity $2,880.25 vs yday $2,896.10 (-15.85) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,877.38 | ▲ 09:30 equity $2,877.38 vs yday $2,877.38 (-0.00) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $565.99 | ▼ -18.47 after sell → book $2,878.24; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 24 | $15.01 | $2.08 | $-4.14 | $924.14 | ▼ -4.14 after sell → book $2,876.16; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 3 | $92.00 | $2.02 | $-39.69 | $1,198.12 | ▼ -39.69 after sell → book $2,874.14; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 95 | $3.32 | $2.30 | $-57.78 | $1,511.22 | ▼ -57.78 after sell → book $2,871.84; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 8 | $44.17 | $2.03 | $-5.89 | $1,862.55 | ▼ -5.89 after sell → book $2,869.81; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 14 | $21.97 | $2.05 | $-42.16 | $2,168.08 | ▼ -42.16 after sell → book $2,867.75; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 44 | $8.58 | $2.14 | $+5.86 | $2,543.46 | ▲ +5.86 after sell → book $2,865.61; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 9 | $35.80 | $2.04 | $-20.70 | $2,863.57 | ▼ -20.70 after sell → book $2,863.57; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 33 | $10.74 | $2.09 | — | $2,506.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $2,153.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 51 | $6.90 | $2.14 | — | $1,799.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $1,442.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 52 | $13.71 | $2.15 | — | $718.27 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $719.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 16 | $22.32 | $2.04 | — | $1,083.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 1 | $257.00 | $1.99 | — | $824.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 404 | $1.78 | $5.21 | — | $2,153.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $719.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 7 | $47.60 | $2.01 | — | $489.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 23 | $15.09 | $2.06 | — | $140.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; leftover $357.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 29 | $23.88 | $2.08 | — | $23.67 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $719.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 39 | $18.40 | $2.11 | — | $1,433.34 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $719.34 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,961.11 | ▲ close $5,392.22 vs 09:30 $5,321.61 (session +83.14) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.15 | ▲ close $2,965.03 vs 09:30 $2,863.57 (session +117.78) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.67 | ▼ close $2,713.87 vs 09:30 $2,877.38 (session -151.96) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,321.61 | ▲ 09:30 equity $5,321.61 vs yday $5,321.61 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,863.57 | ▲ 09:30 equity $2,863.57 vs yday $2,863.57 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,877.38 | ▲ 09:30 equity $2,877.38 vs yday $2,877.38 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 778 | $1.71 | $10.21 | — | $7,961.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $1330.40 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 89 | $14.85 | $2.32 | — | $6,640.94 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1330.40 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 2 | $8.74 | $0.18 | — | $122.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; leftover $17.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 28 | $25.18 | $2.07 | — | $1,431.66 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $712.92 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 123 | $5.79 | $2.36 | — | $6.57 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $712.92 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 4 | $3.62 | $0.16 | — | $107.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; leftover $17.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 1 | $15.70 | $0.16 | — | $92.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; leftover $17.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 90 | $7.87 | $2.26 | — | $721.10 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $712.92 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,610.82 | ▼ close $5,372.41 vs 09:30 $5,422.02 (session -43.67) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.01 | ▲ close $2,987.93 vs 09:30 $2,977.08 (session +11.35) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.57 | ▲ close $2,889.00 vs 09:30 $2,743.08 (session +159.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,961.11 | ▲ 09:30 equity $5,422.02 vs yday $5,392.22 (+29.80) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.15 | ▲ 09:30 equity $2,977.08 vs yday $2,965.03 (+12.05) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.67 | ▲ 09:30 equity $2,743.08 vs yday $2,713.87 (+29.21) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 52 | $13.89 | $2.17 | $+5.05 | $1,449.51 | ▲ +5.05 after sell → book $2,738.79; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 29 | $23.84 | $2.10 | $-5.33 | $2,138.77 | ▼ -5.33 after sell → book $2,736.69; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 39 | $18.15 | $2.13 | $-13.98 | $729.40 | ▼ -13.98 after sell → book $2,740.96; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 290 | $4.67 | $3.84 | — | $9,311.57 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $1355.50 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 17 | $76.55 | $2.10 | — | $10,610.82 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1355.50 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,610.82 | ▲ close $5,529.81 vs 09:30 $5,394.94 (session +134.87) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.01 | ▼ close $2,992.78 vs 09:30 $2,994.59 (session -1.81) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,780.12 | ▲ close $2,780.12 vs 09:30 $2,792.18 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,610.82 | ▲ 09:30 equity $5,394.94 vs yday $5,372.41 (+22.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.01 | ▲ 09:30 equity $2,994.59 vs yday $2,987.93 (+6.66) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.57 | ▼ 09:30 equity $2,792.18 vs yday $2,889.00 (-96.82) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 28 | $26.44 | $2.09 | $+31.11 | $1,371.77 | ▲ +31.11 after sell → book $2,784.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 123 | $5.81 | $2.39 | $-2.29 | $2,780.12 | ▼ -2.29 after sell → book $2,780.12; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 404 | $1.56 | $5.29 | $-97.36 | $633.54 | ▼ -97.36 after sell → book $2,786.89; vs 09:30 mark -5.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 90 | $7.76 | $2.28 | $-14.44 | $2,067.88 | ▼ -14.44 after sell → book $2,782.51; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,158.89 | ▲ close $5,548.47 vs 09:30 $5,547.30 (session +13.46) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,925.92 | ▼ close $2,972.78 vs 09:30 $2,990.43 (session -1.18) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,780.12 | ▲ close $2,780.12 vs 09:30 $2,780.12 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 778 | $1.58 | $10.04 | $+80.89 | $8,158.89 | ▲ +80.89 after sell → book $5,535.01; vs 09:30 mark -10.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 89 | $13.60 | $2.26 | $+106.67 | $9,398.17 | ▲ +106.67 after sell → book $5,545.05; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,610.82 | ▲ 09:30 equity $5,547.30 vs yday $5,529.81 (+17.49) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.01 | ▼ 09:30 equity $2,990.43 vs yday $2,992.78 (-2.35) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,780.12 | ▲ 09:30 equity $2,780.12 vs yday $2,780.12 (+0.00) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 33 | $10.51 | $2.11 | $-11.95 | $436.73 | ▼ -11.95 after sell → book $2,988.32; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $800.95 | ▲ +10.48 after sell → book $2,986.31; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 51 | $9.39 | $2.16 | $+122.68 | $1,277.68 | ▲ +122.68 after sell → book $2,984.15; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $1,617.56 | ▼ -16.60 after sell → book $2,982.13; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 16 | $21.67 | $2.06 | $-14.50 | $1,962.23 | ▼ -14.50 after sell → book $2,980.08; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 1 | $252.92 | $2.01 | $-8.09 | $2,213.13 | ▼ -8.09 after sell → book $2,978.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 7 | $56.94 | $2.03 | $+61.34 | $2,609.68 | ▲ +61.34 after sell → book $2,976.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 23 | $13.84 | $2.08 | $-32.89 | $2,925.92 | ▼ -32.89 after sell → book $2,973.95; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,583.28 | ▲ close $5,583.28 vs 09:30 $5,589.06 (session +0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,972.19 | ▲ close $2,972.19 vs 09:30 $2,972.74 (session +0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,780.12 | ▲ close $2,780.12 vs 09:30 $2,780.12 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 290 | $4.36 | $3.74 | $+82.32 | $6,890.75 | ▲ +82.32 after sell → book $5,585.32; vs 09:30 mark -3.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 17 | $76.79 | $2.04 | $-8.22 | $5,583.28 | ▼ -8.22 after sell → book $5,583.28; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,158.89 | ▲ 09:30 equity $5,589.06 vs yday $5,548.47 (+40.59) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,925.92 | ▼ 09:30 equity $2,972.74 vs yday $2,972.78 (-0.04) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,780.12 | ▲ 09:30 equity $2,780.12 vs yday $2,780.12 (+0.00) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 2 | $8.26 | $0.19 | $-1.33 | $2,942.25 | ▼ -1.33 after sell → book $2,972.55; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 4 | $3.76 | $0.18 | $+0.24 | $2,957.11 | ▲ +0.24 after sell → book $2,972.37; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 1 | $15.26 | $0.18 | $-0.78 | $2,972.19 | ▼ -0.78 after sell → book $2,972.19; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 1 | $242.17 | $1.99 | — | $2,028.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 141 | $4.91 | $2.41 | — | $1,388.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $695.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 221 | $3.13 | $2.85 | — | $1.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $695.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 11 | $32.01 | $2.02 | — | $1,674.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 62 | $5.91 | $2.18 | — | $2,272.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 5 | $71.71 | $2.00 | — | $1,313.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 257 | $2.70 | $3.32 | — | $2,082.91 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $695.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 112 | $6.16 | $2.33 | — | $695.94 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $695.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 6 | $56.02 | $2.01 | — | $975.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 39 | $9.37 | $2.11 | — | $608.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 2 | $164.43 | $2.00 | — | $2,641.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; leftover $371.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 28 | $13.10 | $2.07 | — | $239.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; leftover $371.52 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,241.62 | ▼ close $5,568.14 vs 09:30 $5,583.28 (session -2.35) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.35 | ▲ close $2,968.93 vs 09:30 $2,972.19 (session +13.12) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.36 | ▲ close $2,841.13 vs 09:30 $2,780.12 (session +71.92) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,583.28 | ▲ 09:30 equity $5,583.28 vs yday $5,583.28 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,972.19 | ▲ 09:30 equity $2,972.19 vs yday $2,972.19 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,780.12 | ▲ 09:30 equity $2,780.12 vs yday $2,780.12 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 22 | $24.97 | $2.09 | — | $7,688.11 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 275 | $2.03 | $3.62 | — | $7,140.86 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 165 | $3.37 | $2.54 | — | $8,241.62 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 4 | $112.83 | $2.03 | — | $6,032.58 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $558.33 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 158 | $3.52 | $2.52 | — | $6,586.23 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; leftover $558.33 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,241.62 | ▼ close $5,507.92 vs 09:30 $5,583.27 (session -75.35) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.35 | ▲ close $3,075.55 vs 09:30 $2,969.84 (session +105.71) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $673.24 | ▲ close $3,027.73 vs 09:30 $2,880.14 (session +149.95) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,241.62 | ▲ 09:30 equity $5,583.27 vs yday $5,568.14 (+15.13) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.35 | ▲ 09:30 equity $2,969.84 vs yday $2,968.93 (+0.91) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.36 | ▲ 09:30 equity $2,880.14 vs yday $2,841.13 (+39.01) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 112 | $6.02 | $2.35 | $-20.36 | $673.24 | ▼ -20.36 after sell → book $2,877.78; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,241.62 | ▼ close $5,466.95 vs 09:30 $5,508.12 (session -41.17) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.35 | ▲ close $3,086.66 vs 09:30 $3,067.29 (session +19.37) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,192.85 | ▲ close $3,128.33 vs 09:30 $3,071.99 (session +61.68) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,241.62 | ▲ 09:30 equity $5,508.12 vs yday $5,507.92 (+0.20) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.35 | ▼ 09:30 equity $3,067.29 vs yday $3,075.55 (-8.26) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $673.24 | ▲ 09:30 equity $3,071.99 vs yday $3,027.73 (+44.26) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 141 | $5.11 | $2.45 | $+23.34 | $1,391.31 | ▲ +23.34 after sell → book $3,069.55; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 221 | $3.64 | $2.90 | $+106.96 | $2,192.85 | ▲ +106.96 after sell → book $3,066.65; vs 09:30 mark -2.90 | dropped from list after 2 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 46 | $33.14 | $2.13 | — | $1,523.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; leftover $1524.88 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 406 | $1.80 | $5.24 | — | $1,456.81 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $730.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 31 | $23.29 | $2.08 | — | $732.74 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $730.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 49 | $14.62 | $2.14 | — | $14.22 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $730.95 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 37 | $40.93 | $2.10 | — | $6.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $1524.88 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,175.53 | ▼ close $5,264.88 vs 09:30 $5,475.32 (session -192.95) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.68 | ▲ close $3,105.23 vs 09:30 $3,066.30 (session +59.70) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.22 | ▼ close $3,121.65 vs 09:30 $3,133.47 (session -2.36) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 22 | $24.42 | $2.06 | $+7.95 | $6,084.00 | ▲ +7.95 after sell → book $5,465.25; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 275 | $1.85 | $3.55 | $+42.33 | $6,623.30 | ▲ +42.33 after sell → book $5,467.31; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 165 | $3.75 | $2.48 | $-67.72 | $5,462.77 | ▼ -67.72 after sell → book $5,462.77; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 4 | $118.18 | $2.00 | $-25.42 | $7,766.90 | ▼ -25.42 after sell → book $5,473.32; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 158 | $3.98 | $2.46 | $-77.66 | $7,135.59 | ▼ -77.66 after sell → book $5,470.85; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,241.62 | ▲ 09:30 equity $5,475.32 vs yday $5,466.95 (+8.37) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.35 | ▼ 09:30 equity $3,066.30 vs yday $3,086.66 (-20.36) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,192.85 | ▲ 09:30 equity $3,133.47 vs yday $3,128.33 (+5.14) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 1 | $253.34 | $2.01 | $+7.16 | $1,154.03 | ▲ +7.16 after sell → book $3,060.08; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 11 | $30.57 | $2.04 | $-19.91 | $1,488.25 | ▼ -19.91 after sell → book $3,058.03; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 62 | $6.25 | $2.20 | $+16.71 | $902.70 | ▲ +16.71 after sell → book $3,062.09; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 5 | $78.12 | $2.02 | $+28.02 | $1,876.83 | ▲ +28.02 after sell → book $3,056.01; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 6 | $61.93 | $2.03 | $+31.42 | $2,246.38 | ▲ +31.42 after sell → book $3,053.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 39 | $9.40 | $2.13 | $-3.06 | $2,610.85 | ▼ -3.06 after sell → book $3,051.85; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 2 | $140.03 | $2.02 | $-52.81 | $517.40 | ▼ -52.81 after sell → book $3,064.29; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 28 | $15.75 | $2.09 | $+70.03 | $3,049.76 | ▲ +70.03 after sell → book $3,049.76; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 73 | $18.61 | $2.27 | — | $6,819.03 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1365.69 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 199 | $6.83 | $2.67 | — | $8,175.53 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; leftover $1365.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 31 | $22.46 | $2.08 | — | $733.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $715.74 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 19 | $36.76 | $2.05 | — | $32.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $715.74 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,770.68 | ▲ close $5,341.31 vs 09:30 $5,246.43 (session +99.53) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.68 | ▲ close $3,250.76 vs 09:30 $3,206.87 (session +43.89) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.65 | ▲ close $3,250.25 vs 09:30 $3,136.44 (session +122.20) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,175.53 | ▼ 09:30 equity $5,246.43 vs yday $5,264.88 (-18.45) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.68 | ▲ 09:30 equity $3,206.87 vs yday $3,105.23 (+101.64) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.22 | ▲ 09:30 equity $3,136.44 vs yday $3,121.65 (+14.79) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 31 | $24.09 | $2.10 | $+20.61 | $758.91 | ▲ +20.61 after sell → book $3,134.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 49 | $13.77 | $2.16 | $-45.94 | $1,431.48 | ▼ -45.94 after sell → book $3,132.18; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 164 | $7.95 | $2.56 | — | $9,476.77 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1311.61 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 16 | $81.00 | $2.09 | — | $10,770.68 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; leftover $1311.61 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 244 | $3.04 | $3.15 | — | $750.97 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $743.24 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 25 | $29.32 | $2.06 | — | $1,494.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $743.24 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 9 | $81.40 | $2.02 | — | $16.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $743.24 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,420.23 | ▲ close $5,420.85 vs 09:30 $5,355.02 (session +68.16) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.68 | ▼ close $3,326.08 vs 09:30 $3,326.25 (session -0.17) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.36 | ▲ close $3,233.17 vs 09:30 $3,228.66 (session +21.22) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,770.68 | ▲ 09:30 equity $5,355.02 vs yday $5,341.31 (+13.71) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.68 | ▲ 09:30 equity $3,326.25 vs yday $3,250.76 (+75.49) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.65 | ▼ 09:30 equity $3,228.66 vs yday $3,250.25 (-21.59) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 31 | $21.30 | $2.10 | $-40.15 | $1,481.29 | ▼ -40.15 after sell → book $3,221.24; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 19 | $39.50 | $2.07 | $+47.95 | $2,229.73 | ▲ +47.95 after sell → book $3,219.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 406 | $1.96 | $5.31 | $+54.41 | $823.10 | ▲ +54.41 after sell → book $3,223.35; vs 09:30 mark -5.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 77 | $34.44 | $2.33 | — | $13,420.23 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2677.51 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 338 | $2.47 | $4.36 | — | $2,502.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $835.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 506 | $1.65 | $6.53 | — | $830.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $835.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 70 | $11.67 | $2.20 | — | $11.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $835.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 49 | $16.91 | $2.14 | — | $1,671.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $835.37 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,990.98 | ▼ close $5,193.77 vs 09:30 $5,324.59 (session -121.43) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,360.79 | ▲ close $3,360.79 vs 09:30 $3,365.06 (session +0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.02 | ▲ close $3,429.86 vs 09:30 $3,352.18 (session +103.59) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 73 | $22.11 | $2.21 | $-259.98 | $11,803.99 | ▼ -259.98 after sell → book $5,322.38; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 199 | $6.55 | $2.59 | $+50.46 | $10,497.96 | ▲ +50.46 after sell → book $5,319.80; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,420.23 | ▼ 09:30 equity $5,324.59 vs yday $5,420.85 (-96.26) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.68 | ▲ 09:30 equity $3,365.06 vs yday $3,326.08 (+38.98) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.36 | ▲ 09:30 equity $3,352.18 vs yday $3,233.17 (+119.01) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 244 | $4.00 | $3.20 | $+229.11 | $2,631.81 | ▲ +229.11 after sell → book $3,343.53; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 46 | $40.03 | $2.15 | $+312.66 | $1,845.91 | ▲ +312.66 after sell → book $3,362.91; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 257 | $3.55 | $3.37 | $+211.77 | $925.34 | ▲ +211.77 after sell → book $3,348.81; vs 09:30 mark -3.37 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 25 | $29.43 | $2.08 | $-1.40 | $1,659.00 | ▼ -1.40 after sell → book $3,346.72; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 37 | $41.00 | $2.12 | $-1.63 | $3,360.79 | ▼ -1.63 after sell → book $3,360.79; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 9 | $79.08 | $2.04 | $-24.93 | $3,341.49 | ▼ -24.93 after sell → book $3,341.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 161 | $8.26 | $2.55 | — | $11,825.27 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $1329.95 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 2 | $583.88 | $2.05 | — | $12,990.98 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; leftover $1329.95 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,850.93 | ▼ close $5,169.13 vs 09:30 $5,207.57 (session -33.88) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,360.79 | ▲ close $3,360.79 vs 09:30 $3,360.79 (session +0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.02 | ▲ close $3,394.86 vs 09:30 $3,392.06 (session +2.80) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 164 | $8.28 | $2.48 | $-58.34 | $11,631.40 | ▼ -58.34 after sell → book $5,205.09; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,990.98 | ▲ 09:30 equity $5,207.57 vs yday $5,193.77 (+13.80) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,360.79 | ▲ 09:30 equity $3,360.79 vs yday $3,360.79 (-0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.02 | ▼ 09:30 equity $3,392.06 vs yday $3,429.86 (-37.80) | — | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 13 | $93.97 | $2.08 | — | $12,850.93 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $1301.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 14 | $47.57 | $2.03 | — | $2,692.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $672.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 3 | $196.78 | $2.00 | — | $2,100.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $672.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 18 | $35.74 | $2.04 | — | $1,455.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $672.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 301 | $2.70 | $3.88 | — | $1,621.97 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $812.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 14 | $47.15 | $2.03 | — | $792.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $672.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 6 | $109.67 | $2.01 | — | $132.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $672.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 181 | $4.49 | $2.53 | — | $11.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $812.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 19 | $41.76 | $2.05 | — | $826.49 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $812.85 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,871.75 | ▲ close $4,946.25 vs 09:30 $4,851.81 (session +98.62) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.91 | ▼ close $3,305.90 vs 09:30 $3,360.79 (session -44.77) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.26 | ▼ close $3,300.27 vs 09:30 $3,436.52 (session -116.78) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 16 | $82.00 | $2.04 | $-20.13 | $11,536.89 | ▼ -20.13 after sell → book $4,849.77; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,850.93 | ▼ 09:30 equity $4,851.81 vs yday $5,169.13 (-317.32) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,360.79 | ▲ 09:30 equity $3,360.79 vs yday $3,360.79 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.02 | ▲ 09:30 equity $3,436.52 vs yday $3,394.86 (+41.66) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 506 | $1.41 | $6.62 | $-134.59 | $1,544.78 | ▼ -134.59 after sell → book $3,427.74; vs 09:30 mark -6.62 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 70 | $12.80 | $2.22 | $+74.68 | $2,438.56 | ▲ +74.68 after sell → book $3,425.52; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 49 | $16.92 | $2.16 | $-3.80 | $837.94 | ▼ -3.80 after sell → book $3,434.36; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 20 | $116.85 | $2.14 | — | $13,871.75 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $2424.89 | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,669.21 | ▼ close $4,879.77 vs 09:30 $4,992.00 (session -110.23) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.91 | ▲ close $3,355.29 vs 09:30 $3,306.20 (session +49.09) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,302.94 | ▲ close $3,913.29 vs 09:30 $3,280.62 (session +641.73) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 2 | $600.27 | $2.00 | $-36.82 | $12,669.21 | ▼ -36.82 after sell → book $4,990.00; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,871.75 | ▲ 09:30 equity $4,992.00 vs yday $4,946.25 (+45.75) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.91 | ▲ 09:30 equity $3,306.20 vs yday $3,305.90 (+0.30) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.26 | ▼ 09:30 equity $3,280.62 vs yday $3,300.27 (-19.65) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 338 | $2.68 | $4.43 | $+62.19 | $912.68 | ▲ +62.19 after sell → book $3,276.19; vs 09:30 mark -4.43 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 181 | $3.92 | $2.57 | $-107.37 | $2,302.94 | ▼ -107.37 after sell → book $3,271.55; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 19 | $36.02 | $2.07 | $-113.08 | $1,595.08 | ▼ -113.08 after sell → book $3,274.13; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 25 | $16.21 | $2.06 | — | $430.89 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $409.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 13 | $29.76 | $2.03 | — | $838.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $409.04 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 26 | $15.58 | $2.07 | — | $23.71 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $409.04 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,576.49 | ▼ close $4,566.90 vs 09:30 $4,583.77 (session -12.99) | 16:00 close · cash $12,576.49 · equity $4,566.90 vs 09:30 $4,583.77 (-16.87; session marks -12.99) · 6 name(s) marked open→close (per-name table). AEHL×150 09:30 $9.05 → close $9.36 -46.50; BAND×19 09:30 $61.83 → close $61.83 -0.00; HALO×9 09:30 $115.36 → close $113.90 +13.14; PAYX×10 09:30 $101.59 → close $101.59 +0.00; USFD×12 09:30 $93.82 → close $93.82 +0.00; RSKD×291 09:30 $7.85 → close $7.78 +20.37 | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.02 | ▼ 09:30 equity $4,583.77 vs yday $4,598.53 (-14.76) | 09:30 open · cash $10,296.02 (unchanged overnight, no fees) · equity $4,583.77 vs prior close $4,598.53 (-14.76) · 5 name(s) re-marked at the open (per-name table). AEHL×150 yday $8.96 → 09:30 $9.05 -13.50; BAND×19 yday $61.83 → 09:30 $61.83 -0.00; HALO×9 yday $115.22 → 09:30 $115.36 -1.26; PAYX×10 yday $101.59 → 09:30 $101.59 -0.00; USFD×12 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 291 | $7.85 | $3.88 | — | $12,576.49 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $2291.89 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 2.54 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.54 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 2.54 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 2.54 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 2.54 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 2.54 < 1 share @ 9.89 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | cash | leftover split 13.94 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 13.94 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 13.94 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 13.94 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 13.94 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 13.94 < 1 share @ 43.08 |
| 2026-08-24 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANF` | cash | leftover split 32.00 < 1 share @ 131.37 |
| 2026-08-26 | `BOX` | cash | leftover split 32.00 < 1 share @ 34.30 |
| 2026-08-26 | `DY` | cash | leftover split 32.00 < 1 share @ 326.91 |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | cash | leftover split 13.75 < 1 share @ 80.60 |
| 2026-08-27 | `BILI` | cash | leftover split 13.75 < 1 share @ 16.18 |
| 2026-08-27 | `CM` | cash | leftover split 13.75 < 1 share @ 118.77 |
| 2026-08-27 | `CMBT` | cash | leftover split 13.75 < 1 share @ 17.78 |
| 2026-08-27 | `HQY` | cash | leftover split 13.75 < 1 share @ 97.16 |
| 2026-08-27 | `RY` | cash | leftover split 13.75 < 1 share @ 206.82 |
| 2026-08-27 | `TD` | cash | leftover split 13.75 < 1 share @ 120.17 |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CSIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CSIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CPB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AMBA` | cash | leftover split 17.52 < 1 share @ 63.18 |
| 2026-09-04 | `DOCU` | cash | leftover split 17.52 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 17.52 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 17.52 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 17.52 < 1 share @ 98.15 |
| 2026-09-08 | `AI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CPB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MAMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DSGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `KR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DSGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `KR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ALMU` | cash | leftover split 3.34 < 1 share @ 11.21 |
| 2026-09-17 | `LEN` | cash | leftover split 3.34 < 1 share @ 81.00 |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `CBRL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `KBH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PAYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.67 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 3.67 < 1 share @ 7.23 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 77 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2677.51 |
| `AEHL` | 161 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $1329.95 |
| `USFD` | 13 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $1301.27 |
| `HALO` | 20 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $2424.89 |
| `CBRL` | 14 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $672.16 |
| `CTAS` | 3 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $672.16 |
| `GIS` | 18 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $672.16 |
| `KBH` | 14 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $672.16 |
| `PAYX` | 6 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $672.16 |
| `GLND` | 301 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $812.85 |
