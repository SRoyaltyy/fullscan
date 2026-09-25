# Factor mine action — `combo_sh_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_hot_n4_h1 w=0.7,0.3 net=priority

Cash book **+9.21%** ($10,921) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 215 · skips 159 · realized $+1962.92.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 70%, union_hot_n4_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 70%, union_hot_n4_h1 30%.
- Member: short_news_r_h3 (70% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,977.49.

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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 31 | $24.68 | $2.08 | — | $9,599.76 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $777.52; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 39 | $19.57 | $2.11 | — | $8,834.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $777.52; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 353 | $2.20 | $4.55 | — | $8,053.27 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $777.52; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 69 | $11.12 | $2.20 | — | $7,283.79 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $777.52; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1462 | $1.18 | $19.16 | — | $8,989.79 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1726.00; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 90 | $19.17 | $2.34 | — | $10,712.75 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1726.00; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 135 | $12.70 | $2.48 | — | $12,424.10 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1726.00; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,424.10 | ▼ close $10,283.26 vs 09:30 $10,412.10 (session -48.75) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,424.10 | ▼ 09:30 equity $10,125.67 vs yday $10,283.26 (-157.59) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 31 | $24.83 | $2.10 | $+0.46 | $13,191.72 | ▲ +0.46 after sell → book $10,123.57; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 39 | $19.57 | $2.13 | $-4.23 | $13,952.83 | ▼ -4.23 after sell → book $10,121.44; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 353 | $2.08 | $4.62 | $-49.77 | $14,684.21 | ▼ -49.77 after sell → book $10,116.82; vs 09:30 mark -4.62 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 69 | $9.57 | $2.22 | $-111.37 | $15,342.32 | ▼ -111.37 after sell → book $10,114.60; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 274 | $4.19 | $3.53 | — | $14,190.73 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1150.67; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 167 | $6.87 | $2.49 | — | $13,040.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1150.67; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 84 | $13.64 | $2.24 | — | $11,892.94 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1150.67; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 27 | $41.23 | $2.07 | — | $10,777.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1150.67; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 878 | $1.15 | $11.51 | — | $11,775.85 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $1010.43; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 283 | $3.56 | $3.74 | — | $12,779.60 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $1010.43; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $13,760.17 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $1010.43; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 335 | $3.01 | $4.41 | — | $14,764.10 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $1010.43; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 148 | $6.80 | $2.50 | — | $15,768.01 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $1010.43; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,768.01 | ▲ close $10,225.55 vs 09:30 $10,125.67 (session +145.57) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,768.01 | ▲ 09:30 equity $10,386.81 vs yday $10,225.55 (+161.26) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 274 | $3.94 | $3.59 | $-75.62 | $16,843.98 | ▼ -75.62 after sell → book $10,383.22; vs 09:30 mark -3.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 84 | $13.31 | $2.27 | $-32.23 | $17,959.75 | ▼ -32.23 after sell → book $10,380.95; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 27 | $41.50 | $2.09 | $+3.13 | $19,078.16 | ▲ +3.13 after sell → book $10,378.86; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,078.16 | ▲ close $10,551.81 vs 09:30 $10,386.81 (session +172.95) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,078.16 | ▼ 09:30 equity $10,538.37 vs yday $10,551.81 (-13.44) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1462 | $1.07 | $18.86 | $+122.80 | $17,494.96 | ▲ +122.80 after sell → book $10,519.51; vs 09:30 mark -18.86 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 135 | $11.75 | $2.40 | $+122.70 | $15,906.32 | ▲ +122.70 after sell → book $10,517.12; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 167 | $7.19 | $2.53 | $+48.42 | $17,104.52 | ▲ +48.42 after sell → book $10,514.59; vs 09:30 mark -2.53 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,104.52 | ▲ close $10,564.48 vs 09:30 $10,538.37 (session +49.89) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,104.52 | ▼ 09:30 equity $10,506.96 vs yday $10,564.48 (-57.52) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 90 | $18.13 | $2.26 | $+89.00 | $15,470.56 | ▲ +89.00 after sell → book $10,504.70; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 878 | $0.96 | $11.09 | $+141.59 | $14,613.95 | ▲ +141.59 after sell → book $10,493.61; vs 09:30 mark -11.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 283 | $4.01 | $3.65 | $-136.15 | $13,474.06 | ▼ -136.15 after sell → book $10,489.96; vs 09:30 mark -3.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,484.01 | ▼ -9.48 after sell → book $10,487.88; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 335 | $2.95 | $4.32 | $+11.36 | $11,491.43 | ▲ +11.36 after sell → book $10,483.55; vs 09:30 mark -4.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 148 | $6.81 | $2.43 | $-6.41 | $10,481.12 | ▼ -6.41 after sell → book $10,481.12; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 66 | $11.81 | $2.19 | — | $9,699.14 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $786.08; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $8,946.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $786.08; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 683 | $1.15 | $8.81 | — | $8,152.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $786.08; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 573 | $1.37 | $7.39 | — | $7,359.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $786.08; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $7,971.09 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 34 | $21.40 | $2.13 | — | $8,696.56 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 168 | $4.43 | $2.55 | — | $9,438.24 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 4 | $173.90 | $2.04 | — | $10,131.80 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 15 | $46.85 | $2.07 | — | $10,832.48 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 7 | $106.38 | $2.05 | — | $11,575.09 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 162 | $4.61 | $2.53 | — | $12,319.37 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $747.19; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,319.37 | ▲ close $10,482.84 vs 09:30 $10,506.96 (session +37.54) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,319.37 | ▲ 09:30 equity $10,524.84 vs yday $10,482.84 (+42.00) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 66 | $11.57 | $2.21 | $-20.57 | $13,080.79 | ▼ -20.57 after sell → book $10,522.64; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 573 | $1.46 | $7.50 | $+36.68 | $13,909.87 | ▲ +36.68 after sell → book $10,515.14; vs 09:30 mark -7.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 464 | $4.49 | $5.99 | — | $11,820.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $2086.48; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 306 | $6.81 | $3.95 | — | $9,732.72 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $2086.48; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 337 | $3.11 | $4.44 | — | $10,776.34 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $1050.52; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $11,754.37 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1050.52; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 27 | $38.40 | $2.12 | — | $12,789.05 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1050.52; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 50 | $20.90 | $2.19 | — | $13,831.87 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1050.52; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 38 | $27.00 | $2.15 | — | $14,855.71 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $1050.52; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,855.71 | ▼ close $10,483.82 vs 09:30 $10,524.84 (session -8.41) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,855.71 | ▲ 09:30 equity $11,274.99 vs yday $10,483.82 (+791.17) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $15,567.19 | ▼ -41.23 after sell → book $11,272.97; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 683 | $1.83 | $8.93 | $+446.70 | $16,808.14 | ▲ +446.70 after sell → book $11,264.03; vs 09:30 mark -8.94 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 464 | $4.32 | $6.08 | $-90.94 | $18,806.55 | ▼ -90.94 after sell → book $11,257.96; vs 09:30 mark -6.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 306 | $8.03 | $4.02 | $+365.35 | $21,259.71 | ▲ +365.35 after sell → book $11,253.94; vs 09:30 mark -4.02 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,259.71 | ▲ close $11,293.32 vs 09:30 $11,274.99 (session +39.38) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,259.71 | ▲ 09:30 equity $11,402.53 vs yday $11,293.32 (+109.21) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $20,621.71 | ▼ -26.68 after sell → book $11,400.53; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 34 | $20.90 | $2.09 | $+12.78 | $19,909.02 | ▲ +12.78 after sell → book $11,398.44; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 168 | $4.42 | $2.49 | $-3.37 | $19,163.96 | ▼ -3.37 after sell → book $11,395.94; vs 09:30 mark -2.50 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 4 | $170.64 | $2.00 | $+9.00 | $18,479.40 | ▲ +9.00 after sell → book $11,393.94; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 15 | $43.63 | $2.04 | $+44.19 | $17,822.92 | ▲ +44.19 after sell → book $11,391.91; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 7 | $105.58 | $2.01 | $+1.54 | $17,081.84 | ▲ +1.54 after sell → book $11,389.89; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 162 | $4.77 | $2.48 | $-30.93 | $16,306.63 | ▼ -30.93 after sell → book $11,387.42; vs 09:30 mark -2.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 50 | $24.11 | $2.14 | — | $15,098.99 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1223.00; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 783 | $1.56 | $10.10 | — | $13,867.41 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1223.00; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 300 | $4.07 | $3.87 | — | $12,642.54 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1223.00; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 64 | $19.04 | $2.18 | — | $11,421.80 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1223.00; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 104 | $13.62 | $2.37 | — | $12,836.43 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1421.14; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 26 | $54.51 | $2.13 | — | $14,251.56 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1421.14; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 8 | $175.01 | $2.07 | — | $15,649.57 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1421.14; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $16,740.57 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1421.14; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,740.57 | ▲ close $11,715.30 vs 09:30 $11,402.53 (session +354.79) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,740.57 | ▼ 09:30 equity $11,670.74 vs yday $11,715.30 (-44.56) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 337 | $2.83 | $4.35 | $+85.57 | $15,782.51 | ▲ +85.57 after sell → book $11,666.39; vs 09:30 mark -4.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $14,809.85 | ▲ +5.37 after sell → book $11,664.37; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 27 | $38.41 | $2.07 | $-4.46 | $13,770.70 | ▼ -4.46 after sell → book $11,662.29; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 50 | $20.50 | $2.14 | $+15.67 | $12,743.56 | ▲ +15.67 after sell → book $11,660.15; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 38 | $26.00 | $2.10 | $+33.74 | $11,753.46 | ▲ +33.74 after sell → book $11,658.05; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 50 | $26.61 | $2.16 | $+120.70 | $13,081.80 | ▲ +120.70 after sell → book $11,655.89; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 783 | $1.60 | $10.24 | $+10.98 | $14,324.36 | ▲ +10.98 after sell → book $11,645.65; vs 09:30 mark -10.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 64 | $20.72 | $2.20 | $+103.13 | $15,648.24 | ▲ +103.13 after sell → book $11,643.45; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 110 | $14.11 | $2.32 | — | $14,093.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1564.82; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 269 | $5.81 | $3.47 | — | $12,527.46 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1564.82; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 135 | $11.59 | $2.40 | — | $10,961.09 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1564.82; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $12,028.73 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1163.53; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 95 | $12.22 | $2.33 | — | $13,187.30 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1163.53; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 229 | $5.08 | $3.03 | — | $14,347.59 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1163.53; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 8 | $132.64 | $2.06 | — | $15,406.64 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1163.53; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $16,404.29 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1163.53; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,404.29 | ▼ close $11,575.41 vs 09:30 $11,670.74 (session -48.32) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,404.29 | ▲ 09:30 equity $11,621.52 vs yday $11,575.41 (+46.11) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 110 | $14.20 | $2.35 | $+5.23 | $17,963.94 | ▲ +5.23 after sell → book $11,619.17; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 269 | $6.50 | $3.53 | $+178.61 | $19,708.91 | ▲ +178.61 after sell → book $11,615.64; vs 09:30 mark -3.53 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 135 | $12.18 | $2.43 | $+75.50 | $21,350.78 | ▲ +75.50 after sell → book $11,613.21; vs 09:30 mark -2.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 232 | $9.19 | $2.99 | — | $19,215.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $2135.08; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 14 | $144.18 | $2.03 | — | $17,195.16 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $2135.08; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 115 | $18.50 | $2.33 | — | $15,065.32 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $2135.08; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 38 | $74.54 | $2.21 | — | $17,895.63 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2901.46; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 52 | $55.25 | $2.26 | — | $20,766.37 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2901.46; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,766.37 | ▼ close $11,585.01 vs 09:30 $11,621.52 (session -16.37) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,766.37 | ▼ 09:30 equity $11,398.66 vs yday $11,585.01 (-186.35) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 300 | $3.69 | $3.93 | $-121.80 | $21,869.44 | ▼ -121.80 after sell → book $11,394.73; vs 09:30 mark -3.93 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 104 | $13.90 | $2.30 | $-33.27 | $20,421.54 | ▼ -33.27 after sell → book $11,392.43; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 26 | $52.49 | $2.07 | $+48.32 | $19,054.73 | ▲ +48.32 after sell → book $11,390.36; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 8 | $172.76 | $2.01 | $+13.91 | $17,670.63 | ▲ +13.91 after sell → book $11,388.34; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $16,625.18 | ▲ +45.54 after sell → book $11,386.35; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 115 | $18.15 | $2.37 | $-44.96 | $18,710.05 | ▼ -44.96 after sell → book $11,383.97; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 200 | $14.00 | $2.59 | — | $15,907.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2806.51; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 19 | $146.07 | $2.05 | — | $13,130.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2806.51; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 11 | $252.24 | $2.13 | — | $15,902.60 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2844.83; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 94 | $30.18 | $2.39 | — | $18,737.13 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2844.83; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,737.13 | ▲ close $11,808.84 vs 09:30 $11,398.66 (session +434.03) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,737.13 | ▲ 09:30 equity $11,821.06 vs yday $11,808.84 (+12.22) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $17,690.72 | ▲ +21.24 after sell → book $11,819.06; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 95 | $11.10 | $2.27 | $+101.79 | $16,633.95 | ▲ +101.79 after sell → book $11,816.78; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 229 | $4.97 | $2.95 | $+18.06 | $15,491.72 | ▲ +18.06 after sell → book $11,813.83; vs 09:30 mark -2.95 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 8 | $127.45 | $2.01 | $+37.44 | $14,470.11 | ▲ +37.44 after sell → book $11,811.82; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $13,196.15 | ▼ -276.31 after sell → book $11,809.81; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 232 | $9.50 | $3.05 | $+65.88 | $15,397.10 | ▲ +65.88 after sell → book $11,806.76; vs 09:30 mark -3.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 14 | $134.10 | $2.06 | $-145.21 | $17,272.44 | ▼ -145.21 after sell → book $11,804.70; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 19 | $148.03 | $2.08 | $+33.11 | $20,082.93 | ▲ +33.11 after sell → book $11,802.62; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,082.93 | ▼ close $11,758.19 vs 09:30 $11,821.06 (session -44.43) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,082.93 | ▲ 09:30 equity $11,876.42 vs yday $11,758.19 (+118.23) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 38 | $73.22 | $2.10 | $+45.84 | $17,298.47 | ▲ +45.84 after sell → book $11,874.32; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 52 | $54.76 | $2.15 | $+21.08 | $14,448.80 | ▲ +21.08 after sell → book $11,872.17; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 200 | $13.04 | $2.64 | $-197.23 | $17,054.16 | ▼ -197.23 after sell → book $11,869.53; vs 09:30 mark -2.64 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,054.16 | ▲ close $11,886.51 vs 09:30 $11,876.42 (session +16.98) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,054.16 | ▲ 09:30 equity $11,944.03 vs yday $11,886.51 (+57.52) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 11 | $235.71 | $2.02 | $+177.68 | $14,459.33 | ▲ +177.68 after sell → book $11,942.01; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 94 | $26.78 | $2.27 | $+314.94 | $11,939.74 | ▲ +314.94 after sell → book $11,939.74; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,939.74 | ▲ close $11,939.74 vs 09:30 $11,944.03 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,939.74 | ▲ 09:30 equity $11,939.74 vs yday $11,939.74 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 503 | $1.78 | $6.49 | — | $11,037.91 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $895.48; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 48 | $18.40 | $2.13 | — | $10,152.57 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $895.48; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 65 | $13.71 | $2.19 | — | $9,259.24 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $895.48; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 37 | $23.88 | $2.10 | — | $8,373.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $895.48; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 200 | $14.85 | $2.73 | — | $11,340.85 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2981.71; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1743 | $1.71 | $22.88 | — | $14,298.50 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2981.71; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,298.50 | ▼ close $11,898.32 vs 09:30 $11,939.74 (session -2.90) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,298.50 | ▲ 09:30 equity $12,001.70 vs yday $11,898.32 (+103.38) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 48 | $18.15 | $2.15 | $-16.29 | $15,167.54 | ▼ -16.29 after sell → book $11,999.54; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 65 | $13.89 | $2.21 | $+7.31 | $16,068.19 | ▲ +7.31 after sell → book $11,997.34; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 37 | $23.84 | $2.12 | $-5.70 | $16,948.15 | ▼ -5.70 after sell → book $11,995.22; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 67 | $25.18 | $2.19 | — | $15,258.90 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1694.81; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 215 | $7.87 | $2.77 | — | $13,564.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1694.81; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 292 | $5.79 | $3.77 | — | $11,869.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1694.81; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 641 | $4.67 | $8.48 | — | $14,854.62 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2996.62; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 39 | $76.55 | $2.22 | — | $17,837.84 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2996.62; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,837.84 | ▲ close $12,155.76 vs 09:30 $12,001.70 (session +179.98) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,837.84 | ▼ 09:30 equity $12,038.64 vs yday $12,155.76 (-117.12) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 503 | $1.56 | $6.58 | $-121.22 | $18,618.46 | ▼ -121.22 after sell → book $12,032.06; vs 09:30 mark -6.58 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 67 | $26.44 | $2.22 | $+80.01 | $20,387.72 | ▲ +80.01 after sell → book $12,029.84; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 215 | $7.76 | $2.82 | $-29.25 | $22,053.30 | ▼ -29.25 after sell → book $12,027.02; vs 09:30 mark -2.82 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 292 | $5.81 | $3.83 | $-1.76 | $23,745.99 | ▼ -1.76 after sell → book $12,023.19; vs 09:30 mark -3.83 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,745.99 | ▲ close $12,323.04 vs 09:30 $12,038.64 (session +299.85) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,745.99 | ▲ 09:30 equity $12,362.37 vs yday $12,323.04 (+39.33) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 200 | $13.60 | $2.59 | $+244.68 | $21,023.40 | ▲ +244.68 after sell → book $12,359.78; vs 09:30 mark -2.59 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1743 | $1.58 | $22.48 | $+181.23 | $18,246.97 | ▲ +181.23 after sell → book $12,337.29; vs 09:30 mark -22.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,246.97 | ▲ close $12,367.44 vs 09:30 $12,362.37 (session +30.15) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,246.97 | ▲ 09:30 equity $12,457.40 vs yday $12,367.44 (+89.96) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 641 | $4.36 | $8.27 | $+181.96 | $15,443.94 | ▲ +181.96 after sell → book $12,449.13; vs 09:30 mark -8.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 39 | $76.79 | $2.11 | $-13.69 | $12,447.03 | ▼ -13.69 after sell → book $12,447.03; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,447.03 | ▲ close $12,447.03 vs 09:30 $12,457.40 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,447.03 | ▲ 09:30 equity $12,447.03 vs yday $12,447.03 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 345 | $2.70 | $4.45 | — | $11,511.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $933.53; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 190 | $4.91 | $2.56 | — | $10,575.62 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $933.53; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 151 | $6.16 | $2.44 | — | $9,643.01 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $933.53; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 298 | $3.13 | $3.84 | — | $8,706.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $933.53; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 11 | $112.83 | $2.08 | — | $9,945.54 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1243.37; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 353 | $3.52 | $4.66 | — | $11,183.44 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1243.37; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 612 | $2.03 | $8.04 | — | $12,417.76 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1243.37; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 49 | $24.97 | $2.19 | — | $13,639.10 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1243.37; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 368 | $3.37 | $4.85 | — | $14,874.41 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1243.37; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,874.41 | ▲ close $12,495.52 vs 09:30 $12,447.03 (session +83.60) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,874.41 | ▲ 09:30 equity $12,587.07 vs yday $12,495.52 (+91.55) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 151 | $6.02 | $2.48 | $-26.06 | $15,780.95 | ▼ -26.06 after sell → book $12,584.59; vs 09:30 mark -2.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,780.95 | ▲ close $12,630.72 vs 09:30 $12,587.07 (session +46.13) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,780.95 | ▲ 09:30 equity $12,689.60 vs yday $12,630.72 (+58.88) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 190 | $5.11 | $2.60 | $+32.84 | $16,749.25 | ▲ +32.84 after sell → book $12,687.00; vs 09:30 mark -2.60 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 298 | $3.64 | $3.90 | $+144.23 | $17,830.06 | ▲ +144.23 after sell → book $12,683.09; vs 09:30 mark -3.91 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,830.06 | ▼ close $12,653.78 vs 09:30 $12,689.60 (session -29.31) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,830.06 | ▲ 09:30 equity $12,679.06 vs yday $12,653.78 (+25.28) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 11 | $118.18 | $2.02 | $-62.90 | $16,528.06 | ▼ -62.90 after sell → book $12,677.04; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 353 | $3.98 | $4.55 | $-171.59 | $15,118.57 | ▼ -171.59 after sell → book $12,672.49; vs 09:30 mark -4.55 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 612 | $1.85 | $7.89 | $+94.22 | $13,978.47 | ▲ +94.22 after sell → book $12,664.59; vs 09:30 mark -7.90 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 49 | $24.42 | $2.14 | $+22.62 | $12,779.76 | ▲ +22.62 after sell → book $12,662.46; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 368 | $3.75 | $4.75 | $-149.44 | $11,395.01 | ▼ -149.44 after sell → book $12,657.71; vs 09:30 mark -4.75 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 633 | $1.80 | $8.17 | — | $10,247.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1139.50; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 48 | $23.29 | $2.13 | — | $9,127.39 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1139.50; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 77 | $14.62 | $2.22 | — | $7,999.43 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1139.50; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 169 | $18.61 | $2.64 | — | $11,141.88 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $3161.30; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 462 | $6.83 | $6.15 | — | $14,291.19 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $3161.30; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,291.19 | ▼ close $12,211.12 vs 09:30 $12,679.06 (session -425.28) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,291.19 | ▼ 09:30 equity $12,186.10 vs yday $12,211.12 (-25.02) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 48 | $24.09 | $2.15 | $+34.11 | $15,445.36 | ▲ +34.11 after sell → book $12,183.95; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 77 | $13.77 | $2.24 | $-69.91 | $16,503.41 | ▼ -69.91 after sell → book $12,181.71; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 134 | $36.76 | $2.39 | — | $11,575.17 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $4951.02; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 382 | $7.95 | $5.10 | — | $14,606.98 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $3044.83; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 37 | $81.00 | $2.22 | — | $17,601.76 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $3044.83; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,601.76 | ▲ close $12,743.60 vs 09:30 $12,186.10 (session +571.60) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,601.76 | ▲ 09:30 equity $12,902.36 vs yday $12,743.60 (+158.76) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 633 | $1.96 | $8.28 | $+84.83 | $18,834.16 | ▲ +84.83 after sell → book $12,894.08; vs 09:30 mark -8.28 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 134 | $39.50 | $2.46 | $+362.31 | $24,124.70 | ▲ +362.31 after sell → book $12,891.62; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 82 | $29.32 | $2.24 | — | $21,718.22 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $2412.47; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 794 | $3.04 | $10.24 | — | $19,298.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $2412.47; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 29 | $81.40 | $2.08 | — | $16,935.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $2412.47; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 186 | $34.44 | $2.81 | — | $23,338.55 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6438.53; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,338.55 | ▲ close $13,263.54 vs 09:30 $12,902.36 (session +389.28) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,338.55 | ▲ 09:30 equity $13,423.19 vs yday $13,263.54 (+159.65) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 345 | $3.55 | $4.52 | $+284.28 | $24,558.78 | ▲ +284.28 after sell → book $13,418.67; vs 09:30 mark -4.52 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 169 | $22.11 | $2.50 | $-596.63 | $20,819.69 | ▼ -596.63 after sell → book $13,416.17; vs 09:30 mark -2.50 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 462 | $6.55 | $5.96 | $+117.25 | $17,787.64 | ▲ +117.25 after sell → book $13,410.22; vs 09:30 mark -5.95 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 82 | $29.43 | $2.27 | $+4.52 | $20,198.63 | ▲ +4.52 after sell → book $13,407.95; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 794 | $4.00 | $10.40 | $+745.57 | $23,364.23 | ▲ +745.57 after sell → book $13,397.55; vs 09:30 mark -10.40 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 29 | $79.08 | $2.11 | $-71.46 | $25,655.44 | ▼ -71.46 after sell → book $13,395.44; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 779 | $2.47 | $10.05 | — | $23,721.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $1924.16; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 113 | $16.91 | $2.33 | — | $21,808.10 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1924.16; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1166 | $1.65 | $15.04 | — | $19,869.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $1924.16; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 164 | $11.67 | $2.48 | — | $17,952.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $1924.16; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 404 | $8.26 | $5.40 | — | $21,284.44 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3341.38; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $24,201.72 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3341.38; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,201.72 | ▼ close $13,320.56 vs 09:30 $13,423.19 (session -37.46) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,201.72 | ▼ 09:30 equity $13,267.20 vs yday $13,320.56 (-53.36) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 382 | $8.28 | $4.93 | $-134.18 | $21,035.75 | ▼ -134.18 after sell → book $13,262.28; vs 09:30 mark -4.92 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 230 | $9.11 | $2.97 | — | $18,937.48 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $2103.57; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 290 | $7.23 | $3.74 | — | $16,837.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $2103.57; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 35 | $93.97 | $2.22 | — | $20,123.77 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3313.89; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,123.77 | ▼ close $12,970.26 vs 09:30 $13,267.20 (session -283.09) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,123.77 | ▼ 09:30 equity $12,218.81 vs yday $12,970.26 (-751.45) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 37 | $82.00 | $2.10 | $-41.32 | $17,087.67 | ▼ -41.32 after sell → book $12,216.71; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 113 | $16.92 | $2.36 | $-3.56 | $18,997.26 | ▼ -3.56 after sell → book $12,214.34; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1166 | $1.41 | $15.25 | $-310.13 | $20,626.07 | ▼ -310.13 after sell → book $12,199.09; vs 09:30 mark -15.25 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 164 | $12.80 | $2.53 | $+180.31 | $22,722.75 | ▲ +180.31 after sell → book $12,196.57; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 230 | $8.39 | $3.02 | $-171.59 | $24,649.43 | ▼ -171.59 after sell → book $12,193.55; vs 09:30 mark -3.02 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 290 | $6.83 | $3.81 | $-123.55 | $26,626.32 | ▼ -123.55 after sell → book $12,189.74; vs 09:30 mark -3.81 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 986 | $2.70 | $12.72 | — | $23,951.40 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2662.63; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 63 | $41.76 | $2.18 | — | $21,318.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $2662.63; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 593 | $4.49 | $7.65 | — | $18,648.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $2662.63; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 52 | $116.85 | $2.37 | — | $24,721.95 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6083.60; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,721.95 | ▼ close $12,120.63 vs 09:30 $12,218.81 (session -44.19) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,721.95 | ▲ 09:30 equity $12,151.88 vs yday $12,120.63 (+31.25) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 779 | $2.68 | $10.20 | $+143.35 | $26,799.48 | ▲ +143.35 after sell → book $12,141.69; vs 09:30 mark -10.19 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $23,796.12 | ▼ -86.07 after sell → book $12,139.68; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 63 | $36.02 | $2.21 | $-365.69 | $26,063.49 | ▼ -365.69 after sell → book $12,137.47; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 593 | $3.92 | $7.77 | $-350.46 | $28,383.25 | ▼ -350.46 after sell → book $12,129.71; vs 09:30 mark -7.76 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,383.25 | ▲ close $13,944.61 vs 09:30 $12,151.88 (session +1,814.90) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,570.00 | ▲ 09:30 equity $11,358.49 vs yday $10,902.13 (+456.36) | 09:30 open · cash $17,570.00 (unchanged overnight, no fees) · equity $11,358.49 vs prior close $10,902.13 (+456.36) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 59 | $29.76 | $2.17 | — | $15,811.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $1757.00; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 108 | $16.21 | $2.31 | — | $14,059.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1757.00; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 112 | $15.58 | $2.33 | — | $12,311.59 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $1757.00; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 723 | $7.85 | $9.65 | — | $17,977.49 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $5675.84; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,977.49 | ▼ close $10,921.26 vs 09:30 $11,358.49 (session -420.77) | 16:00 close · cash $17,977.49 · equity $10,921.26 vs 09:30 $11,358.49 (-437.23; session marks -420.77) · 11 name(s) marked open→close (per-name table). AEHL×310 09:30 $9.05 → close $9.36 -96.10; BAND×41 09:30 $61.83 → close $61.83 -0.00; GLND×686 09:30 $6.06 → close $5.54 -356.72; HALO×20 09:30 $115.36 → close $113.90 +29.20; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; VICR×6 09:30 $276.06 → close $276.06 -0.00; TJGC×59 09:30 $29.76 → close $26.24 -207.68; SECZ×108 09:30 $16.21 → close $15.96 -27.00; USDE×112 09:30 $15.58 → close $17.25 +186.92; RSKD×723 09:30 $7.85 → close $7.78 +50.61 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
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
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 186 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6438.53; owner short_news_r_h3 |
| `AEHL` | 404 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3341.38; owner short_news_r_h3 |
| `USFD` | 35 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3313.89; owner short_news_r_h3 |
| `GLND` | 986 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2662.63; owner union_hot_n4_h1 |
| `HALO` | 52 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6083.60; owner short_news_r_h3 |
