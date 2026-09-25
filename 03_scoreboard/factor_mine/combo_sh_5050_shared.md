# Factor mine action — `combo_sh_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_hot_n4_h1 w=0.5,0.5 net=priority

Cash book **+21.84%** ($12,184) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 215 · skips 159 · realized $+2615.03.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, union_hot_n4_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_hot_n4_h1 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: union_hot_n4_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13,872.20.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1462 | $1.18 | $19.16 | — | $6,897.95 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1725.44; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 90 | $19.17 | $2.34 | — | $8,620.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1725.44; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 135 | $12.70 | $2.48 | — | $10,332.26 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1725.44; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,332.26 | ▼ close $10,223.22 vs 09:30 $10,412.10 (session -105.46) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,332.26 | ▼ 09:30 equity $10,025.50 vs yday $10,223.22 (-197.72) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $11,621.25 | ▲ +3.49 after sell → book $10,023.34; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 66 | $19.57 | $2.21 | $-4.40 | $12,910.66 | ▼ -4.40 after sell → book $10,021.13; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 589 | $2.08 | $7.71 | $-83.04 | $14,131.02 | ▼ -83.04 after sell → book $10,013.42; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 116 | $9.57 | $2.37 | $-184.51 | $15,238.77 | ▼ -184.51 after sell → book $10,011.05; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 454 | $4.19 | $5.86 | — | $13,330.66 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1904.85; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 277 | $6.87 | $3.57 | — | $11,424.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1904.85; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 139 | $13.64 | $2.41 | — | $9,525.73 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1904.85; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 46 | $41.23 | $2.13 | — | $7,627.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1904.85; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 869 | $1.15 | $11.39 | — | $8,614.98 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $999.71; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 280 | $3.56 | $3.70 | — | $9,608.08 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $999.71; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $10,588.65 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $999.71; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 332 | $3.01 | $4.38 | — | $11,583.60 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $999.71; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 147 | $6.80 | $2.49 | — | $12,580.70 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $999.71; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,580.70 | ▲ close $10,127.37 vs 09:30 $10,025.50 (session +154.36) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,580.70 | ▲ 09:30 equity $10,290.66 vs yday $10,127.37 (+163.29) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 454 | $3.94 | $5.95 | $-125.30 | $14,363.52 | ▼ -125.30 after sell → book $10,284.72; vs 09:30 mark -5.94 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 139 | $13.31 | $2.44 | $-50.72 | $16,211.16 | ▼ -50.72 after sell → book $10,282.27; vs 09:30 mark -2.45 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 46 | $41.50 | $2.15 | $+8.14 | $18,118.01 | ▲ +8.14 after sell → book $10,280.12; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,118.01 | ▲ close $10,405.61 vs 09:30 $10,290.66 (session +125.50) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,118.01 | ▼ 09:30 equity $10,404.58 vs yday $10,405.61 (-1.03) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1462 | $1.07 | $18.86 | $+122.80 | $16,534.81 | ▲ +122.80 after sell → book $10,385.72; vs 09:30 mark -18.86 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 135 | $11.75 | $2.40 | $+122.70 | $14,946.16 | ▲ +122.70 after sell → book $10,383.32; vs 09:30 mark -2.40 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 277 | $7.19 | $3.64 | $+81.43 | $16,934.16 | ▲ +81.43 after sell → book $10,379.69; vs 09:30 mark -3.63 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,934.16 | ▲ close $10,429.54 vs 09:30 $10,404.58 (session +49.85) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,934.16 | ▼ 09:30 equity $10,372.97 vs yday $10,429.54 (-56.57) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 90 | $18.13 | $2.26 | $+89.00 | $15,300.20 | ▲ +89.00 after sell → book $10,370.71; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 869 | $0.96 | $10.98 | $+140.14 | $14,452.38 | ▲ +140.14 after sell → book $10,359.74; vs 09:30 mark -10.97 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 280 | $4.01 | $3.61 | $-134.71 | $13,324.57 | ▼ -134.71 after sell → book $10,356.13; vs 09:30 mark -3.61 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,334.51 | ▼ -9.48 after sell → book $10,354.04; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 332 | $2.95 | $4.28 | $+11.26 | $11,350.83 | ▲ +11.26 after sell → book $10,349.76; vs 09:30 mark -4.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 147 | $6.81 | $2.43 | $-6.39 | $10,347.33 | ▼ -6.39 after sell → book $10,347.33; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 109 | $11.81 | $2.32 | — | $9,057.18 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1293.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,854.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1293.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1124 | $1.15 | $14.50 | — | $6,546.94 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1293.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 944 | $1.37 | $12.18 | — | $5,241.49 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1293.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $5,852.80 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 34 | $21.40 | $2.13 | — | $6,578.27 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 166 | $4.43 | $2.55 | — | $7,311.10 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 4 | $173.90 | $2.04 | — | $8,004.66 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 15 | $46.85 | $2.07 | — | $8,705.34 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $9,341.57 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 159 | $4.61 | $2.52 | — | $10,072.04 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $736.88; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,072.04 | ▲ close $10,318.36 vs 09:30 $10,372.97 (session +17.45) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,072.04 | ▲ 09:30 equity $10,424.80 vs yday $10,318.36 (+106.44) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 109 | $11.57 | $2.35 | $-31.37 | $11,330.82 | ▼ -31.37 after sell → book $10,422.45; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 944 | $1.46 | $12.35 | $+60.44 | $12,696.72 | ▲ +60.44 after sell → book $10,410.11; vs 09:30 mark -12.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 706 | $4.49 | $9.11 | — | $9,517.67 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $3174.18; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 466 | $6.81 | $6.01 | — | $6,338.20 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $3174.18; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 334 | $3.11 | $4.40 | — | $7,372.54 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $1039.50; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $8,350.57 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1039.50; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 27 | $38.40 | $2.12 | — | $9,385.25 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1039.50; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 49 | $20.90 | $2.19 | — | $10,407.16 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1039.50; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 38 | $27.00 | $2.15 | — | $11,431.01 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $1039.50; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,431.01 | ▼ close $10,351.50 vs 09:30 $10,424.80 (session -30.56) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,431.01 | ▲ 09:30 equity $11,572.92 vs yday $10,351.50 (+1,221.42) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $12,570.58 | ▼ -63.57 after sell → book $11,570.89; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1124 | $1.83 | $14.70 | $+735.12 | $14,612.79 | ▲ +735.12 after sell → book $11,556.18; vs 09:30 mark -14.71 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 706 | $4.32 | $9.25 | $-138.38 | $17,653.46 | ▼ -138.38 after sell → book $11,546.93; vs 09:30 mark -9.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 466 | $8.03 | $6.12 | $+556.39 | $21,389.32 | ▲ +556.39 after sell → book $11,540.81; vs 09:30 mark -6.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,389.32 | ▲ close $11,581.79 vs 09:30 $11,572.92 (session +40.98) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,389.32 | ▲ 09:30 equity $11,689.72 vs yday $11,581.79 (+107.93) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $20,751.33 | ▼ -26.68 after sell → book $11,687.73; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 34 | $20.90 | $2.09 | $+12.78 | $20,038.63 | ▲ +12.78 after sell → book $11,685.63; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 166 | $4.42 | $2.49 | $-3.37 | $19,302.43 | ▼ -3.37 after sell → book $11,683.15; vs 09:30 mark -2.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 4 | $170.64 | $2.00 | $+9.00 | $18,617.86 | ▲ +9.00 after sell → book $11,681.14; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 15 | $43.63 | $2.04 | $+44.19 | $17,961.38 | ▲ +44.19 after sell → book $11,679.11; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $17,325.89 | ▲ +0.75 after sell → book $11,677.10; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 159 | $4.77 | $2.47 | $-30.43 | $16,564.99 | ▼ -30.43 after sell → book $11,674.63; vs 09:30 mark -2.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 85 | $24.11 | $2.25 | — | $14,513.40 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $2070.62; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1327 | $1.56 | $17.12 | — | $12,426.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $2070.62; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 508 | $4.07 | $6.55 | — | $10,352.05 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $2070.62; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 108 | $19.04 | $2.31 | — | $8,293.41 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $2070.62; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 106 | $13.62 | $2.38 | — | $9,735.29 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1455.80; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 26 | $54.51 | $2.13 | — | $11,150.42 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1455.80; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 8 | $175.01 | $2.07 | — | $12,548.42 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1455.80; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $13,639.43 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1455.80; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,639.43 | ▲ close $12,281.06 vs 09:30 $11,689.72 (session +643.28) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,639.43 | ▼ 09:30 equity $12,077.20 vs yday $12,281.06 (-203.86) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 334 | $2.83 | $4.31 | $+84.81 | $12,689.90 | ▲ +84.81 after sell → book $12,072.89; vs 09:30 mark -4.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $11,717.23 | ▲ +5.37 after sell → book $12,070.86; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 27 | $38.41 | $2.07 | $-4.46 | $10,678.09 | ▼ -4.46 after sell → book $12,068.79; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 49 | $20.50 | $2.14 | $+15.28 | $9,671.46 | ▲ +15.28 after sell → book $12,066.66; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 38 | $26.00 | $2.10 | $+33.74 | $8,681.35 | ▲ +33.74 after sell → book $12,064.55; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 85 | $26.61 | $2.28 | $+207.98 | $10,940.93 | ▲ +207.98 after sell → book $12,062.28; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1327 | $1.60 | $17.36 | $+18.61 | $13,046.77 | ▲ +18.61 after sell → book $12,044.92; vs 09:30 mark -17.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 108 | $20.72 | $2.35 | $+176.78 | $15,282.18 | ▲ +176.78 after sell → book $12,042.57; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 180 | $14.11 | $2.53 | — | $12,739.85 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $2547.03; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 438 | $5.81 | $5.65 | — | $10,189.42 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $2547.03; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 219 | $11.59 | $2.83 | — | $7,649.48 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $2547.03; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $8,717.13 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1203.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 98 | $12.22 | $2.34 | — | $9,912.34 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1203.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 236 | $5.08 | $3.13 | — | $11,108.10 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1203.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 9 | $132.64 | $2.07 | — | $12,299.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1203.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 6 | $199.94 | $2.06 | — | $13,497.37 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1203.16; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,497.37 | ▲ close $12,053.22 vs 09:30 $12,077.20 (session +33.30) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,497.37 | ▲ 09:30 equity $12,211.91 vs yday $12,053.22 (+158.69) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 180 | $14.20 | $2.58 | $+11.09 | $16,050.78 | ▲ +11.09 after sell → book $12,209.32; vs 09:30 mark -2.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 438 | $6.50 | $5.75 | $+290.82 | $18,892.04 | ▲ +290.82 after sell → book $12,203.58; vs 09:30 mark -5.74 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 219 | $12.18 | $2.88 | $+124.60 | $21,556.58 | ▲ +124.60 after sell → book $12,200.70; vs 09:30 mark -2.88 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 390 | $9.19 | $5.03 | — | $17,967.45 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $3592.76; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 24 | $144.18 | $2.06 | — | $14,505.06 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $3592.76; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 194 | $18.50 | $2.57 | — | $10,913.49 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $3592.76; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 40 | $74.54 | $2.23 | — | $13,892.87 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $3047.76; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 55 | $55.25 | $2.27 | — | $16,929.34 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $3047.76; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,929.34 | ▼ close $12,175.51 vs 09:30 $12,211.91 (session -11.02) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,929.34 | ▼ 09:30 equity $11,870.25 vs yday $12,175.51 (-305.26) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 508 | $3.69 | $6.65 | $-206.25 | $18,797.21 | ▼ -206.25 after sell → book $11,863.60; vs 09:30 mark -6.65 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 106 | $13.90 | $2.31 | $-33.83 | $17,321.50 | ▼ -33.83 after sell → book $11,861.29; vs 09:30 mark -2.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 26 | $52.49 | $2.07 | $+48.32 | $15,954.69 | ▲ +48.32 after sell → book $11,859.22; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 8 | $172.76 | $2.01 | $+13.91 | $14,570.60 | ▲ +13.91 after sell → book $11,857.21; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $13,525.14 | ▲ +45.54 after sell → book $11,855.21; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 194 | $18.15 | $2.63 | $-73.10 | $17,043.61 | ▼ -73.10 after sell → book $11,852.57; vs 09:30 mark -2.64 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 304 | $14.00 | $3.92 | — | $12,783.69 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $4260.90; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 29 | $146.07 | $2.08 | — | $8,545.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $4260.90; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 11 | $252.24 | $2.13 | — | $11,318.09 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2961.64; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 98 | $30.18 | $2.40 | — | $14,273.32 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2961.64; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,273.32 | ▲ close $12,284.04 vs 09:30 $11,870.25 (session +442.01) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,273.32 | ▼ 09:30 equity $12,239.29 vs yday $12,284.04 (-44.75) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $13,226.92 | ▲ +21.24 after sell → book $12,237.29; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 98 | $11.10 | $2.28 | $+105.13 | $12,136.84 | ▲ +105.13 after sell → book $12,235.01; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 236 | $4.97 | $3.04 | $+18.61 | $10,959.69 | ▲ +18.61 after sell → book $12,231.96; vs 09:30 mark -3.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 9 | $127.45 | $2.02 | $+42.62 | $9,810.62 | ▲ +42.62 after sell → book $12,229.94; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 6 | $254.39 | $2.01 | $-330.77 | $8,282.28 | ▼ -330.77 after sell → book $12,227.94; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 390 | $9.50 | $5.13 | $+110.74 | $11,982.15 | ▲ +110.74 after sell → book $12,222.81; vs 09:30 mark -5.13 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 24 | $134.10 | $2.10 | $-246.08 | $15,198.45 | ▼ -246.08 after sell → book $12,220.71; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 29 | $148.03 | $2.12 | $+52.64 | $19,489.20 | ▲ +52.64 after sell → book $12,218.59; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,489.20 | ▼ close $12,124.04 vs 09:30 $12,239.29 (session -94.55) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,489.20 | ▲ 09:30 equity $12,219.89 vs yday $12,124.04 (+95.85) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 40 | $73.22 | $2.11 | $+48.46 | $16,558.29 | ▲ +48.46 after sell → book $12,217.78; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 55 | $54.76 | $2.15 | $+22.52 | $13,544.34 | ▲ +22.52 after sell → book $12,215.63; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 304 | $13.04 | $4.00 | $-299.77 | $17,504.49 | ▼ -299.77 after sell → book $12,211.62; vs 09:30 mark -4.01 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,504.49 | ▲ close $12,228.04 vs 09:30 $12,219.89 (session +16.42) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,504.49 | ▲ 09:30 equity $12,287.24 vs yday $12,228.04 (+59.20) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 11 | $235.71 | $2.02 | $+177.68 | $14,909.66 | ▲ +177.68 after sell → book $12,285.22; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 98 | $26.78 | $2.28 | $+328.51 | $12,282.94 | ▲ +328.51 after sell → book $12,282.94; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,282.94 | ▲ close $12,282.94 vs 09:30 $12,287.24 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,282.94 | ▲ 09:30 equity $12,282.94 vs yday $12,282.94 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 862 | $1.78 | $11.12 | — | $10,737.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1535.37; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 83 | $18.40 | $2.24 | — | $9,208.02 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1535.37; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 111 | $13.71 | $2.32 | — | $7,683.88 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1535.37; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 64 | $23.88 | $2.18 | — | $6,153.38 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1535.37; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 206 | $14.85 | $2.80 | — | $9,209.68 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $3066.27; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1793 | $1.71 | $23.54 | — | $12,252.18 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $3066.27; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,252.18 | ▼ close $12,106.09 vs 09:30 $12,282.94 (session -132.65) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,252.18 | ▲ 09:30 equity $12,237.29 vs yday $12,106.09 (+131.20) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 83 | $18.15 | $2.26 | $-25.25 | $13,756.36 | ▼ -25.25 after sell → book $12,235.02; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 111 | $13.89 | $2.35 | $+15.30 | $15,295.80 | ▲ +15.30 after sell → book $12,232.67; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 64 | $23.84 | $2.20 | $-6.95 | $16,819.35 | ▼ -6.95 after sell → book $12,230.46; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 111 | $25.18 | $2.32 | — | $14,022.05 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $2803.23; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 356 | $7.87 | $4.59 | — | $11,215.74 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $2803.23; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 484 | $5.79 | $6.24 | — | $8,407.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $2803.23; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 654 | $4.67 | $8.65 | — | $11,452.66 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $3054.33; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 39 | $76.55 | $2.22 | — | $14,435.89 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3054.33; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,435.89 | ▲ close $12,573.19 vs 09:30 $12,237.29 (session +366.76) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,435.89 | ▼ 09:30 equity $12,342.87 vs yday $12,573.19 (-230.32) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 862 | $1.56 | $11.27 | $-207.72 | $15,773.64 | ▼ -207.72 after sell → book $12,331.59; vs 09:30 mark -11.28 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 111 | $26.44 | $2.36 | $+135.17 | $18,706.12 | ▲ +135.17 after sell → book $12,329.23; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 356 | $7.76 | $4.67 | $-48.43 | $21,464.01 | ▼ -48.43 after sell → book $12,324.56; vs 09:30 mark -4.67 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 484 | $5.81 | $6.35 | $-2.91 | $24,269.70 | ▼ -2.91 after sell → book $12,318.21; vs 09:30 mark -6.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,269.70 | ▲ close $12,626.35 vs 09:30 $12,342.87 (session +308.14) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,269.70 | ▲ 09:30 equity $12,666.72 vs yday $12,626.35 (+40.37) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 206 | $13.60 | $2.66 | $+252.04 | $21,465.44 | ▲ +252.04 after sell → book $12,664.06; vs 09:30 mark -2.66 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1793 | $1.58 | $23.13 | $+186.42 | $18,609.37 | ▲ +186.42 after sell → book $12,640.93; vs 09:30 mark -23.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,609.37 | ▲ close $12,671.47 vs 09:30 $12,666.72 (session +30.54) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,609.37 | ▲ 09:30 equity $12,763.12 vs yday $12,671.47 (+91.65) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 654 | $4.36 | $8.44 | $+185.65 | $15,749.50 | ▲ +185.65 after sell → book $12,754.69; vs 09:30 mark -8.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 39 | $76.79 | $2.11 | $-13.69 | $12,752.58 | ▼ -13.69 after sell → book $12,752.58; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,752.58 | ▲ close $12,752.58 vs 09:30 $12,763.12 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,752.58 | ▲ 09:30 equity $12,752.58 vs yday $12,752.58 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 590 | $2.70 | $7.61 | — | $11,151.97 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1594.07; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 324 | $4.91 | $4.18 | — | $9,556.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1594.07; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 258 | $6.16 | $3.33 | — | $7,964.34 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1594.07; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 509 | $3.13 | $6.57 | — | $6,364.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1594.07; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 11 | $112.83 | $2.08 | — | $7,603.71 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1272.92; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 361 | $3.52 | $4.76 | — | $8,869.67 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1272.92; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 627 | $2.03 | $8.24 | — | $10,134.24 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1272.92; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 50 | $24.97 | $2.19 | — | $11,380.55 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1272.92; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 377 | $3.37 | $4.97 | — | $12,646.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1272.92; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,646.07 | ▲ close $12,861.53 vs 09:30 $12,752.58 (session +152.87) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,646.07 | ▲ 09:30 equity $12,990.43 vs yday $12,861.53 (+128.90) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 258 | $6.02 | $3.38 | $-42.83 | $14,195.84 | ▼ -42.83 after sell → book $12,987.04; vs 09:30 mark -3.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,195.84 | ▲ close $13,170.71 vs 09:30 $12,990.43 (session +183.67) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,195.84 | ▲ 09:30 equity $13,272.02 vs yday $13,170.71 (+101.31) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 324 | $5.11 | $4.25 | $+56.37 | $15,847.24 | ▲ +56.37 after sell → book $13,267.78; vs 09:30 mark -4.24 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 509 | $3.64 | $6.67 | $+246.36 | $17,693.33 | ▲ +246.36 after sell → book $13,261.11; vs 09:30 mark -6.67 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,693.33 | ▲ close $13,290.55 vs 09:30 $13,272.02 (session +29.44) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,693.33 | ▲ 09:30 equity $13,321.27 vs yday $13,290.55 (+30.72) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 11 | $118.18 | $2.02 | $-62.90 | $16,391.33 | ▼ -62.90 after sell → book $13,319.25; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 361 | $3.98 | $4.66 | $-175.48 | $14,949.89 | ▼ -175.48 after sell → book $13,314.59; vs 09:30 mark -4.66 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 627 | $1.85 | $8.09 | $+96.53 | $13,781.85 | ▲ +96.53 after sell → book $13,306.50; vs 09:30 mark -8.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 50 | $24.42 | $2.14 | $+23.17 | $12,558.71 | ▲ +23.17 after sell → book $13,304.36; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 377 | $3.75 | $4.86 | $-153.09 | $11,140.10 | ▼ -153.09 after sell → book $13,299.50; vs 09:30 mark -4.86 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1031 | $1.80 | $13.30 | — | $9,271.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1856.68; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 79 | $23.29 | $2.23 | — | $7,428.86 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1856.68; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 126 | $14.62 | $2.37 | — | $5,584.38 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1856.68; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 150 | $18.61 | $2.56 | — | $8,373.31 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2792.19; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 408 | $6.83 | $5.43 | — | $11,154.52 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2792.19; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,154.52 | ▼ close $12,898.68 vs 09:30 $13,321.27 (session -374.93) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,154.52 | ▼ 09:30 equity $12,891.91 vs yday $12,898.68 (-6.77) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 79 | $24.09 | $2.26 | $+58.72 | $13,055.38 | ▲ +58.72 after sell → book $12,889.66; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 126 | $13.77 | $2.40 | $-111.87 | $14,787.99 | ▼ -111.87 after sell → book $12,887.25; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 201 | $36.76 | $2.60 | — | $7,396.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $7394.00; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 405 | $7.95 | $5.41 | — | $10,610.98 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $3221.16; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 39 | $81.00 | $2.23 | — | $13,767.75 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $3221.16; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,767.75 | ▲ close $13,656.50 vs 09:30 $12,891.91 (session +779.48) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,767.75 | ▲ 09:30 equity $13,864.39 vs yday $13,656.50 (+207.89) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1031 | $1.96 | $13.49 | $+138.17 | $15,775.02 | ▲ +138.17 after sell → book $13,850.90; vs 09:30 mark -13.49 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 201 | $39.50 | $2.69 | $+545.45 | $23,711.83 | ▲ +545.45 after sell → book $13,848.21; vs 09:30 mark -2.69 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 134 | $29.32 | $2.39 | — | $19,780.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $3951.97; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1302 | $3.04 | $16.80 | — | $15,812.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $3951.97; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 48 | $81.40 | $2.13 | — | $11,902.86 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $3951.97; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 200 | $34.44 | $2.87 | — | $18,787.99 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6913.44; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,787.99 | ▲ close $14,377.89 vs 09:30 $13,864.39 (session +553.87) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,787.99 | ▲ 09:30 equity $14,767.98 vs yday $14,377.89 (+390.09) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 590 | $3.55 | $7.73 | $+486.16 | $20,874.76 | ▲ +486.16 after sell → book $14,760.25; vs 09:30 mark -7.73 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 150 | $22.11 | $2.44 | $-530.00 | $17,555.82 | ▼ -530.00 after sell → book $14,757.81; vs 09:30 mark -2.44 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 408 | $6.55 | $5.26 | $+103.55 | $14,878.16 | ▲ +103.55 after sell → book $14,752.55; vs 09:30 mark -5.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 134 | $29.43 | $2.45 | $+9.90 | $18,819.34 | ▲ +9.90 after sell → book $14,750.11; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 1302 | $4.00 | $17.05 | $+1222.58 | $24,010.28 | ▲ +1,222.58 after sell → book $14,733.05; vs 09:30 mark -17.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 48 | $79.08 | $2.17 | $-115.67 | $27,803.95 | ▼ -115.67 after sell → book $14,730.88; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1407 | $2.47 | $18.15 | — | $24,310.51 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $3475.49; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 205 | $16.91 | $2.64 | — | $20,841.31 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $3475.49; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 2106 | $1.65 | $27.17 | — | $17,339.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $3475.49; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 297 | $11.67 | $3.83 | — | $13,869.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $3475.49; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 444 | $8.26 | $5.93 | — | $17,530.93 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3669.77; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 6 | $583.88 | $2.14 | — | $21,032.07 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3669.77; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,032.07 | ▲ close $14,803.04 vs 09:30 $14,767.98 (session +132.03) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,032.07 | ▼ 09:30 equity $14,686.24 vs yday $14,803.04 (-116.80) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 405 | $8.28 | $5.22 | $-142.26 | $17,675.47 | ▼ -142.26 after sell → book $14,681.01; vs 09:30 mark -5.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 323 | $9.11 | $4.17 | — | $14,728.78 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $2945.91; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 407 | $7.23 | $5.25 | — | $11,780.91 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $2945.91; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 39 | $93.97 | $2.25 | — | $15,443.50 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3667.90; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,443.50 | ▼ close $14,291.69 vs 09:30 $14,686.24 (session -377.66) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,443.50 | ▼ 09:30 equity $13,525.27 vs yday $14,291.69 (-766.42) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 39 | $82.00 | $2.11 | $-43.34 | $12,243.39 | ▼ -43.34 after sell → book $13,523.16; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 205 | $16.92 | $2.71 | $-3.30 | $15,709.28 | ▼ -3.30 after sell → book $13,520.45; vs 09:30 mark -2.71 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 2106 | $1.41 | $27.54 | $-560.15 | $18,651.20 | ▼ -560.15 after sell → book $13,492.91; vs 09:30 mark -27.54 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 297 | $12.80 | $3.91 | $+327.87 | $22,448.89 | ▲ +327.87 after sell → book $13,489.00; vs 09:30 mark -3.91 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 323 | $8.39 | $4.24 | $-240.97 | $25,154.62 | ▼ -240.97 after sell → book $13,484.76; vs 09:30 mark -4.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 407 | $6.83 | $5.34 | $-173.39 | $27,929.09 | ▼ -173.39 after sell → book $13,479.42; vs 09:30 mark -5.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1724 | $2.70 | $22.24 | — | $23,252.05 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $4654.85; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 111 | $41.76 | $2.32 | — | $18,614.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $4654.85; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 1036 | $4.49 | $13.36 | — | $13,949.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $4654.85; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 57 | $116.85 | $2.41 | — | $20,607.41 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6720.75; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,607.41 | ▼ close $13,186.83 vs 09:30 $13,525.27 (session -252.26) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,607.41 | ▼ 09:30 equity $13,171.48 vs yday $13,186.83 (-15.35) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1407 | $2.68 | $18.41 | $+258.91 | $24,359.75 | ▲ +258.91 after sell → book $13,153.07; vs 09:30 mark -18.41 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 6 | $600.27 | $2.01 | $-102.49 | $20,756.13 | ▼ -102.49 after sell → book $13,151.06; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 111 | $36.02 | $2.37 | $-641.28 | $24,752.53 | ▼ -641.28 after sell → book $13,148.69; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 1036 | $3.92 | $13.57 | $-612.27 | $28,805.26 | ▼ -612.27 after sell → book $13,135.12; vs 09:30 mark -13.57 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,805.26 | ▲ close $16,491.90 vs 09:30 $13,171.48 (session +3,356.78) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,888.74 | ▲ 09:30 equity $12,880.43 vs yday $12,090.29 (+790.14) | 09:30 open · cash $14,888.74 (unchanged overnight, no fees) · equity $12,880.43 vs prior close $12,090.29 (+790.14) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 83 | $29.76 | $2.24 | — | $12,416.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $2481.46; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 153 | $16.21 | $2.45 | — | $9,933.84 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $2481.46; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 159 | $15.58 | $2.47 | — | $7,453.98 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $2481.46; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 819 | $7.85 | $10.93 | — | $13,872.20 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $6436.64; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,872.20 | ▼ close $12,183.91 vs 09:30 $12,880.43 (session -678.43) | 16:00 close · cash $13,872.20 · equity $12,183.91 vs 09:30 $12,880.43 (-696.52; session marks -678.43) · 11 name(s) marked open→close (per-name table). AEHL×317 09:30 $9.05 → close $9.36 -98.27; BAND×42 09:30 $61.83 → close $61.83 -0.00; GLND×1157 09:30 $6.06 → close $5.54 -601.64; HALO×20 09:30 $115.36 → close $113.90 +29.20; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×24 09:30 $93.82 → close $93.82 +0.00; VICR×11 09:30 $276.06 → close $276.06 -0.00; TJGC×83 09:30 $29.76 → close $26.24 -292.16; SECZ×153 09:30 $16.21 → close $15.96 -38.25; USDE×159 09:30 $15.58 → close $17.25 +265.36; RSKD×819 09:30 $7.85 → close $7.78 +57.33 | — |

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
| `FIVN` | 200 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6913.44; owner short_news_r_h3 |
| `AEHL` | 444 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3669.77; owner short_news_r_h3 |
| `USFD` | 39 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3667.90; owner short_news_r_h3 |
| `GLND` | 1724 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $4654.85; owner union_hot_n4_h1 |
| `HALO` | 57 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6720.75; owner short_news_r_h3 |
