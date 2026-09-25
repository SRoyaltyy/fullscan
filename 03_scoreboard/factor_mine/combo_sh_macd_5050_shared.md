# Factor mine action — `combo_sh_macd_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_macd_h3/union_hot_n4_h1 w=0.5,0.5 net=priority

Cash book **+19.92%** ($11,992) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 193 · skips 122 · realized $+3569.64.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_macd_h3 50%, union_hot_n4_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_macd_h3 50%, union_hot_n4_h1 50%.
- Member: short_news_r_macd_h3 (50% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13,751.78.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1462 | $1.18 | $19.16 | — | $6,897.95 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1725.44; owner short_news_r_macd_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 90 | $19.17 | $2.34 | — | $8,620.91 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1725.44; owner short_news_r_macd_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 135 | $12.70 | $2.48 | — | $10,332.26 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1725.44; owner short_news_r_macd_h3 | — |
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
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1448 | $1.15 | $18.98 | — | $9,273.24 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ⚪; ret5=-12.2; combo leftover $1666.18; owner short_news_r_macd_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 553 | $3.01 | $7.28 | — | $10,930.49 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; ⚪; ret5=-5.3; combo leftover $1666.18; owner short_news_r_macd_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 245 | $6.80 | $3.26 | — | $12,593.23 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight; ⚪; ret5=+10.4; combo leftover $1666.18; owner short_news_r_macd_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,593.23 | ▲ close $10,211.30 vs 09:30 $10,025.50 (session +243.73) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,593.23 | ▲ 09:30 equity $10,440.09 vs yday $10,211.30 (+228.79) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 454 | $3.94 | $5.95 | $-125.30 | $14,376.04 | ▼ -125.30 after sell → book $10,434.14; vs 09:30 mark -5.95 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 139 | $13.31 | $2.44 | $-50.72 | $16,223.69 | ▼ -50.72 after sell → book $10,431.70; vs 09:30 mark -2.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 46 | $41.50 | $2.15 | $+8.14 | $18,130.53 | ▲ +8.14 after sell → book $10,429.54; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,130.53 | ▲ close $10,575.80 vs 09:30 $10,440.09 (session +146.26) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,130.53 | ▲ 09:30 equity $10,602.23 vs yday $10,575.80 (+26.43) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1462 | $1.07 | $18.86 | $+122.80 | $16,547.33 | ▲ +122.80 after sell → book $10,583.37; vs 09:30 mark -18.86 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 135 | $11.75 | $2.40 | $+122.70 | $14,958.69 | ▲ +122.70 after sell → book $10,580.98; vs 09:30 mark -2.39 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 277 | $7.19 | $3.64 | $+81.43 | $16,946.68 | ▲ +81.43 after sell → book $10,577.34; vs 09:30 mark -3.64 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,946.68 | ▲ close $10,655.65 vs 09:30 $10,602.23 (session +78.31) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,946.68 | ▼ 09:30 equity $10,620.76 vs yday $10,655.65 (-34.89) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 90 | $18.13 | $2.26 | $+89.00 | $15,312.72 | ▲ +89.00 after sell → book $10,618.50; vs 09:30 mark -2.26 | short_news_r_macd_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1448 | $0.96 | $18.29 | $+233.51 | $13,900.01 | ▲ +233.51 after sell → book $10,600.21; vs 09:30 mark -18.29 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 553 | $2.95 | $7.13 | $+18.76 | $12,261.53 | ▲ +18.76 after sell → book $10,593.08; vs 09:30 mark -7.13 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 245 | $6.81 | $3.16 | $-8.87 | $10,589.92 | ▼ -8.87 after sell → book $10,589.92; vs 09:30 mark -3.16 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 112 | $11.81 | $2.33 | — | $9,264.31 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1323.74; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,061.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1323.74; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1151 | $1.15 | $14.85 | — | $6,722.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1323.74; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 966 | $1.37 | $12.46 | — | $5,386.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1323.74; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $5,998.11 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 35 | $21.40 | $2.14 | — | $6,744.98 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-25.2; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 170 | $4.43 | $2.56 | — | $7,495.52 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-23.1; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 4 | $173.90 | $2.04 | — | $8,189.08 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 16 | $46.85 | $2.08 | — | $8,936.60 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=+5.0; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 7 | $106.38 | $2.05 | — | $9,679.21 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=-1.7; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 163 | $4.61 | $2.54 | — | $10,428.10 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $754.16; owner short_news_r_macd_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,428.10 | ▲ close $10,569.51 vs 09:30 $10,620.76 (session +26.69) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,428.10 | ▲ 09:30 equity $10,678.81 vs yday $10,569.51 (+109.30) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 112 | $11.57 | $2.35 | $-32.12 | $11,721.58 | ▼ -32.12 after sell → book $10,676.45; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 966 | $1.46 | $12.63 | $+61.85 | $13,119.31 | ▲ +61.85 after sell → book $10,663.82; vs 09:30 mark -12.63 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 730 | $4.49 | $9.42 | — | $9,832.19 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $3279.83; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 481 | $6.81 | $6.20 | — | $6,550.38 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $3279.83; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 14 | $89.10 | $2.09 | — | $7,795.69 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1331.02; owner short_news_r_macd_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 34 | $38.40 | $2.15 | — | $9,099.14 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $1331.02; owner short_news_r_macd_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 63 | $20.90 | $2.24 | — | $10,413.61 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1331.02; owner short_news_r_macd_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 49 | $27.00 | $2.19 | — | $11,734.41 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+10.1; combo leftover $1331.02; owner short_news_r_macd_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,734.41 | ▼ close $10,568.22 vs 09:30 $10,678.81 (session -71.31) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,734.41 | ▲ 09:30 equity $11,818.24 vs yday $10,568.22 (+1,250.02) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $12,873.98 | ▼ -63.57 after sell → book $11,816.21; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1151 | $1.83 | $15.06 | $+752.78 | $14,965.25 | ▲ +752.78 after sell → book $11,801.15; vs 09:30 mark -15.06 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 730 | $4.32 | $9.56 | $-143.08 | $18,109.29 | ▼ -143.08 after sell → book $11,791.59; vs 09:30 mark -9.56 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 481 | $8.03 | $6.32 | $+574.30 | $21,965.41 | ▲ +574.30 after sell → book $11,785.28; vs 09:30 mark -6.31 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,965.41 | ▼ close $11,766.50 vs 09:30 $11,818.24 (session -18.78) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,965.41 | ▲ 09:30 equity $11,899.59 vs yday $11,766.50 (+133.09) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $21,327.41 | ▼ -26.68 after sell → book $11,897.59; vs 09:30 mark -2.00 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 35 | $20.90 | $2.10 | $+13.27 | $20,593.81 | ▲ +13.27 after sell → book $11,895.49; vs 09:30 mark -2.10 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 170 | $4.42 | $2.50 | $-3.36 | $19,839.91 | ▼ -3.36 after sell → book $11,892.99; vs 09:30 mark -2.50 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 4 | $170.64 | $2.00 | $+9.00 | $19,155.35 | ▲ +9.00 after sell → book $11,890.99; vs 09:30 mark -2.00 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 16 | $43.63 | $2.04 | $+47.40 | $18,455.23 | ▲ +47.40 after sell → book $11,888.95; vs 09:30 mark -2.04 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 7 | $105.58 | $2.01 | $+1.54 | $17,714.16 | ▲ +1.54 after sell → book $11,886.94; vs 09:30 mark -2.01 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 163 | $4.77 | $2.48 | $-31.10 | $16,934.17 | ▼ -31.10 after sell → book $11,884.46; vs 09:30 mark -2.48 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 87 | $24.11 | $2.25 | — | $14,834.35 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $2116.77; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1356 | $1.56 | $17.49 | — | $12,701.50 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $2116.77; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 520 | $4.07 | $6.71 | — | $10,578.39 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $2116.77; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 111 | $19.04 | $2.32 | — | $8,462.63 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $2116.77; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 145 | $13.62 | $2.52 | — | $10,435.73 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1975.95; owner short_news_r_macd_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 36 | $54.51 | $2.18 | — | $12,395.92 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+15.1; combo leftover $1975.95; owner short_news_r_macd_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 5 | $364.35 | $2.08 | — | $14,215.59 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1975.95; owner short_news_r_macd_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,215.59 | ▲ close $12,486.64 vs 09:30 $11,899.59 (session +637.73) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,215.59 | ▼ 09:30 equity $12,378.26 vs yday $12,486.64 (-108.38) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 14 | $88.24 | $2.03 | $+7.92 | $12,978.19 | ▲ +7.92 after sell → book $12,376.22; vs 09:30 mark -2.04 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 34 | $38.41 | $2.09 | $-4.58 | $11,670.16 | ▼ -4.58 after sell → book $12,374.13; vs 09:30 mark -2.09 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 63 | $20.50 | $2.18 | $+20.78 | $10,376.48 | ▲ +20.78 after sell → book $12,371.95; vs 09:30 mark -2.18 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 49 | $26.00 | $2.14 | $+44.67 | $9,100.35 | ▲ +44.67 after sell → book $12,369.82; vs 09:30 mark -2.13 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 87 | $26.61 | $2.28 | $+212.96 | $11,413.13 | ▲ +212.96 after sell → book $12,367.53; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1356 | $1.60 | $17.73 | $+19.01 | $13,565.00 | ▲ +19.01 after sell → book $12,349.80; vs 09:30 mark -17.73 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 111 | $20.72 | $2.36 | $+181.80 | $15,862.56 | ▲ +181.80 after sell → book $12,347.44; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 187 | $14.11 | $2.55 | — | $13,221.44 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $2643.76; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 455 | $5.81 | $5.87 | — | $10,572.02 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $2643.76; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 228 | $11.59 | $2.94 | — | $7,927.70 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $2643.76; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $8,995.34 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1233.61; owner short_news_r_macd_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 100 | $12.22 | $2.35 | — | $10,214.99 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1233.61; owner short_news_r_macd_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 242 | $5.08 | $3.21 | — | $11,441.15 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1233.61; owner short_news_r_macd_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 9 | $132.64 | $2.07 | — | $12,632.84 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1233.61; owner short_news_r_macd_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 6 | $199.94 | $2.06 | — | $13,830.42 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight,overnight_mega; ret5=+2.1; combo leftover $1233.61; owner short_news_r_macd_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,830.42 | ▼ close $12,314.18 vs 09:30 $12,378.26 (session -10.16) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,830.42 | ▲ 09:30 equity $12,476.40 vs yday $12,314.18 (+162.22) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 187 | $14.20 | $2.60 | $+11.68 | $16,483.21 | ▲ +11.68 after sell → book $12,473.79; vs 09:30 mark -2.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 455 | $6.50 | $5.97 | $+302.11 | $19,434.74 | ▲ +302.11 after sell → book $12,467.82; vs 09:30 mark -5.97 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 228 | $12.18 | $3.00 | $+129.72 | $22,208.78 | ▲ +129.72 after sell → book $12,464.82; vs 09:30 mark -3.00 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 805 | $9.19 | $10.38 | — | $14,800.45 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $7402.93; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 51 | $144.18 | $2.14 | — | $7,445.12 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $7402.93; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 400 | $18.50 | $5.16 | — | $39.96 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $7402.93; owner union_hot_n4_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.96 | ▲ close $12,688.77 vs 09:30 $12,476.40 (session +241.64) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.96 | ▼ 09:30 equity $12,164.25 vs yday $12,688.77 (-524.52) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 520 | $3.69 | $6.81 | $-211.12 | $1,951.95 | ▼ -211.12 after sell → book $12,157.44; vs 09:30 mark -6.81 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 145 | $13.90 | $2.42 | $-44.82 | $-65.97 | ▼ -44.82 after sell → book $12,155.01; vs 09:30 mark -2.43 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 36 | $52.49 | $2.10 | $+68.44 | $-1,957.71 | ▲ +68.44 after sell → book $12,152.92; vs 09:30 mark -2.09 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 5 | $347.82 | $2.00 | $+78.57 | $-3,698.81 | ▲ +78.57 after sell → book $12,150.91; vs 09:30 mark -2.01 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 400 | $18.15 | $5.28 | $-150.44 | $3,555.90 | ▼ -150.44 after sell → book $12,145.63; vs 09:30 mark -5.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 63 | $14.00 | $2.18 | — | $2,671.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $888.98; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $146.07 | $2.01 | — | $1,793.30 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $888.98; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 3 | $252.24 | $2.04 | — | $2,547.98 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $896.65; owner short_news_r_macd_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 29 | $30.18 | $2.12 | — | $3,421.07 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+12.1; combo leftover $896.65; owner short_news_r_macd_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,421.07 | ▲ close $12,277.49 vs 09:30 $12,164.25 (session +140.22) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,421.07 | ▼ 09:30 equity $12,092.59 vs yday $12,277.49 (-184.90) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $2,374.67 | ▲ +21.24 after sell → book $12,090.59; vs 09:30 mark -2.00 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 100 | $11.10 | $2.29 | $+107.36 | $1,262.38 | ▲ +107.36 after sell → book $12,088.30; vs 09:30 mark -2.29 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 242 | $4.97 | $3.12 | $+19.08 | $55.31 | ▲ +19.08 after sell → book $12,085.18; vs 09:30 mark -3.12 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 9 | $127.45 | $2.02 | $+42.62 | $-1,093.76 | ▲ +42.62 after sell → book $12,083.16; vs 09:30 mark -2.02 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 6 | $254.39 | $2.01 | $-330.77 | $-2,622.11 | ▼ -330.77 after sell → book $12,081.15; vs 09:30 mark -2.01 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 805 | $9.50 | $10.58 | $+228.59 | $5,014.81 | ▲ +228.59 after sell → book $12,070.57; vs 09:30 mark -10.58 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 51 | $134.10 | $2.21 | $-518.43 | $11,851.71 | ▼ -518.43 after sell → book $12,068.37; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 6 | $148.03 | $2.03 | $+7.72 | $12,737.86 | ▲ +7.72 after sell → book $12,066.34; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,737.86 | ▼ close $12,038.03 vs 09:30 $12,092.59 (session -28.31) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,737.86 | ▲ 09:30 equity $12,054.37 vs yday $12,038.03 (+16.34) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 63 | $13.04 | $2.20 | $-64.86 | $13,557.18 | ▼ -64.86 after sell → book $12,052.17; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,557.18 | ▲ close $12,056.33 vs 09:30 $12,054.37 (session +4.16) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,557.18 | ▲ 09:30 equity $12,073.43 vs yday $12,056.33 (+17.10) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 3 | $235.71 | $2.00 | $+45.55 | $12,848.05 | ▲ +45.55 after sell → book $12,071.43; vs 09:30 mark -2.00 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 29 | $26.78 | $2.08 | $+94.40 | $12,069.35 | ▲ +94.40 after sell → book $12,069.35; vs 09:30 mark -2.08 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,069.35 | ▲ close $12,069.35 vs 09:30 $12,073.43 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,069.35 | ▲ 09:30 equity $12,069.35 vs yday $12,069.35 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 847 | $1.78 | $10.93 | — | $10,550.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1508.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 81 | $18.40 | $2.23 | — | $9,058.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1508.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 110 | $13.71 | $2.32 | — | $7,547.71 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1508.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 63 | $23.88 | $2.18 | — | $6,041.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1508.67; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 3523 | $1.71 | $46.24 | — | $12,019.18 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $6025.85; owner short_news_r_macd_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,019.18 | ▲ close $12,039.20 vs 09:30 $12,069.35 (session +33.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,019.18 | ▲ 09:30 equity $12,171.14 vs yday $12,039.20 (+131.94) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 81 | $18.15 | $2.26 | $-24.74 | $13,487.07 | ▼ -24.74 after sell → book $12,168.88; vs 09:30 mark -2.26 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 110 | $13.89 | $2.35 | $+15.13 | $15,012.62 | ▲ +15.13 after sell → book $12,166.53; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 63 | $23.84 | $2.20 | $-6.90 | $16,512.34 | ▼ -6.90 after sell → book $12,164.33; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 109 | $25.18 | $2.32 | — | $13,765.40 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $2752.06; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 349 | $7.87 | $4.50 | — | $11,014.27 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $2752.06; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 475 | $5.79 | $6.13 | — | $8,257.89 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $2752.06; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 650 | $4.67 | $8.60 | — | $11,284.79 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer; ret5=+11.9; combo leftover $3037.85; owner short_news_r_macd_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 39 | $76.55 | $2.22 | — | $14,268.02 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3037.85; owner short_news_r_macd_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,268.02 | ▲ close $12,404.22 vs 09:30 $12,171.14 (session +263.66) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,268.02 | ▼ 09:30 equity $12,124.57 vs yday $12,404.22 (-279.65) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 847 | $1.56 | $11.08 | $-204.11 | $15,582.50 | ▼ -204.11 after sell → book $12,113.50; vs 09:30 mark -11.07 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 109 | $26.44 | $2.36 | $+132.66 | $18,462.10 | ▲ +132.66 after sell → book $12,111.14; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 349 | $7.76 | $4.58 | $-47.47 | $21,165.76 | ▼ -47.47 after sell → book $12,106.56; vs 09:30 mark -4.58 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 475 | $5.81 | $6.23 | $-2.86 | $23,919.28 | ▼ -2.86 after sell → book $12,100.33; vs 09:30 mark -6.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,919.28 | ▲ close $12,363.45 vs 09:30 $12,124.57 (session +263.12) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,919.28 | ▲ 09:30 equity $12,402.58 vs yday $12,363.45 (+39.13) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 3523 | $1.58 | $45.45 | $+366.30 | $18,307.49 | ▲ +366.30 after sell → book $12,357.13; vs 09:30 mark -45.45 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,307.49 | ▲ close $12,387.55 vs 09:30 $12,402.58 (session +30.42) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,307.49 | ▲ 09:30 equity $12,478.68 vs yday $12,387.55 (+91.13) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 650 | $4.36 | $8.38 | $+184.51 | $15,465.11 | ▲ +184.51 after sell → book $12,470.30; vs 09:30 mark -8.38 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 39 | $76.79 | $2.11 | $-13.69 | $12,468.19 | ▼ -13.69 after sell → book $12,468.19; vs 09:30 mark -2.11 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,468.19 | ▲ close $12,468.19 vs 09:30 $12,478.68 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,468.19 | ▲ 09:30 equity $12,468.19 vs yday $12,468.19 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 577 | $2.70 | $7.44 | — | $10,902.85 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1558.52; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 317 | $4.91 | $4.09 | — | $9,342.29 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1558.52; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 253 | $6.16 | $3.26 | — | $7,780.54 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1558.52; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 497 | $3.13 | $6.41 | — | $6,218.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1558.52; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 18 | $112.83 | $2.13 | — | $8,247.43 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $2072.84; owner short_news_r_macd_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 83 | $24.97 | $2.33 | — | $10,317.61 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+10.8; combo leftover $2072.84; owner short_news_r_macd_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 615 | $3.37 | $8.11 | — | $12,382.05 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $2072.84; owner short_news_r_macd_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,382.05 | ▲ close $12,557.96 vs 09:30 $12,468.19 (session +123.54) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,382.05 | ▲ 09:30 equity $12,682.68 vs yday $12,557.96 (+124.72) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 253 | $6.02 | $3.32 | $-42.00 | $13,901.79 | ▼ -42.00 after sell → book $12,679.36; vs 09:30 mark -3.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,901.79 | ▲ close $12,941.69 vs 09:30 $12,682.68 (session +262.33) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,901.79 | ▲ 09:30 equity $12,991.59 vs yday $12,941.69 (+49.90) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 317 | $5.11 | $4.15 | $+55.16 | $15,517.51 | ▲ +55.16 after sell → book $12,987.44; vs 09:30 mark -4.15 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 497 | $3.64 | $6.51 | $+240.55 | $17,320.08 | ▲ +240.55 after sell → book $12,980.93; vs 09:30 mark -6.51 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,320.08 | ▼ close $12,888.03 vs 09:30 $12,991.59 (session -92.90) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,320.08 | ▲ 09:30 equity $12,971.55 vs yday $12,888.03 (+83.52) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 18 | $118.18 | $2.04 | $-100.38 | $15,190.80 | ▼ -100.38 after sell → book $12,969.51; vs 09:30 mark -2.04 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 83 | $24.42 | $2.24 | $+41.08 | $13,161.70 | ▲ +41.08 after sell → book $12,967.27; vs 09:30 mark -2.24 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 615 | $3.75 | $7.93 | $-249.74 | $10,847.51 | ▼ -249.74 after sell → book $12,959.33; vs 09:30 mark -7.94 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1004 | $1.80 | $12.95 | — | $9,027.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1807.92; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 77 | $23.29 | $2.22 | — | $7,231.81 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1807.92; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 123 | $14.62 | $2.36 | — | $5,431.19 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1807.92; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 795 | $6.83 | $10.58 | — | $10,850.46 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+11.2; combo leftover $5431.19; owner short_news_r_macd_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,850.46 | ▲ close $13,221.64 vs 09:30 $12,971.55 (session +290.42) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,850.46 | ▲ 09:30 equity $13,260.00 vs yday $13,221.64 (+38.36) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 77 | $24.09 | $2.25 | $+57.13 | $12,703.14 | ▲ +57.13 after sell → book $13,257.75; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 123 | $13.77 | $2.39 | $-109.30 | $14,394.46 | ▼ -109.30 after sell → book $13,255.36; vs 09:30 mark -2.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 320 | $22.46 | $4.13 | — | $7,203.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $7197.23; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 195 | $36.76 | $2.58 | — | $32.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $7197.23; owner union_hot_n4_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.36 | ▲ close $13,312.65 vs 09:30 $13,260.00 (session +63.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.36 | ▲ 09:30 equity $13,461.35 vs yday $13,312.65 (+148.70) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1004 | $1.96 | $13.13 | $+134.55 | $1,987.06 | ▲ +134.55 after sell → book $13,448.21; vs 09:30 mark -13.14 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 320 | $21.30 | $4.24 | $-379.56 | $8,798.83 | ▼ -379.56 after sell → book $13,443.98; vs 09:30 mark -4.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 195 | $39.50 | $2.67 | $+529.06 | $16,498.66 | ▲ +529.06 after sell → book $13,441.31; vs 09:30 mark -2.67 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 187 | $29.32 | $2.55 | — | $11,013.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $5499.55; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1812 | $3.04 | $23.37 | — | $5,490.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $5499.55; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 67 | $81.40 | $2.19 | — | $34.48 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $5499.55; owner union_hot_n4_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.48 | ▲ close $13,953.35 vs 09:30 $13,461.35 (session +540.16) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.48 | ▲ 09:30 equity $14,925.35 vs yday $13,953.35 (+972.00) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 577 | $3.55 | $7.56 | $+475.45 | $2,075.28 | ▲ +475.45 after sell → book $14,917.80; vs 09:30 mark -7.55 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 795 | $6.55 | $10.26 | $+201.76 | $-3,142.23 | ▲ +201.76 after sell → book $14,907.54; vs 09:30 mark -10.26 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 187 | $29.43 | $2.63 | $+15.39 | $2,358.56 | ▲ +15.39 after sell → book $14,904.92; vs 09:30 mark -2.62 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 1812 | $4.00 | $23.73 | $+1701.47 | $9,582.82 | ▲ +1,701.47 after sell → book $14,881.18; vs 09:30 mark -23.74 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 67 | $79.08 | $2.24 | $-159.88 | $14,878.94 | ▼ -159.88 after sell → book $14,878.94; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 752 | $2.47 | $9.70 | — | $13,011.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $1859.87; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 109 | $16.91 | $2.32 | — | $11,166.29 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1859.87; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1127 | $1.65 | $14.54 | — | $9,292.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $1859.87; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 159 | $11.67 | $2.47 | — | $7,434.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $1859.87; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 449 | $8.26 | $6.00 | — | $11,136.95 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ret5=+7.7; combo leftover $3712.48; owner short_news_r_macd_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 6 | $583.88 | $2.14 | — | $14,638.08 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+8.5; combo leftover $3712.48; owner short_news_r_macd_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,638.08 | ▲ close $15,489.87 vs 09:30 $14,925.35 (session +648.10) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,638.08 | ▼ 09:30 equity $15,457.71 vs yday $15,489.87 (-32.16) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 267 | $9.11 | $3.44 | — | $12,202.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $2439.68; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 337 | $7.23 | $4.35 | — | $9,761.41 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $2439.68; owner union_hot_n4_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,761.41 | ▼ close $15,114.96 vs 09:30 $15,457.71 (session -334.97) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,761.41 | ▼ 09:30 equity $14,817.26 vs yday $15,114.96 (-297.70) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 109 | $16.92 | $2.35 | $-3.58 | $11,603.34 | ▼ -3.58 after sell → book $14,814.91; vs 09:30 mark -2.35 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1127 | $1.41 | $14.74 | $-299.76 | $13,177.67 | ▼ -299.76 after sell → book $14,800.17; vs 09:30 mark -14.74 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 159 | $12.80 | $2.51 | $+174.69 | $15,210.36 | ▲ +174.69 after sell → book $14,797.66; vs 09:30 mark -2.51 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 267 | $8.39 | $3.51 | $-199.19 | $17,446.99 | ▼ -199.19 after sell → book $14,794.16; vs 09:30 mark -3.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 337 | $6.83 | $4.42 | $-143.57 | $19,744.28 | ▼ -143.57 after sell → book $14,789.74; vs 09:30 mark -4.42 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1218 | $2.70 | $15.71 | — | $16,439.96 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $3290.71; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 78 | $41.76 | $2.22 | — | $13,180.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $3290.71; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 732 | $4.49 | $9.44 | — | $9,884.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $3290.71; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `FIVN` | 189 | $38.91 | $2.85 | — | $17,234.53 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.6; combo leftover $7381.18; owner short_news_r_macd_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,234.53 | ▼ close $14,458.40 vs 09:30 $14,817.26 (session -301.10) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,234.53 | ▲ 09:30 equity $14,475.17 vs yday $14,458.40 (+16.77) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 752 | $2.68 | $9.84 | $+138.38 | $19,240.05 | ▲ +138.38 after sell → book $14,465.33; vs 09:30 mark -9.84 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AEHL` | 449 | $8.21 | $5.79 | $+10.66 | $15,547.97 | ▲ +10.66 after sell → book $14,459.53; vs 09:30 mark -5.80 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 6 | $600.27 | $2.01 | $-102.49 | $11,944.34 | ▼ -102.49 after sell → book $14,457.53; vs 09:30 mark -2.00 | short_news_r_macd_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 78 | $36.02 | $2.26 | $-451.81 | $14,752.03 | ▼ -451.81 after sell → book $14,455.27; vs 09:30 mark -2.26 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 732 | $3.92 | $9.59 | $-432.61 | $17,615.55 | ▼ -432.61 after sell → book $14,445.68; vs 09:30 mark -9.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,615.55 | ▲ close $17,203.11 vs 09:30 $14,475.17 (session +2,757.43) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,418.97 | ▲ 09:30 equity $12,492.97 vs yday $11,873.30 (+619.67) | 09:30 open · cash $17,418.97 (unchanged overnight, no fees) · equity $12,492.97 vs prior close $11,873.30 (+619.67) | — |
| 2026-09-25 09:30 ET | **COVER** | `AEHL` | 270 | $9.05 | $3.48 | $-220.39 | $14,971.99 | ▼ -220.39 after sell → book $12,489.49; vs 09:30 mark -3.48 | short_news_r_macd_h3: dropped from list after 4 sess (min 3) | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 83 | $29.76 | $2.24 | — | $12,499.67 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $2495.33; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 153 | $16.21 | $2.45 | — | $10,017.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $2495.33; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 160 | $15.58 | $2.47 | — | $7,521.64 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $2495.33; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 795 | $7.85 | $10.61 | — | $13,751.78 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $6241.16; owner short_news_r_macd_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,751.78 | ▼ close $11,992.34 vs 09:30 $12,492.97 (session -479.38) | 16:00 close · cash $13,751.78 · equity $11,992.34 vs 09:30 $12,492.97 (-500.63; session marks -479.38) · 8 name(s) marked open→close (per-name table). BAND×88 09:30 $61.83 → close $61.83 -0.00; FIVN×137 09:30 $36.66 → close $36.66 +0.00; GLND×907 09:30 $6.06 → close $5.54 -471.64; VICR×9 09:30 $276.06 → close $276.06 -0.00; TJGC×83 09:30 $29.76 → close $26.24 -292.16; SECZ×153 09:30 $16.21 → close $15.96 -38.25; USDE×160 09:30 $15.58 → close $17.25 +267.02; RSKD×795 09:30 $7.85 → close $7.78 +55.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_macd_h3 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_macd_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_macd_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_macd_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AAP` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_macd_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_macd_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_macd_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `INTU` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_macd_h3 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_macd_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_macd_h3 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_macd_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_macd_h3 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_macd_h3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AEHL` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `AMD` | min_hold | short_news_r_macd_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-23 | `AEHL` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `AMD` | min_hold | short_news_r_macd_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 1218 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $3290.71; owner union_hot_n4_h1 |
| `FIVN` | 189 | 2026-09-23 @ $38.91 | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.6; combo leftover $7381.18; owner short_news_r_macd_h3 |
