# Factor mine action — `combo_sh_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_hot_n4_h1 w=0.3,0.7 net=priority

Cash book **+34.67%** ($13,467) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 215 · skips 159 · realized $+2678.50.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 30%, union_hot_n4_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 30%, union_hot_n4_h1 70%.
- Member: short_news_r_h3 (30% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,659.68.

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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 73 | $24.68 | $2.21 | — | $8,563.07 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $1814.21; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 92 | $19.57 | $2.27 | — | $6,760.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $1814.21; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 824 | $2.20 | $10.63 | — | $4,936.94 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $1814.21; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 163 | $11.12 | $2.48 | — | $3,121.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $1814.21; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 881 | $1.18 | $11.55 | — | $4,149.93 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1040.63; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 54 | $19.17 | $2.20 | — | $5,182.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1040.63; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 81 | $12.70 | $2.28 | — | $6,208.92 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1040.63; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,208.92 | ▼ close $10,157.21 vs 09:30 $10,412.10 (session -176.10) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,208.92 | ▼ 09:30 equity $9,958.67 vs yday $10,157.21 (-198.54) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 73 | $24.83 | $2.24 | $+6.51 | $8,019.27 | ▲ +6.51 after sell → book $9,956.43; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 92 | $19.57 | $2.30 | $-4.56 | $9,817.42 | ▼ -4.56 after sell → book $9,954.14; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 824 | $2.08 | $10.78 | $-116.17 | $11,524.68 | ▼ -116.17 after sell → book $9,943.36; vs 09:30 mark -10.78 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 163 | $9.57 | $2.52 | $-257.65 | $13,082.07 | ▼ -257.65 after sell → book $9,940.84; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 546 | $4.19 | $7.04 | — | $10,787.28 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $2289.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 333 | $6.87 | $4.30 | — | $8,495.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $2289.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 167 | $13.64 | $2.49 | — | $6,214.91 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $2289.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 55 | $41.23 | $2.15 | — | $3,945.10 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $2289.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 686 | $1.15 | $8.99 | — | $4,725.01 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $789.02; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 221 | $3.56 | $2.92 | — | $5,508.85 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $789.02; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 24 | $31.70 | $2.10 | — | $6,267.55 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $789.02; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 262 | $3.01 | $3.45 | — | $7,052.71 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $789.02; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 116 | $6.80 | $2.39 | — | $7,839.12 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $789.02; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,839.12 | ▲ close $10,004.02 vs 09:30 $9,958.67 (session +99.02) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,839.12 | ▲ 09:30 equity $10,114.87 vs yday $10,004.02 (+110.85) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 546 | $3.94 | $7.15 | $-150.69 | $9,983.21 | ▼ -150.69 after sell → book $10,107.72; vs 09:30 mark -7.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 167 | $13.31 | $2.54 | $-60.14 | $12,203.44 | ▼ -60.14 after sell → book $10,105.18; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 55 | $41.50 | $2.18 | $+10.51 | $14,483.76 | ▲ +10.51 after sell → book $10,103.00; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,483.76 | ▲ close $10,138.43 vs 09:30 $10,114.87 (session +35.43) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,483.76 | ▲ 09:30 equity $10,148.81 vs yday $10,138.43 (+10.38) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 881 | $1.07 | $11.36 | $+74.00 | $13,529.73 | ▲ +74.00 after sell → book $10,137.45; vs 09:30 mark -11.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 81 | $11.75 | $2.23 | $+72.03 | $12,575.74 | ▲ +72.03 after sell → book $10,135.21; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 333 | $7.19 | $4.37 | $+97.89 | $14,965.64 | ▲ +97.89 after sell → book $10,130.84; vs 09:30 mark -4.37 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,965.64 | ▲ close $10,162.31 vs 09:30 $10,148.81 (session +31.46) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,965.64 | ▼ 09:30 equity $10,110.95 vs yday $10,162.31 (-51.36) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 54 | $18.13 | $2.15 | $+51.81 | $13,984.47 | ▲ +51.81 after sell → book $10,108.80; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 686 | $0.96 | $8.66 | $+110.62 | $13,315.19 | ▲ +110.62 after sell → book $10,100.13; vs 09:30 mark -8.67 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 221 | $4.01 | $2.85 | $-106.33 | $12,425.02 | ▼ -106.33 after sell → book $10,097.28; vs 09:30 mark -2.85 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 24 | $31.87 | $2.06 | $-8.24 | $11,658.08 | ▼ -8.24 after sell → book $10,095.22; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 262 | $2.95 | $3.38 | $+8.89 | $10,881.80 | ▲ +8.89 after sell → book $10,091.84; vs 09:30 mark -3.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 116 | $6.81 | $2.34 | $-5.89 | $10,089.50 | ▼ -5.89 after sell → book $10,089.50; vs 09:30 mark -2.34 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 149 | $11.81 | $2.44 | — | $8,326.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1765.66; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 11 | $150.14 | $2.02 | — | $6,673.07 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1765.66; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1535 | $1.15 | $19.80 | — | $4,888.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1765.66; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1288 | $1.37 | $16.62 | — | $3,106.84 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1765.66; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 2 | $204.45 | $2.03 | — | $3,513.71 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 20 | $21.40 | $2.08 | — | $3,939.63 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 100 | $4.43 | $2.33 | — | $4,380.30 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 2 | $173.90 | $2.03 | — | $4,726.08 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 9 | $46.85 | $2.05 | — | $5,145.68 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 4 | $106.38 | $2.03 | — | $5,569.17 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 96 | $4.61 | $2.32 | — | $6,009.41 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $443.83; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,009.41 | ▼ close $10,004.24 vs 09:30 $10,110.95 (session -29.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,009.41 | ▲ 09:30 equity $10,192.10 vs yday $10,004.24 (+187.86) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 149 | $11.57 | $2.48 | $-41.42 | $7,730.86 | ▼ -41.42 after sell → book $10,189.62; vs 09:30 mark -2.48 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1288 | $1.46 | $16.84 | $+82.46 | $9,594.50 | ▲ +82.46 after sell → book $10,172.78; vs 09:30 mark -16.84 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 747 | $4.49 | $9.64 | — | $6,230.83 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $3358.08; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 493 | $6.81 | $6.36 | — | $2,867.14 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $3358.08; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 184 | $3.11 | $2.60 | — | $3,436.79 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $573.43; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 6 | $89.10 | $2.04 | — | $3,969.34 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $573.43; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 14 | $38.40 | $2.07 | — | $4,504.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $573.43; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 27 | $20.90 | $2.11 | — | $5,067.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $573.43; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 21 | $27.00 | $2.09 | — | $5,631.98 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $573.43; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,631.98 | ▲ close $10,145.97 vs 09:30 $10,192.10 (session +0.09) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,631.98 | ▲ 09:30 equity $11,559.15 vs yday $10,145.97 (+1,413.18) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 11 | $142.70 | $2.05 | $-85.91 | $7,199.64 | ▼ -85.91 after sell → book $11,557.11; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1535 | $1.83 | $20.08 | $+1003.92 | $9,988.61 | ▲ +1,003.92 after sell → book $11,537.03; vs 09:30 mark -20.08 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 747 | $4.32 | $9.79 | $-146.41 | $13,205.86 | ▼ -146.41 after sell → book $11,527.24; vs 09:30 mark -9.79 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 493 | $8.03 | $6.47 | $+588.63 | $17,158.18 | ▲ +588.63 after sell → book $11,520.77; vs 09:30 mark -6.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,158.18 | ▲ close $11,541.24 vs 09:30 $11,559.15 (session +20.47) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,158.18 | ▲ 09:30 equity $11,602.20 vs yday $11,541.24 (+60.96) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 2 | $212.00 | $2.00 | $-19.12 | $16,732.18 | ▼ -19.12 after sell → book $11,600.20; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 20 | $20.90 | $2.05 | $+5.87 | $16,312.13 | ▲ +5.87 after sell → book $11,598.15; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 100 | $4.42 | $2.29 | $-3.62 | $15,867.84 | ▼ -3.62 after sell → book $11,595.86; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 2 | $170.64 | $2.00 | $+2.50 | $15,524.57 | ▲ +2.50 after sell → book $11,593.87; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 9 | $43.63 | $2.02 | $+24.91 | $15,129.88 | ▲ +24.91 after sell → book $11,591.85; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 4 | $105.58 | $2.00 | $-0.84 | $14,705.56 | ▼ -0.84 after sell → book $11,589.85; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 96 | $4.77 | $2.28 | $-19.95 | $14,245.36 | ▼ -19.95 after sell → book $11,587.57; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 103 | $24.11 | $2.30 | — | $11,759.73 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $2492.94; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1598 | $1.56 | $20.61 | — | $9,246.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $2492.94; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 612 | $4.07 | $7.89 | — | $6,747.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $2492.94; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 130 | $19.04 | $2.38 | — | $4,269.92 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $2492.94; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 78 | $13.62 | $2.28 | — | $5,330.40 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1067.48; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 19 | $54.51 | $2.10 | — | $6,363.99 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1067.48; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 6 | $175.01 | $2.06 | — | $7,411.99 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1067.48; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 2 | $364.35 | $2.04 | — | $8,138.66 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1067.48; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,138.66 | ▲ close $12,376.01 vs 09:30 $11,602.20 (session +830.09) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,138.66 | ▼ 09:30 equity $12,020.62 vs yday $12,376.01 (-355.39) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 184 | $2.83 | $2.54 | $+46.38 | $7,615.40 | ▲ +46.38 after sell → book $12,018.08; vs 09:30 mark -2.54 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 6 | $88.24 | $2.01 | $+1.11 | $7,083.95 | ▲ +1.11 after sell → book $12,016.07; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 14 | $38.41 | $2.03 | $-4.24 | $6,544.18 | ▼ -4.24 after sell → book $12,014.04; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 27 | $20.50 | $2.07 | $+6.62 | $5,988.60 | ▲ +6.62 after sell → book $12,011.96; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 21 | $26.00 | $2.05 | $+16.86 | $5,440.55 | ▲ +16.86 after sell → book $12,009.91; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 103 | $26.61 | $2.34 | $+252.86 | $8,179.04 | ▲ +252.86 after sell → book $12,007.57; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1598 | $1.60 | $20.90 | $+22.41 | $10,714.94 | ▲ +22.41 after sell → book $11,986.67; vs 09:30 mark -20.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 130 | $20.72 | $2.42 | $+213.60 | $13,406.12 | ▲ +213.60 after sell → book $11,984.25; vs 09:30 mark -2.42 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 221 | $14.11 | $2.85 | — | $10,284.96 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $3128.09; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 538 | $5.81 | $6.94 | — | $7,152.24 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $3128.09; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 270 | $11.59 | $3.48 | — | $4,020.81 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $3128.09; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 3 | $213.94 | $2.04 | — | $4,660.59 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $804.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 65 | $12.22 | $2.23 | — | $5,452.66 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $804.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 158 | $5.08 | $2.52 | — | $6,252.78 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $804.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 6 | $132.64 | $2.05 | — | $7,046.57 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $804.16; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $7,844.29 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $804.16; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,844.29 | ▲ close $12,104.82 vs 09:30 $12,020.62 (session +144.72) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,844.29 | ▲ 09:30 equity $12,414.23 vs yday $12,104.82 (+309.41) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 221 | $14.20 | $2.91 | $+14.13 | $10,979.57 | ▲ +14.13 after sell → book $12,411.31; vs 09:30 mark -2.92 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 538 | $6.50 | $7.06 | $+357.22 | $14,469.52 | ▲ +357.22 after sell → book $12,404.26; vs 09:30 mark -7.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 270 | $12.18 | $3.55 | $+153.61 | $17,754.56 | ▲ +153.61 after sell → book $12,400.70; vs 09:30 mark -3.56 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 450 | $9.19 | $5.80 | — | $13,613.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $4142.73; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 28 | $144.18 | $2.07 | — | $9,574.14 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $4142.73; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 223 | $18.50 | $2.88 | — | $5,445.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $4142.73; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 36 | $74.54 | $2.20 | — | $8,127.00 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2722.88; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 49 | $55.25 | $2.24 | — | $10,832.01 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2722.88; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,832.01 | ▲ close $12,397.05 vs 09:30 $12,414.23 (session +11.55) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,832.01 | ▼ 09:30 equity $12,036.98 vs yday $12,397.05 (-360.07) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 612 | $3.69 | $8.01 | $-248.47 | $13,082.28 | ▼ -248.47 after sell → book $12,028.97; vs 09:30 mark -8.01 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 78 | $13.90 | $2.22 | $-25.95 | $11,995.85 | ▼ -25.95 after sell → book $12,026.74; vs 09:30 mark -2.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 19 | $52.49 | $2.05 | $+34.24 | $10,996.49 | ▲ +34.24 after sell → book $12,024.69; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 6 | $172.76 | $2.01 | $+9.44 | $9,957.93 | ▲ +9.44 after sell → book $12,022.69; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 2 | $347.82 | $2.00 | $+29.03 | $9,260.29 | ▲ +29.03 after sell → book $12,020.69; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 223 | $18.15 | $2.95 | $-83.87 | $13,304.79 | ▼ -83.87 after sell → book $12,017.74; vs 09:30 mark -2.95 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 332 | $14.00 | $4.28 | — | $8,652.51 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $4656.68; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 31 | $146.07 | $2.08 | — | $4,122.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $4656.68; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 8 | $252.24 | $2.10 | — | $6,138.08 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2061.13; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 68 | $30.18 | $2.28 | — | $8,188.05 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2061.13; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,188.05 | ▲ close $12,323.27 vs 09:30 $12,036.98 (session +316.27) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,188.05 | ▼ 09:30 equity $12,207.83 vs yday $12,323.27 (-115.44) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 3 | $208.88 | $2.00 | $+11.14 | $7,559.41 | ▲ +11.14 after sell → book $12,205.83; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 65 | $11.10 | $2.19 | $+68.39 | $6,835.72 | ▲ +68.39 after sell → book $12,203.64; vs 09:30 mark -2.19 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 158 | $4.97 | $2.46 | $+11.60 | $6,047.21 | ▲ +11.60 after sell → book $12,201.18; vs 09:30 mark -2.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 6 | $127.45 | $2.01 | $+27.08 | $5,280.50 | ▲ +27.08 after sell → book $12,199.17; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $4,260.94 | ▼ -221.85 after sell → book $12,197.17; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 450 | $9.50 | $5.91 | $+127.78 | $8,530.02 | ▲ +127.78 after sell → book $12,191.25; vs 09:30 mark -5.92 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 28 | $134.10 | $2.11 | $-286.43 | $12,282.71 | ▼ -286.43 after sell → book $12,189.14; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 31 | $148.03 | $2.13 | $+56.55 | $16,869.51 | ▲ +56.55 after sell → book $12,187.01; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,869.51 | ▼ close $12,069.29 vs 09:30 $12,207.83 (session -117.72) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,869.51 | ▲ 09:30 equity $12,118.83 vs yday $12,069.29 (+49.54) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 36 | $73.22 | $2.10 | $+43.22 | $14,231.49 | ▲ +43.22 after sell → book $12,116.73; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 49 | $54.76 | $2.14 | $+19.63 | $11,546.11 | ▲ +19.63 after sell → book $12,114.59; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 332 | $13.04 | $4.37 | $-327.38 | $15,871.02 | ▼ -327.38 after sell → book $12,110.22; vs 09:30 mark -4.37 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,871.02 | ▲ close $12,122.62 vs 09:30 $12,118.83 (session +12.40) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,871.02 | ▲ 09:30 equity $12,164.30 vs yday $12,122.62 (+41.68) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 8 | $235.71 | $2.01 | $+128.13 | $13,983.33 | ▲ +128.13 after sell → book $12,162.29; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 68 | $26.78 | $2.19 | $+226.73 | $12,160.09 | ▲ +226.73 after sell → book $12,160.09; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,160.09 | ▲ close $12,160.09 vs 09:30 $12,164.30 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,160.09 | ▲ 09:30 equity $12,160.09 vs yday $12,160.09 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1195 | $1.78 | $15.42 | — | $10,017.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $2128.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 115 | $18.40 | $2.33 | — | $7,899.24 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $2128.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 155 | $13.71 | $2.46 | — | $5,771.74 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $2128.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 89 | $23.88 | $2.26 | — | $3,644.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $2128.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 122 | $14.85 | $2.44 | — | $5,453.42 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $1822.08; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1065 | $1.71 | $13.98 | — | $7,260.59 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $1822.08; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,260.59 | ▼ close $11,785.57 vs 09:30 $12,160.09 (session -335.64) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,260.59 | ▲ 09:30 equity $11,912.94 vs yday $11,785.57 (+127.37) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 115 | $18.15 | $2.37 | $-33.46 | $9,345.47 | ▼ -33.46 after sell → book $11,910.57; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 155 | $13.89 | $2.50 | $+22.95 | $11,495.92 | ▲ +22.95 after sell → book $11,908.07; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 89 | $23.84 | $2.29 | $-8.11 | $13,615.39 | ▼ -8.11 after sell → book $11,905.78; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 126 | $25.18 | $2.37 | — | $10,440.35 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $3176.93; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 403 | $7.87 | $5.20 | — | $7,263.54 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $3176.93; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 548 | $5.79 | $7.07 | — | $4,083.55 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $3176.93; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 437 | $4.67 | $5.78 | — | $6,118.56 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2041.77; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 26 | $76.55 | $2.15 | — | $8,106.71 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2041.77; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,106.71 | ▲ close $12,399.99 vs 09:30 $11,912.94 (session +516.77) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,106.71 | ▼ 09:30 equity $12,077.86 vs yday $12,399.99 (-322.13) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1195 | $1.56 | $15.63 | $-287.97 | $9,961.25 | ▼ -287.97 after sell → book $12,062.23; vs 09:30 mark -15.63 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 126 | $26.44 | $2.42 | $+153.98 | $13,290.28 | ▲ +153.98 after sell → book $12,059.82; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 403 | $7.76 | $5.29 | $-54.82 | $16,412.27 | ▼ -54.82 after sell → book $12,054.53; vs 09:30 mark -5.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 548 | $5.81 | $7.19 | $-3.29 | $19,588.96 | ▼ -3.29 after sell → book $12,047.34; vs 09:30 mark -7.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,588.96 | ▲ close $12,239.35 vs 09:30 $12,077.86 (session +192.01) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,588.96 | ▲ 09:30 equity $12,263.58 vs yday $12,239.35 (+24.23) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 122 | $13.60 | $2.36 | $+147.70 | $17,927.41 | ▲ +147.70 after sell → book $12,261.23; vs 09:30 mark -2.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1065 | $1.58 | $13.74 | $+110.73 | $16,230.97 | ▲ +110.73 after sell → book $12,247.49; vs 09:30 mark -13.74 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,230.97 | ▲ close $12,267.88 vs 09:30 $12,263.58 (session +20.39) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,230.97 | ▲ 09:30 equity $12,329.11 vs yday $12,267.88 (+61.23) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 437 | $4.36 | $5.64 | $+124.05 | $14,320.01 | ▲ +124.05 after sell → book $12,323.47; vs 09:30 mark -5.64 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 26 | $76.79 | $2.07 | $-10.46 | $12,321.40 | ▼ -10.46 after sell → book $12,321.40; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,321.40 | ▲ close $12,321.40 vs 09:30 $12,329.11 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,321.40 | ▲ 09:30 equity $12,321.40 vs yday $12,321.40 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 798 | $2.70 | $10.29 | — | $10,156.51 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $2156.25; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 439 | $4.91 | $5.66 | — | $7,995.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $2156.25; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 350 | $6.16 | $4.51 | — | $5,834.84 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $2156.25; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 688 | $3.13 | $8.88 | — | $3,672.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $2156.25; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $4,347.49 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $734.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 208 | $3.52 | $2.75 | — | $5,076.90 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $734.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 361 | $2.03 | $4.75 | — | $5,804.98 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $734.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 29 | $24.97 | $2.12 | — | $6,527.00 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $734.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 217 | $3.37 | $2.87 | — | $7,255.42 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $734.50; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,255.42 | ▲ close $12,495.22 vs 09:30 $12,321.40 (session +217.68) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,255.42 | ▲ 09:30 equity $12,638.34 vs yday $12,495.22 (+143.12) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 350 | $6.02 | $4.59 | $-58.11 | $9,357.83 | ▼ -58.11 after sell → book $12,633.75; vs 09:30 mark -4.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,357.83 | ▲ close $13,005.28 vs 09:30 $12,638.34 (session +371.53) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,357.83 | ▲ 09:30 equity $13,142.43 vs yday $13,005.28 (+137.15) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 439 | $5.11 | $5.75 | $+76.38 | $11,595.37 | ▲ +76.38 after sell → book $13,136.68; vs 09:30 mark -5.75 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 688 | $3.64 | $9.01 | $+333.00 | $14,090.68 | ▲ +333.00 after sell → book $13,127.67; vs 09:30 mark -9.01 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,090.68 | ▲ close $13,257.80 vs 09:30 $13,142.43 (session +130.13) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,090.68 | ▲ 09:30 equity $13,284.66 vs yday $13,257.80 (+26.86) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 6 | $118.18 | $2.01 | $-36.12 | $13,379.59 | ▼ -36.12 after sell → book $13,282.65; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 208 | $3.98 | $2.68 | $-101.11 | $12,549.07 | ▼ -101.11 after sell → book $13,279.97; vs 09:30 mark -2.68 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 361 | $1.85 | $4.66 | $+55.58 | $11,876.56 | ▲ +55.58 after sell → book $13,275.31; vs 09:30 mark -4.66 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 29 | $24.42 | $2.08 | $+11.76 | $11,166.30 | ▲ +11.76 after sell → book $13,273.23; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 217 | $3.75 | $2.80 | $-88.12 | $10,349.75 | ▼ -88.12 after sell → book $13,270.43; vs 09:30 mark -2.80 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1341 | $1.80 | $17.30 | — | $7,918.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $2414.94; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 103 | $23.29 | $2.30 | — | $5,517.49 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $2414.94; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 165 | $14.62 | $2.48 | — | $3,102.70 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $2414.94; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 83 | $18.61 | $2.31 | — | $4,645.02 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1551.35; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 227 | $6.83 | $3.02 | — | $6,192.41 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1551.35; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,192.41 | ▼ close $13,038.33 vs 09:30 $13,284.66 (session -204.69) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,192.41 | ▲ 09:30 equity $13,060.09 vs yday $13,038.33 (+21.76) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 103 | $24.09 | $2.34 | $+77.77 | $8,671.35 | ▲ +77.77 after sell → book $13,057.76; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 165 | $13.77 | $2.53 | $-145.27 | $10,940.87 | ▼ -145.27 after sell → book $13,055.23; vs 09:30 mark -2.53 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 208 | $36.76 | $2.68 | — | $3,292.10 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $7658.61; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 207 | $7.95 | $2.76 | — | $4,934.99 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $1646.05; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 20 | $81.00 | $2.12 | — | $6,552.87 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $1646.05; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,552.87 | ▲ close $13,833.83 vs 09:30 $13,060.09 (session +786.17) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,552.87 | ▲ 09:30 equity $14,004.40 vs yday $13,833.83 (+170.57) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1341 | $1.96 | $17.54 | $+179.72 | $9,163.69 | ▲ +179.72 after sell → book $13,986.86; vs 09:30 mark -17.54 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 208 | $39.50 | $2.78 | $+564.45 | $17,376.91 | ▲ +564.45 after sell → book $13,984.08; vs 09:30 mark -2.78 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 138 | $29.32 | $2.40 | — | $13,328.34 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $4054.61; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1335 | $3.04 | $17.22 | — | $9,259.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $4054.61; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 49 | $81.40 | $2.14 | — | $5,268.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $4054.61; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 152 | $34.44 | $2.66 | — | $10,500.88 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5268.66; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,500.88 | ▲ close $14,458.22 vs 09:30 $14,004.40 (session +498.56) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,500.88 | ▲ 09:30 equity $14,958.47 vs yday $14,458.22 (+500.25) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 798 | $3.55 | $10.45 | $+657.56 | $13,323.33 | ▲ +657.56 after sell → book $14,948.02; vs 09:30 mark -10.45 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 83 | $22.11 | $2.24 | $-295.05 | $11,485.96 | ▼ -295.05 after sell → book $14,945.78; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 227 | $6.55 | $2.93 | $+57.61 | $9,996.19 | ▲ +57.61 after sell → book $14,942.86; vs 09:30 mark -2.92 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 138 | $29.43 | $2.46 | $+10.32 | $14,055.07 | ▲ +10.32 after sell → book $14,940.40; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 1335 | $4.00 | $17.49 | $+1253.57 | $19,377.58 | ▲ +1,253.57 after sell → book $14,922.91; vs 09:30 mark -17.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 49 | $79.08 | $2.18 | $-118.00 | $23,250.32 | ▼ -118.00 after sell → book $14,920.73; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1647 | $2.47 | $21.25 | — | $19,160.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $4068.81; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 240 | $16.91 | $3.10 | — | $15,099.49 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $4068.81; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 2465 | $1.65 | $31.80 | — | $11,000.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $4068.81; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 348 | $11.67 | $4.49 | — | $6,934.79 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $4068.81; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 419 | $8.26 | $5.60 | — | $10,390.14 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3467.40; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $13,307.42 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3467.40; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,307.42 | ▲ close $15,208.68 vs 09:30 $14,958.47 (session +356.29) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,307.42 | ▼ 09:30 equity $15,060.33 vs yday $15,208.68 (-148.35) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 207 | $8.28 | $2.67 | $-72.71 | $11,591.82 | ▼ -72.71 after sell → book $15,057.66; vs 09:30 mark -2.67 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 296 | $9.11 | $3.82 | — | $8,891.44 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $2704.76; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 374 | $7.23 | $4.82 | — | $6,182.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $2704.76; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 32 | $93.97 | $2.20 | — | $9,187.44 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3091.30; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,187.44 | ▼ close $14,711.93 vs 09:30 $15,060.33 (session -334.89) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,187.44 | ▼ 09:30 equity $14,164.51 vs yday $14,711.93 (-547.42) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 20 | $82.00 | $2.05 | $-24.17 | $7,545.39 | ▼ -24.17 after sell → book $14,162.46; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 240 | $16.92 | $3.17 | $-3.86 | $11,603.02 | ▼ -3.86 after sell → book $14,159.29; vs 09:30 mark -3.17 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 2465 | $1.41 | $32.24 | $-655.63 | $15,046.43 | ▼ -655.63 after sell → book $14,127.05; vs 09:30 mark -32.24 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 348 | $12.80 | $4.58 | $+384.17 | $19,496.25 | ▲ +384.17 after sell → book $14,122.47; vs 09:30 mark -4.58 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 296 | $8.39 | $3.89 | $-220.83 | $21,975.80 | ▼ -220.83 after sell → book $14,118.58; vs 09:30 mark -3.89 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 374 | $6.83 | $4.91 | $-159.33 | $24,525.32 | ▼ -159.33 after sell → book $14,113.68; vs 09:30 mark -4.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 2119 | $2.70 | $27.34 | — | $18,776.68 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $5722.57; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 137 | $41.76 | $2.40 | — | $13,053.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $5722.57; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 1274 | $4.49 | $16.43 | — | $7,316.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $5722.57; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 60 | $116.85 | $2.43 | — | $14,325.04 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $7033.75; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,325.04 | ▼ close $13,678.74 vs 09:30 $14,164.51 (session -386.34) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,325.04 | ▼ 09:30 equity $13,603.03 vs yday $13,678.74 (-75.71) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1647 | $2.68 | $21.55 | $+303.07 | $18,717.44 | ▲ +303.07 after sell → book $13,581.48; vs 09:30 mark -21.55 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $15,714.09 | ▼ -86.07 after sell → book $13,579.47; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 137 | $36.02 | $2.46 | $-790.56 | $20,647.05 | ▼ -790.56 after sell → book $13,577.01; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 1274 | $3.92 | $16.69 | $-752.93 | $25,630.81 | ▼ -752.93 after sell → book $13,560.32; vs 09:30 mark -16.69 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,630.81 | ▲ close $17,725.46 vs 09:30 $13,603.03 (session +4,165.14) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,715.86 | ▲ 09:30 equity $14,365.17 vs yday $13,307.33 (+1,057.84) | 09:30 open · cash $12,715.86 (unchanged overnight, no fees) · equity $14,365.17 vs prior close $13,307.33 (+1057.84) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 99 | $29.76 | $2.29 | — | $9,767.33 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $2967.03; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 183 | $16.21 | $2.54 | — | $6,798.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $2967.03; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 190 | $15.58 | $2.56 | — | $3,835.39 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $2967.03; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 488 | $7.85 | $6.51 | — | $7,659.68 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $3835.39; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,659.68 | ▼ close $13,467.31 vs 09:30 $14,365.17 (session -883.96) | 16:00 close · cash $7,659.68 · equity $13,467.31 vs 09:30 $14,365.17 (-897.86; session marks -883.96) · 11 name(s) marked open→close (per-name table). AEHL×252 09:30 $9.05 → close $9.36 -78.12; BAND×43 09:30 $61.83 → close $61.83 -0.00; GLND×1526 09:30 $6.06 → close $5.54 -793.52; HALO×21 09:30 $115.36 → close $113.90 +30.66; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; VICR×15 09:30 $276.06 → close $276.06 -0.00; TJGC×99 09:30 $29.76 → close $26.24 -348.48; SECZ×183 09:30 $16.21 → close $15.96 -45.75; USDE×190 09:30 $15.58 → close $17.25 +317.09; RSKD×488 09:30 $7.85 → close $7.78 +34.16 | — |

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
| `FIVN` | 152 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5268.66; owner short_news_r_h3 |
| `AEHL` | 419 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3467.40; owner short_news_r_h3 |
| `USFD` | 32 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3091.30; owner short_news_r_h3 |
| `GLND` | 2119 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $5722.57; owner union_hot_n4_h1 |
| `HALO` | 60 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $7033.75; owner short_news_r_h3 |
