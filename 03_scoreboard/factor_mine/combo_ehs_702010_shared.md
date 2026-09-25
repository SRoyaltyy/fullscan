# Factor mine action — `combo_ehs_702010_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/union_hot_n4_h1/short_news_r_h3 w=0.7,0.2,0.1 net=priority

Cash book **-4.57%** ($9,543) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 346 · skips 365 · realized $+3165.91.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 70%, union_hot_n4_h1 20%, short_news_r_h3 10%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 70%, union_hot_n4_h1 20%, short_news_r_h3 10%.
- Member: union_e_fresh_h3 (70% · long · hold 3).
- Member: union_hot_n4_h1 (20% · long · hold 1).
- Member: short_news_r_h3 (10% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $-140.52.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 4801 | $0.81 | $53.29 | — | $6,057.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $3888.89; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 176 | $22.01 | $2.52 | — | $2,181.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $3888.89; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 15 | $45.98 | $2.04 | — | $1,489.89 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $727.21; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 31 | $23.33 | $2.08 | — | $764.57 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $727.21; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 14 | $50.62 | $2.03 | — | $53.82 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $727.21; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.82 | ▲ close $10,626.87 vs 09:30 $10,000.00 (session +688.83) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.82 | ▲ 09:30 equity $10,770.76 vs yday $10,626.87 (+143.89) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 15 | $44.09 | $2.06 | $-32.44 | $713.11 | ▼ -32.44 after sell → book $10,768.70; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 31 | $22.92 | $2.10 | $-16.90 | $1,421.53 | ▼ -16.90 after sell → book $10,766.60; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 14 | $55.29 | $2.05 | $+61.25 | $2,193.54 | ▲ +61.25 after sell → book $10,764.55; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 127 | $1.50 | $2.29 | — | $2,000.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 9 | $19.57 | $1.79 | — | $1,822.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 17 | $11.12 | $1.94 | — | $1,631.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 14 | $13.55 | $1.94 | — | $1,440.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 17 | $10.83 | $1.89 | — | $1,254.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 162 | $1.18 | $2.40 | — | $1,060.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 10 | $19.17 | $1.95 | — | $867.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 19 | $9.89 | $1.94 | — | $677.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $191.93; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 9 | $24.68 | $2.02 | — | $452.93 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $225.69; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 102 | $2.20 | $2.30 | — | $226.23 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $225.69; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 17 | $12.70 | $2.07 | — | $439.98 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $226.23; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $439.98 | ▲ close $11,475.61 vs 09:30 $10,770.76 (session +733.56) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $439.98 | ▼ 09:30 equity $11,352.59 vs yday $11,475.61 (-123.02) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 9 | $24.83 | $2.04 | $-2.70 | $661.41 | ▼ -2.70 after sell → book $11,350.55; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 102 | $2.08 | $2.32 | $-16.35 | $871.76 | ▼ -16.35 after sell → book $11,348.23; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 34 | $4.19 | $1.53 | — | $727.77 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $145.29; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 21 | $6.87 | $1.51 | — | $582.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $145.29; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 10 | $13.64 | $1.39 | — | $444.20 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $145.29; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 3 | $41.23 | $1.25 | — | $319.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $145.29; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 55 | $1.15 | $0.82 | — | $381.70 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $63.85; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 17 | $3.56 | $0.68 | — | $441.54 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $63.85; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 2 | $31.70 | $0.66 | — | $504.28 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $63.85; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 21 | $3.01 | $0.72 | — | $566.77 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $63.85; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 9 | $6.80 | $0.66 | — | $627.31 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $63.85; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $627.31 | ▲ close $11,713.69 vs 09:30 $11,352.59 (session +374.66) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $627.31 | ▼ 09:30 equity $11,615.23 vs yday $11,713.69 (-98.46) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 4801 | $1.14 | $62.77 | $+1468.27 | $6,037.68 | ▲ +1,468.27 after sell → book $11,552.46; vs 09:30 mark -62.77 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 176 | $22.82 | $2.58 | $+137.46 | $10,051.42 | ▲ +137.46 after sell → book $11,549.88; vs 09:30 mark -2.58 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 34 | $3.94 | $1.46 | $-11.49 | $10,183.91 | ▼ -11.49 after sell → book $11,548.41; vs 09:30 mark -1.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 10 | $13.31 | $1.38 | $-6.08 | $10,315.63 | ▼ -6.08 after sell → book $11,547.03; vs 09:30 mark -1.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 3 | $41.50 | $1.27 | $-1.71 | $10,438.86 | ▼ -1.71 after sell → book $11,545.76; vs 09:30 mark -1.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,438.86 | ▲ close $11,556.98 vs 09:30 $11,615.23 (session +11.23) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,438.86 | ▼ 09:30 equity $11,546.47 vs yday $11,556.98 (-10.51) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 127 | $1.42 | $2.22 | $-14.66 | $10,616.98 | ▼ -14.66 after sell → book $11,544.25; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 9 | $19.58 | $1.81 | $-3.51 | $10,791.40 | ▼ -3.51 after sell → book $11,542.45; vs 09:30 mark -1.80 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 17 | $9.10 | $1.62 | $-37.90 | $10,944.48 | ▼ -37.90 after sell → book $11,540.83; vs 09:30 mark -1.62 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 14 | $13.01 | $1.88 | $-11.38 | $11,124.73 | ▼ -11.38 after sell → book $11,538.94; vs 09:30 mark -1.89 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 17 | $10.85 | $1.92 | $-3.47 | $11,307.27 | ▼ -3.47 after sell → book $11,537.03; vs 09:30 mark -1.91 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 162 | $1.07 | $2.26 | $-22.47 | $11,478.35 | ▼ -22.47 after sell → book $11,534.77; vs 09:30 mark -2.26 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 10 | $18.98 | $1.95 | $-5.80 | $11,666.20 | ▼ -5.80 after sell → book $11,532.82; vs 09:30 mark -1.95 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 19 | $11.50 | $2.07 | $+26.49 | $11,882.64 | ▲ +26.49 after sell → book $11,530.76; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 17 | $11.75 | $2.04 | $+11.96 | $11,680.85 | ▲ +11.96 after sell → book $11,528.72; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 21 | $7.19 | $1.59 | $+3.62 | $11,830.24 | ▲ +3.62 after sell → book $11,527.12; vs 09:30 mark -1.60 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,830.24 | ▲ close $11,527.69 vs 09:30 $11,546.47 (session +0.57) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,830.24 | ▼ 09:30 equity $11,522.04 vs yday $11,527.69 (-5.65) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 55 | $0.96 | $0.69 | $+8.77 | $11,776.58 | ▲ +8.77 after sell → book $11,521.35; vs 09:30 mark -0.69 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 17 | $4.01 | $0.73 | $-9.15 | $11,707.60 | ▼ -9.15 after sell → book $11,520.62; vs 09:30 mark -0.73 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 2 | $31.87 | $0.64 | $-1.65 | $11,643.21 | ▼ -1.65 after sell → book $11,519.97; vs 09:30 mark -0.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 21 | $2.95 | $0.68 | $-0.14 | $11,580.58 | ▼ -0.14 after sell → book $11,519.29; vs 09:30 mark -0.68 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 9 | $6.81 | $0.64 | $-1.39 | $11,518.65 | ▼ -1.39 after sell → book $11,518.65; vs 09:30 mark -0.64 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 10 | $97.43 | $2.02 | — | $10,542.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 227 | $4.43 | $2.93 | — | $9,533.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 3359 | $0.30 | $20.15 | — | $8,505.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 21 | $46.85 | $2.05 | — | $7,520.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 111 | $9.01 | $2.32 | — | $6,517.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 259 | $3.89 | $3.34 | — | $5,506.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 29 | $34.05 | $2.08 | — | $4,517.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 44 | $22.44 | $2.12 | — | $3,527.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $1007.88; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 3 | $150.14 | $2.00 | — | $3,075.32 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $587.96; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 511 | $1.15 | $6.59 | — | $2,481.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $587.96; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 49 | $11.81 | $2.14 | — | $1,900.01 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $587.96; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 429 | $1.37 | $5.53 | — | $1,306.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $587.96; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $1,509.18 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $261.35; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 12 | $21.40 | $2.05 | — | $1,763.92 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $261.35; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 1 | $173.90 | $1.77 | — | $1,936.06 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $261.35; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 2 | $106.38 | $2.02 | — | $2,146.79 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $261.35; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 56 | $4.61 | $2.19 | — | $2,402.77 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $261.35; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,402.77 | ▲ close $11,519.28 vs 09:30 $11,522.04 (session +63.96) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,402.77 | ▲ 09:30 equity $11,615.14 vs yday $11,519.28 (+95.86) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 49 | $11.57 | $2.16 | $-16.30 | $2,967.54 | ▼ -16.30 after sell → book $11,612.98; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 429 | $1.46 | $5.62 | $+27.46 | $3,588.27 | ▲ +27.46 after sell → book $11,607.37; vs 09:30 mark -5.61 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 3 | $115.18 | $2.00 | — | $3,240.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $418.63; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 23 | $17.93 | $2.06 | — | $2,826.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $418.63; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 4 | $93.98 | $2.00 | — | $2,448.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $418.63; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 9 | $43.08 | $2.02 | — | $2,058.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $418.63; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 182 | $2.30 | $2.54 | — | $1,637.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $418.63; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 121 | $4.49 | $2.35 | — | $1,091.73 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $545.79; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 80 | $6.81 | $2.23 | — | $544.70 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $545.79; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 35 | $3.11 | $1.22 | — | $652.33 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $108.94; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 1 | $89.10 | $0.92 | — | $740.51 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $108.94; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 2 | $38.40 | $0.80 | — | $816.52 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $108.94; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 5 | $20.90 | $1.08 | — | $919.93 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $108.94; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 4 | $27.00 | $1.11 | — | $1,026.82 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $108.94; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,026.82 | ▲ close $11,835.15 vs 09:30 $11,615.14 (session +248.11) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,026.82 | ▲ 09:30 equity $12,200.61 vs yday $11,835.15 (+365.46) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 3 | $142.70 | $2.02 | $-26.34 | $1,452.90 | ▼ -26.34 after sell → book $12,198.60; vs 09:30 mark -2.01 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 511 | $1.83 | $6.69 | $+334.20 | $2,381.34 | ▲ +334.20 after sell → book $12,191.91; vs 09:30 mark -6.69 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 121 | $4.32 | $2.38 | $-25.31 | $2,901.68 | ▼ -25.31 after sell → book $12,189.53; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 80 | $8.03 | $2.25 | $+93.12 | $3,541.83 | ▲ +93.12 after sell → book $12,187.27; vs 09:30 mark -2.26 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,541.83 | ▲ close $12,260.19 vs 09:30 $12,200.61 (session +72.92) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,541.83 | ▲ 09:30 equity $12,287.92 vs yday $12,260.19 (+27.73) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 10 | $104.00 | $2.04 | $+61.64 | $4,579.79 | ▲ +61.64 after sell → book $12,285.88; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 227 | $4.42 | $2.98 | $-8.17 | $5,580.15 | ▼ -8.17 after sell → book $12,282.90; vs 09:30 mark -2.98 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 3359 | $0.31 | $21.06 | $-7.62 | $6,600.38 | ▼ -7.62 after sell → book $12,261.84; vs 09:30 mark -21.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 21 | $43.63 | $2.07 | $-71.75 | $7,514.54 | ▼ -71.75 after sell → book $12,259.77; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 111 | $9.23 | $2.35 | $+19.75 | $8,536.72 | ▲ +19.75 after sell → book $12,257.42; vs 09:30 mark -2.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 259 | $5.24 | $3.40 | $+342.91 | $9,890.48 | ▲ +342.91 after sell → book $12,254.02; vs 09:30 mark -3.40 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 29 | $34.72 | $2.10 | $+15.26 | $10,895.27 | ▲ +15.26 after sell → book $12,251.93; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 44 | $21.85 | $2.14 | $-30.22 | $11,854.53 | ▼ -30.22 after sell → book $12,249.79; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $11,640.53 | ▼ -11.56 after sell → book $12,247.79; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 12 | $20.90 | $2.03 | $+1.92 | $11,387.71 | ▲ +1.92 after sell → book $12,245.77; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 1 | $170.64 | $1.71 | $-0.22 | $11,215.36 | ▼ -0.22 after sell → book $12,244.06; vs 09:30 mark -1.71 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 2 | $105.58 | $2.00 | $-2.42 | $11,002.20 | ▼ -2.42 after sell → book $12,242.06; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 56 | $4.77 | $2.16 | $-13.30 | $10,732.92 | ▼ -13.30 after sell → book $12,239.90; vs 09:30 mark -2.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 5 | $175.01 | $2.00 | — | $9,855.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 10 | $88.94 | $2.02 | — | $8,964.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 61 | $15.28 | $2.17 | — | $8,030.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 6 | $142.36 | $2.01 | — | $7,174.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 184 | $5.10 | $2.54 | — | $6,233.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 19 | $47.89 | $2.05 | — | $5,321.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 67 | $13.92 | $2.19 | — | $4,386.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 206 | $4.54 | $2.66 | — | $3,447.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $939.13; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 23 | $24.11 | $2.06 | — | $2,890.78 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $574.56; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 368 | $1.56 | $4.75 | — | $2,311.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $574.56; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 141 | $4.07 | $2.41 | — | $1,735.67 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $574.56; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 30 | $19.04 | $2.08 | — | $1,162.39 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $574.56; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 28 | $13.62 | $2.10 | — | $1,541.79 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $387.46; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 7 | $54.51 | $2.04 | — | $1,921.31 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $387.46; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 1 | $364.35 | $2.02 | — | $2,283.64 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $387.46; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,283.64 | ▼ close $12,153.34 vs 09:30 $12,287.92 (session -51.45) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,283.64 | ▼ 09:30 equity $12,071.77 vs yday $12,153.34 (-81.57) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 3 | $124.67 | $2.02 | $+24.45 | $2,655.63 | ▲ +24.45 after sell → book $12,069.75; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 23 | $18.14 | $2.08 | $+0.58 | $3,070.77 | ▲ +0.58 after sell → book $12,067.67; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 4 | $94.60 | $2.02 | $-1.54 | $3,447.15 | ▼ -1.54 after sell → book $12,065.65; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 9 | $44.39 | $2.04 | $+7.74 | $3,844.62 | ▲ +7.74 after sell → book $12,063.61; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 182 | $2.35 | $2.58 | $+3.99 | $4,269.75 | ▲ +3.99 after sell → book $12,061.04; vs 09:30 mark -2.57 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 35 | $2.83 | $1.10 | $+7.49 | $4,169.60 | ▲ +7.49 after sell → book $12,059.94; vs 09:30 mark -1.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 1 | $88.24 | $0.89 | $-0.94 | $4,080.48 | ▼ -0.94 after sell → book $12,059.06; vs 09:30 mark -0.88 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 2 | $38.41 | $0.77 | $-1.59 | $4,002.88 | ▼ -1.59 after sell → book $12,058.28; vs 09:30 mark -0.78 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 5 | $20.50 | $1.04 | $-0.12 | $3,899.34 | ▼ -0.12 after sell → book $12,057.24; vs 09:30 mark -1.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 4 | $26.00 | $1.05 | $+1.83 | $3,794.29 | ▲ +1.83 after sell → book $12,056.19; vs 09:30 mark -1.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 23 | $26.61 | $2.08 | $+53.36 | $4,404.24 | ▲ +53.36 after sell → book $12,054.11; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 368 | $1.60 | $4.82 | $+5.15 | $4,988.22 | ▲ +5.15 after sell → book $12,049.29; vs 09:30 mark -4.82 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 30 | $20.72 | $2.10 | $+46.22 | $5,607.72 | ▲ +46.22 after sell → book $12,047.19; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1122 | $0.58 | $9.91 | — | $4,943.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $654.23; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 125 | $5.21 | $2.37 | — | $4,290.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $654.23; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 4 | $131.37 | $2.00 | — | $3,762.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $654.23; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 35 | $18.26 | $2.10 | — | $3,121.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $654.23; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 19 | $34.30 | $2.05 | — | $2,467.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $654.23; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 2 | $326.91 | $2.00 | — | $1,811.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $654.23; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 28 | $14.11 | $2.07 | — | $1,414.68 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $402.63; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 69 | $5.81 | $2.20 | — | $1,011.59 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $402.63; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 34 | $11.59 | $2.09 | — | $615.61 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $402.63; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 10 | $12.22 | $1.28 | — | $736.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $123.12; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 24 | $5.08 | $1.31 | — | $857.14 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $123.12; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $857.14 | ▲ close $12,310.47 vs 09:30 $12,071.77 (session +292.64) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $857.14 | ▲ 09:30 equity $12,353.12 vs yday $12,310.47 (+42.65) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 28 | $14.20 | $2.09 | $-1.65 | $1,252.65 | ▼ -1.65 after sell → book $12,351.03; vs 09:30 mark -2.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 69 | $6.50 | $2.22 | $+43.19 | $1,698.93 | ▲ +43.19 after sell → book $12,348.81; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 34 | $12.18 | $2.11 | $+16.03 | $2,110.94 | ▲ +16.03 after sell → book $12,346.70; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 2 | $80.60 | $1.62 | — | $1,948.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 11 | $16.18 | $1.81 | — | $1,768.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 1 | $118.77 | $1.19 | — | $1,648.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 10 | $17.78 | $1.81 | — | $1,468.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 13 | $13.41 | $1.78 | — | $1,292.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 1 | $97.16 | $0.97 | — | $1,194.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 1 | $120.17 | $1.20 | — | $1,073.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $184.71; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 38 | $9.19 | $2.10 | — | $721.81 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $357.71; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 2 | $144.18 | $2.00 | — | $431.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $357.71; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 2 | $74.54 | $1.52 | — | $579.02 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $215.73; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 3 | $55.25 | $1.69 | — | $743.08 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $215.73; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $743.08 | ▼ close $12,274.42 vs 09:30 $12,353.12 (session -54.57) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $743.08 | ▼ 09:30 equity $12,236.74 vs yday $12,274.42 (-37.68) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 5 | $172.76 | $2.02 | $-15.28 | $1,604.85 | ▼ -15.28 after sell → book $12,234.71; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 10 | $93.30 | $2.04 | $+39.54 | $2,535.81 | ▲ +39.54 after sell → book $12,232.67; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 61 | $18.15 | $2.19 | $+170.70 | $3,640.77 | ▲ +170.70 after sell → book $12,230.48; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 6 | $132.80 | $2.03 | $-61.40 | $4,435.54 | ▼ -61.40 after sell → book $12,228.45; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 184 | $4.58 | $2.58 | $-100.80 | $5,275.68 | ▼ -100.80 after sell → book $12,225.87; vs 09:30 mark -2.58 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 19 | $48.42 | $2.07 | $+5.96 | $6,193.59 | ▲ +5.96 after sell → book $12,223.80; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 67 | $15.66 | $2.21 | $+112.18 | $7,240.60 | ▲ +112.18 after sell → book $12,221.59; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 206 | $3.38 | $2.70 | $-245.35 | $7,934.18 | ▼ -245.35 after sell → book $12,218.89; vs 09:30 mark -2.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 141 | $3.69 | $2.45 | $-58.44 | $8,452.02 | ▼ -58.44 after sell → book $12,216.44; vs 09:30 mark -2.45 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 28 | $13.90 | $2.07 | $-11.88 | $8,060.75 | ▼ -11.88 after sell → book $12,214.37; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 7 | $52.49 | $2.01 | $+10.09 | $7,691.30 | ▲ +10.09 after sell → book $12,212.36; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 1 | $347.82 | $1.99 | $+12.51 | $7,341.49 | ▲ +12.51 after sell → book $12,210.36; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 2 | $261.16 | $2.00 | — | $6,817.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 42 | $15.01 | $2.12 | — | $6,184.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 6 | $103.89 | $2.01 | — | $5,559.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 165 | $3.88 | $2.48 | — | $4,916.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 14 | $44.40 | $2.03 | — | $4,292.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 26 | $24.69 | $2.07 | — | $3,648.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 76 | $8.35 | $2.22 | — | $3,012.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 17 | $37.65 | $2.04 | — | $2,370.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $642.38; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 112 | $14.00 | $2.33 | — | $799.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1580.09; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 1 | $252.24 | $2.02 | — | $1,050.04 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $399.91; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 13 | $30.18 | $2.06 | — | $1,440.32 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $399.91; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,440.32 | ▼ close $11,997.81 vs 09:30 $12,236.74 (session -189.18) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,440.32 | ▼ 09:30 equity $11,988.53 vs yday $11,997.81 (-9.28) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 1122 | $0.51 | $9.28 | $-101.10 | $2,003.25 | ▼ -101.10 after sell → book $11,979.24; vs 09:30 mark -9.29 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 125 | $5.00 | $2.40 | $-31.01 | $2,625.86 | ▼ -31.01 after sell → book $11,976.85; vs 09:30 mark -2.39 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 4 | $148.03 | $2.02 | $+62.62 | $3,215.95 | ▲ +62.62 after sell → book $11,974.82; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 35 | $19.25 | $2.12 | $+30.44 | $3,887.59 | ▲ +30.44 after sell → book $11,972.71; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 19 | $34.72 | $2.07 | $+3.87 | $4,545.20 | ▲ +3.87 after sell → book $11,970.64; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 2 | $298.01 | $2.02 | $-61.81 | $5,139.21 | ▼ -61.81 after sell → book $11,968.63; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 10 | $11.10 | $1.14 | $+8.78 | $5,027.07 | ▲ +8.78 after sell → book $11,967.49; vs 09:30 mark -1.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 24 | $4.97 | $1.27 | $-0.06 | $4,906.40 | ▼ -0.06 after sell → book $11,966.22; vs 09:30 mark -1.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 38 | $9.50 | $2.12 | $+7.55 | $5,265.28 | ▲ +7.55 after sell → book $11,964.10; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 2 | $134.10 | $2.02 | $-24.17 | $5,531.46 | ▼ -24.17 after sell → book $11,962.08; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,531.46 | ▼ close $11,924.48 vs 09:30 $11,988.53 (session -37.61) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,531.46 | ▼ 09:30 equity $11,850.01 vs yday $11,924.48 (-74.47) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 2 | $79.83 | $1.62 | $-4.78 | $5,689.50 | ▼ -4.78 after sell → book $11,848.39; vs 09:30 mark -1.62 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 11 | $15.97 | $1.81 | $-5.93 | $5,863.36 | ▼ -5.93 after sell → book $11,846.58; vs 09:30 mark -1.81 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 1 | $113.66 | $1.16 | $-7.46 | $5,975.86 | ▼ -7.46 after sell → book $11,845.42; vs 09:30 mark -1.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 10 | $18.28 | $1.88 | $+1.31 | $6,156.78 | ▲ +1.31 after sell → book $11,843.54; vs 09:30 mark -1.88 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 13 | $12.18 | $1.64 | $-19.41 | $6,313.48 | ▼ -19.41 after sell → book $11,841.90; vs 09:30 mark -1.64 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 1 | $96.65 | $0.99 | $-2.47 | $6,409.14 | ▼ -2.47 after sell → book $11,840.91; vs 09:30 mark -0.99 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 1 | $120.54 | $1.23 | $-2.06 | $6,528.45 | ▼ -2.06 after sell → book $11,839.68; vs 09:30 mark -1.23 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 2 | $73.22 | $1.47 | $-0.35 | $6,380.54 | ▼ -0.35 after sell → book $11,838.21; vs 09:30 mark -1.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 3 | $54.76 | $1.65 | $-1.87 | $6,214.61 | ▼ -1.87 after sell → book $11,836.56; vs 09:30 mark -1.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 112 | $13.04 | $2.36 | $-112.20 | $7,672.73 | ▼ -112.20 after sell → book $11,834.20; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,672.73 | ▼ close $11,785.18 vs 09:30 $11,850.01 (session -49.02) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,672.73 | ▼ 09:30 equity $11,762.70 vs yday $11,785.18 (-22.48) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 2 | $246.70 | $2.02 | $-32.93 | $8,164.12 | ▼ -32.93 after sell → book $11,760.68; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 42 | $15.01 | $2.14 | $-4.25 | $8,792.40 | ▼ -4.25 after sell → book $11,758.54; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 6 | $92.00 | $2.03 | $-75.38 | $9,342.37 | ▼ -75.38 after sell → book $11,756.52; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 165 | $3.32 | $2.52 | $-97.41 | $9,887.65 | ▼ -97.41 after sell → book $11,753.99; vs 09:30 mark -2.53 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 14 | $44.17 | $2.05 | $-7.30 | $10,503.98 | ▼ -7.30 after sell → book $11,751.94; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 26 | $21.97 | $2.09 | $-74.88 | $11,073.11 | ▼ -74.88 after sell → book $11,749.85; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 76 | $8.58 | $2.24 | $+13.02 | $11,722.95 | ▲ +13.02 after sell → book $11,747.61; vs 09:30 mark -2.24 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 17 | $35.80 | $2.06 | $-35.55 | $12,329.40 | ▼ -35.55 after sell → book $11,745.55; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 1 | $235.71 | $1.99 | $+12.52 | $12,091.70 | ▲ +12.52 after sell → book $11,743.56; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 13 | $26.78 | $2.03 | $+40.11 | $11,741.53 | ▲ +40.11 after sell → book $11,741.53; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,741.53 | ▲ close $11,741.53 vs 09:30 $11,762.70 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,741.53 | ▲ 09:30 equity $11,741.53 vs yday $11,741.53 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 95 | $10.74 | $2.27 | — | $10,718.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $10,013.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 148 | $6.90 | $2.43 | — | $8,989.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $8,278.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 46 | $22.32 | $2.13 | — | $7,249.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 3 | $257.00 | $2.00 | — | $6,476.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 21 | $47.60 | $2.05 | — | $5,474.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 68 | $15.09 | $2.19 | — | $4,446.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1027.38; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 416 | $1.78 | $5.37 | — | $3,700.73 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $741.10; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 40 | $18.40 | $2.11 | — | $2,962.62 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $741.10; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 54 | $13.71 | $2.15 | — | $2,220.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $741.10; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 31 | $23.88 | $2.08 | — | $1,477.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $741.10; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 49 | $14.85 | $2.18 | — | $2,203.24 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $738.88; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 432 | $1.71 | $5.67 | — | $2,936.29 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $738.88; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,936.29 | ▲ close $11,968.14 vs 09:30 $11,741.53 (session +263.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,936.29 | ▲ 09:30 equity $12,043.16 vs yday $11,968.14 (+75.02) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 40 | $18.15 | $2.13 | $-14.24 | $3,660.16 | ▼ -14.24 after sell → book $12,041.03; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 54 | $13.89 | $2.17 | $+5.40 | $4,408.05 | ▲ +5.40 after sell → book $12,038.86; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 31 | $23.84 | $2.10 | $-5.43 | $5,144.98 | ▼ -5.43 after sell → book $12,036.75; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 7 | $63.18 | $2.01 | — | $4,700.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 51 | $8.74 | $2.14 | — | $4,252.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 6 | $68.52 | $2.01 | — | $3,839.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 124 | $3.62 | $2.36 | — | $3,389.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 2 | $167.55 | $2.00 | — | $3,051.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 10 | $44.90 | $2.02 | — | $2,600.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 4 | $98.15 | $2.00 | — | $2,206.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 28 | $15.70 | $2.07 | — | $1,764.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $450.19; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 15 | $25.18 | $2.04 | — | $1,384.95 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $392.15; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 49 | $7.87 | $2.14 | — | $997.18 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $392.15; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 67 | $5.79 | $2.19 | — | $607.06 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $392.15; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 64 | $4.67 | $2.21 | — | $903.73 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $303.53; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 3 | $76.55 | $2.03 | — | $1,131.36 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $303.53; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,131.36 | ▲ close $12,123.23 vs 09:30 $12,043.16 (session +113.69) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,131.36 | ▼ 09:30 equity $12,051.56 vs yday $12,123.23 (-71.67) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 416 | $1.56 | $5.45 | $-100.25 | $1,776.95 | ▼ -100.25 after sell → book $12,046.11; vs 09:30 mark -5.45 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 15 | $26.44 | $2.06 | $+14.81 | $2,171.50 | ▲ +14.81 after sell → book $12,044.06; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 49 | $7.76 | $2.16 | $-9.68 | $2,549.58 | ▼ -9.68 after sell → book $12,041.90; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 67 | $5.81 | $2.21 | $-3.06 | $2,936.64 | ▼ -3.06 after sell → book $12,039.69; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,936.64 | ▲ close $12,064.17 vs 09:30 $12,051.56 (session +24.48) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,936.64 | ▼ 09:30 equity $12,040.14 vs yday $12,064.17 (-24.03) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 95 | $10.51 | $2.30 | $-26.90 | $3,932.79 | ▼ -26.90 after sell → book $12,037.84; vs 09:30 mark -2.30 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 2 | $366.23 | $2.02 | $+24.97 | $4,663.23 | ▲ +24.97 after sell → book $12,035.82; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 148 | $9.39 | $2.47 | $+363.62 | $6,050.48 | ▲ +363.62 after sell → book $12,033.35; vs 09:30 mark -2.47 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 2 | $341.90 | $2.02 | $-29.19 | $6,732.27 | ▼ -29.19 after sell → book $12,031.34; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 46 | $21.67 | $2.15 | $-34.18 | $7,726.94 | ▼ -34.18 after sell → book $12,029.19; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 3 | $252.92 | $2.02 | $-16.26 | $8,483.68 | ▼ -16.26 after sell → book $12,027.17; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 21 | $56.94 | $2.07 | $+192.01 | $9,677.35 | ▲ +192.01 after sell → book $12,025.10; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 68 | $13.84 | $2.22 | $-89.41 | $10,616.25 | ▼ -89.41 after sell → book $12,022.88; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 49 | $13.60 | $2.14 | $+56.94 | $9,947.71 | ▲ +56.94 after sell → book $12,020.74; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 432 | $1.58 | $5.57 | $+44.91 | $9,259.58 | ▲ +44.91 after sell → book $12,015.17; vs 09:30 mark -5.57 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,259.58 | ▼ close $11,995.55 vs 09:30 $12,040.14 (session -19.62) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,259.58 | ▼ 09:30 equity $11,986.58 vs yday $11,995.55 (-8.97) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 7 | $67.44 | $2.03 | $+25.78 | $9,729.63 | ▲ +25.78 after sell → book $11,984.55; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 51 | $8.26 | $2.16 | $-28.79 | $10,148.73 | ▼ -28.79 after sell → book $11,982.39; vs 09:30 mark -2.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 6 | $64.60 | $2.03 | $-27.56 | $10,534.30 | ▼ -27.56 after sell → book $11,980.36; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 124 | $3.76 | $2.39 | $+13.23 | $10,998.15 | ▲ +13.23 after sell → book $11,977.97; vs 09:30 mark -2.39 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 2 | $142.43 | $2.02 | $-54.25 | $11,280.99 | ▼ -54.25 after sell → book $11,975.95; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 10 | $38.23 | $2.04 | $-70.81 | $11,661.20 | ▼ -70.81 after sell → book $11,973.91; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 4 | $98.71 | $2.02 | $-1.78 | $12,054.02 | ▼ -1.78 after sell → book $11,971.89; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 28 | $15.26 | $2.09 | $-16.49 | $12,479.20 | ▼ -16.49 after sell → book $11,969.79; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 64 | $4.36 | $2.18 | $+15.45 | $12,197.98 | ▲ +15.45 after sell → book $11,967.61; vs 09:30 mark -2.18 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 3 | $76.79 | $2.00 | $-4.74 | $11,965.61 | ▼ -4.74 after sell → book $11,965.61; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.61 | ▲ close $11,965.61 vs 09:30 $11,986.58 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.61 | ▲ 09:30 equity $11,965.61 vs yday $11,965.61 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $10,977.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 177 | $5.91 | $2.52 | — | $9,928.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $8,957.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 32 | $32.01 | $2.09 | — | $7,931.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 14 | $71.71 | $2.03 | — | $6,925.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 18 | $56.02 | $2.04 | — | $5,914.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 111 | $9.37 | $2.32 | — | $4,872.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 79 | $13.10 | $2.23 | — | $3,835.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $1046.99; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 236 | $2.70 | $3.04 | — | $3,195.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $639.24; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 130 | $4.91 | $2.38 | — | $2,554.53 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $639.24; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 103 | $6.16 | $2.30 | — | $1,917.75 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $639.24; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 204 | $3.13 | $2.63 | — | $1,276.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $639.24; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 2 | $112.83 | $2.02 | — | $1,500.24 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $255.32; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 72 | $3.52 | $2.23 | — | $1,751.45 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $255.32; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 125 | $2.03 | $2.40 | — | $2,002.79 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $255.32; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 10 | $24.97 | $2.05 | — | $2,250.45 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $255.32; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 75 | $3.37 | $2.24 | — | $2,500.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $255.32; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,500.95 | ▲ close $12,035.45 vs 09:30 $11,965.61 (session +108.38) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,500.95 | ▲ 09:30 equity $12,090.05 vs yday $12,035.45 (+54.60) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 103 | $6.02 | $2.33 | $-19.05 | $3,118.69 | ▼ -19.05 after sell → book $12,087.73; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,118.69 | ▲ close $12,500.94 vs 09:30 $12,090.05 (session +413.21) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,118.69 | ▲ 09:30 equity $12,512.91 vs yday $12,500.94 (+11.97) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 130 | $5.11 | $2.41 | $+21.21 | $3,780.58 | ▲ +21.21 after sell → book $12,510.50; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 204 | $3.64 | $2.68 | $+98.73 | $4,520.46 | ▲ +98.73 after sell → book $12,507.82; vs 09:30 mark -2.68 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,520.46 | ▲ close $12,593.49 vs 09:30 $12,512.91 (session +85.67) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,520.46 | ▼ 09:30 equity $12,538.70 vs yday $12,593.49 (-54.79) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 6 | $140.03 | $2.03 | $-150.44 | $5,358.61 | ▼ -150.44 after sell → book $12,536.67; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 177 | $6.25 | $2.56 | $+55.10 | $6,462.30 | ▲ +55.10 after sell → book $12,534.11; vs 09:30 mark -2.56 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 4 | $253.34 | $2.02 | $+40.66 | $7,473.64 | ▲ +40.66 after sell → book $12,532.09; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 32 | $30.57 | $2.11 | $-50.27 | $8,449.77 | ▼ -50.27 after sell → book $12,529.98; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 14 | $78.12 | $2.05 | $+85.66 | $9,541.40 | ▲ +85.66 after sell → book $12,527.93; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 18 | $61.93 | $2.06 | $+102.27 | $10,654.08 | ▲ +102.27 after sell → book $12,525.87; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 111 | $9.40 | $2.35 | $-1.34 | $11,695.13 | ▼ -1.34 after sell → book $12,523.52; vs 09:30 mark -2.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 79 | $15.75 | $2.25 | $+204.87 | $12,937.13 | ▲ +204.87 after sell → book $12,521.27; vs 09:30 mark -2.25 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 2 | $118.18 | $2.00 | $-14.71 | $12,698.77 | ▼ -14.71 after sell → book $12,519.27; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 72 | $3.98 | $2.21 | $-37.56 | $12,410.00 | ▼ -37.56 after sell → book $12,517.06; vs 09:30 mark -2.21 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 125 | $1.85 | $2.37 | $+17.73 | $12,176.39 | ▲ +17.73 after sell → book $12,514.70; vs 09:30 mark -2.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 10 | $24.42 | $2.02 | $+1.43 | $11,930.17 | ▲ +1.43 after sell → book $12,512.68; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 75 | $3.75 | $2.21 | $-32.96 | $11,646.70 | ▼ -32.96 after sell → book $12,510.46; vs 09:30 mark -2.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 123 | $33.14 | $2.36 | — | $7,568.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $4076.35; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 99 | $40.93 | $2.29 | — | $3,513.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $4076.35; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 433 | $1.80 | $5.59 | — | $2,728.78 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $780.84; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 33 | $23.29 | $2.09 | — | $1,958.12 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $780.84; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 53 | $14.62 | $2.15 | — | $1,181.11 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $780.84; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 31 | $18.61 | $2.12 | — | $1,755.91 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $590.56; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 86 | $6.83 | $2.29 | — | $2,341.00 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $590.56; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,341.00 | ▲ close $12,584.10 vs 09:30 $12,538.70 (session +92.51) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,341.00 | ▲ 09:30 equity $12,860.03 vs yday $12,584.10 (+275.93) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 33 | $24.09 | $2.11 | $+22.20 | $3,133.86 | ▲ +22.20 after sell → book $12,857.92; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 53 | $13.77 | $2.17 | $-49.37 | $3,861.50 | ▼ -49.37 after sell → book $12,855.75; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 150 | $11.21 | $2.44 | — | $2,177.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $1689.41; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 20 | $81.00 | $2.05 | — | $555.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $1689.41; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 69 | $7.95 | $2.23 | — | $1,101.83 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $555.51; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,101.83 | ▲ close $13,137.66 vs 09:30 $12,860.03 (session +288.63) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,101.83 | ▲ 09:30 equity $13,276.01 vs yday $13,137.66 (+138.35) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 433 | $1.96 | $5.67 | $+58.03 | $1,944.84 | ▲ +58.03 after sell → book $13,270.34; vs 09:30 mark -5.67 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 14 | $29.32 | $2.03 | — | $1,532.33 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $432.19; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 142 | $3.04 | $2.42 | — | $1,098.94 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $432.19; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 5 | $81.40 | $2.00 | — | $689.94 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $432.19; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 20 | $34.44 | $2.09 | — | $1,376.65 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $689.94; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,376.65 | ▲ close $13,366.92 vs 09:30 $13,276.01 (session +105.12) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,376.65 | ▲ 09:30 equity $13,580.87 vs yday $13,366.92 (+213.95) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 236 | $3.55 | $3.09 | $+194.46 | $2,211.35 | ▲ +194.46 after sell → book $13,577.77; vs 09:30 mark -3.10 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 123 | $40.03 | $2.42 | $+842.69 | $7,132.62 | ▲ +842.69 after sell → book $13,575.35; vs 09:30 mark -2.42 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 99 | $41.00 | $2.34 | $+2.31 | $11,189.29 | ▲ +2.31 after sell → book $13,573.02; vs 09:30 mark -2.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 31 | $22.11 | $2.08 | $-112.70 | $10,501.80 | ▼ -112.70 after sell → book $13,570.94; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 86 | $6.55 | $2.25 | $+19.54 | $9,936.25 | ▲ +19.54 after sell → book $13,568.69; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 14 | $29.43 | $2.05 | $-2.54 | $10,346.22 | ▼ -2.54 after sell → book $13,566.64; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 142 | $4.00 | $2.45 | $+132.16 | $10,911.77 | ▲ +132.16 after sell → book $13,564.19; vs 09:30 mark -2.45 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 5 | $79.08 | $2.02 | $-15.63 | $11,305.14 | ▼ -15.63 after sell → book $13,562.16; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 762 | $2.47 | $9.83 | — | $9,413.17 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $1884.19; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 111 | $16.91 | $2.32 | — | $7,533.84 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1884.19; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1141 | $1.65 | $14.72 | — | $5,636.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $1884.19; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 161 | $11.67 | $2.47 | — | $3,755.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $1884.19; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 227 | $8.26 | $3.03 | — | $5,627.11 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $1877.56; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 3 | $583.88 | $2.07 | — | $7,376.68 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $1877.56; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,376.68 | ▲ close $14,013.30 vs 09:30 $13,580.87 (session +485.59) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,376.68 | ▼ 09:30 equity $13,951.49 vs yday $14,013.30 (-61.81) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 69 | $8.28 | $2.20 | $-26.86 | $6,803.51 | ▼ -26.86 after sell → book $13,949.29; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 165 | $9.11 | $2.48 | — | $5,297.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $1511.89; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 209 | $7.23 | $2.70 | — | $3,784.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $1511.89; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 20 | $93.97 | $2.13 | — | $5,661.38 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $1892.05; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,661.38 | ▼ close $13,750.28 vs 09:30 $13,951.49 (session -191.71) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,661.38 | ▼ 09:30 equity $13,707.23 vs yday $13,750.28 (-43.05) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 150 | $13.82 | $2.48 | $+386.58 | $7,731.90 | ▲ +386.58 after sell → book $13,704.75; vs 09:30 mark -2.48 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 20 | $82.00 | $2.07 | $+15.88 | $9,369.83 | ▲ +15.88 after sell → book $13,702.68; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 111 | $16.92 | $2.36 | $-3.57 | $11,245.59 | ▼ -3.57 after sell → book $13,700.32; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1141 | $1.41 | $14.92 | $-303.48 | $12,839.48 | ▼ -303.48 after sell → book $13,685.40; vs 09:30 mark -14.92 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 161 | $12.80 | $2.52 | $+176.94 | $14,897.76 | ▲ +176.94 after sell → book $13,682.88; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 165 | $8.39 | $2.52 | $-123.81 | $16,279.59 | ▼ -123.81 after sell → book $13,680.36; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 209 | $6.83 | $2.74 | $-89.04 | $17,704.32 | ▼ -89.04 after sell → book $13,677.62; vs 09:30 mark -2.74 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 52 | $47.57 | $2.15 | — | $15,228.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2478.60; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 12 | $196.78 | $2.03 | — | $12,865.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2478.60; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 69 | $35.74 | $2.20 | — | $10,396.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2478.60; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 52 | $47.15 | $2.15 | — | $7,942.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2478.60; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 22 | $109.67 | $2.06 | — | $5,528.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2478.60; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 454 | $2.70 | $5.86 | — | $4,296.49 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1228.48; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 29 | $41.76 | $2.08 | — | $3,083.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1228.48; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 273 | $4.49 | $3.52 | — | $1,854.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1228.48; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 15 | $116.85 | $2.11 | — | $3,604.73 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $1854.08; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,604.73 | ▼ close $13,211.59 vs 09:30 $13,707.23 (session -441.90) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,604.73 | ▲ 09:30 equity $13,240.15 vs yday $13,211.59 (+28.56) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 762 | $2.68 | $9.97 | $+140.22 | $5,636.91 | ▲ +140.22 after sell → book $13,230.18; vs 09:30 mark -9.97 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 3 | $600.27 | $2.00 | $-53.24 | $3,834.10 | ▼ -53.24 after sell → book $13,228.18; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 29 | $36.02 | $2.10 | $-170.49 | $4,876.73 | ▼ -170.49 after sell → book $13,226.08; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 273 | $3.92 | $3.58 | $-161.34 | $5,944.68 | ▼ -161.34 after sell → book $13,222.51; vs 09:30 mark -3.57 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,944.68 | ▲ close $14,185.65 vs 09:30 $13,240.15 (session +963.15) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-140.52 | ▲ 09:30 equity $9,664.12 vs yday $9,586.25 (+77.87) | 09:30 open · cash $-140.52 (unchanged overnight, no fees) · equity $9,664.12 vs prior close $9,586.25 (+77.87) | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-140.52 | ▼ close $9,542.73 vs 09:30 $9,664.12 (session -121.39) | 16:00 close · cash $-140.52 · equity $9,542.73 vs 09:30 $9,664.12 (-121.39; session marks -121.39) · 15 name(s) marked open→close (per-name table). ABVX×21 09:30 $94.87 → close $94.87 +0.00; AEHL×169 09:30 $9.05 → close $9.36 -52.39; ANAB×40 09:30 $51.70 → close $51.70 +0.00; BAND×5 09:30 $61.83 → close $61.83 -0.00; CBRL×20 09:30 $52.39 → close $51.81 -11.60; CTAS×4 09:30 $197.68 → close $197.68 -0.00; GIS×27 09:30 $34.83 → close $34.83 +0.00; GLND×116 09:30 $6.06 → close $5.54 -60.32; HALO×2 09:30 $115.36 → close $113.90 +2.92; KBH×20 09:30 $47.65 → close $47.65 +0.00; MLKN×109 09:30 $19.91 → close $19.91 -0.00; PAYX×6 09:30 $101.59 → close $101.59 +0.00; THO×31 09:30 $70.93 → close $70.93 +0.00; USFD×7 09:30 $93.82 → close $93.82 +0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00 | — |

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
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
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
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
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
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-21 | `DE` | cash | leftover split 418.63 < 1 share @ 623.26 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-26 | `BE` | cash | leftover split 123.12 < 1 share @ 213.94 |
| 2026-08-26 | `NEM` | cash | leftover split 123.12 < 1 share @ 132.64 |
| 2026-08-26 | `CRM` | cash | leftover split 123.12 < 1 share @ 199.94 |
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
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `RY` | cash | leftover split 184.71 < 1 share @ 206.82 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
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
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
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
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
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
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
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
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 20 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $689.94; owner short_news_r_h3 |
| `AEHL` | 227 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $1877.56; owner short_news_r_h3 |
| `USFD` | 20 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $1892.05; owner short_news_r_h3 |
| `CBRL` | 52 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2478.60; owner union_e_fresh_h3 |
| `CTAS` | 12 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2478.60; owner union_e_fresh_h3 |
| `GIS` | 69 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2478.60; owner union_e_fresh_h3 |
| `KBH` | 52 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2478.60; owner union_e_fresh_h3 |
| `PAYX` | 22 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2478.60; owner union_e_fresh_h3 |
| `GLND` | 454 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1228.48; owner union_hot_n4_h1 |
| `HALO` | 15 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $1854.08; owner short_news_r_h3 |
