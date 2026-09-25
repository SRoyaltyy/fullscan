# Factor mine action — `combo_seh_333_skip`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_e_fresh_h3/union_hot_n4_h1 w=0.33,0.33,0.33 net=skip

Cash book **+11.26%** ($11,126) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 347 · skips 373 · realized $+3348.50.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 33%, union_e_fresh_h3 33%, union_hot_n4_h1 33%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name on opposite sides, both sit. If they agree on the side, the earlier claim still takes the only lot. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 33%, union_e_fresh_h3 33%, union_hot_n4_h1 33%.
- Member: short_news_r_h3 (33% · short · hold 3).
- Member: union_e_fresh_h3 (33% · long · hold 3).
- Member: union_hot_n4_h1 (33% · long · hold 1).
- Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day.

### When it buys

- At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- If two kids want the same name on opposite sides, skip it entirely. If they agree, the earlier claim (fresh-E, then heat, then other longs, then shorts) takes the only lot.
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,121.26.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 36 | $45.98 | $2.10 | — | $3,319.25 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 71 | $23.33 | $2.20 | — | $1,660.62 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $38.59 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.59 | ▲ close $10,449.19 vs 09:30 $10,000.00 (session +492.16) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.59 | ▲ 09:30 equity $10,528.70 vs yday $10,449.19 (+79.51) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 36 | $44.09 | $2.12 | $-72.26 | $1,623.71 | ▼ -72.26 after sell → book $10,526.58; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 71 | $22.92 | $2.23 | $-33.54 | $3,248.80 | ▼ -33.54 after sell → book $10,524.35; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,015.97 | ▲ +145.14 after sell → book $10,522.24; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 185 | $1.50 | $2.54 | — | $4,735.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $278.66; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 14 | $19.57 | $2.03 | — | $4,459.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $278.66; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 25 | $11.12 | $2.06 | — | $4,179.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $278.66; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 20 | $13.55 | $2.05 | — | $3,906.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $278.66; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 25 | $10.83 | $2.06 | — | $3,633.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $278.66; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 28 | $9.89 | $2.07 | — | $3,354.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $278.66; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 33 | $24.68 | $2.09 | — | $2,538.32 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $838.71; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 381 | $2.20 | $4.91 | — | $1,695.20 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $838.71; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 133 | $12.70 | $2.47 | — | $3,381.17 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1695.20; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,381.17 | ▲ close $11,047.69 vs 09:30 $10,528.70 (session +547.75) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,381.17 | ▼ 09:30 equity $10,918.37 vs yday $11,047.69 (-129.32) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 33 | $24.83 | $2.11 | $+0.75 | $4,198.45 | ▲ +0.75 after sell → book $10,916.26; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 381 | $2.08 | $4.99 | $-53.72 | $4,987.85 | ▼ -53.72 after sell → book $10,911.28; vs 09:30 mark -4.98 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 148 | $4.19 | $2.43 | — | $4,365.29 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $623.48; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 90 | $6.87 | $2.26 | — | $3,744.73 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $623.48; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 45 | $13.64 | $2.12 | — | $3,128.81 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $623.48; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 15 | $41.23 | $2.04 | — | $2,508.32 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $623.48; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 436 | $1.15 | $5.72 | — | $3,004.00 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $501.66; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 140 | $3.56 | $2.46 | — | $3,499.94 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $501.66; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 15 | $31.70 | $2.07 | — | $3,973.38 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $501.66; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 166 | $3.01 | $2.54 | — | $4,470.50 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $501.66; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 73 | $6.80 | $2.24 | — | $4,964.65 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $501.66; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,964.65 | ▲ close $11,163.93 vs 09:30 $10,918.37 (session +276.54) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,964.65 | ▼ 09:30 equity $11,134.41 vs yday $11,163.93 (-29.52) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,442.34 | ▲ +943.78 after sell → book $11,094.06; vs 09:30 mark -40.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $11,018.63 | ▲ +86.83 after sell → book $11,091.69; vs 09:30 mark -2.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 148 | $3.94 | $2.47 | $-41.90 | $11,599.29 | ▼ -41.90 after sell → book $11,089.23; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 45 | $13.31 | $2.15 | $-19.12 | $12,196.09 | ▼ -19.12 after sell → book $11,087.08; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 15 | $41.50 | $2.06 | $-0.04 | $12,816.54 | ▼ -0.04 after sell → book $11,085.03; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,816.54 | ▲ close $11,150.79 vs 09:30 $11,134.41 (session +65.76) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,816.54 | ▼ 09:30 equity $11,113.51 vs yday $11,150.79 (-37.28) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 185 | $1.42 | $2.59 | $-19.93 | $13,076.65 | ▼ -19.93 after sell → book $11,110.92; vs 09:30 mark -2.59 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 14 | $19.58 | $2.05 | $-3.94 | $13,348.72 | ▼ -3.94 after sell → book $11,108.87; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 25 | $9.10 | $2.08 | $-54.65 | $13,574.13 | ▼ -54.65 after sell → book $11,106.78; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 20 | $13.01 | $2.07 | $-14.92 | $13,832.26 | ▼ -14.92 after sell → book $11,104.71; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 25 | $10.85 | $2.08 | $-3.65 | $14,101.43 | ▼ -3.65 after sell → book $11,102.63; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 28 | $11.50 | $2.09 | $+40.77 | $14,421.33 | ▲ +40.77 after sell → book $11,100.53; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 133 | $11.75 | $2.39 | $+120.83 | $12,856.19 | ▲ +120.83 after sell → book $11,098.14; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 90 | $7.19 | $2.28 | $+24.26 | $13,501.01 | ▲ +24.26 after sell → book $11,095.86; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,501.01 | ▲ close $11,100.06 vs 09:30 $11,113.51 (session +4.20) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,501.01 | ▼ 09:30 equity $11,054.16 vs yday $11,100.06 (-45.90) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 436 | $0.96 | $5.51 | $+70.30 | $13,075.63 | ▲ +70.30 after sell → book $11,048.65; vs 09:30 mark -5.51 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 140 | $4.01 | $2.41 | $-68.57 | $12,511.12 | ▼ -68.57 after sell → book $11,046.24; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 15 | $31.87 | $2.04 | $-6.65 | $12,031.04 | ▼ -6.65 after sell → book $11,044.21; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 166 | $2.95 | $2.49 | $+4.93 | $11,538.85 | ▲ +4.93 after sell → book $11,041.72; vs 09:30 mark -2.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 73 | $6.81 | $2.21 | $-5.18 | $11,039.51 | ▼ -5.18 after sell → book $11,039.51; vs 09:30 mark -2.21 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 6 | $97.43 | $2.01 | — | $10,452.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $613.31; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2044 | $0.30 | $12.26 | — | $9,827.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $613.31; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 68 | $9.01 | $2.19 | — | $9,212.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $613.31; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 157 | $3.89 | $2.46 | — | $8,599.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $613.31; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 18 | $34.05 | $2.04 | — | $7,984.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $613.31; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 27 | $22.44 | $2.07 | — | $7,376.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $613.31; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $6,173.37 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1229.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1069 | $1.15 | $13.79 | — | $4,930.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1229.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 897 | $1.37 | $11.57 | — | $3,689.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1229.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $4,301.08 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $737.95; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 34 | $21.40 | $2.13 | — | $5,026.55 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $737.95; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 4 | $173.90 | $2.04 | — | $5,720.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $737.95; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $6,356.34 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $737.95; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 160 | $4.61 | $2.53 | — | $7,091.41 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $737.95; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,091.41 | ▲ close $11,052.11 vs 09:30 $11,054.16 (session +73.80) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,091.41 | ▲ 09:30 equity $11,174.39 vs yday $11,052.11 (+122.28) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 897 | $1.46 | $11.73 | $+57.43 | $8,389.30 | ▲ +57.43 after sell → book $11,162.66; vs 09:30 mark -11.73 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 3 | $115.18 | $2.00 | — | $8,041.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $399.49; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 9 | $42.41 | $2.02 | — | $7,658.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-26.1; combo leftover $399.49; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 22 | $17.93 | $2.06 | — | $7,261.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $399.49; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 4 | $93.98 | $2.00 | — | $6,883.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $399.49; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 9 | $43.08 | $2.02 | — | $6,493.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $399.49; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 173 | $2.30 | $2.51 | — | $6,093.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $399.49; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 339 | $4.49 | $4.37 | — | $4,566.88 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1523.34; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 223 | $6.81 | $2.88 | — | $3,045.37 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1523.34; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 195 | $3.11 | $2.63 | — | $3,649.19 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $609.07; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 6 | $89.10 | $2.04 | — | $4,181.75 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $609.07; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 15 | $38.40 | $2.07 | — | $4,755.68 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $609.07; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 29 | $20.90 | $2.11 | — | $5,359.66 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $609.07; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 22 | $27.00 | $2.09 | — | $5,951.57 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $609.07; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,951.57 | ▲ close $11,366.66 vs 09:30 $11,174.39 (session +234.80) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,951.57 | ▲ 09:30 equity $12,205.96 vs yday $11,366.66 (+839.30) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $7,091.14 | ▼ -63.57 after sell → book $12,203.93; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1069 | $1.83 | $13.98 | $+699.15 | $9,033.42 | ▲ +699.15 after sell → book $12,189.94; vs 09:30 mark -13.99 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 339 | $4.32 | $4.44 | $-66.44 | $10,493.46 | ▼ -66.44 after sell → book $12,185.50; vs 09:30 mark -4.44 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 223 | $8.03 | $2.93 | $+266.26 | $12,281.22 | ▲ +266.26 after sell → book $12,182.57; vs 09:30 mark -2.93 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,281.22 | ▲ close $12,233.20 vs 09:30 $12,205.96 (session +50.63) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,281.22 | ▲ 09:30 equity $12,311.86 vs yday $12,233.20 (+78.66) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 6 | $104.00 | $2.03 | $+35.38 | $12,903.20 | ▲ +35.38 after sell → book $12,309.84; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 2044 | $0.31 | $12.82 | $-4.64 | $13,524.02 | ▼ -4.64 after sell → book $12,297.02; vs 09:30 mark -12.82 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 68 | $9.23 | $2.22 | $+10.55 | $14,149.44 | ▲ +10.55 after sell → book $12,294.80; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 157 | $5.24 | $2.50 | $+206.99 | $14,969.63 | ▲ +206.99 after sell → book $12,292.31; vs 09:30 mark -2.49 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 18 | $34.72 | $2.06 | $+7.95 | $15,592.52 | ▲ +7.95 after sell → book $12,290.24; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 27 | $21.85 | $2.09 | $-20.09 | $16,180.38 | ▼ -20.09 after sell → book $12,288.15; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $15,542.38 | ▼ -26.68 after sell → book $12,286.15; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 34 | $20.90 | $2.09 | $+12.78 | $14,829.69 | ▲ +12.78 after sell → book $12,284.06; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 4 | $170.64 | $2.00 | $+9.00 | $14,145.13 | ▲ +9.00 after sell → book $12,282.06; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $13,509.64 | ▲ +0.75 after sell → book $12,280.05; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 160 | $4.77 | $2.47 | $-30.60 | $12,743.97 | ▼ -30.60 after sell → book $12,277.58; vs 09:30 mark -2.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 6 | $88.94 | $2.01 | — | $12,208.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 39 | $15.28 | $2.11 | — | $11,610.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $11,038.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 118 | $5.10 | $2.34 | — | $10,434.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 12 | $47.89 | $2.03 | — | $9,858.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 43 | $13.92 | $2.12 | — | $9,257.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 133 | $4.54 | $2.39 | — | $8,650.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $606.86; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 44 | $24.11 | $2.12 | — | $7,587.49 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1081.31; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 693 | $1.56 | $8.94 | — | $6,497.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1081.31; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 265 | $4.07 | $3.42 | — | $5,415.50 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1081.31; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 56 | $19.04 | $2.16 | — | $4,347.10 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1081.31; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 106 | $13.62 | $2.38 | — | $5,788.98 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1449.03; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 26 | $54.51 | $2.13 | — | $7,204.11 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1449.03; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $8,295.11 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1449.03; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,295.11 | ▲ close $12,436.47 vs 09:30 $12,311.86 (session +197.08) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,295.11 | ▼ 09:30 equity $12,375.81 vs yday $12,436.47 (-60.66) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 3 | $124.67 | $2.02 | $+24.45 | $8,667.10 | ▲ +24.45 after sell → book $12,373.79; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AAP` | 9 | $43.87 | $2.04 | $+9.09 | $9,059.89 | ▲ +9.09 after sell → book $12,371.75; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 22 | $18.14 | $2.08 | $+0.38 | $9,456.90 | ▲ +0.38 after sell → book $12,369.68; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 4 | $94.60 | $2.02 | $-1.54 | $9,833.27 | ▼ -1.54 after sell → book $12,367.65; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 9 | $44.39 | $2.04 | $+7.74 | $10,230.75 | ▲ +7.74 after sell → book $12,365.62; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 173 | $2.35 | $2.55 | $+3.59 | $10,634.75 | ▲ +3.59 after sell → book $12,363.07; vs 09:30 mark -2.55 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 195 | $2.83 | $2.58 | $+49.39 | $10,080.32 | ▲ +49.39 after sell → book $12,360.49; vs 09:30 mark -2.58 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 6 | $88.24 | $2.01 | $+1.11 | $9,548.88 | ▲ +1.11 after sell → book $12,358.49; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 15 | $38.41 | $2.04 | $-4.26 | $8,970.69 | ▼ -4.26 after sell → book $12,356.45; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 29 | $20.50 | $2.08 | $+7.41 | $8,374.11 | ▲ +7.41 after sell → book $12,354.37; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 22 | $26.00 | $2.06 | $+17.85 | $7,800.06 | ▲ +17.85 after sell → book $12,352.32; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 44 | $26.61 | $2.14 | $+105.74 | $8,968.76 | ▲ +105.74 after sell → book $12,350.18; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 693 | $1.60 | $9.06 | $+9.72 | $10,068.49 | ▲ +9.72 after sell → book $12,341.11; vs 09:30 mark -9.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 56 | $20.72 | $2.18 | $+89.74 | $11,226.63 | ▲ +89.74 after sell → book $12,338.93; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 1069 | $0.58 | $9.44 | — | $10,593.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $623.70; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 119 | $5.21 | $2.35 | — | $9,971.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $623.70; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 4 | $131.37 | $2.00 | — | $9,444.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $623.70; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 34 | $18.26 | $2.09 | — | $8,821.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $623.70; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 18 | $34.30 | $2.04 | — | $8,201.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $623.70; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $7,872.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $623.70; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 92 | $14.11 | $2.27 | — | $6,572.48 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1312.14; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 225 | $5.81 | $2.90 | — | $5,262.33 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1312.14; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 113 | $11.59 | $2.33 | — | $3,950.90 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1312.14; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 3 | $213.94 | $2.04 | — | $4,590.68 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $790.18; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 64 | $12.22 | $2.22 | — | $5,370.54 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $790.18; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 155 | $5.08 | $2.51 | — | $6,155.42 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $790.18; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 5 | $132.64 | $2.04 | — | $6,816.58 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $790.18; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 3 | $199.94 | $2.04 | — | $7,414.37 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $790.18; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,414.37 | ▲ close $12,477.02 vs 09:30 $12,375.81 (session +176.34) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,414.37 | ▲ 09:30 equity $12,535.33 vs yday $12,477.02 (+58.31) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 92 | $14.20 | $2.29 | $+3.72 | $8,718.47 | ▲ +3.72 after sell → book $12,533.03; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 225 | $6.50 | $2.95 | $+149.40 | $10,178.02 | ▲ +149.40 after sell → book $12,530.08; vs 09:30 mark -2.95 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 113 | $12.18 | $2.36 | $+62.55 | $11,552.00 | ▲ +62.55 after sell → book $12,527.72; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 5 | $80.60 | $2.00 | — | $11,147.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 29 | $16.18 | $2.08 | — | $10,675.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 4 | $118.77 | $2.00 | — | $10,198.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 27 | $17.78 | $2.07 | — | $9,716.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 35 | $13.41 | $2.10 | — | $9,245.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 4 | $97.16 | $2.00 | — | $8,854.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 2 | $206.82 | $2.00 | — | $8,438.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 4 | $120.17 | $2.00 | — | $7,956.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $481.33; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 216 | $9.19 | $2.79 | — | $5,968.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $1989.02; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 13 | $144.18 | $2.03 | — | $4,091.89 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1989.02; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 27 | $74.54 | $2.15 | — | $6,102.32 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2045.94; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 37 | $55.25 | $2.18 | — | $8,144.38 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2045.94; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,144.38 | ▲ close $12,548.10 vs 09:30 $12,535.33 (session +45.78) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,144.38 | ▼ 09:30 equity $12,371.30 vs yday $12,548.10 (-176.80) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 6 | $93.30 | $2.03 | $+22.12 | $8,702.15 | ▲ +22.12 after sell → book $12,369.27; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 39 | $18.15 | $2.13 | $+107.70 | $9,407.88 | ▲ +107.70 after sell → book $12,367.14; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 4 | $132.80 | $2.02 | $-42.26 | $9,937.06 | ▼ -42.26 after sell → book $12,365.12; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 118 | $4.58 | $2.37 | $-66.08 | $10,475.12 | ▼ -66.08 after sell → book $12,362.75; vs 09:30 mark -2.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 12 | $48.42 | $2.05 | $+2.29 | $11,054.12 | ▲ +2.29 after sell → book $12,360.70; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 43 | $15.66 | $2.14 | $+70.56 | $11,725.36 | ▲ +70.56 after sell → book $12,358.56; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 133 | $3.38 | $2.42 | $-159.76 | $12,172.48 | ▼ -159.76 after sell → book $12,356.14; vs 09:30 mark -2.42 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 265 | $3.69 | $3.47 | $-107.59 | $13,146.85 | ▼ -107.59 after sell → book $12,352.67; vs 09:30 mark -3.47 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 106 | $13.90 | $2.31 | $-33.83 | $11,671.15 | ▼ -33.83 after sell → book $12,350.36; vs 09:30 mark -2.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 26 | $52.49 | $2.07 | $+48.32 | $10,304.34 | ▲ +48.32 after sell → book $12,348.29; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $9,258.88 | ▲ +45.54 after sell → book $12,346.29; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $8,995.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 25 | $15.01 | $2.06 | — | $8,618.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 3 | $103.89 | $2.00 | — | $8,304.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 99 | $3.88 | $2.29 | — | $7,918.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 8 | $44.40 | $2.01 | — | $7,561.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 15 | $24.69 | $2.04 | — | $7,188.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 46 | $8.35 | $2.13 | — | $6,802.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 10 | $37.65 | $2.02 | — | $6,424.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $385.79; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 229 | $14.00 | $2.95 | — | $3,215.08 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $3212.02; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 6 | $252.24 | $2.07 | — | $4,726.45 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $1607.54; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 53 | $30.18 | $2.22 | — | $6,323.78 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $1607.54; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,323.78 | ▲ close $12,399.55 vs 09:30 $12,371.30 (session +77.05) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,323.78 | ▼ 09:30 equity $12,367.57 vs yday $12,399.55 (-31.98) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 1069 | $0.51 | $8.85 | $-96.32 | $6,860.12 | ▼ -96.32 after sell → book $12,358.72; vs 09:30 mark -8.85 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 119 | $5.00 | $2.38 | $-29.71 | $7,452.74 | ▼ -29.71 after sell → book $12,356.35; vs 09:30 mark -2.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 4 | $148.03 | $2.02 | $+62.62 | $8,042.84 | ▲ +62.62 after sell → book $12,354.33; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 34 | $19.25 | $2.11 | $+29.46 | $8,695.23 | ▲ +29.46 after sell → book $12,352.21; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 18 | $34.72 | $2.06 | $+3.45 | $9,318.12 | ▲ +3.45 after sell → book $12,350.15; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 1 | $298.01 | $2.01 | $-32.91 | $9,614.12 | ▼ -32.91 after sell → book $12,348.14; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 3 | $208.88 | $2.00 | $+11.14 | $8,985.48 | ▲ +11.14 after sell → book $12,346.14; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 64 | $11.10 | $2.18 | $+67.27 | $8,272.90 | ▲ +67.27 after sell → book $12,343.96; vs 09:30 mark -2.18 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 155 | $4.97 | $2.46 | $+11.31 | $7,499.32 | ▲ +11.31 after sell → book $12,341.50; vs 09:30 mark -2.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 5 | $127.45 | $2.00 | $+21.90 | $6,860.07 | ▲ +21.90 after sell → book $12,339.50; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 3 | $254.39 | $2.00 | $-167.38 | $6,094.90 | ▼ -167.38 after sell → book $12,337.50; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 216 | $9.50 | $2.84 | $+61.33 | $8,144.06 | ▲ +61.33 after sell → book $12,334.66; vs 09:30 mark -2.84 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 13 | $134.10 | $2.05 | $-135.12 | $9,885.31 | ▼ -135.12 after sell → book $12,332.61; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,885.31 | ▼ close $12,214.43 vs 09:30 $12,367.57 (session -118.18) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,885.31 | ▼ 09:30 equity $12,212.90 vs yday $12,214.43 (-1.53) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 5 | $79.83 | $2.02 | $-7.88 | $10,282.43 | ▼ -7.88 after sell → book $12,210.87; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 29 | $15.97 | $2.10 | $-10.26 | $10,743.46 | ▼ -10.26 after sell → book $12,208.77; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 4 | $113.66 | $2.02 | $-24.46 | $11,196.08 | ▼ -24.46 after sell → book $12,206.75; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 27 | $18.28 | $2.09 | $+9.34 | $11,687.55 | ▲ +9.34 after sell → book $12,204.66; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 35 | $12.18 | $2.12 | $-47.26 | $12,111.74 | ▼ -47.26 after sell → book $12,202.55; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 4 | $96.65 | $2.02 | $-6.06 | $12,496.31 | ▼ -6.06 after sell → book $12,200.52; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 2 | $203.78 | $2.02 | $-10.09 | $12,901.86 | ▼ -10.09 after sell → book $12,198.51; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 4 | $120.54 | $2.02 | $-2.54 | $13,382.00 | ▼ -2.54 after sell → book $12,196.49; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 27 | $73.22 | $2.07 | $+31.42 | $11,402.98 | ▲ +31.42 after sell → book $12,194.41; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 37 | $54.76 | $2.10 | $+13.85 | $9,374.76 | ▲ +13.85 after sell → book $12,192.31; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 229 | $13.04 | $3.02 | $-225.81 | $12,357.91 | ▼ -225.81 after sell → book $12,189.30; vs 09:30 mark -3.01 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,357.91 | ▼ close $12,171.40 vs 09:30 $12,212.90 (session -17.90) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,357.91 | ▲ 09:30 equity $12,186.48 vs yday $12,171.40 (+15.08) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $12,602.59 | ▼ -18.47 after sell → book $12,184.46; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 25 | $15.01 | $2.08 | $-4.15 | $12,975.76 | ▼ -4.15 after sell → book $12,182.38; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 3 | $92.00 | $2.02 | $-39.69 | $13,249.74 | ▼ -39.69 after sell → book $12,180.36; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 99 | $3.32 | $2.31 | $-60.04 | $13,576.11 | ▼ -60.04 after sell → book $12,178.05; vs 09:30 mark -2.31 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 8 | $44.17 | $2.03 | $-5.89 | $13,927.43 | ▼ -5.89 after sell → book $12,176.01; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 15 | $21.97 | $2.06 | $-44.89 | $14,254.93 | ▼ -44.89 after sell → book $12,173.96; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 46 | $8.58 | $2.15 | $+6.30 | $14,647.46 | ▲ +6.30 after sell → book $12,171.81; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 10 | $35.80 | $2.04 | $-22.56 | $15,003.37 | ▼ -22.56 after sell → book $12,169.77; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 6 | $235.71 | $2.01 | $+95.10 | $13,587.10 | ▲ +95.10 after sell → book $12,167.76; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 53 | $26.78 | $2.15 | $+175.84 | $12,165.61 | ▲ +175.84 after sell → book $12,165.61; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,165.61 | ▲ close $12,165.61 vs 09:30 $12,186.48 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,165.61 | ▲ 09:30 equity $12,165.61 vs yday $12,165.61 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 47 | $10.74 | $2.13 | — | $11,658.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $11,304.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 73 | $6.90 | $2.21 | — | $10,798.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $10,442.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 22 | $22.32 | $2.06 | — | $9,949.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 1 | $257.00 | $1.99 | — | $9,690.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 10 | $47.60 | $2.02 | — | $9,212.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 33 | $15.09 | $2.09 | — | $8,712.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $506.90; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 611 | $1.78 | $7.88 | — | $7,616.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1089.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 59 | $18.40 | $2.17 | — | $6,528.94 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1089.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 79 | $13.71 | $2.23 | — | $5,443.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1089.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 45 | $23.88 | $2.12 | — | $4,366.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1089.02; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 147 | $14.85 | $2.53 | — | $6,547.32 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2183.45; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1276 | $1.71 | $16.75 | — | $8,712.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2183.45; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,712.53 | ▲ close $12,211.63 vs 09:30 $12,165.61 (session +96.19) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,712.53 | ▲ 09:30 equity $12,319.70 vs yday $12,211.63 (+108.07) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 59 | $18.15 | $2.19 | $-19.10 | $9,781.19 | ▼ -19.10 after sell → book $12,317.51; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 79 | $13.89 | $2.25 | $+9.74 | $10,876.25 | ▲ +9.74 after sell → book $12,315.26; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 45 | $23.84 | $2.15 | $-6.07 | $11,946.91 | ▼ -6.07 after sell → book $12,313.12; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 7 | $63.18 | $2.01 | — | $11,502.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 56 | $8.74 | $2.16 | — | $11,011.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 7 | $68.52 | $2.01 | — | $10,529.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 137 | $3.62 | $2.40 | — | $10,031.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 2 | $167.55 | $2.00 | — | $9,694.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 11 | $44.90 | $2.02 | — | $9,198.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 5 | $98.15 | $2.00 | — | $8,705.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 31 | $15.70 | $2.08 | — | $8,217.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $497.79; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 54 | $25.18 | $2.15 | — | $6,855.30 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1369.53; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 174 | $7.87 | $2.51 | — | $5,483.41 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1369.53; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 236 | $5.79 | $3.04 | — | $4,113.93 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1369.53; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 440 | $4.67 | $5.82 | — | $6,162.91 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2056.96; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 26 | $76.55 | $2.15 | — | $8,151.06 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2056.96; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,151.06 | ▲ close $12,462.14 vs 09:30 $12,319.70 (session +181.38) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,151.06 | ▼ 09:30 equity $12,320.10 vs yday $12,462.14 (-142.04) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 611 | $1.56 | $7.99 | $-147.24 | $9,099.28 | ▼ -147.24 after sell → book $12,312.11; vs 09:30 mark -7.99 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 54 | $26.44 | $2.17 | $+63.71 | $10,524.87 | ▲ +63.71 after sell → book $12,309.94; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 174 | $7.76 | $2.55 | $-24.20 | $11,872.55 | ▼ -24.20 after sell → book $12,307.38; vs 09:30 mark -2.56 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 236 | $5.81 | $3.09 | $-1.42 | $13,240.62 | ▼ -1.42 after sell → book $12,304.29; vs 09:30 mark -3.09 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,240.62 | ▲ close $12,499.00 vs 09:30 $12,320.10 (session +194.71) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,240.62 | ▼ 09:30 equity $12,493.69 vs yday $12,499.00 (-5.31) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 47 | $10.51 | $2.15 | $-15.33 | $13,732.44 | ▼ -15.33 after sell → book $12,491.54; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $14,096.66 | ▲ +10.48 after sell → book $12,489.53; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 73 | $9.39 | $2.23 | $+177.33 | $14,779.89 | ▲ +177.33 after sell → book $12,487.29; vs 09:30 mark -2.24 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $15,119.78 | ▼ -16.60 after sell → book $12,485.28; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 22 | $21.67 | $2.08 | $-18.43 | $15,594.45 | ▼ -18.43 after sell → book $12,483.21; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 1 | $252.92 | $2.01 | $-8.09 | $15,845.35 | ▼ -8.09 after sell → book $12,481.19; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 10 | $56.94 | $2.04 | $+89.34 | $16,412.71 | ▲ +89.34 after sell → book $12,479.15; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 33 | $13.84 | $2.11 | $-45.45 | $16,867.32 | ▼ -45.45 after sell → book $12,477.04; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 147 | $13.60 | $2.43 | $+178.79 | $14,865.69 | ▲ +178.79 after sell → book $12,474.61; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1276 | $1.58 | $16.46 | $+132.67 | $12,833.15 | ▲ +132.67 after sell → book $12,458.15; vs 09:30 mark -16.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,833.15 | ▼ close $12,449.72 vs 09:30 $12,493.69 (session -8.44) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,833.15 | ▲ 09:30 equity $12,492.12 vs yday $12,449.72 (+42.40) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 7 | $67.44 | $2.03 | $+25.78 | $13,303.20 | ▲ +25.78 after sell → book $12,490.09; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 56 | $8.26 | $2.18 | $-31.22 | $13,763.58 | ▼ -31.22 after sell → book $12,487.91; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 7 | $64.60 | $2.03 | $-31.48 | $14,213.75 | ▼ -31.48 after sell → book $12,485.88; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 137 | $3.76 | $2.43 | $+15.03 | $14,726.44 | ▲ +15.03 after sell → book $12,483.44; vs 09:30 mark -2.44 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 2 | $142.43 | $2.02 | $-54.25 | $15,009.28 | ▼ -54.25 after sell → book $12,481.43; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 11 | $38.23 | $2.04 | $-77.49 | $15,427.71 | ▼ -77.49 after sell → book $12,479.38; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 5 | $98.71 | $2.02 | $-1.23 | $15,919.24 | ▼ -1.23 after sell → book $12,477.36; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 31 | $15.26 | $2.10 | $-17.83 | $16,390.20 | ▼ -17.83 after sell → book $12,475.26; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 440 | $4.36 | $5.68 | $+124.90 | $14,466.12 | ▲ +124.90 after sell → book $12,469.58; vs 09:30 mark -5.68 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 26 | $76.79 | $2.07 | $-10.46 | $12,467.51 | ▼ -10.46 after sell → book $12,467.51; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,467.51 | ▲ close $12,467.51 vs 09:30 $12,492.12 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,467.51 | ▲ 09:30 equity $12,467.51 vs yday $12,467.51 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $11,972.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 87 | $5.91 | $2.25 | — | $11,455.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $10,969.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 16 | $32.01 | $2.04 | — | $10,455.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 7 | $71.71 | $2.01 | — | $9,951.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 9 | $56.02 | $2.02 | — | $9,445.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 55 | $9.37 | $2.15 | — | $8,927.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 39 | $13.10 | $2.11 | — | $8,414.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $519.48; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 389 | $2.70 | $5.02 | — | $7,359.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1051.82; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 214 | $4.91 | $2.76 | — | $6,305.76 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1051.82; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 170 | $6.16 | $2.50 | — | $5,256.06 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1051.82; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 336 | $3.13 | $4.33 | — | $4,200.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1051.82; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 7 | $112.83 | $2.05 | — | $4,987.84 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $840.01; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 238 | $3.52 | $3.14 | — | $5,822.45 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $840.01; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 413 | $2.03 | $5.43 | — | $6,655.42 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $840.01; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 33 | $24.97 | $2.13 | — | $7,477.29 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $840.01; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 249 | $3.37 | $3.29 | — | $8,313.14 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $840.01; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,313.14 | ▲ close $12,543.71 vs 09:30 $12,467.51 (session +123.42) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,313.14 | ▲ 09:30 equity $12,633.94 vs yday $12,543.71 (+90.23) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 170 | $6.02 | $2.54 | $-28.84 | $9,334.00 | ▼ -28.84 after sell → book $12,631.40; vs 09:30 mark -2.54 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,334.00 | ▲ close $12,903.87 vs 09:30 $12,633.94 (session +272.47) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,334.00 | ▲ 09:30 equity $12,956.27 vs yday $12,903.87 (+52.40) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 214 | $5.11 | $2.81 | $+37.23 | $10,424.73 | ▲ +37.23 after sell → book $12,953.46; vs 09:30 mark -2.81 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 336 | $3.64 | $4.40 | $+162.63 | $11,643.37 | ▲ +162.63 after sell → book $12,949.06; vs 09:30 mark -4.40 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,643.37 | ▲ close $12,995.33 vs 09:30 $12,956.27 (session +46.27) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,643.37 | ▼ 09:30 equity $12,984.05 vs yday $12,995.33 (-11.28) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 3 | $140.03 | $2.02 | $-77.22 | $12,061.44 | ▼ -77.22 after sell → book $12,982.03; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 87 | $6.25 | $2.28 | $+25.05 | $12,602.92 | ▲ +25.05 after sell → book $12,979.76; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 2 | $253.34 | $2.02 | $+18.33 | $13,107.58 | ▲ +18.33 after sell → book $12,977.74; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 16 | $30.57 | $2.06 | $-27.14 | $13,594.64 | ▼ -27.14 after sell → book $12,975.68; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 7 | $78.12 | $2.03 | $+40.83 | $14,139.45 | ▲ +40.83 after sell → book $12,973.65; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 9 | $61.93 | $2.04 | $+49.14 | $14,694.79 | ▲ +49.14 after sell → book $12,971.62; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 55 | $9.40 | $2.17 | $-2.68 | $15,209.61 | ▼ -2.68 after sell → book $12,969.44; vs 09:30 mark -2.18 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 39 | $15.75 | $2.13 | $+99.12 | $15,821.73 | ▲ +99.12 after sell → book $12,967.31; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 7 | $118.18 | $2.01 | $-41.48 | $14,992.46 | ▼ -41.48 after sell → book $12,965.30; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 238 | $3.98 | $3.07 | $-115.69 | $14,042.15 | ▼ -115.69 after sell → book $12,962.23; vs 09:30 mark -3.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 413 | $1.85 | $5.33 | $+63.58 | $13,272.78 | ▲ +63.58 after sell → book $12,956.91; vs 09:30 mark -5.32 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 33 | $24.42 | $2.09 | $+13.93 | $12,464.83 | ▲ +13.93 after sell → book $12,954.82; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 249 | $3.75 | $3.21 | $-101.12 | $11,527.86 | ▼ -101.12 after sell → book $12,951.60; vs 09:30 mark -3.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 57 | $33.14 | $2.16 | — | $9,636.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $1921.31; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 46 | $40.93 | $2.13 | — | $7,751.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1921.31; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 717 | $1.80 | $9.25 | — | $6,451.97 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1291.97; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 55 | $23.29 | $2.15 | — | $5,168.86 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1291.97; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 88 | $14.62 | $2.25 | — | $3,880.05 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1291.97; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 104 | $18.61 | $2.39 | — | $5,813.10 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1940.02; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 284 | $6.83 | $3.78 | — | $7,749.04 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1940.02; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,749.04 | ▼ close $12,751.37 vs 09:30 $12,984.05 (session -176.12) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,749.04 | ▲ 09:30 equity $12,870.65 vs yday $12,751.37 (+119.28) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 55 | $24.09 | $2.18 | $+39.67 | $9,071.81 | ▲ +39.67 after sell → book $12,868.47; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 88 | $13.77 | $2.28 | $-79.33 | $10,281.30 | ▼ -79.33 after sell → book $12,866.20; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 458 | $11.21 | $5.91 | — | $5,141.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $5140.65; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 646 | $7.95 | $8.62 | — | $10,268.29 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $5141.21; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,268.29 | ▲ close $13,458.17 vs 09:30 $12,870.65 (session +606.50) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,268.29 | ▼ 09:30 equity $13,449.88 vs yday $13,458.17 (-8.29) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 717 | $1.96 | $9.38 | $+96.09 | $11,664.23 | ▲ +96.09 after sell → book $13,440.50; vs 09:30 mark -9.38 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 66 | $29.32 | $2.19 | — | $9,726.92 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1944.04; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 640 | $3.04 | $8.26 | — | $7,776.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1944.04; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 23 | $81.40 | $2.06 | — | $5,902.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1944.04; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 171 | $34.44 | $2.74 | — | $11,788.50 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5902.00; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,788.50 | ▲ close $14,076.63 vs 09:30 $13,449.88 (session +651.38) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,788.50 | ▲ 09:30 equity $14,330.77 vs yday $14,076.63 (+254.14) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 389 | $3.55 | $5.09 | $+320.54 | $13,164.36 | ▲ +320.54 after sell → book $14,325.68; vs 09:30 mark -5.09 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 57 | $40.03 | $2.19 | $+388.38 | $15,443.88 | ▲ +388.38 after sell → book $14,323.49; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 46 | $41.00 | $2.15 | $-1.06 | $17,327.73 | ▼ -1.06 after sell → book $14,321.34; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 104 | $22.11 | $2.30 | $-368.69 | $15,025.98 | ▼ -368.69 after sell → book $14,319.03; vs 09:30 mark -2.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 284 | $6.55 | $3.66 | $+72.08 | $13,162.12 | ▲ +72.08 after sell → book $14,315.37; vs 09:30 mark -3.66 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 66 | $29.43 | $2.21 | $+2.86 | $15,102.29 | ▲ +2.86 after sell → book $14,313.16; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 640 | $4.00 | $8.38 | $+600.96 | $17,653.90 | ▲ +600.96 after sell → book $14,304.77; vs 09:30 mark -8.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 23 | $79.08 | $2.08 | $-57.50 | $19,470.66 | ▼ -57.50 after sell → book $14,302.69; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 985 | $2.47 | $12.71 | — | $17,025.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $2433.83; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 143 | $16.91 | $2.42 | — | $14,604.45 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $2433.83; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1475 | $1.65 | $19.03 | — | $12,151.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $2433.83; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 208 | $11.67 | $2.68 | — | $9,721.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $2433.83; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 431 | $8.26 | $5.76 | — | $13,275.94 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3566.46; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 6 | $583.88 | $2.14 | — | $16,777.07 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3566.46; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,777.07 | ▲ close $14,698.09 vs 09:30 $14,330.77 (session +440.14) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,777.07 | ▼ 09:30 equity $14,623.32 vs yday $14,698.09 (-74.77) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 646 | $8.28 | $8.33 | $-226.91 | $11,423.09 | ▼ -226.91 after sell → book $14,614.99; vs 09:30 mark -8.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 208 | $9.11 | $2.68 | — | $9,525.53 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $1903.85; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 263 | $7.23 | $3.39 | — | $7,620.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $1903.85; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 38 | $93.97 | $2.24 | — | $11,189.26 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3652.23; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,189.26 | ▼ close $14,327.69 vs 09:30 $14,623.32 (session -278.99) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,189.26 | ▼ 09:30 equity $13,861.31 vs yday $14,327.69 (-466.38) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 458 | $13.82 | $6.03 | $+1183.44 | $17,512.79 | ▲ +1,183.44 after sell → book $13,855.27; vs 09:30 mark -6.04 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 143 | $16.92 | $2.46 | $-3.45 | $19,929.89 | ▼ -3.45 after sell → book $13,852.81; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1475 | $1.41 | $19.29 | $-392.32 | $21,990.35 | ▼ -392.32 after sell → book $13,833.52; vs 09:30 mark -19.29 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 208 | $12.80 | $2.74 | $+229.62 | $24,650.01 | ▲ +229.62 after sell → book $13,830.78; vs 09:30 mark -2.74 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 208 | $8.39 | $2.73 | $-155.17 | $26,392.40 | ▼ -155.17 after sell → book $13,828.05; vs 09:30 mark -2.73 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 263 | $6.83 | $3.45 | $-112.04 | $28,185.24 | ▼ -112.04 after sell → book $13,824.60; vs 09:30 mark -3.45 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 39 | $47.57 | $2.11 | — | $26,327.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1879.02; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 9 | $196.78 | $2.02 | — | $24,554.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1879.02; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 52 | $35.74 | $2.15 | — | $22,694.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1879.02; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 39 | $47.15 | $2.11 | — | $20,853.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1879.02; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 17 | $109.67 | $2.04 | — | $18,986.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1879.02; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1172 | $2.70 | $15.12 | — | $15,807.33 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $3164.47; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 75 | $41.76 | $2.21 | — | $12,673.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $3164.47; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 704 | $4.49 | $9.08 | — | $9,503.07 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $3164.47; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 58 | $116.85 | $2.41 | — | $16,277.96 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6893.88; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,277.96 | ▼ close $13,539.65 vs 09:30 $13,861.31 (session -245.70) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,277.96 | ▲ 09:30 equity $13,571.66 vs yday $13,539.65 (+32.01) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 985 | $2.68 | $12.89 | $+181.25 | $18,904.87 | ▲ +181.25 after sell → book $13,558.76; vs 09:30 mark -12.90 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 6 | $600.27 | $2.01 | $-102.49 | $15,301.24 | ▼ -102.49 after sell → book $13,556.76; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 75 | $36.02 | $2.25 | $-434.59 | $18,000.87 | ▼ -434.59 after sell → book $13,554.51; vs 09:30 mark -2.25 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 704 | $3.92 | $9.22 | $-416.06 | $20,754.85 | ▼ -416.06 after sell → book $13,545.29; vs 09:30 mark -9.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,754.85 | ▲ close $15,843.93 vs 09:30 $13,571.66 (session +2,298.64) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,866.46 | ▲ 09:30 equity $11,457.27 vs yday $11,055.26 (+402.01) | 09:30 open · cash $6,866.46 (unchanged overnight, no fees) · equity $11,457.27 vs prior close $11,055.26 (+402.01) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $5,090.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $2288.82; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 28 | $29.76 | $2.07 | — | $4,255.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $848.41; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 52 | $16.21 | $2.15 | — | $3,410.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $848.41; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 54 | $15.58 | $2.15 | — | $2,566.51 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $848.41; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 326 | $7.85 | $4.35 | — | $5,121.26 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $2566.51; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,121.26 | ▼ close $11,125.95 vs 09:30 $11,457.27 (session -318.60) | 16:00 close · cash $5,121.26 · equity $11,125.95 vs 09:30 $11,457.27 (-331.32; session marks -318.60) · 20 name(s) marked open→close (per-name table). ABVX×15 09:30 $94.87 → close $94.87 +0.00; AEHL×319 09:30 $9.05 → close $9.36 -98.89; ANAB×29 09:30 $51.70 → close $51.70 +0.00; BAND×42 09:30 $61.83 → close $61.83 -0.00; CBRL×32 09:30 $52.39 → close $51.81 -18.56; CTAS×7 09:30 $197.68 → close $197.68 -0.00; GIS×43 09:30 $34.83 → close $34.83 +0.00; GLND×586 09:30 $6.06 → close $5.54 -304.72; HALO×21 09:30 $115.36 → close $113.90 +30.66; KBH×32 09:30 $47.65 → close $47.65 +0.00; MLKN×79 09:30 $19.91 → close $19.91 -0.00; PAYX×21 09:30 $101.59 → close $101.59 +0.00; THO×22 09:30 $70.93 → close $70.93 +0.00; USFD×26 09:30 $93.82 → close $93.82 +0.00; VICR×5 09:30 $276.06 → close $276.06 -0.00; COST×2 09:30 $887.00 → close $922.76 +71.53; TJGC×28 09:30 $29.76 → close $26.24 -98.56; SECZ×52 09:30 $16.21 → close $15.96 -13.00; USDE×54 09:30 $15.58 → close $17.25 +90.12; RSKD×326 09:30 $7.85 → close $7.78 +22.82 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `EU` | net | net skip: short_news_r_h3 short dropped |
| 2026-08-14 | `LUNR` | net | net skip: short_news_r_h3 short dropped |
| 2026-08-14 | `EU` | net | net skip: union_e_fresh_h3 long dropped |
| 2026-08-14 | `LUNR` | net | net skip: union_e_fresh_h3 long dropped |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `ARX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AIRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `MH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `CLBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `NMAX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
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
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
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
| 2026-08-20 | `TOYO` | net | net skip: short_news_r_h3 short dropped |
| 2026-08-20 | `ABCL` | net | net skip: short_news_r_h3 short dropped |
| 2026-08-20 | `AAP` | net | net skip: short_news_r_h3 short dropped |
| 2026-08-20 | `TOYO` | net | net skip: union_e_fresh_h3 long dropped |
| 2026-08-20 | `AAP` | net | net skip: union_e_fresh_h3 long dropped |
| 2026-08-20 | `ABCL` | net | net skip: union_hot_n4_h1 long dropped |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-21 | `DE` | cash | leftover split 399.49 < 1 share @ 623.26 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BMO` | net | net skip: short_news_r_h3 short dropped |
| 2026-08-25 | `BMO` | net | net skip: union_e_fresh_h3 long dropped |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
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
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
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
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
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
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
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
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
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
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `LEN` | net | net skip: short_news_r_h3 short dropped |
| 2026-09-17 | `LEN` | net | net skip: union_e_fresh_h3 long dropped |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
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
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
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
| `FIVN` | 171 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5902.00; owner short_news_r_h3 |
| `AEHL` | 431 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3566.46; owner short_news_r_h3 |
| `USFD` | 38 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3652.23; owner short_news_r_h3 |
| `CBRL` | 39 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $1879.02; owner union_e_fresh_h3 |
| `CTAS` | 9 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1879.02; owner union_e_fresh_h3 |
| `GIS` | 52 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1879.02; owner union_e_fresh_h3 |
| `KBH` | 39 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $1879.02; owner union_e_fresh_h3 |
| `PAYX` | 17 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $1879.02; owner union_e_fresh_h3 |
| `GLND` | 1172 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $3164.47; owner union_hot_n4_h1 |
| `HALO` | 58 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6893.88; owner short_news_r_h3 |
