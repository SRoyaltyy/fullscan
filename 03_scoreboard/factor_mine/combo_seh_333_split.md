# Factor mine action — `combo_seh_333_split`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · split short_news_r_h3/union_e_fresh_h3/union_hot_n4_h1 w=0.33,0.33,0.33 net=priority

Cash book **+1.93%** ($10,193) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 342 · skips 367 · realized $+2181.46.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell, each with their own slice of $10,000: short_news_r_h3 33%, union_e_fresh_h3 33%, union_hot_n4_h1 33%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. Each kid gets their own slice of the $10,000 and keeps it — two (or three) tiny books added together. They do not share leftover cash, so the same name can appear in two slices. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,482.19.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2057 | $0.81 | $22.83 | — | $1,644.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1028 | $0.81 | $11.41 | — | $28.90 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $2,503.65 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 35 | $23.33 | $2.10 | — | $1,685.00 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $873.00 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 74 | $22.01 | $2.21 | — | $13.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,333.33 | ▲ close $3,333.33 vs 09:30 $3,333.33 (session +0.00) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.38 | ▲ close $3,588.14 vs 09:30 $3,333.33 (session +279.85) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.90 | ▲ close $3,443.25 vs 09:30 $3,333.33 (session +127.51) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,333.33 | ▲ 09:30 equity $3,333.33 vs yday $3,333.33 (+0.00) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,333.33 | ▲ 09:30 equity $3,333.33 vs yday $3,333.33 (+0.00) | — | — |
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,333.33 | ▲ 09:30 equity $3,333.33 vs yday $3,333.33 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 77 | $11.12 | $2.22 | — | $18.22 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $861.59 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 44 | $19.57 | $2.12 | — | $1,741.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $861.59 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $11.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1 | $1.18 | $0.01 | — | $10.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 34 | $24.68 | $2.09 | — | $2,605.13 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $861.59 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 391 | $2.20 | $5.04 | — | $876.68 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $861.59 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,960.15 | ▲ close $3,333.71 vs 09:30 $3,333.33 (session +10.80) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.67 | ▲ close $3,959.80 vs 09:30 $3,652.81 (session +307.02) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.22 | ▼ close $3,341.11 vs 09:30 $3,465.40 (session -93.75) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,333.33 | ▲ 09:30 equity $3,333.33 vs yday $3,333.33 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.38 | ▲ 09:30 equity $3,652.81 vs yday $3,588.14 (+64.67) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.90 | ▲ 09:30 equity $3,465.40 vs yday $3,443.25 (+22.15) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1028 | $0.93 | $12.82 | $+99.12 | $3,446.34 | ▲ +99.12 after sell → book $3,446.34; vs 09:30 mark -12.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 18 | $44.09 | $2.06 | $-38.13 | $820.46 | ▼ -38.13 after sell → book $3,463.34; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 35 | $22.92 | $2.12 | $-18.56 | $1,620.55 | ▼ -18.56 after sell → book $3,461.23; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 16 | $55.29 | $2.06 | $+70.57 | $2,503.13 | ▲ +70.57 after sell → book $3,459.17; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 470 | $1.18 | $6.17 | — | $3,881.77 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $555.56 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 28 | $19.17 | $2.11 | — | $4,416.42 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $555.56 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 43 | $12.70 | $2.15 | — | $4,960.15 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $555.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 118 | $6.87 | $2.34 | — | $1,635.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $816.01 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 19 | $41.23 | $2.05 | — | $43.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $816.01 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 59 | $13.64 | $2.17 | — | $828.67 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $816.01 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 194 | $4.19 | $2.57 | — | $2,448.60 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $816.01 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,576.57 | ▲ close $3,333.71 vs 09:30 $3,303.29 (session +43.05) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.67 | ▲ close $4,081.69 vs 09:30 $3,909.73 (session +171.96) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.25 | ▲ close $3,263.04 vs 09:30 $3,275.65 (session +8.14) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,960.15 | ▼ 09:30 equity $3,303.29 vs yday $3,333.71 (-30.42) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.67 | ▼ 09:30 equity $3,909.73 vs yday $3,959.80 (-50.07) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.22 | ▼ 09:30 equity $3,275.65 vs yday $3,341.11 (-65.46) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 77 | $9.57 | $2.24 | $-123.81 | $3,264.03 | ▼ -123.81 after sell → book $3,264.03; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 44 | $19.57 | $2.14 | $-4.26 | $1,719.27 | ▼ -4.26 after sell → book $3,271.39; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 34 | $24.83 | $2.11 | $+0.90 | $860.33 | ▲ +0.90 after sell → book $3,273.54; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 391 | $2.08 | $5.12 | $-55.13 | $2,529.39 | ▼ -55.13 after sell → book $3,266.28; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 10 | $31.70 | $2.05 | — | $5,926.60 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $330.33 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 109 | $3.01 | $2.35 | — | $6,252.34 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $330.33 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 48 | $6.80 | $2.16 | — | $6,576.57 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; leftover $330.33 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 287 | $1.15 | $3.77 | — | $5,286.43 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $330.33 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 92 | $3.56 | $2.30 | — | $5,611.65 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $330.33 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,576.57 | ▲ close $3,462.02 vs 09:30 $3,383.24 (session +78.78) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,015.19 | ▼ close $4,017.71 vs 09:30 $4,047.00 (session -0.15) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,374.53 | ▼ close $3,209.97 vs 09:30 $3,266.40 (session -49.56) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,576.57 | ▲ 09:30 equity $3,383.24 vs yday $3,333.71 (+49.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.67 | ▼ 09:30 equity $4,047.00 vs yday $4,081.69 (-34.69) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.25 | ▲ 09:30 equity $3,266.40 vs yday $3,263.04 (+3.36) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 19 | $41.50 | $2.07 | $+1.02 | $2,374.53 | ▲ +1.02 after sell → book $3,259.53; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 2057 | $1.14 | $26.90 | $+629.08 | $2,328.75 | ▲ +629.08 after sell → book $4,020.10; vs 09:30 mark -26.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 59 | $13.31 | $2.19 | $-23.82 | $1,588.10 | ▼ -23.82 after sell → book $3,261.60; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 74 | $22.82 | $2.24 | $+55.49 | $4,015.19 | ▲ +55.49 after sell → book $4,017.86; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 194 | $3.94 | $2.61 | $-53.69 | $805.00 | ▼ -53.69 after sell → book $3,263.79; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,560.24 | ▲ close $3,458.92 vs 09:30 $3,451.42 (session +15.68) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,017.61 | ▲ close $4,017.61 vs 09:30 $4,017.68 (session +0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,220.58 | ▲ close $3,220.58 vs 09:30 $3,222.95 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 470 | $1.07 | $6.06 | $+39.47 | $6,067.61 | ▲ +39.47 after sell → book $3,445.36; vs 09:30 mark -6.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 43 | $11.75 | $2.12 | $+36.36 | $5,560.24 | ▲ +36.36 after sell → book $3,443.24; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,576.57 | ▼ 09:30 equity $3,451.42 vs yday $3,462.02 (-10.60) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,015.19 | ▼ 09:30 equity $4,017.68 vs yday $4,017.71 (-0.03) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,374.53 | ▲ 09:30 equity $3,222.95 vs yday $3,209.97 (+12.98) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 1 | $1.42 | $0.04 | $-0.14 | $4,016.58 | ▼ -0.14 after sell → book $4,017.65; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 118 | $7.19 | $2.37 | $+33.04 | $3,220.58 | ▲ +33.04 after sell → book $3,220.58; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 1 | $1.07 | $0.03 | $-0.16 | $4,017.61 | ▼ -0.16 after sell → book $4,017.61; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 10 | $46.85 | $2.02 | — | $2,042.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 68 | $11.81 | $2.19 | — | $848.23 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $805.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 55 | $9.01 | $2.15 | — | $1,545.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 129 | $3.89 | $2.38 | — | $1,040.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 14 | $34.05 | $2.03 | — | $562.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 22 | $22.44 | $2.06 | — | $66.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 587 | $1.37 | $7.57 | — | $36.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $805.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 700 | $1.15 | $9.03 | — | $1,653.85 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $805.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1674 | $0.30 | $10.04 | — | $2,513.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 5 | $97.43 | $2.00 | — | $3,528.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $502.20 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $2,467.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $805.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 113 | $4.43 | $2.33 | — | $3,025.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $502.20 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,039.01 | ▲ close $3,431.47 vs 09:30 $3,439.71 (session +22.34) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.41 | ▲ close $4,029.46 vs 09:30 $4,017.61 (session +36.86) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.47 | ▼ close $3,168.11 vs 09:30 $3,220.58 (session -31.67) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 10 | $31.87 | $2.02 | $-5.77 | $4,078.16 | ▼ -5.77 after sell → book $3,429.73; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 109 | $2.95 | $2.32 | $+1.87 | $3,754.29 | ▲ +1.87 after sell → book $3,427.41; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 28 | $18.13 | $2.07 | $+24.94 | $5,050.53 | ▲ +24.94 after sell → book $3,437.64; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 48 | $6.81 | $2.13 | $-4.78 | $3,425.28 | ▼ -4.78 after sell → book $3,425.28; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 287 | $0.96 | $3.62 | $+46.28 | $4,770.52 | ▲ +46.28 after sell → book $3,434.01; vs 09:30 mark -3.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 92 | $4.01 | $2.27 | $-46.43 | $4,398.88 | ▼ -46.43 after sell → book $3,431.75; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,560.24 | ▼ 09:30 equity $3,439.71 vs yday $3,458.92 (-19.21) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,017.61 | ▲ 09:30 equity $4,017.61 vs yday $4,017.61 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,220.58 | ▲ 09:30 equity $3,220.58 vs yday $3,220.58 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 4 | $46.85 | $1.91 | — | $4,618.36 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 18 | $11.81 | $2.07 | — | $4,260.74 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $3,627.71 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 46 | $4.61 | $2.15 | — | $5,039.01 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 1 | $173.90 | $1.77 | — | $4,432.87 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 48 | $4.43 | $2.16 | — | $4,050.14 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 2 | $106.38 | $2.02 | — | $4,829.10 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $214.08 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 10 | $21.40 | $2.05 | — | $3,839.66 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $214.08 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 121 | $6.81 | $2.35 | — | $6.30 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $835.18 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 4 | $2.30 | $0.10 | — | $57.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $9.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 186 | $4.49 | $2.55 | — | $832.67 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $835.18 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,653.55 | ▼ close $3,398.18 vs 09:30 $3,414.33 (session -3.66) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.11 | ▲ close $4,123.53 vs 09:30 $4,045.26 (session +78.37) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.30 | ▲ close $3,307.30 vs 09:30 $3,269.80 (session +52.30) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,039.01 | ▼ 09:30 equity $3,414.33 vs yday $3,431.47 (-17.14) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.41 | ▲ 09:30 equity $4,045.26 vs yday $4,029.46 (+15.80) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.47 | ▲ 09:30 equity $3,269.80 vs yday $3,168.11 (+101.69) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 68 | $11.57 | $2.22 | $-21.07 | $821.01 | ▼ -21.07 after sell → book $3,267.58; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 587 | $1.46 | $7.68 | $+37.58 | $1,670.35 | ▲ +37.58 after sell → book $3,259.90; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 13 | $20.90 | $2.06 | — | $6,385.60 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $284.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 3 | $89.10 | $2.03 | — | $5,849.19 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $284.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 2 | $133.11 | $2.02 | — | $5,583.92 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $284.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 10 | $27.00 | $2.05 | — | $6,653.55 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $284.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 91 | $3.11 | $2.30 | — | $5,319.72 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $284.53 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 7 | $38.40 | $2.04 | — | $6,115.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $284.53 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,653.55 | ▲ close $3,443.12 vs 09:30 $3,419.05 (session +24.07) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.11 | ▲ close $4,187.12 vs 09:30 $4,134.01 (session +53.11) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,759.80 | ▲ close $3,759.80 vs 09:30 $3,775.95 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,653.55 | ▲ 09:30 equity $3,419.05 vs yday $3,398.18 (+20.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.11 | ▲ 09:30 equity $4,134.01 vs yday $4,123.53 (+10.48) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.30 | ▲ 09:30 equity $3,775.95 vs yday $3,307.30 (+468.65) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 121 | $8.03 | $2.38 | $+142.88 | $3,759.80 | ▲ +142.88 after sell → book $3,759.80; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 700 | $1.83 | $9.16 | $+457.81 | $1,989.62 | ▲ +457.81 after sell → book $3,764.77; vs 09:30 mark -9.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $717.78 | ▼ -41.23 after sell → book $3,773.93; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 186 | $4.32 | $2.59 | $-36.76 | $2,790.55 | ▼ -36.76 after sell → book $3,762.18; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 49 | $19.04 | $2.14 | — | $20.47 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $939.95 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 2 | $175.01 | $2.00 | — | $3,804.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 5 | $88.94 | $2.00 | — | $3,357.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 34 | $15.28 | $2.09 | — | $2,836.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 602 | $1.56 | $7.77 | — | $1,894.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $939.95 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 3 | $142.36 | $2.00 | — | $2,407.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 101 | $5.10 | $2.29 | — | $1,889.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 10 | $47.89 | $2.02 | — | $1,408.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 37 | $13.92 | $2.10 | — | $891.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 38 | $24.11 | $2.10 | — | $2,841.52 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $939.95 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 114 | $4.54 | $2.33 | — | $371.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; leftover $519.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 230 | $4.07 | $2.97 | — | $955.56 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $939.95 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,540.99 | ▼ close $3,388.05 vs 09:30 $3,456.81 (session -44.78) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $371.24 | ▼ close $4,005.74 vs 09:30 $4,191.48 (session -143.27) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.47 | ▲ close $4,060.80 vs 09:30 $3,759.80 (session +315.97) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 4 | $43.63 | $1.76 | $+9.21 | $5,465.58 | ▲ +9.21 after sell → book $3,445.16; vs 09:30 mark -1.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 18 | $11.00 | $2.03 | $+10.57 | $5,814.21 | ▲ +10.57 after sell → book $3,448.63; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $6,439.56 | ▼ -11.56 after sell → book $3,454.82; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 46 | $4.77 | $2.13 | $-11.64 | $5,030.88 | ▼ -11.64 after sell → book $3,441.04; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 1 | $170.64 | $1.71 | $-0.22 | $5,641.86 | ▼ -0.22 after sell → book $3,446.92; vs 09:30 mark -1.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 48 | $4.42 | $2.13 | $-3.81 | $6,014.24 | ▼ -3.81 after sell → book $3,450.66; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 2 | $105.58 | $2.00 | $-2.42 | $5,252.43 | ▼ -2.42 after sell → book $3,443.17; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 10 | $20.90 | $2.02 | $+0.93 | $6,228.54 | ▲ +0.93 after sell → book $3,452.80; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,653.55 | ▲ 09:30 equity $3,456.81 vs yday $3,443.12 (+13.69) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.11 | ▲ 09:30 equity $4,191.48 vs yday $4,187.12 (+4.36) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,759.80 | ▲ 09:30 equity $3,759.80 vs yday $3,759.80 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 10 | $43.63 | $2.04 | $-36.26 | $2,014.89 | ▼ -36.26 after sell → book $4,174.56; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 55 | $9.23 | $2.17 | $+7.77 | $2,520.36 | ▲ +7.77 after sell → book $4,172.38; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 129 | $5.24 | $2.41 | $+169.36 | $3,193.91 | ▲ +169.36 after sell → book $4,169.97; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 14 | $34.72 | $2.05 | $+5.30 | $3,677.94 | ▲ +5.30 after sell → book $4,167.92; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 22 | $21.85 | $2.08 | $-17.11 | $4,156.57 | ▼ -17.11 after sell → book $4,165.85; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1674 | $0.31 | $10.50 | $-3.80 | $1,580.63 | ▼ -3.80 after sell → book $4,176.60; vs 09:30 mark -10.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 5 | $104.00 | $2.02 | $+28.82 | $575.08 | ▲ +28.82 after sell → book $4,189.45; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 113 | $4.42 | $2.36 | $-5.82 | $1,072.19 | ▼ -5.82 after sell → book $4,187.10; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 7 | $54.51 | $2.04 | — | $5,830.67 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $430.13 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 31 | $13.62 | $2.11 | — | $5,451.14 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $430.13 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 2 | $175.01 | $2.03 | — | $6,178.66 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $430.13 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 1 | $364.35 | $2.02 | — | $6,540.99 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $430.13 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 3 | $18.26 | $0.56 | — | $198.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; leftover $63.42 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 1 | $34.30 | $0.35 | — | $163.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; leftover $63.42 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 70 | $14.11 | $2.20 | — | $2,008.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $999.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 86 | $11.59 | $2.25 | — | $7.68 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $999.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 108 | $0.58 | $0.95 | — | $316.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; leftover $63.42 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 12 | $5.21 | $0.66 | — | $253.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $63.42 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 172 | $5.81 | $2.51 | — | $1,006.24 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $999.32 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,253.95 | ▼ close $3,378.38 vs 09:30 $3,452.52 (session -51.44) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.43 | ▲ close $4,123.32 vs 09:30 $3,994.72 (session +131.25) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.68 | ▲ close $3,961.70 vs 09:30 $3,886.43 (session +94.39) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 13 | $20.50 | $2.03 | $+1.11 | $5,164.67 | ▲ +1.11 after sell → book $3,442.22; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 3 | $88.24 | $2.00 | $-1.45 | $5,704.08 | ▼ -1.45 after sell → book $3,446.26; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 2 | $154.20 | $2.00 | $-46.20 | $5,970.80 | ▼ -46.20 after sell → book $3,448.26; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 10 | $26.00 | $2.02 | $+5.93 | $4,902.65 | ▲ +5.93 after sell → book $3,440.20; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 91 | $2.83 | $2.26 | $+20.92 | $6,281.20 | ▲ +20.92 after sell → book $3,450.26; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 7 | $38.41 | $2.01 | $-4.12 | $5,433.20 | ▼ -4.12 after sell → book $3,444.25; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,540.99 | ▲ 09:30 equity $3,452.52 vs yday $3,388.05 (+64.47) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $371.24 | ▼ 09:30 equity $3,994.72 vs yday $4,005.74 (-11.02) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.47 | ▼ 09:30 equity $3,886.43 vs yday $4,060.80 (-174.37) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 49 | $20.72 | $2.16 | $+78.03 | $2,997.97 | ▲ +78.03 after sell → book $3,874.27; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 602 | $1.60 | $7.88 | $+8.44 | $1,984.85 | ▲ +8.44 after sell → book $3,876.43; vs 09:30 mark -7.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 4 | $2.35 | $0.13 | $-0.03 | $380.51 | ▼ -0.03 after sell → book $3,994.59; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 38 | $26.61 | $2.12 | $+90.77 | $1,029.52 | ▲ +90.77 after sell → book $3,884.30; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 28 | $12.22 | $2.10 | — | $5,454.63 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $344.02 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 67 | $5.08 | $2.22 | — | $5,792.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $344.02 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 1 | $213.94 | $2.02 | — | $5,114.57 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $344.02 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 1 | $199.94 | $2.02 | — | $6,253.95 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; leftover $344.02 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 2 | $132.64 | $2.02 | — | $6,056.02 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $344.02 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 1 | $16.18 | $0.16 | — | $147.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; leftover $20.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 56 | $18.50 | $2.16 | — | $60.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $1053.38 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 114 | $9.19 | $2.33 | — | $2,110.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1053.38 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 1 | $17.78 | $0.18 | — | $129.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; leftover $20.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 1 | $13.41 | $0.14 | — | $115.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; leftover $20.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 7 | $144.18 | $2.01 | — | $1,098.86 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $1053.38 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,898.49 | ▼ close $3,326.09 vs 09:30 $3,342.02 (session -11.78) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.58 | ▼ close $4,108.23 vs 09:30 $4,129.12 (session -20.40) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.71 | ▲ close $4,088.94 vs 09:30 $4,100.96 (session +1.51) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,253.95 | ▼ 09:30 equity $3,342.02 vs yday $3,378.38 (-36.36) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.43 | ▲ 09:30 equity $4,129.12 vs yday $4,123.32 (+5.80) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.68 | ▲ 09:30 equity $4,100.96 vs yday $3,961.70 (+139.26) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 70 | $14.20 | $2.22 | $+1.88 | $999.46 | ▲ +1.88 after sell → book $4,098.74; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 86 | $12.18 | $2.27 | $+46.65 | $3,160.13 | ▲ +46.65 after sell → book $4,093.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 172 | $6.50 | $2.54 | $+113.63 | $2,114.92 | ▲ +113.63 after sell → book $4,096.20; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 11 | $74.54 | $2.07 | — | $7,071.82 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; leftover $835.50 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 15 | $55.25 | $2.08 | — | $7,898.49 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; leftover $835.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $3,574.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $146.07 | $2.01 | — | $87.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $960.31 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 31 | $15.01 | $2.08 | — | $3,107.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 68 | $14.00 | $2.19 | — | $966.42 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $960.31 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 4 | $103.89 | $2.00 | — | $2,689.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 123 | $3.88 | $2.36 | — | $2,209.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 10 | $44.40 | $2.02 | — | $1,763.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 19 | $24.69 | $2.05 | — | $1,292.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 57 | $8.35 | $2.16 | — | $814.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; leftover $479.70 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 12 | $37.65 | $2.03 | — | $360.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $479.70 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,966.17 | ▲ close $3,432.84 vs 09:30 $3,320.20 (session +124.88) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $360.84 | ▼ close $3,941.55 vs 09:30 $4,112.74 (session -137.48) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.99 | ▼ close $3,980.18 vs 09:30 $3,995.36 (session -5.78) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 7 | $52.49 | $2.01 | $+10.09 | $7,096.07 | ▲ +10.09 after sell → book $3,316.10; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 31 | $13.90 | $2.08 | $-12.72 | $7,465.51 | ▼ -12.72 after sell → book $3,318.11; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 2 | $172.76 | $2.00 | $+0.48 | $6,748.55 | ▲ +0.48 after sell → book $3,314.11; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 1 | $347.82 | $1.99 | $+12.51 | $6,398.74 | ▲ +12.51 after sell → book $3,312.11; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,898.49 | ▼ 09:30 equity $3,320.20 vs yday $3,326.09 (-5.89) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.58 | ▲ 09:30 equity $4,112.74 vs yday $4,108.23 (+4.51) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.71 | ▼ 09:30 equity $3,995.36 vs yday $4,088.94 (-93.58) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 2 | $172.76 | $2.02 | $-8.51 | $459.08 | ▼ -8.51 after sell → book $4,110.72; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 5 | $93.30 | $2.02 | $+17.77 | $923.56 | ▲ +17.77 after sell → book $4,108.69; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 34 | $18.15 | $2.11 | $+93.38 | $1,538.54 | ▲ +93.38 after sell → book $4,106.58; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 56 | $18.15 | $2.18 | $-23.94 | $1,920.61 | ▼ -23.94 after sell → book $3,990.16; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 3 | $132.80 | $2.02 | $-32.70 | $1,934.93 | ▼ -32.70 after sell → book $4,104.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 101 | $4.58 | $2.32 | $-57.13 | $2,395.19 | ▼ -57.13 after sell → book $4,102.24; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 10 | $48.42 | $2.04 | $+1.24 | $2,877.35 | ▲ +1.24 after sell → book $4,100.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 37 | $15.66 | $2.12 | $+60.16 | $3,454.65 | ▲ +60.16 after sell → book $4,098.08; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 114 | $3.38 | $2.36 | $-137.50 | $3,837.60 | ▼ -137.50 after sell → book $4,095.72; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 230 | $3.69 | $3.02 | $-93.38 | $906.39 | ▼ -93.38 after sell → book $3,992.34; vs 09:30 mark -3.02 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 27 | $30.18 | $2.11 | — | $7,966.17 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $828.03 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 3 | $252.24 | $2.04 | — | $7,153.42 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $828.03 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,593.62 | ▲ close $3,467.97 vs 09:30 $3,461.64 (session +16.58) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $565.86 | ▲ close $3,970.26 vs 09:30 $3,945.85 (session +26.94) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,991.45 | ▼ close $3,895.85 vs 09:30 $3,936.95 (session -34.68) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 28 | $11.10 | $2.07 | $+27.18 | $7,442.42 | ▲ +27.18 after sell → book $3,457.57; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 67 | $4.97 | $2.19 | $+2.62 | $7,106.90 | ▲ +2.62 after sell → book $3,455.38; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 1 | $208.88 | $1.99 | $+1.05 | $7,755.29 | ▲ +1.05 after sell → book $3,459.65; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 1 | $254.39 | $1.99 | $-58.46 | $6,593.62 | ▼ -58.46 after sell → book $3,451.39; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 2 | $127.45 | $2.00 | $+6.36 | $6,850.01 | ▲ +6.36 after sell → book $3,453.39; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,966.17 | ▲ 09:30 equity $3,461.64 vs yday $3,432.84 (+28.80) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $360.84 | ▲ 09:30 equity $3,945.85 vs yday $3,941.55 (+4.30) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.99 | ▼ 09:30 equity $3,936.95 vs yday $3,980.18 (-43.23) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 6 | $148.03 | $2.03 | $+7.72 | $2,991.45 | ▲ +7.72 after sell → book $3,930.53; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 3 | $19.25 | $0.61 | $+1.81 | $531.51 | ▲ +1.81 after sell → book $3,943.69; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 1 | $34.72 | $0.37 | $-0.30 | $565.86 | ▼ -0.30 after sell → book $3,943.32; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 114 | $9.50 | $2.36 | $+30.65 | $1,168.63 | ▲ +30.65 after sell → book $3,934.59; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 7 | $134.10 | $2.03 | $-74.60 | $2,105.30 | ▼ -74.60 after sell → book $3,932.56; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 108 | $0.51 | $0.90 | $-9.74 | $415.02 | ▼ -9.74 after sell → book $3,944.95; vs 09:30 mark -0.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 12 | $5.00 | $0.66 | $-3.84 | $474.36 | ▼ -3.84 after sell → book $3,944.29; vs 09:30 mark -0.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,962.75 | ▲ close $3,516.30 vs 09:30 $3,515.91 (session +4.44) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $611.75 | ▼ close $3,898.44 vs 09:30 $3,932.30 (session -33.32) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,875.95 | ▲ close $3,875.95 vs 09:30 $3,878.17 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 11 | $73.22 | $2.02 | $+10.43 | $5,786.18 | ▲ +10.43 after sell → book $3,513.89; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 15 | $54.76 | $2.04 | $+3.24 | $4,962.75 | ▲ +3.24 after sell → book $3,511.86; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,593.62 | ▲ 09:30 equity $3,515.91 vs yday $3,467.97 (+47.94) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $565.86 | ▼ 09:30 equity $3,932.30 vs yday $3,970.26 (-37.96) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,991.45 | ▼ 09:30 equity $3,878.17 vs yday $3,895.85 (-17.68) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 1 | $15.97 | $0.18 | $-0.56 | $581.65 | ▼ -0.56 after sell → book $3,932.12; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 68 | $13.04 | $2.22 | $-69.69 | $3,875.95 | ▼ -69.69 after sell → book $3,875.95; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 1 | $18.28 | $0.21 | $+0.11 | $599.72 | ▲ +0.11 after sell → book $3,931.91; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 1 | $12.18 | $0.14 | $-1.51 | $611.75 | ▼ -1.51 after sell → book $3,931.76; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,528.49 | ▲ close $3,528.49 vs 09:30 $3,532.56 (session +0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,860.99 | ▲ close $3,860.99 vs 09:30 $3,877.85 (session +0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,875.95 | ▲ close $3,875.95 vs 09:30 $3,875.95 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 27 | $26.78 | $2.07 | $+87.62 | $3,528.49 | ▲ +87.62 after sell → book $3,528.49; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 3 | $235.71 | $2.00 | $+45.55 | $4,253.62 | ▲ +45.55 after sell → book $3,530.56; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,962.75 | ▲ 09:30 equity $3,532.56 vs yday $3,516.30 (+16.26) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $611.75 | ▼ 09:30 equity $3,877.85 vs yday $3,898.44 (-20.59) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,875.95 | ▲ 09:30 equity $3,875.95 vs yday $3,875.95 (+0.00) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $856.44 | ▼ -18.47 after sell → book $3,875.84; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 31 | $15.01 | $2.10 | $-4.19 | $1,319.65 | ▼ -4.19 after sell → book $3,873.74; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 4 | $92.00 | $2.02 | $-51.58 | $1,685.63 | ▼ -51.58 after sell → book $3,871.72; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 123 | $3.32 | $2.39 | $-73.63 | $2,091.60 | ▼ -73.63 after sell → book $3,869.33; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 10 | $44.17 | $2.04 | $-6.36 | $2,531.26 | ▼ -6.36 after sell → book $3,867.29; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 19 | $21.97 | $2.07 | $-55.79 | $2,946.62 | ▼ -55.79 after sell → book $3,865.22; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 57 | $8.58 | $2.18 | $+8.77 | $3,433.50 | ▲ +8.77 after sell → book $3,863.04; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 12 | $35.80 | $2.05 | $-26.27 | $3,860.99 | ▼ -26.27 after sell → book $3,860.99; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 44 | $10.74 | $2.12 | — | $3,386.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $3,032.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 69 | $6.90 | $2.20 | — | $2,554.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $2,197.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 70 | $13.71 | $2.20 | — | $979.77 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $968.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 21 | $22.32 | $2.05 | — | $1,726.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 1 | $257.00 | $1.99 | — | $1,467.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 544 | $1.78 | $7.02 | — | $2,900.62 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $968.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 10 | $47.60 | $2.02 | — | $989.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 31 | $15.09 | $2.08 | — | $519.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; leftover $482.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 40 | $23.88 | $2.11 | — | $22.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $968.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 52 | $18.40 | $2.15 | — | $1,941.67 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $968.99 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,276.31 | ▲ close $3,574.55 vs 09:30 $3,528.49 (session +55.04) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $519.92 | ▲ close $4,024.67 vs 09:30 $3,860.99 (session +180.13) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.46 | ▼ close $3,657.82 vs 09:30 $3,875.95 (session -204.66) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,528.49 | ▲ 09:30 equity $3,528.49 vs yday $3,528.49 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,860.99 | ▲ 09:30 equity $3,860.99 vs yday $3,860.99 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,875.95 | ▲ 09:30 equity $3,875.95 vs yday $3,875.95 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 515 | $1.71 | $6.76 | — | $5,276.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $882.12 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 59 | $14.85 | $2.21 | — | $4,402.43 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $882.12 | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 1 | $63.18 | $0.63 | — | $456.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; leftover $64.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 7 | $8.74 | $0.63 | — | $394.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; leftover $64.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 38 | $25.18 | $2.10 | — | $1,926.70 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $961.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 166 | $5.79 | $2.49 | — | $0.58 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $961.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 17 | $3.62 | $0.67 | — | $332.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; leftover $64.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 1 | $44.90 | $0.45 | — | $286.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; leftover $64.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 4 | $15.70 | $0.64 | — | $223.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; leftover $64.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 122 | $7.87 | $2.36 | — | $964.20 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $961.88 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,010.30 | ▼ close $3,560.81 vs 09:30 $3,594.29 (session -28.78) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $223.38 | ▲ close $4,043.86 vs 09:30 $4,038.67 (session +8.21) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.58 | ▲ close $3,898.58 vs 09:30 $3,697.28 (session +214.76) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,276.31 | ▲ 09:30 equity $3,594.29 vs yday $3,574.55 (+19.74) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $519.92 | ▲ 09:30 equity $4,038.67 vs yday $4,024.67 (+14.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.46 | ▲ 09:30 equity $3,697.28 vs yday $3,657.82 (+39.46) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 70 | $13.89 | $2.22 | $+8.18 | $1,934.17 | ▲ +8.18 after sell → book $3,692.89; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 40 | $23.84 | $2.13 | $-5.84 | $2,885.64 | ▼ -5.84 after sell → book $3,690.76; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 52 | $18.15 | $2.17 | $-17.31 | $964.10 | ▼ -17.31 after sell → book $3,695.12; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 192 | $4.67 | $2.63 | — | $6,170.32 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $898.57 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 11 | $76.55 | $2.07 | — | $7,010.30 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $898.57 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,010.30 | ▲ close $3,665.16 vs 09:30 $3,575.65 (session +89.51) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $223.38 | ▼ close $4,041.66 vs 09:30 $4,048.87 (session -7.21) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,753.68 | ▲ close $3,753.68 vs 09:30 $3,767.84 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,010.30 | ▲ 09:30 equity $3,575.65 vs yday $3,560.81 (+14.84) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $223.38 | ▲ 09:30 equity $4,048.87 vs yday $4,043.86 (+5.01) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.58 | ▼ 09:30 equity $3,767.84 vs yday $3,898.58 (-130.74) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 38 | $26.44 | $2.12 | $+43.65 | $1,847.41 | ▲ +43.65 after sell → book $3,758.59; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 166 | $5.81 | $2.53 | $-1.69 | $3,753.68 | ▼ -1.69 after sell → book $3,753.68; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 544 | $1.56 | $7.12 | $-131.10 | $844.82 | ▼ -131.10 after sell → book $3,760.72; vs 09:30 mark -7.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 122 | $7.76 | $2.39 | $-18.16 | $2,791.75 | ▼ -18.16 after sell → book $3,756.21; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,385.39 | ▲ close $3,676.75 vs 09:30 $3,676.72 (session +8.84) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,731.67 | ▲ close $4,022.07 vs 09:30 $4,038.30 (session +0.39) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,753.68 | ▲ close $3,753.68 vs 09:30 $3,753.68 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 515 | $1.58 | $6.64 | $+53.54 | $5,385.39 | ▲ +53.54 after sell → book $3,667.91; vs 09:30 mark -6.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 59 | $13.60 | $2.17 | $+69.37 | $6,205.74 | ▲ +69.37 after sell → book $3,674.56; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,010.30 | ▲ 09:30 equity $3,676.72 vs yday $3,665.16 (+11.56) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $223.38 | ▼ 09:30 equity $4,038.30 vs yday $4,041.66 (-3.36) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,753.68 | ▲ 09:30 equity $3,753.68 vs yday $3,753.68 (+0.00) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 44 | $10.51 | $2.14 | $-14.60 | $683.68 | ▼ -14.60 after sell → book $4,036.16; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $1,047.89 | ▲ +10.48 after sell → book $4,034.14; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 69 | $9.39 | $2.22 | $+167.39 | $1,693.59 | ▲ +167.39 after sell → book $4,031.93; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $2,033.47 | ▼ -16.60 after sell → book $4,029.91; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 21 | $21.67 | $2.07 | $-17.78 | $2,486.47 | ▼ -17.78 after sell → book $4,027.84; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 1 | $252.92 | $2.01 | $-8.09 | $2,737.38 | ▼ -8.09 after sell → book $4,025.83; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 10 | $56.94 | $2.04 | $+89.34 | $3,304.74 | ▲ +89.34 after sell → book $4,023.79; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 31 | $13.84 | $2.10 | $-42.94 | $3,731.67 | ▼ -42.94 after sell → book $4,021.68; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,698.99 | ▲ close $3,698.99 vs 09:30 $3,703.58 (session +0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,017.04 | ▲ close $4,017.04 vs 09:30 $4,020.12 (session +0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,753.68 | ▲ close $3,753.68 vs 09:30 $3,753.68 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 192 | $4.36 | $2.57 | $+54.32 | $4,545.71 | ▲ +54.32 after sell → book $3,701.02; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 11 | $76.79 | $2.02 | $-6.73 | $3,698.99 | ▼ -6.73 after sell → book $3,698.99; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,385.39 | ▲ 09:30 equity $3,703.58 vs yday $3,676.75 (+26.83) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,731.67 | ▼ 09:30 equity $4,020.12 vs yday $4,022.07 (-1.95) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,753.68 | ▲ 09:30 equity $3,753.68 vs yday $3,753.68 (+0.00) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 1 | $67.44 | $0.70 | $+2.93 | $3,798.42 | ▲ +2.93 after sell → book $4,019.42; vs 09:30 mark -0.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 7 | $8.26 | $0.62 | $-4.61 | $3,855.62 | ▼ -4.61 after sell → book $4,018.80; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 17 | $3.76 | $0.71 | $+1.09 | $3,918.83 | ▲ +1.09 after sell → book $4,018.09; vs 09:30 mark -0.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 1 | $38.23 | $0.41 | $-7.53 | $3,956.65 | ▼ -7.53 after sell → book $4,017.69; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 4 | $15.26 | $0.64 | $-3.04 | $4,017.04 | ▼ -3.04 after sell → book $4,017.04; vs 09:30 mark -0.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $2,536.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 191 | $4.91 | $2.56 | — | $1,871.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $938.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 296 | $3.13 | $3.82 | — | $2.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $938.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 15 | $32.01 | $2.04 | — | $2,054.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 84 | $5.91 | $2.24 | — | $3,023.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 7 | $71.71 | $2.01 | — | $1,550.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 347 | $2.70 | $4.48 | — | $2,812.31 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $938.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 152 | $6.16 | $2.45 | — | $933.17 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $938.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 8 | $56.02 | $2.01 | — | $1,100.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 53 | $9.37 | $2.15 | — | $601.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $3,521.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; leftover $502.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 38 | $13.10 | $2.10 | — | $101.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; leftover $502.13 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,482.10 | ▼ close $3,684.31 vs 09:30 $3,698.99 (session -3.32) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.73 | ▲ close $4,021.14 vs 09:30 $4,017.04 (session +20.65) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.87 | ▲ close $3,836.42 vs 09:30 $3,753.68 (session +96.04) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,698.99 | ▲ 09:30 equity $3,698.99 vs yday $3,698.99 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,017.04 | ▲ 09:30 equity $4,017.04 vs yday $4,017.04 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,753.68 | ▲ 09:30 equity $3,753.68 vs yday $3,753.68 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 14 | $24.97 | $2.06 | — | $5,117.12 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; leftover $369.90 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 182 | $2.03 | $2.59 | — | $4,769.60 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; leftover $369.90 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 109 | $3.37 | $2.36 | — | $5,482.10 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $369.90 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 3 | $112.83 | $2.03 | — | $4,035.47 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $369.90 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 105 | $3.52 | $2.34 | — | $4,402.73 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; leftover $369.90 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,482.10 | ▼ close $3,647.24 vs 09:30 $3,695.25 (session -48.01) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.73 | ▲ close $4,173.40 vs 09:30 $4,026.07 (session +147.33) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $915.43 | ▲ close $4,089.02 vs 09:30 $3,889.20 (session +202.30) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,482.10 | ▲ 09:30 equity $3,695.25 vs yday $3,684.31 (+10.94) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.73 | ▲ 09:30 equity $4,026.07 vs yday $4,021.14 (+4.93) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.87 | ▲ 09:30 equity $3,889.20 vs yday $3,836.42 (+52.78) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 152 | $6.02 | $2.48 | $-26.21 | $915.43 | ▼ -26.21 after sell → book $3,886.72; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,482.10 | ▼ close $3,616.85 vs 09:30 $3,647.48 (session -30.63) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.73 | ▲ close $4,181.93 vs 09:30 $4,159.07 (session +22.86) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,962.39 | ▲ close $4,225.47 vs 09:30 $4,148.68 (session +83.28) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,482.10 | ▲ 09:30 equity $3,647.48 vs yday $3,647.24 (+0.24) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.73 | ▼ 09:30 equity $4,159.07 vs yday $4,173.40 (-14.33) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $915.43 | ▲ 09:30 equity $4,148.68 vs yday $4,089.02 (+59.66) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 191 | $5.11 | $2.60 | $+33.03 | $1,888.83 | ▲ +33.03 after sell → book $4,146.07; vs 09:30 mark -2.61 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 296 | $3.64 | $3.88 | $+143.26 | $2,962.39 | ▲ +143.26 after sell → book $4,142.19; vs 09:30 mark -3.88 | dropped from list after 2 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 62 | $33.14 | $2.18 | — | $2,077.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; leftover $2067.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 548 | $1.80 | $7.07 | — | $1,968.93 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $987.46 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 42 | $23.29 | $2.12 | — | $988.63 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $987.46 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 67 | $14.62 | $2.19 | — | $6.90 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $987.46 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 50 | $40.93 | $2.14 | — | $28.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $2067.16 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,401.36 | ▼ close $3,480.04 vs 09:30 $3,622.33 (session -126.48) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.82 | ▲ close $4,210.40 vs 09:30 $4,151.03 (session +80.40) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.90 | ▼ close $4,217.62 vs 09:30 $4,232.41 (session -3.42) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 14 | $24.42 | $2.03 | $+3.61 | $4,022.20 | ▲ +3.61 after sell → book $3,613.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 182 | $1.85 | $2.54 | $+27.64 | $4,366.12 | ▲ +27.64 after sell → book $3,615.49; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 109 | $3.75 | $2.32 | $-46.09 | $3,611.14 | ▼ -46.09 after sell → book $3,611.14; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 3 | $118.18 | $2.00 | $-20.06 | $5,125.56 | ▼ -20.06 after sell → book $3,620.33; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 105 | $3.98 | $2.31 | $-52.95 | $4,705.35 | ▼ -52.95 after sell → book $3,618.02; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,482.10 | ▲ 09:30 equity $3,622.33 vs yday $3,616.85 (+5.48) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.73 | ▼ 09:30 equity $4,151.03 vs yday $4,181.93 (-30.90) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,962.39 | ▲ 09:30 equity $4,232.41 vs yday $4,225.47 (+6.94) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 2 | $253.34 | $2.02 | $+18.33 | $1,547.20 | ▲ +18.33 after sell → book $4,144.73; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 15 | $30.57 | $2.06 | $-25.69 | $2,003.70 | ▼ -25.69 after sell → book $4,142.68; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 84 | $6.25 | $2.27 | $+24.05 | $1,042.54 | ▲ +24.05 after sell → book $4,146.75; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 7 | $78.12 | $2.03 | $+40.83 | $2,548.51 | ▲ +40.83 after sell → book $4,140.65; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 8 | $61.93 | $2.03 | $+43.23 | $3,041.91 | ▲ +43.23 after sell → book $4,138.61; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 53 | $9.40 | $2.17 | $-2.73 | $3,537.94 | ▼ -2.73 after sell → book $4,136.44; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 3 | $140.03 | $2.02 | $-77.22 | $519.80 | ▼ -77.22 after sell → book $4,149.01; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 38 | $15.75 | $2.12 | $+96.47 | $4,134.32 | ▲ +96.47 after sell → book $4,134.32; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 48 | $18.61 | $2.18 | — | $4,502.24 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $902.78 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 132 | $6.83 | $2.44 | — | $5,401.36 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; leftover $902.78 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 1 | $11.21 | $0.12 | — | $17.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; leftover $14.41 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 43 | $22.46 | $2.12 | — | $969.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $968.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 26 | $36.76 | $2.07 | — | $11.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $968.46 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,073.47 | ▲ close $3,528.32 vs 09:30 $3,467.92 (session +64.84) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.50 | ▲ close $4,406.76 vs 09:30 $4,347.44 (session +59.43) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.19 | ▲ close $4,392.91 vs 09:30 $4,237.17 (session +164.28) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,401.36 | ▼ 09:30 equity $3,467.92 vs yday $3,480.04 (-12.12) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.82 | ▲ 09:30 equity $4,347.44 vs yday $4,210.40 (+137.04) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.90 | ▲ 09:30 equity $4,237.17 vs yday $4,217.62 (+19.55) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 42 | $24.09 | $2.14 | $+29.35 | $1,016.54 | ▲ +29.35 after sell → book $4,235.03; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 67 | $13.77 | $2.21 | $-61.35 | $1,936.92 | ▼ -61.35 after sell → book $4,232.82; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 109 | $7.95 | $2.37 | — | $6,265.54 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $866.98 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 10 | $81.00 | $2.06 | — | $7,073.47 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; leftover $866.98 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 331 | $3.04 | $4.27 | — | $1,008.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1005.59 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 34 | $29.32 | $2.09 | — | $2,017.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1005.59 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 12 | $81.40 | $2.03 | — | $30.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1005.59 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,827.70 | ▲ close $3,578.34 vs 09:30 $3,536.44 (session +44.11) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.50 | ▲ close $4,509.50 vs 09:30 $4,508.64 (session +0.86) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.12 | ▲ close $4,374.33 vs 09:30 $4,364.12 (session +29.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,073.47 | ▲ 09:30 equity $3,536.44 vs yday $3,528.32 (+8.12) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.50 | ▲ 09:30 equity $4,508.64 vs yday $4,406.76 (+101.88) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.19 | ▼ 09:30 equity $4,364.12 vs yday $4,392.91 (-28.79) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 43 | $21.30 | $2.14 | $-54.14 | $1,991.86 | ▼ -54.14 after sell → book $4,354.81; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 26 | $39.50 | $2.09 | $+67.08 | $3,016.78 | ▲ +67.08 after sell → book $4,352.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 548 | $1.96 | $7.17 | $+73.44 | $1,078.10 | ▲ +73.44 after sell → book $4,356.95; vs 09:30 mark -7.17 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 51 | $34.44 | $2.22 | — | $8,827.70 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1768.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 457 | $2.47 | $5.90 | — | $3,387.83 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $1130.63 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 685 | $1.65 | $8.84 | — | $1,130.50 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $1130.63 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 96 | $11.67 | $2.28 | — | $7.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $1130.63 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 66 | $16.91 | $2.19 | — | $2,269.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $1130.63 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,352.35 | ▼ close $3,435.75 vs 09:30 $3,514.89 (session -70.23) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,544.99 | ▲ close $4,558.60 vs 09:30 $4,562.48 (session +0.48) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▲ close $4,645.59 vs 09:30 $4,535.55 (session +142.27) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 48 | $22.11 | $2.13 | $-172.31 | $7,764.29 | ▼ -172.31 after sell → book $3,512.76; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 132 | $6.55 | $2.39 | $+32.13 | $6,897.30 | ▲ +32.13 after sell → book $3,510.37; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,827.70 | ▼ 09:30 equity $3,514.89 vs yday $3,578.34 (-63.45) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.50 | ▲ 09:30 equity $4,562.48 vs yday $4,509.50 (+52.98) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.12 | ▲ 09:30 equity $4,535.55 vs yday $4,374.33 (+161.22) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 331 | $4.00 | $4.34 | $+310.81 | $3,575.60 | ▲ +310.81 after sell → book $4,524.56; vs 09:30 mark -4.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 62 | $40.03 | $2.21 | $+422.80 | $2,497.15 | ▲ +422.80 after sell → book $4,560.28; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 347 | $3.55 | $4.54 | $+285.93 | $1,257.43 | ▲ +285.93 after sell → book $4,531.01; vs 09:30 mark -4.54 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 34 | $29.43 | $2.11 | $-0.46 | $2,255.94 | ▼ -0.46 after sell → book $4,528.90; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 50 | $41.00 | $2.17 | $-0.81 | $4,544.99 | ▼ -0.81 after sell → book $4,558.11; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 12 | $79.08 | $2.05 | $-31.91 | $4,522.52 | ▼ -31.91 after sell → book $4,522.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 106 | $8.26 | $2.36 | — | $7,770.50 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $877.59 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 1 | $583.88 | $2.03 | — | $8,352.35 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; leftover $877.59 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,291.73 | ▼ close $3,420.76 vs 09:30 $3,441.98 (session -16.84) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,544.99 | ▲ close $4,558.60 vs 09:30 $4,558.60 (session +0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▲ close $4,597.59 vs 09:30 $4,593.75 (session +3.84) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 109 | $8.28 | $2.32 | $-40.11 | $7,448.06 | ▼ -40.11 after sell → book $3,439.66; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,352.35 | ▲ 09:30 equity $3,441.98 vs yday $3,435.75 (+6.23) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,544.99 | ▲ 09:30 equity $4,558.60 vs yday $4,558.60 (-0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $4,593.75 vs yday $4,645.59 (-51.84) | — | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 9 | $93.97 | $2.06 | — | $8,291.73 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $859.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 19 | $47.57 | $2.05 | — | $3,652.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $911.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $2,863.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $911.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 25 | $35.74 | $2.06 | — | $1,968.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $911.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 408 | $2.70 | $5.26 | — | $2,198.93 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $1101.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 19 | $47.15 | $2.05 | — | $1,070.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $911.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 8 | $109.67 | $2.01 | — | $190.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $911.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 245 | $4.49 | $3.16 | — | $7.89 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $1101.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 26 | $41.76 | $2.07 | — | $1,111.10 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1101.93 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,986.67 | ▲ close $3,270.91 vs 09:30 $3,212.62 (session +62.40) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.81 | ▼ close $4,488.84 vs 09:30 $4,558.81 (session -59.63) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.89 | ▼ close $4,471.90 vs 09:30 $4,653.71 (session -157.84) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 10 | $82.00 | $2.02 | $-14.08 | $7,469.71 | ▼ -14.08 after sell → book $3,210.60; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,291.73 | ▼ 09:30 equity $3,212.62 vs yday $3,420.76 (-208.14) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,544.99 | ▲ 09:30 equity $4,558.81 vs yday $4,558.60 (+0.21) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▲ 09:30 equity $4,653.71 vs yday $4,597.59 (+56.12) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 1 | $13.82 | $0.16 | $+2.33 | $4,558.65 | ▲ +2.33 after sell → book $4,558.65; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 685 | $1.41 | $8.96 | $-182.20 | $2,079.30 | ▼ -182.20 after sell → book $4,642.54; vs 09:30 mark -8.96 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 96 | $12.80 | $2.30 | $+103.90 | $3,305.80 | ▲ +103.90 after sell → book $4,640.24; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 66 | $16.92 | $2.21 | $-3.74 | $1,122.41 | ▼ -3.74 after sell → book $4,651.50; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 13 | $116.85 | $2.09 | — | $8,986.67 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1605.30 | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,384.40 | ▼ close $3,222.74 vs 09:30 $3,296.38 (session -71.64) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.81 | ▲ close $4,555.31 vs 09:30 $4,489.17 (session +66.14) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,119.65 | ▲ close $5,302.45 vs 09:30 $4,443.87 (session +869.86) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 1 | $600.27 | $1.99 | $-20.41 | $8,384.40 | ▼ -20.41 after sell → book $3,294.38; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,986.67 | ▲ 09:30 equity $3,296.38 vs yday $3,270.91 (+25.47) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.81 | ▲ 09:30 equity $4,489.17 vs yday $4,488.84 (+0.33) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.89 | ▼ 09:30 equity $4,443.87 vs yday $4,471.90 (-28.03) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 457 | $2.68 | $5.98 | $+84.09 | $1,226.67 | ▲ +84.09 after sell → book $4,437.89; vs 09:30 mark -5.98 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 245 | $3.92 | $3.21 | $-144.80 | $3,119.65 | ▼ -144.80 after sell → book $4,432.59; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 26 | $36.02 | $2.09 | $-153.27 | $2,161.23 | ▼ -153.27 after sell → book $4,435.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 32 | $16.21 | $2.09 | — | $547.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $525.56 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 17 | $29.76 | $2.04 | — | $1,068.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $525.56 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 33 | $15.58 | $2.09 | — | $31.64 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $525.56 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,303.36 | ▼ close $3,027.89 vs 09:30 $3,038.96 (session -8.42) | 16:00 close · cash $8,303.36 · equity $3,027.89 vs 09:30 $3,038.96 (-11.07; session marks -8.42) · 6 name(s) marked open→close (per-name table). AEHL×99 09:30 $9.05 → close $9.36 -30.69; BAND×13 09:30 $61.83 → close $61.83 -0.00; HALO×6 09:30 $115.36 → close $113.90 +8.76; PAYX×6 09:30 $101.59 → close $101.59 +0.00; USFD×8 09:30 $93.82 → close $93.82 +0.00; RSKD×193 09:30 $7.85 → close $7.78 +13.51 | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,790.96 | ▼ 09:30 equity $3,038.96 vs yday $3,048.71 (-9.75) | 09:30 open · cash $6,790.96 (unchanged overnight, no fees) · equity $3,038.96 vs prior close $3,048.71 (-9.75) · 5 name(s) re-marked at the open (per-name table). AEHL×99 yday $8.96 → 09:30 $9.05 -8.91; BAND×13 yday $61.83 → 09:30 $61.83 -0.00; HALO×6 yday $115.22 → 09:30 $115.36 -0.84; PAYX×6 yday $101.59 → 09:30 $101.59 -0.00; USFD×8 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 193 | $7.85 | $2.65 | — | $8,303.36 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $1519.48 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `ARX` | cash | leftover split 1.67 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 1.67 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 1.67 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 1.67 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 1.67 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 1.67 < 1 share @ 9.89 |
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
| 2026-08-21 | `FUTU` | cash | leftover split 9.49 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 9.49 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 9.49 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 9.49 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 9.49 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 9.49 < 1 share @ 43.08 |
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
| 2026-08-26 | `ANF` | cash | leftover split 63.42 < 1 share @ 131.37 |
| 2026-08-26 | `DY` | cash | leftover split 63.42 < 1 share @ 326.91 |
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
| 2026-08-27 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | cash | leftover split 20.43 < 1 share @ 80.60 |
| 2026-08-27 | `CM` | cash | leftover split 20.43 < 1 share @ 118.77 |
| 2026-08-27 | `HQY` | cash | leftover split 20.43 < 1 share @ 97.16 |
| 2026-08-27 | `RY` | cash | leftover split 20.43 < 1 share @ 206.82 |
| 2026-08-27 | `TD` | cash | leftover split 20.43 < 1 share @ 120.17 |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BILI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CMBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CSIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BILI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CMBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `DOCU` | cash | leftover split 64.99 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 64.99 < 1 share @ 167.55 |
| 2026-09-04 | `LULU` | cash | leftover split 64.99 < 1 share @ 98.15 |
| 2026-09-08 | `AI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CPB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AMBA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AMBA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `LEN` | cash | leftover split 14.41 < 1 share @ 81.00 |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ALMU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
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
| 2026-09-22 | `CRML` | cash | leftover split 2.63 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 2.63 < 1 share @ 7.23 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 51 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1768.22 |
| `AEHL` | 106 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $877.59 |
| `USFD` | 9 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $859.91 |
| `HALO` | 13 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1605.30 |
| `CBRL` | 19 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; leftover $911.73 |
| `CTAS` | 4 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $911.73 |
| `GIS` | 25 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $911.73 |
| `KBH` | 19 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; leftover $911.73 |
| `PAYX` | 8 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $911.73 |
| `GLND` | 408 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $1101.93 |
