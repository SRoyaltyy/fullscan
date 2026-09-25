# Factor mine action — `combo_hn_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_news_g_h1 w=0.3,0.7 net=priority

Cash book **-5.15%** ($9,485) · signal-only (no cash/fees) was —. Starts YES **28/30**. Fills 304 · skips 123 · realized $+818.31.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 30%, union_news_g_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 30%, union_news_g_h1 70%.
- Member: union_hot_n4_h1 (30% · long · hold 1).
- Member: union_news_g_h1 (70% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $721.86.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $6,562.13 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $1040.54; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 7 | $146.90 | $2.01 | — | $5,531.82 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $1040.54; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 8 | $120.00 | $2.01 | — | $4,569.81 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $1040.54; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 241 | $4.31 | $3.11 | — | $3,527.99 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $1040.54; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 76 | $13.55 | $2.22 | — | $2,495.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $1040.54; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 78 | $13.18 | $2.22 | — | $1,465.71 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $1040.54; owner union_news_g_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,465.71 | ▲ close $10,361.33 vs 09:30 $10,412.10 (session +18.92) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,465.71 | ▲ 09:30 equity $10,383.83 vs yday $10,361.33 (+22.50) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 31 | $24.83 | $2.10 | $+0.46 | $2,233.34 | ▲ +0.46 after sell → book $10,381.73; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 39 | $19.57 | $2.13 | $-4.23 | $2,994.44 | ▼ -4.23 after sell → book $10,379.60; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 353 | $2.08 | $4.62 | $-49.77 | $3,725.82 | ▼ -49.77 after sell → book $10,374.98; vs 09:30 mark -4.62 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 69 | $9.57 | $2.22 | $-111.37 | $4,383.93 | ▼ -111.37 after sell → book $10,372.76; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $5,117.68 | ▲ +12.09 after sell → book $10,370.75; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 7 | $149.37 | $2.03 | $+13.25 | $6,161.24 | ▲ +13.25 after sell → book $10,368.72; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 8 | $127.40 | $2.03 | $+55.15 | $7,178.40 | ▲ +55.15 after sell → book $10,366.68; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 241 | $4.60 | $3.16 | $+63.62 | $8,283.84 | ▲ +63.62 after sell → book $10,363.52; vs 09:30 mark -3.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 76 | $13.16 | $2.24 | $-34.10 | $9,281.76 | ▼ -34.10 after sell → book $10,361.28; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 78 | $13.84 | $2.25 | $+47.01 | $10,359.04 | ▲ +47.01 after sell → book $10,359.04; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 185 | $4.19 | $2.54 | — | $9,581.34 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $776.93; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 113 | $6.87 | $2.33 | — | $8,802.70 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $776.93; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 56 | $13.64 | $2.16 | — | $8,036.70 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $776.93; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 18 | $41.23 | $2.04 | — | $7,292.52 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $776.93; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 31 | $46.18 | $2.08 | — | $5,858.86 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $1458.50; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 10 | $142.77 | $2.02 | — | $4,429.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $1458.50; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 7 | $202.70 | $2.01 | — | $3,008.23 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $1458.50; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 15 | $92.99 | $2.04 | — | $1,611.34 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $1458.50; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 29 | $49.00 | $2.08 | — | $188.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $1458.50; owner union_news_g_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.26 | ▲ close $10,416.31 vs 09:30 $10,383.83 (session +76.58) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.26 | ▼ 09:30 equity $10,381.24 vs yday $10,416.31 (-35.07) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 185 | $3.94 | $2.59 | $-51.38 | $914.58 | ▼ -51.38 after sell → book $10,378.66; vs 09:30 mark -2.58 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 56 | $13.31 | $2.18 | $-22.82 | $1,657.76 | ▼ -22.82 after sell → book $10,376.48; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 18 | $41.50 | $2.06 | $+0.75 | $2,402.70 | ▲ +0.75 after sell → book $10,374.42; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 31 | $48.00 | $2.10 | $+52.23 | $3,888.59 | ▲ +52.23 after sell → book $10,372.31; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 10 | $148.04 | $2.04 | $+48.64 | $5,366.95 | ▲ +48.64 after sell → book $10,370.27; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 7 | $208.93 | $2.03 | $+39.57 | $6,827.43 | ▲ +39.57 after sell → book $10,368.24; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 15 | $92.38 | $2.06 | $-13.24 | $8,211.07 | ▼ -13.24 after sell → book $10,366.18; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 29 | $45.09 | $2.10 | $-117.56 | $9,516.58 | ▼ -117.56 after sell → book $10,364.08; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,516.58 | ▼ close $10,316.62 vs 09:30 $10,381.24 (session -47.46) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,516.58 | ▲ 09:30 equity $10,329.05 vs yday $10,316.62 (+12.43) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 113 | $7.19 | $2.36 | $+31.47 | $10,326.69 | ▲ +31.47 after sell → book $10,326.69; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,326.69 | ▲ close $10,326.69 vs 09:30 $10,329.05 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,326.69 | ▲ 09:30 equity $10,326.69 vs yday $10,326.69 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $9,573.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $774.50; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 673 | $1.15 | $8.68 | — | $8,791.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $774.50; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 65 | $11.81 | $2.19 | — | $8,021.20 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $774.50; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 565 | $1.37 | $7.29 | — | $7,239.86 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $774.50; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 11 | $91.01 | $2.02 | — | $6,236.73 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1462 | $0.71 | $14.72 | — | $5,188.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 156 | $6.61 | $2.46 | — | $4,155.53 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 64 | $16.00 | $2.18 | — | $3,129.35 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 38 | $26.57 | $2.10 | — | $2,117.59 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 17 | $58.73 | $2.04 | — | $1,117.14 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $85.60 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $1034.27; owner union_news_g_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.60 | ▼ close $10,203.96 vs 09:30 $10,326.69 (session -74.98) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.60 | ▲ 09:30 equity $10,505.90 vs yday $10,203.96 (+301.94) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 65 | $11.57 | $2.21 | $-20.32 | $835.44 | ▼ -20.32 after sell → book $10,503.70; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 565 | $1.46 | $7.39 | $+36.17 | $1,652.95 | ▲ +36.17 after sell → book $10,496.31; vs 09:30 mark -7.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 11 | $95.72 | $2.04 | $+47.74 | $2,703.83 | ▲ +47.74 after sell → book $10,494.26; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1462 | $0.67 | $14.49 | $-77.46 | $3,674.72 | ▼ -77.46 after sell → book $10,479.77; vs 09:30 mark -14.49 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 156 | $6.95 | $2.49 | $+48.87 | $4,756.43 | ▲ +48.87 after sell → book $10,477.28; vs 09:30 mark -2.49 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 64 | $17.66 | $2.20 | $+101.86 | $5,884.46 | ▲ +101.86 after sell → book $10,475.07; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 38 | $26.25 | $2.12 | $-16.39 | $6,879.84 | ▼ -16.39 after sell → book $10,472.95; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $7,901.72 | ▼ -9.66 after sell → book $10,470.87; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 263 | $4.49 | $3.39 | — | $6,717.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1185.26; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 174 | $6.81 | $2.51 | — | $5,530.01 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1185.26; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 6 | $119.43 | $2.01 | — | $4,811.42 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 319 | $2.47 | $4.12 | — | $4,019.37 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 6 | $115.18 | $2.01 | — | $3,326.29 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $2,701.03 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 67 | $11.70 | $2.19 | — | $1,914.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 71 | $11.10 | $2.20 | — | $1,124.99 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 243 | $3.24 | $3.13 | — | $334.54 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $790.00; owner union_news_g_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $334.54 | ▲ close $10,465.04 vs 09:30 $10,505.90 (session +17.72) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $334.54 | ▲ 09:30 equity $10,969.50 vs yday $10,465.04 (+504.46) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $1,046.01 | ▼ -41.23 after sell → book $10,967.47; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 673 | $1.83 | $8.80 | $+440.15 | $2,268.80 | ▲ +440.15 after sell → book $10,958.67; vs 09:30 mark -8.80 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 263 | $4.32 | $3.45 | $-51.55 | $3,401.51 | ▼ -51.55 after sell → book $10,955.22; vs 09:30 mark -3.45 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 174 | $8.03 | $2.55 | $+207.22 | $4,796.18 | ▲ +207.22 after sell → book $10,952.67; vs 09:30 mark -2.55 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 6 | $120.51 | $2.03 | $+2.44 | $5,517.21 | ▲ +2.44 after sell → book $10,950.64; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 319 | $2.40 | $4.18 | $-30.62 | $6,278.64 | ▼ -30.62 after sell → book $10,946.47; vs 09:30 mark -4.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 6 | $121.00 | $2.03 | $+30.88 | $7,002.61 | ▲ +30.88 after sell → book $10,944.44; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $7,653.63 | ▲ +25.77 after sell → book $10,942.42; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 67 | $11.17 | $2.21 | $-39.91 | $8,399.81 | ▼ -39.91 after sell → book $10,940.21; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 71 | $11.48 | $2.22 | $+22.91 | $9,212.67 | ▲ +22.91 after sell → book $10,937.99; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 243 | $2.99 | $3.19 | $-67.07 | $9,936.05 | ▼ -67.07 after sell → book $10,934.80; vs 09:30 mark -3.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,936.05 | ▼ close $10,906.33 vs 09:30 $10,969.50 (session -28.47) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,936.05 | ▲ 09:30 equity $10,920.86 vs yday $10,906.33 (+14.53) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 17 | $57.93 | $2.06 | $-17.70 | $10,918.80 | ▼ -17.70 after sell → book $10,918.80; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 33 | $24.11 | $2.09 | — | $10,121.08 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $818.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 524 | $1.56 | $6.76 | — | $9,296.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $818.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 201 | $4.07 | $2.60 | — | $8,476.22 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $818.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 43 | $19.04 | $2.12 | — | $7,655.38 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $818.91; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 135 | $9.42 | $2.40 | — | $6,381.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $1275.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $35.05 | $2.10 | — | $5,117.38 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $1275.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 44 | $28.86 | $2.12 | — | $3,845.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $1275.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 146 | $8.72 | $2.43 | — | $2,569.87 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $1275.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $118.52 | $2.02 | — | $1,382.65 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $1275.90; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.13 | $2.04 | — | $146.54 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $1275.90; owner union_news_g_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.54 | ▲ close $11,352.69 vs 09:30 $10,920.86 (session +460.56) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.54 | ▼ 09:30 equity $11,138.13 vs yday $11,352.69 (-214.56) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 33 | $26.61 | $2.11 | $+78.30 | $1,022.56 | ▲ +78.30 after sell → book $11,136.02; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 524 | $1.60 | $6.86 | $+7.34 | $1,854.10 | ▲ +7.34 after sell → book $11,129.16; vs 09:30 mark -6.86 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 43 | $20.72 | $2.14 | $+67.98 | $2,742.92 | ▲ +67.98 after sell → book $11,127.02; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 135 | $10.07 | $2.43 | $+82.93 | $4,099.94 | ▲ +82.93 after sell → book $11,124.59; vs 09:30 mark -2.43 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+19.18 | $5,383.02 | ▲ +19.18 after sell → book $11,122.47; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 44 | $27.56 | $2.14 | $-61.46 | $6,593.52 | ▼ -61.46 after sell → book $11,120.33; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 146 | $8.86 | $2.46 | $+15.55 | $7,884.62 | ▲ +15.55 after sell → book $11,117.87; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $+8.74 | $9,080.58 | ▲ +8.74 after sell → book $11,115.83; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+31.26 | $10,347.96 | ▲ +31.26 after sell → book $11,113.77; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 73 | $14.11 | $2.21 | — | $9,315.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1034.80; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 178 | $5.81 | $2.52 | — | $8,279.02 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1034.80; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 89 | $11.59 | $2.26 | — | $7,245.70 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1034.80; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 108 | $11.12 | $2.31 | — | $6,042.42 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $1207.62; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 145 | $8.29 | $2.42 | — | $4,837.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $1207.62; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 69 | $17.41 | $2.20 | — | $3,634.46 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $1207.62; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 107 | $11.22 | $2.31 | — | $2,431.61 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $1207.62; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $1,361.53 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $1207.62; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 10 | $118.50 | $2.02 | — | $174.51 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $1207.62; owner union_news_g_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.51 | ▲ close $11,368.90 vs 09:30 $11,138.13 (session +275.38) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.51 | ▲ 09:30 equity $11,533.58 vs yday $11,368.90 (+164.68) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 73 | $14.20 | $2.23 | $+2.13 | $1,208.88 | ▲ +2.13 after sell → book $11,531.35; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 178 | $6.50 | $2.56 | $+117.73 | $2,363.31 | ▲ +117.73 after sell → book $11,528.78; vs 09:30 mark -2.57 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 89 | $12.18 | $2.28 | $+48.42 | $3,445.05 | ▲ +48.42 after sell → book $11,526.50; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $4,511.95 | ▼ -3.18 after sell → book $11,524.48; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 4 | $144.18 | $2.00 | — | $3,933.23 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $676.79; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 36 | $18.50 | $2.10 | — | $3,265.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $676.79; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 26 | $41.44 | $2.07 | — | $2,185.62 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $1088.38; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 15 | $70.30 | $2.04 | — | $1,129.09 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1088.38; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 18 | $60.00 | $2.04 | — | $47.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $1088.38; owner union_news_g_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.04 | ▼ close $11,426.09 vs 09:30 $11,533.58 (session -88.14) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.04 | ▼ 09:30 equity $11,295.89 vs yday $11,426.09 (-130.20) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 201 | $3.69 | $2.64 | $-81.62 | $786.09 | ▼ -81.62 after sell → book $11,293.25; vs 09:30 mark -2.64 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 108 | $11.27 | $2.34 | $+11.54 | $2,000.91 | ▲ +11.54 after sell → book $11,290.91; vs 09:30 mark -2.34 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 69 | $17.70 | $2.22 | $+15.59 | $3,219.99 | ▲ +15.59 after sell → book $11,288.69; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 107 | $11.00 | $2.34 | $-28.19 | $4,394.65 | ▼ -28.19 after sell → book $11,286.35; vs 09:30 mark -2.34 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 10 | $115.66 | $2.04 | $-32.46 | $5,549.21 | ▼ -32.46 after sell → book $11,284.31; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 36 | $18.15 | $2.12 | $-16.82 | $6,200.49 | ▼ -16.82 after sell → book $11,282.19; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 15 | $65.29 | $2.06 | $-79.24 | $7,177.79 | ▼ -79.24 after sell → book $11,280.14; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 18 | $58.75 | $2.06 | $-26.61 | $8,233.22 | ▼ -26.61 after sell → book $11,278.07; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 88 | $14.00 | $2.25 | — | $6,998.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $1234.98; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $5,828.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1234.98; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 29 | $32.90 | $2.08 | — | $4,872.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $971.40; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 112 | $8.61 | $2.33 | — | $3,905.57 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $971.40; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 6 | $141.76 | $2.01 | — | $3,053.01 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $971.40; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 50 | $19.25 | $2.14 | — | $2,088.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $971.40; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 51 | $18.75 | $2.14 | — | $1,129.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $971.40; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 33 | $28.91 | $2.09 | — | $173.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $971.40; owner union_news_g_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.85 | ▼ close $11,068.14 vs 09:30 $11,295.89 (session -192.88) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.85 | ▼ 09:30 equity $11,031.89 vs yday $11,068.14 (-36.25) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 145 | $9.50 | $2.46 | $+170.56 | $1,548.89 | ▲ +170.56 after sell → book $11,029.43; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 4 | $134.10 | $2.02 | $-44.34 | $2,083.27 | ▼ -44.34 after sell → book $11,027.41; vs 09:30 mark -2.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 26 | $42.00 | $2.09 | $+10.40 | $3,173.18 | ▲ +10.40 after sell → book $11,025.32; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $4,355.39 | ▲ +11.63 after sell → book $11,023.29; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 29 | $31.15 | $2.10 | $-54.92 | $5,256.64 | ▼ -54.92 after sell → book $11,021.19; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 112 | $8.52 | $2.35 | $-14.76 | $6,208.53 | ▼ -14.76 after sell → book $11,018.84; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 6 | $132.30 | $2.03 | $-60.80 | $7,000.30 | ▼ -60.80 after sell → book $11,016.81; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 50 | $17.87 | $2.16 | $-73.30 | $7,891.64 | ▼ -73.30 after sell → book $11,014.65; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 51 | $19.25 | $2.16 | $+21.19 | $8,871.23 | ▲ +21.19 after sell → book $11,012.49; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 33 | $28.06 | $2.11 | $-32.25 | $9,795.10 | ▼ -32.25 after sell → book $11,010.38; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.10 | ▼ close $10,965.50 vs 09:30 $11,031.89 (session -44.88) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.10 | ▼ 09:30 equity $10,942.62 vs yday $10,965.50 (-22.88) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 88 | $13.04 | $2.28 | $-89.01 | $10,940.34 | ▼ -89.01 after sell → book $10,940.34; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,940.34 | ▲ close $10,940.34 vs 09:30 $10,942.62 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,940.34 | ▲ 09:30 equity $10,940.34 vs yday $10,940.34 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,940.34 | ▲ close $10,940.34 vs 09:30 $10,940.34 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,940.34 | ▲ 09:30 equity $10,940.34 vs yday $10,940.34 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 460 | $1.78 | $5.93 | — | $10,115.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $820.53; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 44 | $18.40 | $2.12 | — | $9,303.88 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $820.53; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 59 | $13.71 | $2.17 | — | $8,492.83 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $820.53; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $7,678.81 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $820.53; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 33 | $32.88 | $2.09 | — | $6,591.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 144 | $7.59 | $2.42 | — | $5,496.30 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $4,791.06 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 69 | $15.87 | $2.20 | — | $3,693.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $2,636.61 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $1,571.14 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 23 | $47.60 | $2.06 | — | $474.29 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $1096.97; owner union_news_g_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $474.29 | ▼ close $10,888.39 vs 09:30 $10,940.34 (session -24.88) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $474.29 | ▼ 09:30 equity $10,883.65 vs yday $10,888.39 (-4.74) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 44 | $18.15 | $2.14 | $-15.26 | $1,270.74 | ▼ -15.26 after sell → book $10,881.50; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 59 | $13.89 | $2.19 | $+6.27 | $2,088.07 | ▲ +6.27 after sell → book $10,879.32; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $2,896.51 | ▼ -5.56 after sell → book $10,877.20; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 33 | $32.48 | $2.11 | $-17.40 | $3,966.25 | ▼ -17.40 after sell → book $10,875.10; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 144 | $7.79 | $2.46 | $+23.92 | $5,085.55 | ▲ +23.92 after sell → book $10,872.64; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $5,775.57 | ▼ -15.23 after sell → book $10,870.63; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $6,852.65 | ▲ +19.86 after sell → book $10,868.61; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $7,815.64 | ▼ -102.48 after sell → book $10,866.59; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 23 | $53.85 | $2.08 | $+139.61 | $9,052.11 | ▲ +139.61 after sell → book $10,864.51; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 35 | $25.18 | $2.10 | — | $8,168.71 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $905.21; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 115 | $7.87 | $2.33 | — | $7,261.33 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $905.21; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 156 | $5.79 | $2.46 | — | $6,355.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $905.21; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $5,300.19 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1271.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 655 | $1.94 | $8.45 | — | $4,021.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1271.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $2,782.87 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1271.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $1,596.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1271.13; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $384.33 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1271.13; owner union_news_g_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $384.33 | ▲ close $11,146.03 vs 09:30 $10,883.65 (session +304.92) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $384.33 | ▼ 09:30 equity $11,107.85 vs yday $11,146.03 (-38.18) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 460 | $1.56 | $6.02 | $-110.85 | $1,098.21 | ▼ -110.85 after sell → book $11,101.83; vs 09:30 mark -6.02 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 69 | $16.74 | $2.22 | $+55.61 | $2,251.05 | ▲ +55.61 after sell → book $11,099.61; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 35 | $26.44 | $2.12 | $+39.89 | $3,174.34 | ▲ +39.89 after sell → book $11,097.50; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 115 | $7.76 | $2.36 | $-17.35 | $4,064.37 | ▼ -17.35 after sell → book $11,095.13; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 156 | $5.81 | $2.49 | $-1.83 | $4,968.24 | ▼ -1.83 after sell → book $11,092.64; vs 09:30 mark -2.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $5,981.10 | ▼ -42.58 after sell → book $11,090.62; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 655 | $1.94 | $8.57 | $-17.02 | $7,243.23 | ▼ -17.02 after sell → book $11,082.05; vs 09:30 mark -8.57 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $8,580.00 | ▲ +150.67 after sell → book $11,080.02; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,580.00 | ▼ close $11,036.04 vs 09:30 $11,107.85 (session -43.98) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,580.00 | ▲ 09:30 equity $11,081.98 vs yday $11,036.04 (+45.94) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 9 | $141.82 | $2.04 | $+36.18 | $9,854.35 | ▲ +36.18 after sell → book $11,079.95; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 16 | $76.60 | $2.06 | $+11.10 | $11,077.89 | ▲ +11.10 after sell → book $11,077.89; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,077.89 | ▲ close $11,077.89 vs 09:30 $11,081.98 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,077.89 | ▲ 09:30 equity $11,077.89 vs yday $11,077.89 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,077.89 | ▲ close $11,077.89 vs 09:30 $11,077.89 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,077.89 | ▲ 09:30 equity $11,077.89 vs yday $11,077.89 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 307 | $2.70 | $3.96 | — | $10,245.03 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $830.84; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 169 | $4.91 | $2.50 | — | $9,412.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $830.84; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 134 | $6.16 | $2.39 | — | $8,584.91 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $830.84; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 265 | $3.13 | $3.42 | — | $7,752.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $830.84; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $6,599.02 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1292.01; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 633 | $2.04 | $8.17 | — | $5,299.53 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $1292.01; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 609 | $2.12 | $7.86 | — | $4,000.60 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $1292.01; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 86 | $15.01 | $2.25 | — | $2,707.49 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1292.01; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 5 | $242.17 | $2.00 | — | $1,494.63 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $1292.01; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 9 | $135.71 | $2.02 | — | $271.23 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $1292.01; owner union_news_g_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $271.23 | ▼ close $11,003.45 vs 09:30 $11,077.89 (session -37.86) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $271.23 | ▼ 09:30 equity $10,997.42 vs yday $11,003.45 (-6.03) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 134 | $6.02 | $2.42 | $-23.58 | $1,075.48 | ▼ -23.58 after sell → book $10,994.99; vs 09:30 mark -2.43 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $2,063.39 | ▼ -165.11 after sell → book $10,992.96; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 633 | $2.01 | $8.28 | $-35.44 | $3,327.44 | ▼ -35.44 after sell → book $10,984.68; vs 09:30 mark -8.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 609 | $2.05 | $7.97 | $-58.45 | $4,567.92 | ▼ -58.45 after sell → book $10,976.71; vs 09:30 mark -7.97 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 5 | $261.51 | $2.03 | $+92.67 | $5,873.45 | ▲ +92.67 after sell → book $10,974.69; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 9 | $131.40 | $2.04 | $-42.84 | $7,054.01 | ▼ -42.84 after sell → book $10,972.65; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,054.01 | ▲ close $11,176.98 vs 09:30 $10,997.42 (session +204.33) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,054.01 | ▲ 09:30 equity $11,234.92 vs yday $11,176.98 (+57.94) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 169 | $5.11 | $2.54 | $+28.77 | $7,915.07 | ▲ +28.77 after sell → book $11,232.39; vs 09:30 mark -2.53 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 265 | $3.64 | $3.47 | $+128.26 | $8,876.19 | ▲ +128.26 after sell → book $11,228.91; vs 09:30 mark -3.48 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,876.19 | ▲ close $11,321.51 vs 09:30 $11,234.92 (session +92.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,876.19 | ▲ 09:30 equity $11,335.39 vs yday $11,321.51 (+13.88) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 493 | $1.80 | $6.36 | — | $7,982.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $887.62; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 38 | $23.29 | $2.10 | — | $7,095.31 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $887.62; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 60 | $14.62 | $2.17 | — | $6,215.94 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $887.62; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 59 | $26.27 | $2.17 | — | $4,663.84 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $1553.98; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 223 | $6.95 | $2.88 | — | $3,111.12 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $1553.98; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 38 | $39.99 | $2.10 | — | $1,589.39 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $1553.98; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 8 | $189.17 | $2.01 | — | $74.02 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $1553.98; owner union_news_g_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.02 | ▼ close $11,256.73 vs 09:30 $11,335.39 (session -58.87) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.02 | ▲ 09:30 equity $11,359.46 vs yday $11,256.73 (+102.73) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 38 | $24.09 | $2.12 | $+26.17 | $987.31 | ▲ +26.17 after sell → book $11,357.33; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 60 | $13.77 | $2.19 | $-55.36 | $1,811.32 | ▼ -55.36 after sell → book $11,355.14; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 59 | $26.51 | $2.19 | $+9.80 | $3,373.22 | ▲ +9.80 after sell → book $11,352.95; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 223 | $7.27 | $2.93 | $+65.56 | $4,991.51 | ▲ +65.56 after sell → book $11,350.03; vs 09:30 mark -2.92 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 38 | $37.57 | $2.13 | $-96.19 | $6,417.04 | ▼ -96.19 after sell → book $11,347.90; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 8 | $190.35 | $2.04 | $+5.39 | $7,937.81 | ▲ +5.39 after sell → book $11,345.87; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 53 | $22.46 | $2.15 | — | $6,745.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $1190.67; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 32 | $36.76 | $2.09 | — | $5,566.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $1190.67; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 5 | $170.85 | $2.00 | — | $4,710.62 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $927.81; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 52 | $17.72 | $2.15 | — | $3,787.03 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $927.81; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 524 | $1.77 | $6.76 | — | $2,852.79 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $927.81; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 3 | $238.60 | $2.00 | — | $2,134.99 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $927.81; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 41 | $22.12 | $2.11 | — | $1,225.96 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $927.81; owner union_news_g_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,225.96 | ▲ close $11,514.84 vs 09:30 $11,359.46 (session +188.23) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,225.96 | ▼ 09:30 equity $11,511.10 vs yday $11,514.84 (-3.74) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 86 | $15.87 | $2.27 | $+69.44 | $2,588.51 | ▲ +69.44 after sell → book $11,508.83; vs 09:30 mark -2.27 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 493 | $1.96 | $6.45 | $+66.07 | $3,548.33 | ▲ +66.07 after sell → book $11,502.37; vs 09:30 mark -6.46 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 53 | $21.30 | $2.17 | $-65.80 | $4,675.06 | ▼ -65.80 after sell → book $11,500.20; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 32 | $39.50 | $2.11 | $+83.49 | $5,936.96 | ▲ +83.49 after sell → book $11,498.10; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 5 | $182.33 | $2.02 | $+53.37 | $6,846.58 | ▲ +53.37 after sell → book $11,496.07; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 52 | $17.13 | $2.17 | $-34.99 | $7,735.18 | ▼ -34.99 after sell → book $11,493.91; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 524 | $1.77 | $6.86 | $-13.62 | $8,655.80 | ▼ -13.62 after sell → book $11,487.05; vs 09:30 mark -6.86 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 3 | $236.80 | $2.02 | $-9.42 | $9,364.18 | ▼ -9.42 after sell → book $11,485.03; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 31 | $29.32 | $2.08 | — | $8,453.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $936.42; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 308 | $3.04 | $3.97 | — | $7,514.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $936.42; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 11 | $81.40 | $2.02 | — | $6,617.00 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $936.42; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 117 | $14.07 | $2.34 | — | $4,968.47 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $1654.25; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 111 | $14.79 | $2.32 | — | $3,324.46 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $1654.25; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 219 | $7.54 | $2.83 | — | $1,671.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $1654.25; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 79 | $20.91 | $2.23 | — | $17.35 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $1654.25; owner union_news_g_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.35 | ▼ close $11,381.34 vs 09:30 $11,511.10 (session -85.89) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.35 | ▲ 09:30 equity $11,622.26 vs yday $11,381.34 (+240.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 307 | $3.55 | $4.02 | $+252.97 | $1,103.18 | ▲ +252.97 after sell → book $11,618.24; vs 09:30 mark -4.02 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 41 | $22.78 | $2.13 | $+22.81 | $2,035.03 | ▲ +22.81 after sell → book $11,616.11; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 31 | $29.43 | $2.10 | $-0.78 | $2,945.25 | ▼ -0.78 after sell → book $11,614.00; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 308 | $4.00 | $4.03 | $+289.21 | $4,173.22 | ▲ +289.21 after sell → book $11,609.97; vs 09:30 mark -4.03 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 11 | $79.08 | $2.04 | $-29.59 | $5,041.06 | ▼ -29.59 after sell → book $11,607.93; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 117 | $13.90 | $2.37 | $-24.60 | $6,664.98 | ▼ -24.60 after sell → book $11,605.55; vs 09:30 mark -2.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 111 | $14.58 | $2.35 | $-27.99 | $8,281.01 | ▼ -27.99 after sell → book $11,603.20; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 219 | $7.36 | $2.87 | $-44.02 | $9,889.97 | ▼ -44.02 after sell → book $11,600.32; vs 09:30 mark -2.88 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 79 | $21.65 | $2.25 | $+53.98 | $11,598.07 | ▲ +53.98 after sell → book $11,598.07; vs 09:30 mark -2.25 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 352 | $2.47 | $4.54 | — | $10,724.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $869.86; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 51 | $16.91 | $2.14 | — | $9,859.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $869.86; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 527 | $1.65 | $6.80 | — | $8,983.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $869.86; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 74 | $11.67 | $2.21 | — | $8,117.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $869.86; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 52 | $25.95 | $2.15 | — | $6,765.85 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $1352.90; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 629 | $2.15 | $8.11 | — | $5,405.39 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $1352.90; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 97 | $13.94 | $2.28 | — | $4,050.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $1352.90; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 225 | $6.00 | $2.90 | — | $2,698.02 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $1352.90; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 7 | $190.30 | $2.01 | — | $1,363.91 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $1352.90; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $210.66 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $1352.90; owner union_news_g_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.66 | ▼ close $11,455.01 vs 09:30 $11,622.26 (session -107.91) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.66 | ▼ 09:30 equity $11,398.25 vs yday $11,455.01 (-56.76) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 97 | $13.13 | $2.31 | $-83.16 | $1,481.96 | ▼ -83.16 after sell → book $11,395.94; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 225 | $5.99 | $2.95 | $-8.10 | $2,826.76 | ▼ -8.10 after sell → book $11,392.99; vs 09:30 mark -2.95 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 31 | $9.11 | $2.08 | — | $2,542.27 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $282.68; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 39 | $7.23 | $2.11 | — | $2,258.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $282.68; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 447 | $1.01 | $5.77 | — | $1,800.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $451.64; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 2 | $168.50 | $2.00 | — | $1,461.96 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $451.64; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 105 | $4.30 | $2.31 | — | $1,008.15 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $451.64; owner union_news_g_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,008.15 | ▼ close $11,353.24 vs 09:30 $11,398.25 (session -25.50) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,008.15 | ▲ 09:30 equity $11,609.37 vs yday $11,353.24 (+256.13) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 51 | $16.92 | $2.16 | $-3.80 | $1,868.91 | ▼ -3.80 after sell → book $11,607.21; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 527 | $1.41 | $6.90 | $-140.17 | $2,605.08 | ▼ -140.17 after sell → book $11,600.31; vs 09:30 mark -6.90 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 74 | $12.80 | $2.23 | $+79.17 | $3,550.05 | ▲ +79.17 after sell → book $11,598.08; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 52 | $26.58 | $2.17 | $+28.45 | $4,930.04 | ▲ +28.45 after sell → book $11,595.91; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 629 | $2.09 | $8.23 | $-54.08 | $6,236.42 | ▼ -54.08 after sell → book $11,587.68; vs 09:30 mark -8.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 7 | $174.50 | $2.03 | $-114.64 | $7,455.89 | ▼ -114.64 after sell → book $11,585.65; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 5 | $266.50 | $2.03 | $+177.22 | $8,786.37 | ▲ +177.22 after sell → book $11,583.63; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 31 | $8.39 | $2.10 | $-26.51 | $9,044.35 | ▼ -26.51 after sell → book $11,581.52; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 39 | $6.83 | $2.13 | $-19.83 | $9,308.60 | ▼ -19.83 after sell → book $11,579.40; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 447 | $0.95 | $5.67 | $-38.26 | $9,727.57 | ▼ -38.26 after sell → book $11,573.72; vs 09:30 mark -5.68 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 2 | $183.41 | $2.02 | $+25.80 | $10,092.37 | ▲ +25.80 after sell → book $11,571.71; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 373 | $2.70 | $4.81 | — | $9,080.46 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1009.24; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 24 | $41.76 | $2.06 | — | $8,076.15 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1009.24; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 224 | $4.49 | $2.89 | — | $7,067.51 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1009.24; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 148 | $7.95 | $2.43 | — | $5,888.47 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $1177.92; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 74 | $15.72 | $2.21 | — | $4,722.98 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1177.92; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 965 | $1.22 | $12.45 | — | $3,533.23 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $1177.92; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 906 | $1.30 | $11.69 | — | $2,343.74 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $1177.92; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 29 | $40.00 | $2.08 | — | $1,181.67 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $1177.92; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 5 | $196.78 | $2.00 | — | $195.76 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1177.92; owner union_news_g_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.76 | ▼ close $11,155.28 vs 09:30 $11,609.37 (session -373.80) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.76 | ▼ 09:30 equity $11,052.00 vs yday $11,155.28 (-103.28) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 352 | $2.68 | $4.61 | $+64.77 | $1,134.51 | ▲ +64.77 after sell → book $11,047.39; vs 09:30 mark -4.61 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 105 | $4.12 | $2.33 | $-23.54 | $1,564.78 | ▼ -23.54 after sell → book $11,045.05; vs 09:30 mark -2.34 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 24 | $36.02 | $2.08 | $-141.78 | $2,427.30 | ▼ -141.78 after sell → book $11,042.97; vs 09:30 mark -2.08 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 224 | $3.92 | $2.94 | $-132.39 | $3,303.56 | ▼ -132.39 after sell → book $11,040.03; vs 09:30 mark -2.94 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 148 | $7.38 | $2.47 | $-89.26 | $4,393.33 | ▼ -89.26 after sell → book $11,037.57; vs 09:30 mark -2.46 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 74 | $14.38 | $2.23 | $-103.61 | $5,455.22 | ▼ -103.61 after sell → book $11,035.33; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 965 | $1.17 | $12.62 | $-73.32 | $6,571.65 | ▼ -73.32 after sell → book $11,022.71; vs 09:30 mark -12.62 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 906 | $1.27 | $11.85 | $-50.72 | $7,710.42 | ▼ -50.72 after sell → book $11,010.87; vs 09:30 mark -11.84 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 29 | $39.27 | $2.10 | $-25.34 | $8,847.15 | ▼ -25.34 after sell → book $11,008.77; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 5 | $192.26 | $2.02 | $-26.63 | $9,806.43 | ▼ -26.63 after sell → book $11,006.74; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,806.43 | ▲ close $11,801.98 vs 09:30 $11,052.00 (session +795.24) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,532.50 | ▲ 09:30 equity $9,660.22 vs yday $9,475.62 (+184.60) | 09:30 open · cash $7,532.50 (unchanged overnight, no fees) · equity $9,660.22 vs prior close $9,475.62 (+184.60) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 25 | $29.76 | $2.06 | — | $6,786.43 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $753.25; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 46 | $16.21 | $2.13 | — | $6,038.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $753.25; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 48 | $15.58 | $2.13 | — | $5,288.62 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $753.25; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 342 | $3.86 | $4.41 | — | $3,964.09 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $1322.16; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 4 | $272.16 | $2.00 | — | $2,873.45 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $1322.16; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 17 | $74.15 | $2.04 | — | $1,610.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $1322.16; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $721.86 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; combo leftover $1322.16; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $721.86 | ▼ close $9,485.22 vs 09:30 $9,660.22 (session -158.23) | 16:00 close · cash $721.86 · equity $9,485.22 vs 09:30 $9,660.22 (-175.00; session marks -158.23) · 9 name(s) marked open→close (per-name table). GLND×260 09:30 $6.06 → close $5.54 -135.20; VICR×2 09:30 $276.06 → close $276.06 -0.00; TJGC×25 09:30 $29.76 → close $26.24 -88.00; SECZ×46 09:30 $16.21 → close $15.96 -11.50; USDE×48 09:30 $15.58 → close $17.25 +80.11; ZSQR×342 09:30 $3.86 → close $3.78 -27.36; ILMN×4 09:30 $272.16 → close $270.00 -8.64; RKLB×17 09:30 $74.15 → close $73.95 -3.40; COST×1 09:30 $887.00 → close $922.76 +35.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1040.54 < 1 share @ 1646.93 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-17 | `LITE` | cash | leftover split 927.81 < 1 share @ 934.88 |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 373 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1009.24; owner union_hot_n4_h1 |
