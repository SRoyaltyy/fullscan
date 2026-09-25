# Factor mine action — `combo_hn_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_news_g_h1 w=0.7,0.3 net=priority

Cash book **+11.18%** ($11,117) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 297 · skips 126 · realized $+1716.61.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 70%, union_news_g_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 70%, union_news_g_h1 30%.
- Member: union_hot_n4_h1 (70% · long · hold 1).
- Member: union_news_g_h1 (30% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $638.50.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $2,760.07 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $445.99; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 3 | $146.90 | $2.00 | — | $2,317.38 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $445.99; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 3 | $120.00 | $2.00 | — | $1,955.38 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $445.99; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 103 | $4.31 | $2.30 | — | $1,509.15 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $445.99; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 32 | $13.55 | $2.09 | — | $1,073.46 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $445.99; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 33 | $13.18 | $2.09 | — | $636.43 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $445.99; owner union_news_g_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $636.43 | ▼ close $10,181.61 vs 09:30 $10,412.10 (session -155.26) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $636.43 | ▼ 09:30 equity $10,077.24 vs yday $10,181.61 (-104.37) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 73 | $24.83 | $2.24 | $+6.51 | $2,446.79 | ▲ +6.51 after sell → book $10,075.01; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 92 | $19.57 | $2.30 | $-4.56 | $4,244.93 | ▼ -4.56 after sell → book $10,072.71; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 824 | $2.08 | $10.78 | $-116.17 | $5,952.19 | ▼ -116.17 after sell → book $10,061.93; vs 09:30 mark -10.78 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 163 | $9.57 | $2.52 | $-257.65 | $7,509.58 | ▼ -257.65 after sell → book $10,059.41; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $7,875.45 | ▲ +4.04 after sell → book $10,057.40; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 3 | $149.37 | $2.02 | $+3.39 | $8,321.54 | ▲ +3.39 after sell → book $10,055.38; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 3 | $127.40 | $2.02 | $+18.18 | $8,701.72 | ▲ +18.18 after sell → book $10,053.36; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 103 | $4.60 | $2.33 | $+25.24 | $9,173.20 | ▲ +25.24 after sell → book $10,051.04; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 32 | $13.16 | $2.11 | $-16.67 | $9,592.21 | ▼ -16.67 after sell → book $10,048.93; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 33 | $13.84 | $2.11 | $+17.58 | $10,046.82 | ▲ +17.58 after sell → book $10,046.82; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 419 | $4.19 | $5.41 | — | $8,285.81 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1758.19; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 255 | $6.87 | $3.29 | — | $6,530.67 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1758.19; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 128 | $13.64 | $2.37 | — | $4,782.37 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1758.19; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 42 | $41.23 | $2.12 | — | $3,048.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1758.19; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 13 | $46.18 | $2.03 | — | $2,446.23 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $609.72; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 4 | $142.77 | $2.00 | — | $1,873.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $609.72; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 3 | $202.70 | $2.00 | — | $1,263.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $609.72; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 6 | $92.99 | $2.01 | — | $703.10 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $609.72; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 12 | $49.00 | $2.03 | — | $113.07 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $609.72; owner union_news_g_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.07 | ▲ close $10,070.35 vs 09:30 $10,077.24 (session +46.78) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.07 | ▼ 09:30 equity $10,061.42 vs yday $10,070.35 (-8.93) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 419 | $3.94 | $5.49 | $-115.64 | $1,758.44 | ▼ -115.64 after sell → book $10,055.93; vs 09:30 mark -5.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 128 | $13.31 | $2.41 | $-47.02 | $3,459.72 | ▼ -47.02 after sell → book $10,053.52; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 42 | $41.50 | $2.14 | $+7.08 | $5,200.58 | ▲ +7.08 after sell → book $10,051.39; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 13 | $48.00 | $2.05 | $+19.58 | $5,822.53 | ▲ +19.58 after sell → book $10,049.34; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 4 | $148.04 | $2.02 | $+17.06 | $6,412.66 | ▲ +17.06 after sell → book $10,047.31; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 3 | $208.93 | $2.02 | $+14.67 | $7,037.44 | ▲ +14.67 after sell → book $10,045.30; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 6 | $92.38 | $2.03 | $-7.70 | $7,589.69 | ▼ -7.70 after sell → book $10,043.27; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 12 | $45.09 | $2.05 | $-50.99 | $8,128.72 | ▼ -50.99 after sell → book $10,041.22; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,128.72 | ▼ close $9,934.12 vs 09:30 $10,061.42 (session -107.10) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,128.72 | ▲ 09:30 equity $9,962.17 vs yday $9,934.12 (+28.05) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 255 | $7.19 | $3.35 | $+74.96 | $9,958.82 | ▲ +74.96 after sell → book $9,958.82; vs 09:30 mark -3.35 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,958.82 | ▲ close $9,958.82 vs 09:30 $9,962.17 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,958.82 | ▲ 09:30 equity $9,958.82 vs yday $9,958.82 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 11 | $150.14 | $2.02 | — | $8,305.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1742.79; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1515 | $1.15 | $19.54 | — | $6,543.47 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1742.79; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 147 | $11.81 | $2.43 | — | $4,804.23 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1742.79; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1272 | $1.37 | $16.41 | — | $3,045.18 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1742.79; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $2,679.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 615 | $0.71 | $6.19 | — | $2,238.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 65 | $6.61 | $2.19 | — | $1,806.63 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 27 | $16.00 | $2.07 | — | $1,372.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 16 | $26.57 | $2.04 | — | $945.40 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 7 | $58.73 | $2.01 | — | $532.28 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 9 | $44.76 | $2.02 | — | $127.43 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $435.03; owner union_news_g_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.43 | ▼ close $9,809.78 vs 09:30 $9,958.82 (session -90.12) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.43 | ▲ 09:30 equity $10,114.03 vs yday $9,809.78 (+304.25) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 147 | $11.57 | $2.47 | $-40.91 | $1,825.75 | ▼ -40.91 after sell → book $10,111.56; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1272 | $1.46 | $16.63 | $+81.44 | $3,666.23 | ▲ +81.44 after sell → book $10,094.92; vs 09:30 mark -16.64 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 4 | $95.72 | $2.02 | $+14.82 | $4,047.09 | ▲ +14.82 after sell → book $10,092.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 615 | $0.67 | $6.10 | $-32.59 | $4,455.50 | ▼ -32.59 after sell → book $10,086.80; vs 09:30 mark -6.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 65 | $6.95 | $2.21 | $+18.03 | $4,905.04 | ▲ +18.03 after sell → book $10,084.59; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 27 | $17.66 | $2.09 | $+40.66 | $5,379.77 | ▲ +40.66 after sell → book $10,082.50; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 16 | $26.25 | $2.06 | $-9.22 | $5,797.71 | ▼ -9.22 after sell → book $10,080.44; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 9 | $44.52 | $2.04 | $-6.21 | $6,196.36 | ▼ -6.21 after sell → book $10,078.41; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 483 | $4.49 | $6.23 | — | $4,021.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $2168.72; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 318 | $6.81 | $4.10 | — | $1,851.77 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $2168.72; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $1,610.92 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $264.54; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 107 | $2.47 | $2.31 | — | $1,344.32 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $264.54; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 2 | $115.18 | $2.00 | — | $1,111.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $264.54; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 22 | $11.70 | $2.06 | — | $852.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $264.54; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 23 | $11.10 | $2.06 | — | $595.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $264.54; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 81 | $3.24 | $2.23 | — | $330.59 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $264.54; owner union_news_g_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $330.59 | ▲ close $10,127.32 vs 09:30 $10,114.03 (session +71.89) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $330.59 | ▲ 09:30 equity $11,215.88 vs yday $10,127.32 (+1,088.56) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 11 | $142.70 | $2.05 | $-85.91 | $1,898.24 | ▼ -85.91 after sell → book $11,213.83; vs 09:30 mark -2.05 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1515 | $1.83 | $19.82 | $+990.84 | $4,650.87 | ▲ +990.84 after sell → book $11,194.01; vs 09:30 mark -19.82 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 483 | $4.32 | $6.33 | $-94.67 | $6,731.11 | ▼ -94.67 after sell → book $11,187.69; vs 09:30 mark -6.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 318 | $8.03 | $4.18 | $+379.68 | $9,280.47 | ▲ +379.68 after sell → book $11,183.51; vs 09:30 mark -4.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 2 | $120.51 | $2.02 | $-1.85 | $9,519.48 | ▼ -1.85 after sell → book $11,181.50; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 107 | $2.40 | $2.34 | $-12.14 | $9,773.94 | ▼ -12.14 after sell → book $11,179.16; vs 09:30 mark -2.34 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 2 | $121.00 | $2.02 | $+7.63 | $10,013.92 | ▲ +7.63 after sell → book $11,177.14; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 22 | $11.17 | $2.08 | $-15.79 | $10,257.59 | ▼ -15.79 after sell → book $11,175.07; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 23 | $11.48 | $2.08 | $+4.72 | $10,519.55 | ▲ +4.72 after sell → book $11,172.99; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 81 | $2.99 | $2.26 | $-24.74 | $10,759.48 | ▼ -24.74 after sell → book $11,170.73; vs 09:30 mark -2.26 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,759.48 | ▼ close $11,159.00 vs 09:30 $11,215.88 (session -11.72) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,759.48 | ▲ 09:30 equity $11,164.99 vs yday $11,159.00 (+5.99) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 7 | $57.93 | $2.03 | $-9.64 | $11,162.96 | ▼ -9.64 after sell → book $11,162.96; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 81 | $24.11 | $2.23 | — | $9,207.82 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1953.52; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1252 | $1.56 | $16.15 | — | $7,238.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1953.52; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 479 | $4.07 | $6.18 | — | $5,282.84 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1953.52; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 102 | $19.04 | $2.30 | — | $3,338.46 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1953.52; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 59 | $9.42 | $2.17 | — | $2,780.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $556.41; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 15 | $35.05 | $2.04 | — | $2,252.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $556.41; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 19 | $28.86 | $2.05 | — | $1,702.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $556.41; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 63 | $8.72 | $2.18 | — | $1,150.80 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $556.41; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 4 | $118.52 | $2.00 | — | $674.72 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $556.41; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 7 | $77.13 | $2.01 | — | $132.80 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $556.41; owner union_news_g_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.80 | ▲ close $11,868.57 vs 09:30 $11,164.99 (session +744.91) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.80 | ▼ 09:30 equity $11,475.87 vs yday $11,868.57 (-392.70) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 81 | $26.61 | $2.26 | $+198.00 | $2,285.94 | ▲ +198.00 after sell → book $11,473.60; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1252 | $1.60 | $16.37 | $+17.55 | $4,272.77 | ▲ +17.55 after sell → book $11,457.23; vs 09:30 mark -16.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 102 | $20.72 | $2.33 | $+166.73 | $6,383.88 | ▲ +166.73 after sell → book $11,454.90; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 59 | $10.07 | $2.19 | $+34.00 | $6,975.82 | ▲ +34.00 after sell → book $11,452.71; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 15 | $35.70 | $2.06 | $+5.66 | $7,509.27 | ▲ +5.66 after sell → book $11,450.66; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 19 | $27.56 | $2.07 | $-28.81 | $8,030.84 | ▼ -28.81 after sell → book $11,448.59; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 63 | $8.86 | $2.20 | $+4.44 | $8,586.82 | ▲ +4.44 after sell → book $11,446.39; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 4 | $119.80 | $2.02 | $+1.10 | $9,064.00 | ▲ +1.10 after sell → book $11,444.37; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 7 | $79.34 | $2.03 | $+11.43 | $9,617.35 | ▲ +11.43 after sell → book $11,442.34; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 159 | $14.11 | $2.47 | — | $7,371.39 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $2244.05; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 386 | $5.81 | $4.98 | — | $5,123.75 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $2244.05; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 193 | $11.59 | $2.57 | — | $2,885.28 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $2244.05; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 43 | $11.12 | $2.12 | — | $2,405.00 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $480.88; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 58 | $8.29 | $2.16 | — | $1,922.02 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $480.88; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 27 | $17.41 | $2.07 | — | $1,449.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $480.88; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 42 | $11.22 | $2.12 | — | $976.52 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $480.88; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 1 | $267.02 | $1.99 | — | $707.51 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $480.88; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 4 | $118.50 | $2.00 | — | $231.50 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $480.88; owner union_news_g_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $231.50 | ▲ close $11,696.91 vs 09:30 $11,475.87 (session +277.05) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $231.50 | ▲ 09:30 equity $12,017.63 vs yday $11,696.91 (+320.72) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 159 | $14.20 | $2.51 | $+9.33 | $2,486.79 | ▲ +9.33 after sell → book $12,015.12; vs 09:30 mark -2.51 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 386 | $6.50 | $5.06 | $+256.30 | $4,990.73 | ▲ +256.30 after sell → book $12,010.06; vs 09:30 mark -5.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 193 | $12.18 | $2.62 | $+109.65 | $7,338.85 | ▲ +109.65 after sell → book $12,007.44; vs 09:30 mark -2.62 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 1 | $267.23 | $2.01 | $-3.80 | $7,604.07 | ▼ -3.80 after sell → book $12,005.43; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 18 | $144.18 | $2.04 | — | $5,006.78 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $2661.42; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 143 | $18.50 | $2.42 | — | $2,358.86 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $2661.42; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 18 | $41.44 | $2.04 | — | $1,610.90 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $786.29; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 11 | $70.30 | $2.02 | — | $835.58 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $786.29; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 13 | $60.00 | $2.03 | — | $53.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $786.29; owner union_news_g_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.55 | ▼ close $11,749.12 vs 09:30 $12,017.63 (session -245.75) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.55 | ▼ 09:30 equity $11,570.68 vs yday $11,749.12 (-178.44) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 479 | $3.69 | $6.27 | $-194.47 | $1,814.78 | ▼ -194.47 after sell → book $11,564.40; vs 09:30 mark -6.28 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 43 | $11.27 | $2.14 | $+2.19 | $2,297.25 | ▲ +2.19 after sell → book $11,562.26; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 27 | $17.70 | $2.09 | $+3.67 | $2,773.06 | ▲ +3.67 after sell → book $11,560.17; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 42 | $11.00 | $2.14 | $-13.49 | $3,232.93 | ▼ -13.49 after sell → book $11,558.04; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 4 | $115.66 | $2.02 | $-15.38 | $3,693.55 | ▼ -15.38 after sell → book $11,556.02; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 143 | $18.15 | $2.46 | $-54.93 | $6,286.53 | ▼ -54.93 after sell → book $11,553.55; vs 09:30 mark -2.47 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 11 | $65.29 | $2.04 | $-59.18 | $7,002.68 | ▼ -59.18 after sell → book $11,551.51; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 13 | $58.75 | $2.05 | $-20.33 | $7,764.38 | ▼ -20.33 after sell → book $11,549.46; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 194 | $14.00 | $2.57 | — | $5,045.81 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2717.53; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 18 | $146.07 | $2.04 | — | $2,414.50 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2717.53; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 12 | $32.90 | $2.03 | — | $2,017.68 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $402.42; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 46 | $8.61 | $2.13 | — | $1,619.49 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $402.42; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 2 | $141.76 | $2.00 | — | $1,333.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $402.42; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 20 | $19.25 | $2.05 | — | $946.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $402.42; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 21 | $18.75 | $2.05 | — | $551.12 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $402.42; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 13 | $28.91 | $2.03 | — | $173.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $402.42; owner union_news_g_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.26 | ▼ close $11,482.89 vs 09:30 $11,570.68 (session -49.67) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.26 | ▼ 09:30 equity $11,394.49 vs yday $11,482.89 (-88.40) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 58 | $9.50 | $2.18 | $+65.83 | $722.08 | ▲ +65.83 after sell → book $11,392.31; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 18 | $134.10 | $2.07 | $-185.56 | $3,133.81 | ▼ -185.56 after sell → book $11,390.24; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 18 | $42.00 | $2.06 | $+5.97 | $3,887.74 | ▲ +5.97 after sell → book $11,388.17; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 18 | $148.03 | $2.08 | $+31.16 | $6,550.21 | ▲ +31.16 after sell → book $11,386.10; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 12 | $31.15 | $2.05 | $-25.07 | $6,921.96 | ▼ -25.07 after sell → book $11,384.05; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 46 | $8.52 | $2.15 | $-8.42 | $7,311.73 | ▼ -8.42 after sell → book $11,381.90; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 2 | $132.30 | $2.02 | $-22.93 | $7,574.32 | ▼ -22.93 after sell → book $11,379.89; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 20 | $17.87 | $2.07 | $-31.72 | $7,929.65 | ▼ -31.72 after sell → book $11,377.82; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 21 | $19.25 | $2.07 | $+6.37 | $8,331.82 | ▲ +6.37 after sell → book $11,375.74; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 13 | $28.06 | $2.05 | $-15.13 | $8,694.55 | ▼ -15.13 after sell → book $11,373.69; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,694.55 | ▼ close $11,274.75 vs 09:30 $11,394.49 (session -98.94) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,694.55 | ▼ 09:30 equity $11,224.31 vs yday $11,274.75 (-50.44) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 194 | $13.04 | $2.62 | $-191.44 | $11,221.69 | ▼ -191.44 after sell → book $11,221.69; vs 09:30 mark -2.62 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,221.69 | ▲ close $11,221.69 vs 09:30 $11,224.31 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,221.69 | ▲ 09:30 equity $11,221.69 vs yday $11,221.69 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,221.69 | ▲ close $11,221.69 vs 09:30 $11,221.69 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,221.69 | ▲ 09:30 equity $11,221.69 vs yday $11,221.69 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1103 | $1.78 | $14.23 | — | $9,244.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1963.80; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 106 | $18.40 | $2.31 | — | $7,291.41 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1963.80; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 143 | $13.71 | $2.42 | — | $5,328.46 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1963.80; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 82 | $23.88 | $2.24 | — | $3,368.07 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1963.80; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 14 | $32.88 | $2.03 | — | $2,905.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $481.15; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 63 | $7.59 | $2.18 | — | $2,425.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $481.15; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 30 | $15.87 | $2.08 | — | $1,947.19 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $481.15; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $1,593.45 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $481.15; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $1,236.97 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $481.15; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 10 | $47.60 | $2.02 | — | $758.95 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $481.15; owner union_news_g_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $758.95 | ▼ close $10,851.32 vs 09:30 $11,221.69 (session -336.88) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $758.95 | ▲ 09:30 equity $10,913.80 vs yday $10,851.32 (+62.48) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 106 | $18.15 | $2.34 | $-31.15 | $2,680.51 | ▼ -31.15 after sell → book $10,911.46; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 143 | $13.89 | $2.46 | $+20.86 | $4,664.32 | ▲ +20.86 after sell → book $10,909.00; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 82 | $23.84 | $2.27 | $-7.78 | $6,616.94 | ▼ -7.78 after sell → book $10,906.74; vs 09:30 mark -2.26 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 14 | $32.48 | $2.05 | $-9.68 | $7,069.60 | ▼ -9.68 after sell → book $10,904.68; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 63 | $7.79 | $2.20 | $+8.22 | $7,558.17 | ▲ +8.22 after sell → book $10,902.48; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $7,915.86 | ▲ +3.95 after sell → book $10,900.47; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $8,235.52 | ▼ -36.83 after sell → book $10,898.46; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 10 | $53.85 | $2.04 | $+58.44 | $8,771.98 | ▲ +58.44 after sell → book $10,896.42; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 81 | $25.18 | $2.23 | — | $6,730.17 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $2046.79; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 260 | $7.87 | $3.35 | — | $4,680.61 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $2046.79; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 353 | $5.79 | $4.55 | — | $2,632.19 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $2046.79; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $2,366.83 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $526.44; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 271 | $1.94 | $3.50 | — | $1,837.60 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $526.44; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 3 | $137.35 | $2.00 | — | $1,423.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $526.44; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 2 | $236.82 | $2.00 | — | $947.91 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $526.44; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 6 | $75.65 | $2.01 | — | $492.01 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $526.44; owner union_news_g_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $492.01 | ▲ close $11,363.84 vs 09:30 $10,913.80 (session +489.05) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $492.01 | ▼ 09:30 equity $11,131.45 vs yday $11,363.84 (-232.39) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1103 | $1.56 | $14.43 | $-265.80 | $2,203.78 | ▼ -265.80 after sell → book $11,117.03; vs 09:30 mark -14.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 30 | $16.74 | $2.10 | $+21.92 | $2,703.88 | ▲ +21.92 after sell → book $11,114.93; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 81 | $26.44 | $2.26 | $+97.56 | $4,843.25 | ▲ +97.56 after sell → book $11,112.66; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 260 | $7.76 | $3.41 | $-35.37 | $6,857.44 | ▼ -35.37 after sell → book $11,109.25; vs 09:30 mark -3.41 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 353 | $5.81 | $4.63 | $-2.12 | $8,903.74 | ▼ -2.12 after sell → book $11,104.62; vs 09:30 mark -4.63 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 1 | $253.72 | $2.01 | $-13.65 | $9,155.45 | ▼ -13.65 after sell → book $11,102.61; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 271 | $1.94 | $3.55 | $-7.05 | $9,677.64 | ▼ -7.05 after sell → book $11,099.06; vs 09:30 mark -3.55 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 2 | $267.76 | $2.02 | $+57.87 | $10,211.14 | ▲ +57.87 after sell → book $11,097.04; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,211.14 | ▼ close $11,080.96 vs 09:30 $11,131.45 (session -16.08) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,211.14 | ▲ 09:30 equity $11,096.20 vs yday $11,080.96 (+15.24) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 3 | $141.82 | $2.02 | $+9.39 | $10,634.58 | ▲ +9.39 after sell → book $11,094.18; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 6 | $76.60 | $2.03 | $+1.66 | $11,092.15 | ▲ +1.66 after sell → book $11,092.15; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,092.15 | ▲ close $11,092.15 vs 09:30 $11,096.20 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,092.15 | ▲ 09:30 equity $11,092.15 vs yday $11,092.15 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,092.15 | ▲ close $11,092.15 vs 09:30 $11,092.15 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,092.15 | ▲ 09:30 equity $11,092.15 vs yday $11,092.15 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 718 | $2.70 | $9.26 | — | $9,144.29 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1941.13; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 395 | $4.91 | $5.10 | — | $7,199.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1941.13; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 315 | $6.16 | $4.06 | — | $5,255.28 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1941.13; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 620 | $3.13 | $8.00 | — | $3,306.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1941.13; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $2,811.39 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $551.11; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 270 | $2.04 | $3.48 | — | $2,257.11 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $551.11; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 259 | $2.12 | $3.34 | — | $1,704.69 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $551.11; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 36 | $15.01 | $2.10 | — | $1,162.23 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $551.11; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $675.90 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $551.11; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 4 | $135.71 | $2.00 | — | $131.05 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $551.11; owner union_news_g_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.05 | ▲ close $11,197.77 vs 09:30 $11,092.15 (session +146.96) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.05 | ▲ 09:30 equity $11,282.65 vs yday $11,197.77 (+84.88) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 315 | $6.02 | $4.13 | $-52.29 | $2,023.22 | ▼ -52.29 after sell → book $11,278.52; vs 09:30 mark -4.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 3 | $141.42 | $2.02 | $-73.05 | $2,445.46 | ▼ -73.05 after sell → book $11,276.50; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 270 | $2.01 | $3.54 | $-15.12 | $2,984.63 | ▼ -15.12 after sell → book $11,272.97; vs 09:30 mark -3.53 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 259 | $2.05 | $3.39 | $-24.87 | $3,512.18 | ▼ -24.87 after sell → book $11,269.57; vs 09:30 mark -3.40 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $4,033.19 | ▲ +34.67 after sell → book $11,267.56; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 4 | $131.40 | $2.02 | $-21.26 | $4,556.76 | ▼ -21.26 after sell → book $11,265.53; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,556.76 | ▲ close $11,695.49 vs 09:30 $11,282.65 (session +429.96) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,556.76 | ▲ 09:30 equity $11,821.13 vs yday $11,695.49 (+125.64) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 395 | $5.11 | $5.18 | $+68.73 | $6,570.04 | ▲ +68.73 after sell → book $11,815.96; vs 09:30 mark -5.17 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 620 | $3.64 | $8.12 | $+300.08 | $8,818.72 | ▲ +300.08 after sell → book $11,807.84; vs 09:30 mark -8.12 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,818.72 | ▲ close $11,988.08 vs 09:30 $11,821.13 (session +180.24) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,818.72 | ▲ 09:30 equity $12,005.68 vs yday $11,988.08 (+17.60) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1143 | $1.80 | $14.74 | — | $6,746.57 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $2057.70; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 88 | $23.29 | $2.25 | — | $4,694.80 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $2057.70; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 140 | $14.62 | $2.41 | — | $2,645.59 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $2057.70; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 25 | $26.27 | $2.06 | — | $1,986.77 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $661.40; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 95 | $6.95 | $2.27 | — | $1,324.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $661.40; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 16 | $39.99 | $2.04 | — | $682.37 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $661.40; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 3 | $189.17 | $2.00 | — | $112.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $661.40; owner union_news_g_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.86 | ▼ close $11,950.68 vs 09:30 $12,005.68 (session -27.21) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.86 | ▲ 09:30 equity $12,025.01 vs yday $11,950.68 (+74.33) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 88 | $24.09 | $2.29 | $+65.86 | $2,230.50 | ▲ +65.86 after sell → book $12,022.73; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 140 | $13.77 | $2.45 | $-123.86 | $4,155.85 | ▼ -123.86 after sell → book $12,020.28; vs 09:30 mark -2.45 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 25 | $26.51 | $2.08 | $+1.85 | $4,816.51 | ▲ +1.85 after sell → book $12,018.19; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 95 | $7.27 | $2.30 | $+25.82 | $5,504.86 | ▲ +25.82 after sell → book $12,015.89; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 16 | $37.57 | $2.06 | $-42.82 | $6,103.92 | ▼ -42.82 after sell → book $12,013.83; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 3 | $190.35 | $2.02 | $-0.48 | $6,672.96 | ▼ -0.48 after sell → book $12,011.82; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 103 | $22.46 | $2.30 | — | $4,357.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $2335.53; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 63 | $36.76 | $2.18 | — | $2,039.22 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $2335.53; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $1,866.66 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $339.87; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 19 | $17.72 | $2.05 | — | $1,527.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $339.87; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 192 | $1.77 | $2.57 | — | $1,185.52 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $339.87; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 1 | $238.60 | $1.99 | — | $944.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $339.87; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 15 | $22.12 | $2.04 | — | $611.09 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $339.87; owner union_news_g_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $611.09 | ▲ close $12,345.31 vs 09:30 $12,025.01 (session +348.33) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $611.09 | ▼ 09:30 equity $12,297.33 vs yday $12,345.31 (-47.98) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 36 | $15.87 | $2.12 | $+26.74 | $1,180.30 | ▲ +26.74 after sell → book $12,295.22; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1143 | $1.96 | $14.95 | $+153.18 | $3,405.62 | ▲ +153.18 after sell → book $12,280.26; vs 09:30 mark -14.96 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 103 | $21.30 | $2.33 | $-124.11 | $5,597.19 | ▼ -124.11 after sell → book $12,277.93; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 63 | $39.50 | $2.21 | $+168.23 | $8,083.48 | ▲ +168.23 after sell → book $12,275.72; vs 09:30 mark -2.21 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 1 | $182.33 | $1.85 | $+7.92 | $8,263.97 | ▲ +7.92 after sell → book $12,273.88; vs 09:30 mark -1.84 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 19 | $17.13 | $2.07 | $-15.32 | $8,587.37 | ▼ -15.32 after sell → book $12,271.81; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 192 | $1.77 | $2.61 | $-5.17 | $8,924.60 | ▼ -5.17 after sell → book $12,269.20; vs 09:30 mark -2.61 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 1 | $236.80 | $2.01 | $-5.81 | $9,159.39 | ▼ -5.81 after sell → book $12,267.19; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 72 | $29.32 | $2.21 | — | $7,046.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $2137.19; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 704 | $3.04 | $9.08 | — | $4,900.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $2137.19; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 26 | $81.40 | $2.07 | — | $2,781.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $2137.19; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 49 | $14.07 | $2.14 | — | $2,090.38 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $695.49; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 47 | $14.79 | $2.13 | — | $1,393.12 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $695.49; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 92 | $7.54 | $2.27 | — | $697.64 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $695.49; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 33 | $20.91 | $2.09 | — | $5.52 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $695.49; owner union_news_g_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.52 | ▲ close $12,263.76 vs 09:30 $12,297.33 (session +18.55) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.52 | ▲ 09:30 equity $12,645.09 vs yday $12,263.76 (+381.33) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 718 | $3.55 | $9.40 | $+591.64 | $2,545.02 | ▲ +591.64 after sell → book $12,635.69; vs 09:30 mark -9.40 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 15 | $22.78 | $2.06 | $+5.81 | $2,884.66 | ▲ +5.81 after sell → book $12,633.63; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 72 | $29.43 | $2.23 | $+3.48 | $5,001.39 | ▲ +3.48 after sell → book $12,631.40; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 704 | $4.00 | $9.22 | $+661.06 | $7,808.17 | ▲ +661.06 after sell → book $12,622.18; vs 09:30 mark -9.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 26 | $79.08 | $2.09 | $-64.48 | $9,862.15 | ▼ -64.48 after sell → book $12,620.08; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 49 | $13.90 | $2.16 | $-12.62 | $10,541.09 | ▼ -12.62 after sell → book $12,617.92; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 47 | $14.58 | $2.15 | $-14.15 | $11,224.20 | ▼ -14.15 after sell → book $12,615.77; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 92 | $7.36 | $2.29 | $-20.66 | $11,899.03 | ▼ -20.66 after sell → book $12,613.48; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 33 | $21.65 | $2.11 | $+20.22 | $12,611.37 | ▲ +20.22 after sell → book $12,611.37; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 893 | $2.47 | $11.52 | — | $10,394.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $2206.99; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 130 | $16.91 | $2.38 | — | $8,193.46 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $2206.99; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1337 | $1.65 | $17.25 | — | $5,970.17 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $2206.99; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 189 | $11.67 | $2.56 | — | $3,761.98 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $2206.99; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 24 | $25.95 | $2.06 | — | $3,137.12 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $627.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 291 | $2.15 | $3.75 | — | $2,507.71 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $627.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 44 | $13.94 | $2.12 | — | $1,892.23 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $627.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 104 | $6.00 | $2.30 | — | $1,265.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $627.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 3 | $190.30 | $2.00 | — | $693.03 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $627.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 2 | $230.25 | $2.00 | — | $230.53 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $627.00; owner union_news_g_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.53 | ▲ close $12,749.78 vs 09:30 $12,645.09 (session +186.35) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.53 | ▼ 09:30 equity $12,640.08 vs yday $12,749.78 (-109.70) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 44 | $13.13 | $2.14 | $-39.90 | $806.11 | ▼ -39.90 after sell → book $12,637.94; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 104 | $5.99 | $2.33 | $-5.67 | $1,426.74 | ▼ -5.67 after sell → book $12,635.61; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 36 | $9.11 | $2.10 | — | $1,096.69 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $332.91; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 46 | $7.23 | $2.13 | — | $761.98 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $332.91; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 150 | $1.01 | $1.97 | — | $608.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $152.40; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 35 | $4.30 | $1.61 | — | $456.40 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $152.40; owner union_news_g_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.40 | ▼ close $12,593.42 vs 09:30 $12,640.08 (session -34.39) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $456.40 | ▲ 09:30 equity $12,779.76 vs yday $12,593.42 (+186.34) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 130 | $16.92 | $2.42 | $-3.50 | $2,653.58 | ▼ -3.50 after sell → book $12,777.34; vs 09:30 mark -2.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1337 | $1.41 | $17.48 | $-355.61 | $4,521.27 | ▼ -355.61 after sell → book $12,759.86; vs 09:30 mark -17.48 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 189 | $12.80 | $2.61 | $+208.41 | $6,937.86 | ▲ +208.41 after sell → book $12,757.25; vs 09:30 mark -2.61 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 24 | $26.58 | $2.08 | $+10.98 | $7,573.70 | ▲ +10.98 after sell → book $12,755.17; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 291 | $2.09 | $3.81 | $-25.03 | $8,178.08 | ▼ -25.03 after sell → book $12,751.36; vs 09:30 mark -3.81 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 3 | $174.50 | $2.02 | $-51.42 | $8,699.56 | ▼ -51.42 after sell → book $12,749.34; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 2 | $266.50 | $2.02 | $+68.49 | $9,230.54 | ▲ +68.49 after sell → book $12,747.32; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 36 | $8.39 | $2.12 | $-30.14 | $9,530.46 | ▼ -30.14 after sell → book $12,745.20; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 46 | $6.83 | $2.15 | $-22.68 | $9,842.50 | ▼ -22.68 after sell → book $12,743.06; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 150 | $0.95 | $1.91 | $-12.87 | $9,983.09 | ▼ -12.87 after sell → book $12,741.15; vs 09:30 mark -1.91 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 862 | $2.70 | $11.12 | — | $7,644.57 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2329.39; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 55 | $41.76 | $2.15 | — | $5,345.61 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $2329.39; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 518 | $4.49 | $6.68 | — | $3,013.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $2329.39; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 63 | $7.95 | $2.18 | — | $2,510.08 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $502.18; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 31 | $15.72 | $2.08 | — | $2,020.68 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $502.18; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 411 | $1.22 | $5.30 | — | $1,513.95 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $502.18; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 386 | $1.30 | $4.98 | — | $1,007.18 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $502.18; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 12 | $40.00 | $2.03 | — | $525.15 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $502.18; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 2 | $196.78 | $2.00 | — | $129.59 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $502.18; owner union_news_g_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.59 | ▼ close $12,285.67 vs 09:30 $12,779.76 (session -416.95) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.59 | ▼ 09:30 equity $12,193.04 vs yday $12,285.67 (-92.63) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 893 | $2.68 | $11.69 | $+164.32 | $2,511.15 | ▲ +164.32 after sell → book $12,181.36; vs 09:30 mark -11.68 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 35 | $4.12 | $1.57 | $-9.48 | $2,653.78 | ▼ -9.48 after sell → book $12,179.79; vs 09:30 mark -1.57 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 55 | $36.02 | $2.18 | $-319.76 | $4,632.97 | ▼ -319.76 after sell → book $12,177.61; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 518 | $3.92 | $6.78 | $-306.14 | $6,659.34 | ▼ -306.14 after sell → book $12,170.83; vs 09:30 mark -6.78 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 63 | $7.38 | $2.20 | $-40.29 | $7,122.08 | ▼ -40.29 after sell → book $12,168.63; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 31 | $14.38 | $2.10 | $-45.73 | $7,565.76 | ▼ -45.73 after sell → book $12,166.52; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 411 | $1.17 | $5.38 | $-31.23 | $8,041.25 | ▼ -31.23 after sell → book $12,161.14; vs 09:30 mark -5.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 386 | $1.27 | $5.05 | $-21.61 | $8,526.41 | ▼ -21.61 after sell → book $12,156.09; vs 09:30 mark -5.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 12 | $39.27 | $2.05 | $-12.83 | $8,995.61 | ▼ -12.83 after sell → book $12,154.04; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 2 | $192.26 | $2.02 | $-13.05 | $9,378.11 | ▼ -13.05 after sell → book $12,152.03; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,378.11 | ▲ close $13,989.81 vs 09:30 $12,193.04 (session +1,837.78) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,953.68 | ▲ 09:30 equity $11,512.68 vs yday $11,055.43 (+457.25) | 09:30 open · cash $5,953.68 (unchanged overnight, no fees) · equity $11,512.68 vs prior close $11,055.43 (+457.25) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 46 | $29.76 | $2.13 | — | $4,582.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $1389.19; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 85 | $16.21 | $2.25 | — | $3,202.50 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1389.19; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 89 | $15.58 | $2.26 | — | $1,813.52 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $1389.19; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 117 | $3.86 | $2.34 | — | $1,359.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $453.38; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 1 | $272.16 | $1.99 | — | $1,085.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $453.38; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 6 | $74.15 | $2.01 | — | $638.50 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $453.38; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $638.50 | ▼ close $11,117.47 vs 09:30 $11,512.68 (session -382.24) | 16:00 close · cash $638.50 · equity $11,117.47 vs 09:30 $11,512.68 (-395.21; session marks -382.24) · 8 name(s) marked open→close (per-name table). GLND×644 09:30 $6.06 → close $5.54 -334.88; VICR×6 09:30 $276.06 → close $276.06 -0.00; TJGC×46 09:30 $29.76 → close $26.24 -161.92; SECZ×85 09:30 $16.21 → close $15.96 -21.25; USDE×89 09:30 $15.58 → close $17.25 +148.53; ZSQR×117 09:30 $3.86 → close $3.78 -9.36; ILMN×1 09:30 $272.16 → close $270.00 -2.16; RKLB×6 09:30 $74.15 → close $73.95 -1.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 445.99 < 1 share @ 1646.93 |
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
| 2026-08-21 | `DE` | cash | leftover split 264.54 < 1 share @ 623.26 |
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
| 2026-09-03 | `DE` | cash | leftover split 481.15 < 1 share @ 703.25 |
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
| 2026-09-17 | `LITE` | cash | leftover split 339.87 < 1 share @ 934.88 |
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
| 2026-09-22 | `MRNA` | cash | leftover split 152.40 < 1 share @ 168.50 |
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
| `GLND` | 862 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $2329.39; owner union_hot_n4_h1 |
