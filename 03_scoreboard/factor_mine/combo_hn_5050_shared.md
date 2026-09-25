# Factor mine action — `combo_hn_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_news_g_h1 w=0.5,0.5 net=priority

Cash book **+2.49%** ($10,249) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 301 · skips 124 · realized $+1265.29.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_news_g_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_news_g_h1 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: union_news_g_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $908.68.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $4,470.29 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $741.71; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $3,733.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $741.71; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 6 | $120.00 | $2.01 | — | $3,011.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $741.71; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 172 | $4.31 | $2.51 | — | $2,267.96 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $741.71; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 54 | $13.55 | $2.15 | — | $1,534.10 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $741.71; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 56 | $13.18 | $2.16 | — | $793.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $741.71; owner union_news_g_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $793.87 | ▼ close $10,276.58 vs 09:30 $10,412.10 (session -63.25) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $793.87 | ▼ 09:30 equity $10,238.72 vs yday $10,276.58 (-37.86) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $2,082.86 | ▲ +3.49 after sell → book $10,236.55; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 66 | $19.57 | $2.21 | $-4.40 | $3,372.27 | ▼ -4.40 after sell → book $10,234.35; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 589 | $2.08 | $7.71 | $-83.04 | $4,592.63 | ▼ -83.04 after sell → book $10,226.64; vs 09:30 mark -7.71 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 116 | $9.57 | $2.37 | $-184.51 | $5,700.38 | ▼ -184.51 after sell → book $10,224.27; vs 09:30 mark -2.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $6,434.13 | ▲ +12.09 after sell → book $10,222.26; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $7,178.95 | ▲ +8.32 after sell → book $10,220.23; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 6 | $127.40 | $2.03 | $+40.36 | $7,941.32 | ▲ +40.36 after sell → book $10,218.20; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 172 | $4.60 | $2.54 | $+44.83 | $8,729.98 | ▲ +44.83 after sell → book $10,215.66; vs 09:30 mark -2.54 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 54 | $13.16 | $2.17 | $-25.38 | $9,438.45 | ▼ -25.38 after sell → book $10,213.49; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 56 | $13.84 | $2.18 | $+32.62 | $10,211.31 | ▲ +32.62 after sell → book $10,211.31; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 304 | $4.19 | $3.92 | — | $8,933.63 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $1276.41; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 185 | $6.87 | $2.54 | — | $7,660.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $1276.41; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 93 | $13.64 | $2.27 | — | $6,389.34 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $1276.41; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,150.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $1276.41; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 22 | $46.18 | $2.06 | — | $4,132.35 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $1030.07; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 7 | $142.77 | $2.01 | — | $3,130.95 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $1030.07; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 5 | $202.70 | $2.00 | — | $2,115.44 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $1030.07; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 11 | $92.99 | $2.02 | — | $1,090.53 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $1030.07; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 21 | $49.00 | $2.05 | — | $59.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $1030.07; owner union_news_g_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.47 | ▲ close $10,251.00 vs 09:30 $10,238.72 (session +60.66) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.47 | ▼ 09:30 equity $10,227.56 vs yday $10,251.00 (-23.44) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 304 | $3.94 | $3.98 | $-83.90 | $1,253.25 | ▼ -83.90 after sell → book $10,223.58; vs 09:30 mark -3.98 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 93 | $13.31 | $2.29 | $-35.25 | $2,488.79 | ▼ -35.25 after sell → book $10,221.29; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,731.69 | ▲ +3.92 after sell → book $10,219.19; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 22 | $48.00 | $2.08 | $+35.91 | $4,785.61 | ▲ +35.91 after sell → book $10,217.11; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 7 | $148.04 | $2.03 | $+32.85 | $5,819.86 | ▲ +32.85 after sell → book $10,215.08; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 5 | $208.93 | $2.02 | $+27.12 | $6,862.49 | ▲ +27.12 after sell → book $10,213.06; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 11 | $92.38 | $2.04 | $-10.78 | $7,876.62 | ▼ -10.78 after sell → book $10,211.01; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 21 | $45.09 | $2.07 | $-86.24 | $8,821.44 | ▼ -86.24 after sell → book $10,208.94; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,821.44 | ▼ close $10,131.24 vs 09:30 $10,227.56 (session -77.70) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,821.44 | ▲ 09:30 equity $10,151.59 vs yday $10,131.24 (+20.35) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 185 | $7.19 | $2.59 | $+54.07 | $10,149.00 | ▲ +54.07 after sell → book $10,149.00; vs 09:30 mark -2.59 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,149.00 | ▲ close $10,149.00 vs 09:30 $10,151.59 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,149.00 | ▲ 09:30 equity $10,149.00 vs yday $10,149.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $8,945.87 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1268.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1103 | $1.15 | $14.23 | — | $7,663.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1268.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 107 | $11.81 | $2.31 | — | $6,396.68 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1268.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 926 | $1.37 | $11.95 | — | $5,116.11 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1268.63; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 8 | $91.01 | $2.01 | — | $4,386.02 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1033 | $0.71 | $10.40 | — | $3,645.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 110 | $6.61 | $2.32 | — | $2,916.41 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 45 | $16.00 | $2.12 | — | $2,194.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 27 | $26.57 | $2.07 | — | $1,474.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 12 | $58.73 | $2.03 | — | $768.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 16 | $44.76 | $2.04 | — | $49.84 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $730.87; owner union_news_g_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.84 | ▼ close $10,014.99 vs 09:30 $10,149.00 (session -80.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.84 | ▲ 09:30 equity $10,319.54 vs yday $10,014.99 (+304.55) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 107 | $11.57 | $2.34 | $-30.86 | $1,285.49 | ▼ -30.86 after sell → book $10,317.21; vs 09:30 mark -2.33 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 926 | $1.46 | $12.11 | $+59.28 | $2,625.34 | ▲ +59.28 after sell → book $10,305.10; vs 09:30 mark -12.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 8 | $95.72 | $2.03 | $+33.63 | $3,389.07 | ▲ +33.63 after sell → book $10,303.06; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1033 | $0.67 | $10.24 | $-54.73 | $4,075.07 | ▼ -54.73 after sell → book $10,292.82; vs 09:30 mark -10.24 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 110 | $6.95 | $2.35 | $+33.28 | $4,837.22 | ▲ +33.28 after sell → book $10,290.47; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 45 | $17.66 | $2.15 | $+70.43 | $5,629.78 | ▲ +70.43 after sell → book $10,288.33; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 27 | $26.25 | $2.09 | $-12.80 | $6,336.43 | ▼ -12.80 after sell → book $10,286.23; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 16 | $44.52 | $2.06 | $-7.94 | $7,046.70 | ▼ -7.94 after sell → book $10,284.18; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 392 | $4.49 | $5.06 | — | $5,281.56 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $1761.67; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 258 | $6.81 | $3.33 | — | $3,521.25 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $1761.67; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 4 | $119.43 | $2.00 | — | $3,041.53 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $503.04; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 203 | $2.47 | $2.62 | — | $2,537.50 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $503.04; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 4 | $115.18 | $2.00 | — | $2,074.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $503.04; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 42 | $11.70 | $2.12 | — | $1,581.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $503.04; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 45 | $11.10 | $2.12 | — | $1,079.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $503.04; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 155 | $3.24 | $2.46 | — | $575.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $503.04; owner union_news_g_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $575.21 | ▲ close $10,290.94 vs 09:30 $10,319.54 (session +28.46) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $575.21 | ▲ 09:30 equity $11,107.91 vs yday $10,290.94 (+816.97) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 8 | $142.70 | $2.03 | $-63.57 | $1,714.77 | ▼ -63.57 after sell → book $11,105.87; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1103 | $1.83 | $14.43 | $+721.38 | $3,718.84 | ▲ +721.38 after sell → book $11,091.45; vs 09:30 mark -14.42 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 392 | $4.32 | $5.14 | $-76.83 | $5,407.14 | ▼ -76.83 after sell → book $11,086.31; vs 09:30 mark -5.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 258 | $8.03 | $3.39 | $+308.04 | $7,475.49 | ▲ +308.04 after sell → book $11,082.92; vs 09:30 mark -3.39 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 4 | $120.51 | $2.02 | $+0.30 | $7,955.51 | ▲ +0.30 after sell → book $11,080.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 203 | $2.40 | $2.66 | $-19.49 | $8,440.05 | ▼ -19.49 after sell → book $11,078.24; vs 09:30 mark -2.66 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 4 | $121.00 | $2.02 | $+19.26 | $8,922.03 | ▲ +19.26 after sell → book $11,076.22; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 42 | $11.17 | $2.14 | $-26.51 | $9,389.03 | ▼ -26.51 after sell → book $11,074.08; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 45 | $11.48 | $2.15 | $+13.06 | $9,903.49 | ▲ +13.06 after sell → book $11,071.94; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 155 | $2.99 | $2.49 | $-43.70 | $10,364.44 | ▼ -43.70 after sell → book $11,069.44; vs 09:30 mark -2.50 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,364.44 | ▼ close $11,049.34 vs 09:30 $11,107.91 (session -20.10) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,364.44 | ▲ 09:30 equity $11,059.60 vs yday $11,049.34 (+10.26) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 12 | $57.93 | $2.05 | $-13.67 | $11,057.56 | ▼ -13.67 after sell → book $11,057.56; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.11 | $2.16 | — | $9,681.13 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1382.19; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 886 | $1.56 | $11.43 | — | $8,287.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1382.19; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 339 | $4.07 | $4.37 | — | $6,903.44 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1382.19; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 72 | $19.04 | $2.21 | — | $5,530.35 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1382.19; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 97 | $9.42 | $2.28 | — | $4,614.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $921.72; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 26 | $35.05 | $2.07 | — | $3,700.96 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $921.72; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 31 | $28.86 | $2.08 | — | $2,804.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $921.72; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 105 | $8.72 | $2.31 | — | $1,886.31 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $921.72; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 7 | $118.52 | $2.01 | — | $1,054.66 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $921.72; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 11 | $77.13 | $2.02 | — | $204.21 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $921.72; owner union_news_g_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.21 | ▲ close $11,625.59 vs 09:30 $11,059.60 (session +600.97) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.21 | ▼ 09:30 equity $11,323.00 vs yday $11,625.59 (-302.59) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 57 | $26.61 | $2.18 | $+138.16 | $1,718.80 | ▲ +138.16 after sell → book $11,320.82; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 886 | $1.60 | $11.59 | $+12.42 | $3,124.81 | ▲ +12.42 after sell → book $11,309.23; vs 09:30 mark -11.59 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 72 | $20.72 | $2.23 | $+116.52 | $4,614.42 | ▲ +116.52 after sell → book $11,307.00; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 97 | $10.07 | $2.31 | $+58.46 | $5,588.90 | ▲ +58.46 after sell → book $11,304.69; vs 09:30 mark -2.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 26 | $35.70 | $2.09 | $+12.74 | $6,515.01 | ▲ +12.74 after sell → book $11,302.60; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 31 | $27.56 | $2.10 | $-44.49 | $7,367.27 | ▼ -44.49 after sell → book $11,300.50; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 105 | $8.86 | $2.33 | $+10.06 | $8,295.24 | ▲ +10.06 after sell → book $11,298.17; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 7 | $119.80 | $2.03 | $+4.92 | $9,131.81 | ▲ +4.92 after sell → book $11,296.14; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 11 | $79.34 | $2.04 | $+20.24 | $10,002.50 | ▲ +20.24 after sell → book $11,294.09; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 118 | $14.11 | $2.34 | — | $8,335.18 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1667.08; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 286 | $5.81 | $3.69 | — | $6,669.83 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1667.08; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 143 | $11.59 | $2.42 | — | $5,010.76 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1667.08; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 75 | $11.12 | $2.21 | — | $4,174.54 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $835.13; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 100 | $8.29 | $2.29 | — | $3,343.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $835.13; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 47 | $17.41 | $2.13 | — | $2,522.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $835.13; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 74 | $11.22 | $2.21 | — | $1,690.36 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $835.13; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 3 | $267.02 | $2.00 | — | $887.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $835.13; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 7 | $118.50 | $2.01 | — | $55.79 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $835.13; owner union_news_g_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.79 | ▲ close $11,547.93 vs 09:30 $11,323.00 (session +275.14) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.79 | ▲ 09:30 equity $11,793.87 vs yday $11,547.93 (+245.94) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 118 | $14.20 | $2.38 | $+5.90 | $1,729.01 | ▲ +5.90 after sell → book $11,791.49; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 286 | $6.50 | $3.75 | $+189.90 | $3,584.26 | ▲ +189.90 after sell → book $11,787.74; vs 09:30 mark -3.75 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 143 | $12.18 | $2.46 | $+80.21 | $5,323.54 | ▲ +80.21 after sell → book $11,785.28; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 3 | $267.23 | $2.02 | $-3.39 | $6,123.21 | ▼ -3.39 after sell → book $11,783.26; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 10 | $144.18 | $2.02 | — | $4,679.39 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $1530.80; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 82 | $18.50 | $2.24 | — | $3,160.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; combo leftover $1530.80; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 25 | $41.44 | $2.06 | — | $2,122.09 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $1053.39; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 14 | $70.30 | $2.03 | — | $1,135.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1053.39; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 17 | $60.00 | $2.04 | — | $113.82 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $1053.39; owner union_news_g_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.82 | ▼ close $11,606.27 vs 09:30 $11,793.87 (session -166.60) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.82 | ▼ 09:30 equity $11,455.01 vs yday $11,606.27 (-151.26) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 339 | $3.69 | $4.44 | $-137.63 | $1,360.29 | ▼ -137.63 after sell → book $11,450.57; vs 09:30 mark -4.44 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 75 | $11.27 | $2.24 | $+6.80 | $2,203.30 | ▲ +6.80 after sell → book $11,448.33; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 47 | $17.70 | $2.15 | $+9.35 | $3,033.05 | ▲ +9.35 after sell → book $11,446.18; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 74 | $11.00 | $2.23 | $-20.73 | $3,844.82 | ▼ -20.73 after sell → book $11,443.95; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 7 | $115.66 | $2.03 | $-23.92 | $4,652.41 | ▼ -23.92 after sell → book $11,441.92; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 82 | $18.15 | $2.26 | $-33.20 | $6,138.44 | ▼ -33.20 after sell → book $11,439.65; vs 09:30 mark -2.27 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 14 | $65.29 | $2.05 | $-74.22 | $7,050.45 | ▼ -74.22 after sell → book $11,437.60; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 17 | $58.75 | $2.06 | $-25.35 | $8,047.14 | ▼ -25.35 after sell → book $11,435.54; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 143 | $14.00 | $2.42 | — | $6,042.72 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $2011.79; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,141.78 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $2011.79; owner union_hot_n4_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 20 | $32.90 | $2.05 | — | $3,481.73 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $690.30; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 80 | $8.61 | $2.23 | — | $2,790.70 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $690.30; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 4 | $141.76 | $2.00 | — | $2,221.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $690.30; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 35 | $19.25 | $2.10 | — | $1,545.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $690.30; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 36 | $18.75 | $2.10 | — | $868.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $690.30; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 23 | $28.91 | $2.06 | — | $201.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $690.30; owner union_news_g_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.73 | ▼ close $11,296.23 vs 09:30 $11,455.01 (session -122.33) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.73 | ▼ 09:30 equity $11,239.58 vs yday $11,296.23 (-56.65) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 100 | $9.50 | $2.32 | $+116.39 | $1,149.41 | ▲ +116.39 after sell → book $11,237.26; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 10 | $134.10 | $2.04 | $-104.86 | $2,488.37 | ▼ -104.86 after sell → book $11,235.22; vs 09:30 mark -2.04 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 25 | $42.00 | $2.08 | $+9.85 | $3,536.29 | ▲ +9.85 after sell → book $11,233.14; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.03 | $2.05 | $+21.40 | $5,458.62 | ▲ +21.40 after sell → book $11,231.08; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 20 | $31.15 | $2.07 | $-39.12 | $6,079.55 | ▼ -39.12 after sell → book $11,229.01; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 80 | $8.52 | $2.25 | $-11.68 | $6,758.90 | ▼ -11.68 after sell → book $11,226.76; vs 09:30 mark -2.25 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 4 | $132.30 | $2.02 | $-41.86 | $7,286.08 | ▼ -41.86 after sell → book $11,224.74; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 35 | $17.87 | $2.12 | $-52.51 | $7,909.41 | ▼ -52.51 after sell → book $11,222.62; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 36 | $19.25 | $2.12 | $+13.78 | $8,600.29 | ▲ +13.78 after sell → book $11,220.50; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 23 | $28.06 | $2.08 | $-23.69 | $9,243.60 | ▼ -23.69 after sell → book $11,218.43; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,243.60 | ▼ close $11,145.50 vs 09:30 $11,239.58 (session -72.93) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,243.60 | ▼ 09:30 equity $11,108.32 vs yday $11,145.50 (-37.18) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 143 | $13.04 | $2.46 | $-142.16 | $11,105.86 | ▼ -142.16 after sell → book $11,105.86; vs 09:30 mark -2.46 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,105.86 | ▲ close $11,105.86 vs 09:30 $11,108.32 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,105.86 | ▲ 09:30 equity $11,105.86 vs yday $11,105.86 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,105.86 | ▲ close $11,105.86 vs 09:30 $11,105.86 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,105.86 | ▲ 09:30 equity $11,105.86 vs yday $11,105.86 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 779 | $1.78 | $10.05 | — | $9,709.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1388.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 75 | $18.40 | $2.21 | — | $8,326.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1388.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 101 | $13.71 | $2.29 | — | $6,939.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1388.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $23.88 | $2.16 | — | $5,552.77 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1388.23; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 24 | $32.88 | $2.06 | — | $4,761.58 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 104 | $7.59 | $2.30 | — | $3,969.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,264.68 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 49 | $15.87 | $2.14 | — | $2,484.91 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $1,779.44 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $1,068.46 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 16 | $47.60 | $2.04 | — | $304.82 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $793.25; owner union_news_g_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.82 | ▼ close $10,886.42 vs 09:30 $11,105.86 (session -188.19) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $304.82 | ▲ 09:30 equity $10,914.25 vs yday $10,886.42 (+27.83) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 75 | $18.15 | $2.24 | $-23.20 | $1,663.83 | ▼ -23.20 after sell → book $10,912.01; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 101 | $13.89 | $2.32 | $+13.57 | $3,064.40 | ▲ +13.57 after sell → book $10,909.69; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.84 | $2.19 | $-6.67 | $4,444.94 | ▼ -6.67 after sell → book $10,907.51; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 24 | $32.48 | $2.08 | $-13.74 | $5,222.38 | ▼ -13.74 after sell → book $10,905.43; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 104 | $7.79 | $2.33 | $+16.17 | $6,030.21 | ▲ +16.17 after sell → book $10,903.10; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,720.22 | ▼ -15.23 after sell → book $10,901.08; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $7,437.61 | ▲ +11.91 after sell → book $10,899.07; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 2 | $321.67 | $2.02 | $-69.65 | $8,078.93 | ▼ -69.65 after sell → book $10,897.05; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 16 | $53.85 | $2.06 | $+95.90 | $8,938.47 | ▲ +95.90 after sell → book $10,894.99; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 59 | $25.18 | $2.17 | — | $7,450.69 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1489.75; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 189 | $7.87 | $2.56 | — | $5,960.70 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $1489.75; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 257 | $5.79 | $3.32 | — | $4,469.35 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1489.75; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 3 | $263.36 | $2.00 | — | $3,677.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $893.87; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 460 | $1.94 | $5.93 | — | $2,778.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $893.87; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 6 | $137.35 | $2.01 | — | $1,952.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $893.87; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 3 | $236.82 | $2.00 | — | $1,240.37 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $893.87; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 11 | $75.65 | $2.02 | — | $406.20 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $893.87; owner union_news_g_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.20 | ▲ close $11,261.78 vs 09:30 $10,914.25 (session +388.79) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.20 | ▼ 09:30 equity $11,115.17 vs yday $11,261.78 (-146.61) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 779 | $1.56 | $10.19 | $-187.72 | $1,615.15 | ▼ -187.72 after sell → book $11,104.98; vs 09:30 mark -10.19 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 49 | $16.74 | $2.16 | $+38.34 | $2,433.25 | ▲ +38.34 after sell → book $11,102.82; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 59 | $26.44 | $2.19 | $+69.98 | $3,991.02 | ▲ +69.98 after sell → book $11,100.63; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 189 | $7.76 | $2.60 | $-25.95 | $5,455.06 | ▼ -25.95 after sell → book $11,098.03; vs 09:30 mark -2.60 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 257 | $5.81 | $3.37 | $-1.55 | $6,944.86 | ▼ -1.55 after sell → book $11,094.66; vs 09:30 mark -3.37 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 3 | $253.72 | $2.02 | $-32.94 | $7,704.00 | ▼ -32.94 after sell → book $11,092.64; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 460 | $1.94 | $6.02 | $-11.95 | $8,590.38 | ▼ -11.95 after sell → book $11,086.62; vs 09:30 mark -6.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 3 | $267.76 | $2.02 | $+88.80 | $9,391.64 | ▲ +88.80 after sell → book $11,084.60; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,391.64 | ▼ close $11,054.57 vs 09:30 $11,115.17 (session -30.03) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,391.64 | ▲ 09:30 equity $11,085.16 vs yday $11,054.57 (+30.59) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 6 | $141.82 | $2.03 | $+22.78 | $10,240.54 | ▲ +22.78 after sell → book $11,083.14; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 11 | $76.60 | $2.04 | $+6.38 | $11,081.09 | ▲ +6.38 after sell → book $11,081.09; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,081.09 | ▲ close $11,081.09 vs 09:30 $11,085.16 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,081.09 | ▲ 09:30 equity $11,081.09 vs yday $11,081.09 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,081.09 | ▲ close $11,081.09 vs 09:30 $11,081.09 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,081.09 | ▲ 09:30 equity $11,081.09 vs yday $11,081.09 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 513 | $2.70 | $6.62 | — | $9,689.38 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1385.14; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 282 | $4.91 | $3.64 | — | $8,301.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1385.14; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 224 | $6.16 | $2.89 | — | $6,918.39 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1385.14; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 442 | $3.13 | $5.70 | — | $5,529.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1385.14; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 5 | $164.43 | $2.00 | — | $4,705.07 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $921.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 451 | $2.04 | $5.82 | — | $3,779.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $921.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 434 | $2.12 | $5.60 | — | $2,853.53 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $921.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 61 | $15.01 | $2.17 | — | $1,935.75 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $921.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $1,207.24 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $921.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 6 | $135.71 | $2.01 | — | $390.97 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $921.54; owner union_news_g_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $390.97 | ▲ close $11,092.90 vs 09:30 $11,081.09 (session +50.26) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $390.97 | ▲ 09:30 equity $11,129.04 vs yday $11,092.90 (+36.14) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 224 | $6.02 | $2.94 | $-37.19 | $1,736.52 | ▼ -37.19 after sell → book $11,126.11; vs 09:30 mark -2.93 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 5 | $141.42 | $2.02 | $-119.08 | $2,441.59 | ▼ -119.08 after sell → book $11,124.08; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 451 | $2.01 | $5.90 | $-25.25 | $3,342.20 | ▼ -25.25 after sell → book $11,118.18; vs 09:30 mark -5.90 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 434 | $2.05 | $5.68 | $-41.66 | $4,226.22 | ▼ -41.66 after sell → book $11,112.50; vs 09:30 mark -5.68 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 3 | $261.51 | $2.02 | $+54.00 | $5,008.73 | ▲ +54.00 after sell → book $11,110.48; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 6 | $131.40 | $2.03 | $-29.90 | $5,795.10 | ▼ -29.90 after sell → book $11,108.45; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,795.10 | ▲ close $11,425.70 vs 09:30 $11,129.04 (session +317.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,795.10 | ▲ 09:30 equity $11,517.62 vs yday $11,425.70 (+91.92) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 282 | $5.11 | $3.70 | $+49.07 | $7,232.43 | ▲ +49.07 after sell → book $11,513.93; vs 09:30 mark -3.69 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 442 | $3.64 | $5.79 | $+213.93 | $8,835.52 | ▲ +213.93 after sell → book $11,508.14; vs 09:30 mark -5.79 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,835.52 | ▲ close $11,644.68 vs 09:30 $11,517.62 (session +136.54) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,835.52 | ▲ 09:30 equity $11,660.43 vs yday $11,644.68 (+15.75) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 818 | $1.80 | $10.55 | — | $7,352.57 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1472.59; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 63 | $23.29 | $2.18 | — | $5,883.12 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1472.59; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 100 | $14.62 | $2.29 | — | $4,418.83 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1472.59; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 42 | $26.27 | $2.12 | — | $3,313.37 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $1104.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 158 | $6.95 | $2.46 | — | $2,212.81 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $1104.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 27 | $39.99 | $2.07 | — | $1,131.01 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $1104.71; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 5 | $189.17 | $2.00 | — | $183.15 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $1104.71; owner union_news_g_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.15 | ▼ close $11,595.56 vs 09:30 $11,660.43 (session -41.19) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.15 | ▲ 09:30 equity $11,681.15 vs yday $11,595.56 (+85.59) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 63 | $24.09 | $2.20 | $+46.02 | $1,698.62 | ▲ +46.02 after sell → book $11,678.95; vs 09:30 mark -2.20 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 100 | $13.77 | $2.32 | $-89.61 | $3,073.30 | ▼ -89.61 after sell → book $11,676.63; vs 09:30 mark -2.32 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 42 | $26.51 | $2.14 | $+5.83 | $4,184.58 | ▲ +5.83 after sell → book $11,674.49; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 158 | $7.27 | $2.50 | $+45.60 | $5,330.74 | ▲ +45.60 after sell → book $11,671.99; vs 09:30 mark -2.50 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 27 | $37.57 | $2.09 | $-69.50 | $6,343.04 | ▼ -69.50 after sell → book $11,669.90; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 5 | $190.35 | $2.02 | $+1.87 | $7,292.77 | ▲ +1.87 after sell → book $11,667.88; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 81 | $22.46 | $2.23 | — | $5,471.28 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $1823.19; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 49 | $36.76 | $2.14 | — | $3,667.90 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $1823.19; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $3,153.35 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $611.32; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 34 | $17.72 | $2.09 | — | $2,548.78 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $611.32; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 345 | $1.77 | $4.45 | — | $1,933.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $611.32; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 2 | $238.60 | $2.00 | — | $1,454.48 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $611.32; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 27 | $22.12 | $2.07 | — | $855.17 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $611.32; owner union_news_g_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $855.17 | ▲ close $11,918.18 vs 09:30 $11,681.15 (session +267.28) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $855.17 | ▼ 09:30 equity $11,894.33 vs yday $11,918.18 (-23.85) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 61 | $15.87 | $2.19 | $+48.09 | $1,821.05 | ▲ +48.09 after sell → book $11,892.14; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 818 | $1.96 | $10.70 | $+109.63 | $3,413.63 | ▲ +109.63 after sell → book $11,881.44; vs 09:30 mark -10.70 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 81 | $21.30 | $2.26 | $-98.45 | $5,136.67 | ▼ -98.45 after sell → book $11,879.18; vs 09:30 mark -2.26 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 49 | $39.50 | $2.16 | $+129.96 | $7,070.00 | ▲ +129.96 after sell → book $11,877.01; vs 09:30 mark -2.17 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 3 | $182.33 | $2.02 | $+30.42 | $7,614.97 | ▲ +30.42 after sell → book $11,874.99; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 34 | $17.13 | $2.11 | $-24.26 | $8,195.28 | ▼ -24.26 after sell → book $11,872.88; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 345 | $1.77 | $4.52 | $-8.97 | $8,801.41 | ▼ -8.97 after sell → book $11,868.36; vs 09:30 mark -4.52 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 2 | $236.80 | $2.02 | $-7.61 | $9,273.00 | ▼ -7.61 after sell → book $11,866.35; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 52 | $29.32 | $2.15 | — | $7,746.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1545.50; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 509 | $3.04 | $6.57 | — | $6,194.83 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1545.50; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 18 | $81.40 | $2.04 | — | $4,727.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1545.50; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 84 | $14.07 | $2.24 | — | $3,543.47 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $1181.90; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 79 | $14.79 | $2.23 | — | $2,372.83 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $1181.90; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 156 | $7.54 | $2.46 | — | $1,194.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $1181.90; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 56 | $20.91 | $2.16 | — | $21.79 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $1181.90; owner union_news_g_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.79 | ▼ close $11,815.71 vs 09:30 $11,894.33 (session -30.80) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.79 | ▲ 09:30 equity $12,127.78 vs yday $11,815.71 (+312.07) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 513 | $3.55 | $6.72 | $+422.71 | $1,836.23 | ▲ +422.71 after sell → book $12,121.07; vs 09:30 mark -6.71 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 27 | $22.78 | $2.09 | $+13.66 | $2,449.19 | ▲ +13.66 after sell → book $12,118.97; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 52 | $29.43 | $2.17 | $+1.41 | $3,977.39 | ▲ +1.41 after sell → book $12,116.81; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 509 | $4.00 | $6.67 | $+477.95 | $6,006.72 | ▲ +477.95 after sell → book $12,110.14; vs 09:30 mark -6.67 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 18 | $79.08 | $2.07 | $-45.87 | $7,428.09 | ▼ -45.87 after sell → book $12,108.07; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 84 | $13.90 | $2.27 | $-18.79 | $8,593.43 | ▼ -18.79 after sell → book $12,105.81; vs 09:30 mark -2.26 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 79 | $14.58 | $2.25 | $-21.07 | $9,743.00 | ▼ -21.07 after sell → book $12,103.56; vs 09:30 mark -2.25 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 156 | $7.36 | $2.49 | $-32.25 | $10,888.66 | ▼ -32.25 after sell → book $12,101.06; vs 09:30 mark -2.50 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 56 | $21.65 | $2.18 | $+37.10 | $12,098.89 | ▲ +37.10 after sell → book $12,098.89; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 612 | $2.47 | $7.89 | — | $10,579.35 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $1512.36; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 89 | $16.91 | $2.26 | — | $9,072.10 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $1512.36; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 916 | $1.65 | $11.82 | — | $7,548.89 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $1512.36; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 129 | $11.67 | $2.38 | — | $6,041.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $1512.36; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 38 | $25.95 | $2.10 | — | $5,052.88 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $1006.85; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 468 | $2.15 | $6.04 | — | $4,040.64 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $1006.85; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 72 | $13.94 | $2.21 | — | $3,034.75 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $1006.85; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 167 | $6.00 | $2.49 | — | $2,030.26 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $1006.85; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $1,076.76 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $1006.85; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $153.76 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $1006.85; owner union_news_g_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.76 | ▲ close $12,088.51 vs 09:30 $12,127.78 (session +30.81) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.76 | ▼ 09:30 equity $12,006.38 vs yday $12,088.51 (-82.13) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 72 | $13.13 | $2.23 | $-62.75 | $1,096.89 | ▼ -62.75 after sell → book $12,004.15; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 167 | $5.99 | $2.53 | $-6.69 | $2,094.69 | ▼ -6.69 after sell → book $12,001.62; vs 09:30 mark -2.53 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 38 | $9.11 | $2.10 | — | $1,746.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; combo leftover $349.11; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 48 | $7.23 | $2.13 | — | $1,397.23 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; combo leftover $349.11; owner union_hot_n4_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 276 | $1.01 | $3.56 | — | $1,114.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $279.45; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 1 | $168.50 | $1.69 | — | $944.72 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $279.45; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 64 | $4.30 | $2.18 | — | $667.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $279.45; owner union_news_g_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $667.34 | ▼ close $11,957.45 vs 09:30 $12,006.38 (session -32.50) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $667.34 | ▲ 09:30 equity $12,197.15 vs yday $11,957.45 (+239.70) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 89 | $16.92 | $2.28 | $-3.65 | $2,170.94 | ▼ -3.65 after sell → book $12,194.86; vs 09:30 mark -2.29 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 916 | $1.41 | $11.98 | $-243.64 | $3,450.52 | ▼ -243.64 after sell → book $12,182.88; vs 09:30 mark -11.98 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 129 | $12.80 | $2.41 | $+140.98 | $5,099.31 | ▲ +140.98 after sell → book $12,180.47; vs 09:30 mark -2.41 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 38 | $26.58 | $2.12 | $+19.71 | $6,107.22 | ▲ +19.71 after sell → book $12,178.35; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 468 | $2.09 | $6.12 | $-40.24 | $7,079.22 | ▼ -40.24 after sell → book $12,172.22; vs 09:30 mark -6.13 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 5 | $174.50 | $2.02 | $-83.03 | $7,949.69 | ▼ -83.03 after sell → book $12,170.20; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 4 | $266.50 | $2.02 | $+140.98 | $9,013.67 | ▲ +140.98 after sell → book $12,168.18; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 38 | $8.39 | $2.12 | $-31.59 | $9,330.37 | ▼ -31.59 after sell → book $12,166.05; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 48 | $6.83 | $2.15 | $-23.49 | $9,656.05 | ▼ -23.49 after sell → book $12,163.90; vs 09:30 mark -2.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 276 | $0.95 | $3.51 | $-23.63 | $9,914.75 | ▼ -23.63 after sell → book $12,160.39; vs 09:30 mark -3.51 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 1 | $183.41 | $1.86 | $+11.36 | $10,096.29 | ▲ +11.36 after sell → book $12,158.53; vs 09:30 mark -1.86 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 623 | $2.70 | $8.04 | — | $8,406.16 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1682.72; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 40 | $41.76 | $2.11 | — | $6,733.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1682.72; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 374 | $4.49 | $4.82 | — | $5,049.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1682.72; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 105 | $7.95 | $2.31 | — | $4,212.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $841.59; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 53 | $15.72 | $2.15 | — | $3,377.20 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $841.59; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 689 | $1.22 | $8.89 | — | $2,527.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $841.59; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 647 | $1.30 | $8.35 | — | $1,678.28 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $841.59; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 21 | $40.00 | $2.05 | — | $836.23 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $841.59; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $47.11 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $841.59; owner union_news_g_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.11 | ▼ close $11,722.85 vs 09:30 $12,197.15 (session -394.97) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.11 | ▼ 09:30 equity $11,623.28 vs yday $11,722.85 (-99.57) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 612 | $2.68 | $8.01 | $+112.62 | $1,679.26 | ▲ +112.62 after sell → book $11,615.27; vs 09:30 mark -8.01 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 64 | $4.12 | $2.20 | $-15.90 | $1,940.74 | ▼ -15.90 after sell → book $11,613.07; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 40 | $36.02 | $2.13 | $-233.64 | $3,379.61 | ▼ -233.64 after sell → book $11,610.94; vs 09:30 mark -2.13 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 374 | $3.92 | $4.90 | $-221.03 | $4,842.66 | ▼ -221.03 after sell → book $11,606.04; vs 09:30 mark -4.90 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 105 | $7.38 | $2.33 | $-64.49 | $5,615.23 | ▼ -64.49 after sell → book $11,603.71; vs 09:30 mark -2.33 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 53 | $14.38 | $2.17 | $-75.34 | $6,375.20 | ▼ -75.34 after sell → book $11,601.54; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 689 | $1.17 | $9.01 | $-52.35 | $7,172.31 | ▼ -52.35 after sell → book $11,592.53; vs 09:30 mark -9.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 647 | $1.27 | $8.46 | $-36.22 | $7,985.54 | ▼ -36.22 after sell → book $11,584.06; vs 09:30 mark -8.47 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 21 | $39.27 | $2.07 | $-19.46 | $8,808.14 | ▼ -19.46 after sell → book $11,581.99; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 4 | $192.26 | $2.02 | $-22.10 | $9,575.16 | ▼ -22.10 after sell → book $11,579.97; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,575.16 | ▲ close $12,908.21 vs 09:30 $11,623.28 (session +1,328.24) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,740.95 | ▲ 09:30 equity $10,547.95 vs yday $10,231.29 (+316.66) | 09:30 open · cash $6,740.95 (unchanged overnight, no fees) · equity $10,547.95 vs prior close $10,231.29 (+316.66) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 37 | $29.76 | $2.10 | — | $5,637.73 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $1123.49; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 69 | $16.21 | $2.20 | — | $4,517.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1123.49; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 72 | $15.58 | $2.21 | — | $3,393.00 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $1123.49; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 219 | $3.86 | $2.83 | — | $2,544.83 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $848.25; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 3 | $272.16 | $2.00 | — | $1,726.35 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $848.25; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 11 | $74.15 | $2.02 | — | $908.68 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $848.25; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $908.68 | ▼ close $10,249.15 vs 09:30 $10,547.95 (session -285.45) | 16:00 close · cash $908.68 · equity $10,249.15 vs 09:30 $10,547.95 (-298.80; session marks -285.45) · 8 name(s) marked open→close (per-name table). GLND×446 09:30 $6.06 → close $5.54 -231.92; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×37 09:30 $29.76 → close $26.24 -130.24; SECZ×69 09:30 $16.21 → close $15.96 -17.25; USDE×72 09:30 $15.58 → close $17.25 +120.16; ZSQR×219 09:30 $3.86 → close $3.78 -17.52; ILMN×3 09:30 $272.16 → close $270.00 -6.48; RKLB×11 09:30 $74.15 → close $73.95 -2.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 741.71 < 1 share @ 1646.93 |
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
| 2026-08-21 | `DE` | cash | leftover split 503.04 < 1 share @ 623.26 |
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
| 2026-09-17 | `LITE` | cash | leftover split 611.32 < 1 share @ 934.88 |
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
| `GLND` | 623 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1682.72; owner union_hot_n4_h1 |
