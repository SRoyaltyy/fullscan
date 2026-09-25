# Factor mine action — `combo_her_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_hot_n4_h1/union_earn_react_h3 w=0.5,0.5 net=priority

Cash book **-1.59%** ($9,841) · signal-only (no cash/fees) was —. Starts YES **29/30**. Fills 273 · skips 269 · realized $+2068.71.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_hot_n4_h1 50%, union_earn_react_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_hot_n4_h1 50%, union_earn_react_h3 50%.
- Member: union_hot_n4_h1 (50% · long · hold 1).
- Member: union_earn_react_h3 (50% · long · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9.70.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_earn_react_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_earn_react_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 36 | $45.98 | $2.10 | — | $3,319.25 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 71 | $23.33 | $2.20 | — | $1,660.62 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $38.59 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; combo leftover $1658.88; owner union_hot_n4_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.59 | ▲ close $10,449.19 vs 09:30 $10,000.00 (session +492.16) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.59 | ▲ 09:30 equity $10,528.70 vs yday $10,449.19 (+79.51) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 36 | $44.09 | $2.12 | $-72.26 | $1,623.71 | ▼ -72.26 after sell → book $10,526.58; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 71 | $22.92 | $2.23 | $-33.54 | $3,248.80 | ▼ -33.54 after sell → book $10,524.35; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,015.97 | ▲ +145.14 after sell → book $10,522.24; vs 09:30 mark -2.11 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 31 | $9.89 | $2.08 | — | $4,707.14 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 56 | $5.51 | $2.16 | — | $4,396.42 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 71 | $4.37 | $2.20 | — | $4,084.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 11 | $26.25 | $2.02 | — | $3,793.37 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 409 | $0.77 | $4.36 | — | $3,475.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 667 | $0.47 | $5.14 | — | $3,157.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 79 | $3.92 | $2.23 | — | $2,845.19 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; combo leftover $313.50; owner union_earn_react_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 28 | $24.68 | $2.07 | — | $2,152.07 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; combo leftover $711.30; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 36 | $19.57 | $2.10 | — | $1,445.45 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $711.30; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 323 | $2.20 | $4.17 | — | $730.69 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; combo leftover $711.30; owner union_hot_n4_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 63 | $11.12 | $2.18 | — | $27.95 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $711.30; owner union_hot_n4_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.95 | ▲ close $10,817.58 vs 09:30 $10,528.70 (session +326.04) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.95 | ▼ 09:30 equity $10,682.14 vs yday $10,817.58 (-135.44) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 28 | $24.83 | $2.09 | $+0.03 | $721.09 | ▲ +0.03 after sell → book $10,680.04; vs 09:30 mark -2.10 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 36 | $19.57 | $2.12 | $-4.22 | $1,423.50 | ▼ -4.22 after sell → book $10,677.92; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 323 | $2.08 | $4.23 | $-45.54 | $2,092.72 | ▼ -45.54 after sell → book $10,673.69; vs 09:30 mark -4.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 63 | $9.57 | $2.20 | $-102.03 | $2,693.43 | ▼ -102.03 after sell → book $10,671.50; vs 09:30 mark -2.19 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 160 | $4.19 | $2.47 | — | $2,020.56 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; combo leftover $673.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 98 | $6.87 | $2.28 | — | $1,345.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; combo leftover $673.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 49 | $13.64 | $2.14 | — | $674.52 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; combo leftover $673.36; owner union_hot_n4_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 16 | $41.23 | $2.04 | — | $12.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; combo leftover $673.36; owner union_hot_n4_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.80 | ▲ close $10,828.68 vs 09:30 $10,682.14 (session +166.11) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.80 | ▼ 09:30 equity $10,715.76 vs yday $10,828.68 (-112.92) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $3,490.49 | ▲ +943.78 after sell → book $10,675.41; vs 09:30 mark -40.35 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $6,066.78 | ▲ +86.83 after sell → book $10,673.04; vs 09:30 mark -2.37 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 160 | $3.94 | $2.51 | $-44.98 | $6,694.68 | ▼ -44.98 after sell → book $10,670.54; vs 09:30 mark -2.50 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 49 | $13.31 | $2.16 | $-20.46 | $7,344.71 | ▼ -20.46 after sell → book $10,668.38; vs 09:30 mark -2.16 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 16 | $41.50 | $2.06 | $+0.22 | $8,006.65 | ▲ +0.22 after sell → book $10,666.32; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,006.65 | ▼ close $10,627.32 vs 09:30 $10,715.76 (session -39.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,006.65 | ▲ 09:30 equity $10,655.55 vs yday $10,627.32 (+28.23) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 31 | $11.50 | $2.10 | $+45.57 | $8,361.05 | ▲ +45.57 after sell → book $10,653.45; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRJ` | 56 | $5.33 | $2.18 | $-14.42 | $8,657.35 | ▼ -14.42 after sell → book $10,651.27; vs 09:30 mark -2.18 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMPG` | 71 | $3.56 | $2.22 | $-61.80 | $8,907.89 | ▼ -61.80 after sell → book $10,649.05; vs 09:30 mark -2.22 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BRUN` | 11 | $20.38 | $2.04 | $-68.64 | $9,129.97 | ▼ -68.64 after sell → book $10,647.00; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 409 | $0.57 | $3.64 | $-88.16 | $9,359.46 | ▼ -88.16 after sell → book $10,643.37; vs 09:30 mark -3.63 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DEFT` | 667 | $0.43 | $5.02 | $-33.50 | $9,644.59 | ▼ -33.50 after sell → book $10,638.35; vs 09:30 mark -5.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DGXX` | 79 | $3.66 | $2.25 | $-25.02 | $9,931.47 | ▼ -25.02 after sell → book $10,636.09; vs 09:30 mark -2.26 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 98 | $7.19 | $2.31 | $+26.77 | $10,633.78 | ▲ +26.77 after sell → book $10,633.78; vs 09:30 mark -2.31 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,633.78 | ▲ close $10,633.78 vs 09:30 $10,655.55 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,633.78 | ▲ 09:30 equity $10,633.78 vs yday $10,633.78 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 14 | $46.85 | $2.03 | — | $9,975.85 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 73 | $9.01 | $2.21 | — | $9,315.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 170 | $3.89 | $2.50 | — | $8,652.11 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 19 | $34.05 | $2.05 | — | $8,003.12 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 29 | $22.44 | $2.08 | — | $7,350.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 5 | $123.47 | $2.00 | — | $6,730.92 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 13 | $49.00 | $2.03 | — | $6,091.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 66 | $9.94 | $2.19 | — | $5,433.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; combo leftover $664.61; owner union_earn_react_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 9 | $150.14 | $2.02 | — | $4,080.39 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $1358.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1181 | $1.15 | $15.23 | — | $2,707.01 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; combo leftover $1358.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 114 | $11.81 | $2.33 | — | $1,357.76 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $1358.42; owner union_hot_n4_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 981 | $1.37 | $12.65 | — | $1.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; combo leftover $1358.42; owner union_hot_n4_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.14 | ▼ close $10,460.30 vs 09:30 $10,633.78 (session -124.16) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.14 | ▲ 09:30 equity $10,630.63 vs yday $10,460.30 (+170.33) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 114 | $11.57 | $2.36 | $-32.62 | $1,317.76 | ▼ -32.62 after sell → book $10,628.27; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 981 | $1.46 | $12.83 | $+62.81 | $2,737.19 | ▲ +62.81 after sell → book $10,615.44; vs 09:30 mark -12.83 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 15 | $17.93 | $2.04 | — | $2,466.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $273.72; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 2 | $93.98 | $1.89 | — | $2,276.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; combo leftover $273.72; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 6 | $43.08 | $2.01 | — | $2,015.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $273.72; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 119 | $2.30 | $2.35 | — | $1,739.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; combo leftover $273.72; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 1 | $243.85 | $1.99 | — | $1,493.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; combo leftover $273.72; owner union_earn_react_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 166 | $4.49 | $2.49 | — | $746.08 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; combo leftover $746.95; owner union_hot_n4_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 109 | $6.81 | $2.32 | — | $1.47 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; combo leftover $746.95; owner union_hot_n4_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.47 | ▲ close $10,748.55 vs 09:30 $10,630.63 (session +148.19) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.47 | ▲ 09:30 equity $11,438.98 vs yday $10,748.55 (+690.43) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 9 | $142.70 | $2.04 | $-71.01 | $1,283.73 | ▼ -71.01 after sell → book $11,436.95; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1181 | $1.83 | $15.45 | $+772.40 | $3,429.51 | ▲ +772.40 after sell → book $11,421.50; vs 09:30 mark -15.45 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 166 | $4.32 | $2.53 | $-33.23 | $4,144.11 | ▼ -33.23 after sell → book $11,418.97; vs 09:30 mark -2.53 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 109 | $8.03 | $2.35 | $+128.32 | $5,017.03 | ▲ +128.32 after sell → book $11,416.63; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,017.03 | ▲ close $11,513.22 vs 09:30 $11,438.98 (session +96.60) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,017.03 | ▼ 09:30 equity $11,501.93 vs yday $11,513.22 (-11.29) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 14 | $43.63 | $2.05 | $-49.16 | $5,625.80 | ▼ -49.16 after sell → book $11,499.88; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 73 | $9.23 | $2.23 | $+11.62 | $6,297.36 | ▲ +11.62 after sell → book $11,497.65; vs 09:30 mark -2.23 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 170 | $5.24 | $2.54 | $+224.46 | $7,185.62 | ▲ +224.46 after sell → book $11,495.11; vs 09:30 mark -2.54 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 19 | $34.72 | $2.07 | $+8.62 | $7,843.23 | ▲ +8.62 after sell → book $11,493.04; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 29 | $21.85 | $2.10 | $-21.28 | $8,474.79 | ▼ -21.28 after sell → book $11,490.94; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 5 | $117.94 | $2.02 | $-31.68 | $9,062.46 | ▼ -31.68 after sell → book $11,488.92; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BILL` | 13 | $47.98 | $2.05 | $-17.27 | $9,684.22 | ▼ -17.27 after sell → book $11,486.87; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 66 | $8.46 | $2.21 | $-102.08 | $10,240.37 | ▼ -102.08 after sell → book $11,484.66; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 3 | $175.01 | $2.00 | — | $9,713.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 7 | $88.94 | $2.01 | — | $9,088.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 41 | $15.28 | $2.11 | — | $8,460.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 4 | $142.36 | $2.00 | — | $7,888.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 125 | $5.10 | $2.37 | — | $7,248.85 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 13 | $47.89 | $2.03 | — | $6,624.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 45 | $13.92 | $2.12 | — | $5,995.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 140 | $4.54 | $2.41 | — | $5,357.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; combo leftover $640.02; owner union_earn_react_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 55 | $24.11 | $2.15 | — | $4,028.81 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; combo leftover $1339.25; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 858 | $1.56 | $11.07 | — | $2,679.26 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; combo leftover $1339.25; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 329 | $4.07 | $4.24 | — | $1,335.99 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $1339.25; owner union_hot_n4_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 70 | $19.04 | $2.20 | — | $0.99 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; combo leftover $1339.25; owner union_hot_n4_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.99 | ▲ close $11,720.62 vs 09:30 $11,501.93 (session +272.68) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.99 | ▼ 09:30 equity $11,462.20 vs yday $11,720.62 (-258.42) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 15 | $18.14 | $2.06 | $-1.01 | $271.03 | ▼ -1.01 after sell → book $11,460.14; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 2 | $94.60 | $1.92 | $-2.56 | $458.32 | ▼ -2.56 after sell → book $11,458.23; vs 09:30 mark -1.91 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 6 | $44.39 | $2.03 | $+3.82 | $722.63 | ▲ +3.82 after sell → book $11,456.20; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 119 | $2.35 | $2.38 | $+1.23 | $999.90 | ▲ +1.23 after sell → book $11,453.82; vs 09:30 mark -2.38 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ROST` | 1 | $242.50 | $2.01 | $-5.36 | $1,240.39 | ▼ -5.36 after sell → book $11,451.81; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 55 | $26.61 | $2.18 | $+133.17 | $2,701.76 | ▲ +133.17 after sell → book $11,449.63; vs 09:30 mark -2.18 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 858 | $1.60 | $11.22 | $+12.03 | $4,063.34 | ▲ +12.03 after sell → book $11,438.41; vs 09:30 mark -11.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 70 | $20.72 | $2.22 | $+113.18 | $5,511.52 | ▲ +113.18 after sell → book $11,436.19; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 66 | $5.21 | $2.19 | — | $5,165.47 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 2 | $131.37 | $2.00 | — | $4,900.73 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.3; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 18 | $18.26 | $2.04 | — | $4,570.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 10 | $34.30 | $2.02 | — | $4,224.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.7; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $3,896.09 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-15.2; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 67 | $5.08 | $2.19 | — | $3,553.53 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 1 | $323.47 | $1.99 | — | $3,228.07 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.0; combo leftover $344.47; owner union_earn_react_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 76 | $14.11 | $2.22 | — | $2,153.49 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; combo leftover $1076.02; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 185 | $5.81 | $2.54 | — | $1,076.10 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $1076.02; owner union_hot_n4_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 92 | $11.59 | $2.27 | — | $8.01 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; combo leftover $1076.02; owner union_hot_n4_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.01 | ▲ close $11,758.86 vs 09:30 $11,462.20 (session +344.13) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.01 | ▲ 09:30 equity $11,924.11 vs yday $11,758.86 (+165.25) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 76 | $14.20 | $2.24 | $+2.38 | $1,084.97 | ▲ +2.38 after sell → book $11,921.87; vs 09:30 mark -2.24 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 185 | $6.50 | $2.59 | $+122.52 | $2,284.89 | ▲ +122.52 after sell → book $11,919.29; vs 09:30 mark -2.58 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 92 | $12.18 | $2.29 | $+50.18 | $3,403.15 | ▲ +50.18 after sell → book $11,916.99; vs 09:30 mark -2.30 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 2 | $80.60 | $1.62 | — | $3,240.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.0; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 13 | $16.18 | $2.03 | — | $3,027.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.7; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 1 | $118.77 | $1.19 | — | $2,908.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.3; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 11 | $17.78 | $1.99 | — | $2,710.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.2; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 15 | $13.41 | $2.04 | — | $2,507.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.1; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 2 | $97.16 | $1.95 | — | $2,310.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 1 | $206.82 | $1.99 | — | $2,102.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.2; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 1 | $120.17 | $1.20 | — | $1,980.80 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $212.70; owner union_earn_react_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 107 | $9.19 | $2.31 | — | $995.16 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; combo leftover $990.40; owner union_hot_n4_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 6 | $144.18 | $2.01 | — | $128.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; combo leftover $990.40; owner union_hot_n4_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.07 | ▼ close $11,861.19 vs 09:30 $11,924.11 (session -37.47) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.07 | ▼ 09:30 equity $11,768.08 vs yday $11,861.19 (-93.11) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 3 | $172.76 | $2.02 | $-10.77 | $644.33 | ▼ -10.77 after sell → book $11,766.06; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 7 | $93.30 | $2.03 | $+26.48 | $1,295.40 | ▲ +26.48 after sell → book $11,764.03; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 41 | $18.15 | $2.13 | $+113.42 | $2,037.41 | ▲ +113.42 after sell → book $11,761.89; vs 09:30 mark -2.14 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 4 | $132.80 | $2.02 | $-42.26 | $2,566.59 | ▼ -42.26 after sell → book $11,759.87; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 125 | $4.58 | $2.40 | $-69.76 | $3,136.70 | ▼ -69.76 after sell → book $11,757.48; vs 09:30 mark -2.39 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 13 | $48.42 | $2.05 | $+2.81 | $3,764.11 | ▲ +2.81 after sell → book $11,755.43; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 45 | $15.66 | $2.15 | $+74.03 | $4,466.66 | ▲ +74.03 after sell → book $11,753.28; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 140 | $3.38 | $2.44 | $-167.95 | $4,937.42 | ▼ -167.95 after sell → book $11,750.84; vs 09:30 mark -2.44 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 329 | $3.69 | $4.31 | $-133.57 | $6,147.12 | ▼ -133.57 after sell → book $11,746.53; vs 09:30 mark -4.31 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $5,883.97 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 25 | $15.01 | $2.06 | — | $5,506.65 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 3 | $103.89 | $2.00 | — | $5,192.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 99 | $3.88 | $2.29 | — | $4,806.58 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 8 | $44.40 | $2.01 | — | $4,449.36 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 15 | $24.69 | $2.04 | — | $4,076.98 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 46 | $8.35 | $2.13 | — | $3,690.75 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 10 | $37.65 | $2.02 | — | $3,312.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; combo leftover $384.20; owner union_earn_react_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 236 | $14.00 | $3.04 | — | $5.24 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; combo leftover $3312.28; owner union_hot_n4_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.24 | ▼ close $11,576.06 vs 09:30 $11,768.08 (session -150.89) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.24 | ▼ 09:30 equity $11,535.03 vs yday $11,576.06 (-41.03) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 66 | $5.00 | $2.21 | $-18.26 | $333.03 | ▼ -18.26 after sell → book $11,532.82; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 2 | $148.03 | $2.02 | $+29.31 | $627.07 | ▲ +29.31 after sell → book $11,530.80; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 18 | $19.25 | $2.06 | $+13.71 | $971.51 | ▲ +13.71 after sell → book $11,528.74; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 10 | $34.72 | $2.04 | $+0.14 | $1,316.67 | ▲ +0.14 after sell → book $11,526.70; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 1 | $298.01 | $2.01 | $-32.91 | $1,612.66 | ▼ -32.91 after sell → book $11,524.68; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSCO` | 67 | $5.20 | $2.21 | $+3.64 | $1,958.85 | ▲ +3.64 after sell → book $11,522.47; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `INTU` | 1 | $356.05 | $2.01 | $+28.57 | $2,312.89 | ▲ +28.57 after sell → book $11,520.46; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 107 | $9.50 | $2.34 | $+28.52 | $3,327.05 | ▲ +28.52 after sell → book $11,518.12; vs 09:30 mark -2.34 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 6 | $134.10 | $2.03 | $-64.52 | $4,129.62 | ▼ -64.52 after sell → book $11,516.09; vs 09:30 mark -2.03 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,129.62 | ▼ close $11,393.78 vs 09:30 $11,535.03 (session -122.31) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,129.62 | ▼ 09:30 equity $11,295.50 vs yday $11,393.78 (-98.28) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 2 | $79.83 | $1.62 | $-4.78 | $4,287.66 | ▼ -4.78 after sell → book $11,293.88; vs 09:30 mark -1.62 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 13 | $15.97 | $2.05 | $-6.81 | $4,493.22 | ▼ -6.81 after sell → book $11,291.83; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 1 | $113.66 | $1.16 | $-7.46 | $4,605.72 | ▼ -7.46 after sell → book $11,290.67; vs 09:30 mark -1.16 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 11 | $18.28 | $2.04 | $+1.47 | $4,804.76 | ▲ +1.47 after sell → book $11,288.63; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 15 | $12.18 | $1.89 | $-22.38 | $4,985.57 | ▼ -22.38 after sell → book $11,286.74; vs 09:30 mark -1.89 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 2 | $96.65 | $1.96 | $-4.93 | $5,176.91 | ▼ -4.93 after sell → book $11,284.78; vs 09:30 mark -1.96 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 1 | $203.78 | $2.01 | $-7.05 | $5,378.67 | ▼ -7.05 after sell → book $11,282.76; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 1 | $120.54 | $1.23 | $-2.06 | $5,497.98 | ▼ -2.06 after sell → book $11,281.53; vs 09:30 mark -1.23 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 236 | $13.04 | $3.11 | $-232.71 | $8,572.32 | ▼ -232.71 after sell → book $11,278.43; vs 09:30 mark -3.10 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,572.32 | ▼ close $11,251.51 vs 09:30 $11,295.50 (session -26.92) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,572.32 | ▼ 09:30 equity $11,234.49 vs yday $11,251.51 (-17.02) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $8,817.00 | ▼ -18.47 after sell → book $11,232.47; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 25 | $15.01 | $2.08 | $-4.15 | $9,190.17 | ▼ -4.15 after sell → book $11,230.39; vs 09:30 mark -2.08 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 3 | $92.00 | $2.02 | $-39.69 | $9,464.15 | ▼ -39.69 after sell → book $11,228.37; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 99 | $3.32 | $2.31 | $-60.04 | $9,790.52 | ▼ -60.04 after sell → book $11,226.06; vs 09:30 mark -2.31 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 8 | $44.17 | $2.03 | $-5.89 | $10,141.84 | ▼ -5.89 after sell → book $11,224.02; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 15 | $21.97 | $2.06 | $-44.89 | $10,469.34 | ▼ -44.89 after sell → book $11,221.97; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 46 | $8.58 | $2.15 | $+6.30 | $10,861.87 | ▲ +6.30 after sell → book $11,219.82; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 10 | $35.80 | $2.04 | $-22.56 | $11,217.78 | ▼ -22.56 after sell → book $11,217.78; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,217.78 | ▲ close $11,217.78 vs 09:30 $11,234.49 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,217.78 | ▲ 09:30 equity $11,217.78 vs yday $11,217.78 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 65 | $10.74 | $2.19 | — | $10,517.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,163.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 101 | $6.90 | $2.29 | — | $9,464.24 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,107.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 31 | $22.32 | $2.08 | — | $8,413.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,897.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 14 | $47.60 | $2.03 | — | $7,229.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 46 | $15.09 | $2.13 | — | $6,533.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; combo leftover $701.11; owner union_earn_react_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 917 | $1.78 | $11.83 | — | $4,888.97 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; combo leftover $1633.27; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 88 | $18.40 | $2.25 | — | $3,267.52 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; combo leftover $1633.27; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 119 | $13.71 | $2.35 | — | $1,633.68 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; combo leftover $1633.27; owner union_hot_n4_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 68 | $23.88 | $2.19 | — | $7.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1633.27; owner union_hot_n4_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.65 | ▼ close $11,102.59 vs 09:30 $11,217.78 (session -79.86) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.65 | ▲ 09:30 equity $11,186.24 vs yday $11,102.59 (+83.65) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 88 | $18.15 | $2.28 | $-26.54 | $1,602.57 | ▼ -26.54 after sell → book $11,183.96; vs 09:30 mark -2.28 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 119 | $13.89 | $2.38 | $+16.69 | $3,253.10 | ▲ +16.69 after sell → book $11,181.58; vs 09:30 mark -2.38 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 68 | $23.84 | $2.22 | $-7.13 | $4,872.00 | ▼ -7.13 after sell → book $11,179.36; vs 09:30 mark -2.22 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 4 | $63.18 | $2.00 | — | $4,617.28 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 34 | $8.74 | $2.09 | — | $4,318.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 4 | $68.52 | $2.00 | — | $4,041.94 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.4; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 84 | $3.62 | $2.24 | — | $3,736.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 1 | $167.55 | $1.68 | — | $3,566.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 6 | $44.90 | $2.01 | — | $3,295.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 3 | $98.15 | $2.00 | — | $2,998.95 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.9; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 19 | $15.70 | $2.05 | — | $2,698.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; combo leftover $304.50; owner union_earn_react_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 35 | $25.18 | $2.10 | — | $1,815.21 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $899.54; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 114 | $7.87 | $2.33 | — | $915.70 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; combo leftover $899.54; owner union_hot_n4_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 155 | $5.79 | $2.46 | — | $15.79 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $899.54; owner union_hot_n4_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.79 | ▲ close $11,457.22 vs 09:30 $11,186.24 (session +300.82) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.79 | ▼ 09:30 equity $11,269.42 vs yday $11,457.22 (-187.80) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 917 | $1.56 | $11.99 | $-220.98 | $1,438.91 | ▼ -220.98 after sell → book $11,257.43; vs 09:30 mark -11.99 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 35 | $26.44 | $2.12 | $+39.89 | $2,362.19 | ▲ +39.89 after sell → book $11,255.31; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 114 | $7.76 | $2.36 | $-17.23 | $3,244.47 | ▼ -17.23 after sell → book $11,252.95; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 155 | $5.81 | $2.49 | $-1.85 | $4,142.53 | ▼ -1.85 after sell → book $11,250.46; vs 09:30 mark -2.49 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,142.53 | ▼ close $11,225.28 vs 09:30 $11,269.42 (session -25.18) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,142.53 | ▼ 09:30 equity $11,204.49 vs yday $11,225.28 (-20.79) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 65 | $10.51 | $2.21 | $-19.67 | $4,823.47 | ▼ -19.67 after sell → book $11,202.28; vs 09:30 mark -2.21 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $5,187.69 | ▲ +10.48 after sell → book $11,200.27; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 101 | $9.39 | $2.32 | $+246.88 | $6,133.76 | ▲ +246.88 after sell → book $11,197.95; vs 09:30 mark -2.32 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $6,473.65 | ▼ -16.60 after sell → book $11,195.94; vs 09:30 mark -2.01 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 31 | $21.67 | $2.10 | $-24.34 | $7,143.32 | ▼ -24.34 after sell → book $11,193.84; vs 09:30 mark -2.10 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 2 | $252.92 | $2.02 | $-12.17 | $7,647.14 | ▼ -12.17 after sell → book $11,191.82; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 14 | $56.94 | $2.05 | $+126.68 | $8,442.25 | ▲ +126.68 after sell → book $11,189.77; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 46 | $13.84 | $2.15 | $-61.78 | $9,076.74 | ▼ -61.78 after sell → book $11,187.62; vs 09:30 mark -2.15 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,076.74 | ▼ close $11,170.52 vs 09:30 $11,204.49 (session -17.10) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,076.74 | ▼ 09:30 equity $11,159.43 vs yday $11,170.52 (-11.09) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 4 | $67.44 | $2.02 | $+13.02 | $9,344.48 | ▲ +13.02 after sell → book $11,157.41; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 34 | $8.26 | $2.11 | $-20.52 | $9,623.21 | ▼ -20.52 after sell → book $11,155.30; vs 09:30 mark -2.11 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 4 | $64.60 | $2.02 | $-19.70 | $9,879.58 | ▼ -19.70 after sell → book $11,153.27; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 84 | $3.76 | $2.27 | $+7.67 | $10,193.16 | ▲ +7.67 after sell → book $11,151.01; vs 09:30 mark -2.26 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 1 | $142.43 | $1.45 | $-28.25 | $10,334.14 | ▼ -28.25 after sell → book $11,149.56; vs 09:30 mark -1.45 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 6 | $38.23 | $2.03 | $-44.09 | $10,561.46 | ▼ -44.09 after sell → book $11,147.53; vs 09:30 mark -2.03 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 3 | $98.71 | $2.02 | $-2.34 | $10,855.57 | ▼ -2.34 after sell → book $11,145.51; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 19 | $15.26 | $2.07 | $-12.47 | $11,143.45 | ▼ -12.47 after sell → book $11,143.45; vs 09:30 mark -2.06 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,143.45 | ▲ close $11,143.45 vs 09:30 $11,159.43 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,143.45 | ▲ 09:30 equity $11,143.45 vs yday $11,143.45 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $10,483.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $9,997.39 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.1; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 21 | $32.01 | $2.05 | — | $9,323.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.4; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 9 | $71.71 | $2.02 | — | $8,675.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.1; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 12 | $56.02 | $2.03 | — | $8,001.45 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.2; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 74 | $9.37 | $2.21 | — | $7,305.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.5; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 53 | $13.10 | $2.15 | — | $6,609.41 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.9; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 5 | $135.71 | $2.00 | — | $5,928.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-9.2; combo leftover $696.47; owner union_earn_react_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 548 | $2.70 | $7.07 | — | $4,442.19 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1482.21; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 301 | $4.91 | $3.88 | — | $2,960.39 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1482.21; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 240 | $6.16 | $3.10 | — | $1,478.90 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1482.21; owner union_hot_n4_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 470 | $3.13 | $6.06 | — | $1.74 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; combo leftover $1482.21; owner union_hot_n4_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.74 | ▲ close $11,276.96 vs 09:30 $11,143.45 (session +170.08) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.74 | ▲ 09:30 equity $11,349.82 vs yday $11,276.96 (+72.86) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 240 | $6.02 | $3.15 | $-39.84 | $1,443.39 | ▼ -39.84 after sell → book $11,346.67; vs 09:30 mark -3.15 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,443.39 | ▲ close $11,802.99 vs 09:30 $11,349.82 (session +456.32) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,443.39 | ▲ 09:30 equity $11,881.91 vs yday $11,802.99 (+78.92) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 301 | $5.11 | $3.95 | $+52.37 | $2,977.55 | ▲ +52.37 after sell → book $11,877.96; vs 09:30 mark -3.95 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 470 | $3.64 | $6.15 | $+227.48 | $4,682.20 | ▲ +227.48 after sell → book $11,871.81; vs 09:30 mark -6.15 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,682.20 | ▲ close $12,025.01 vs 09:30 $11,881.91 (session +153.20) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,682.20 | ▼ 09:30 equity $12,000.99 vs yday $12,025.01 (-24.02) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 4 | $140.03 | $2.02 | $-101.62 | $5,240.30 | ▼ -101.62 after sell → book $11,998.97; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 2 | $253.34 | $2.02 | $+18.33 | $5,744.96 | ▲ +18.33 after sell → book $11,996.95; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 21 | $30.57 | $2.07 | $-34.37 | $6,384.86 | ▼ -34.37 after sell → book $11,994.88; vs 09:30 mark -2.07 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 9 | $78.12 | $2.04 | $+53.64 | $7,085.90 | ▲ +53.64 after sell → book $11,992.84; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 12 | $61.93 | $2.05 | $+66.85 | $7,827.01 | ▲ +66.85 after sell → book $11,990.79; vs 09:30 mark -2.05 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 74 | $9.40 | $2.23 | $-2.23 | $8,520.38 | ▼ -2.23 after sell → book $11,988.56; vs 09:30 mark -2.23 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 53 | $15.75 | $2.17 | $+136.13 | $9,352.96 | ▲ +136.13 after sell → book $11,986.39; vs 09:30 mark -2.17 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 5 | $125.55 | $2.02 | $-54.83 | $9,978.69 | ▼ -54.83 after sell → book $11,984.37; vs 09:30 mark -2.02 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 121 | $40.93 | $2.35 | — | $5,023.80 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $4989.34; owner union_earn_react_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 930 | $1.80 | $12.00 | — | $3,337.81 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; combo leftover $1674.60; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 71 | $23.29 | $2.20 | — | $1,682.01 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1674.60; owner union_hot_n4_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 114 | $14.62 | $2.33 | — | $13.00 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; combo leftover $1674.60; owner union_hot_n4_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.00 | ▼ close $11,917.30 vs 09:30 $12,000.99 (session -48.18) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.00 | ▲ 09:30 equity $11,990.16 vs yday $11,917.30 (+72.86) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 71 | $24.09 | $2.23 | $+52.37 | $1,721.16 | ▲ +52.37 after sell → book $11,987.93; vs 09:30 mark -2.23 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 114 | $13.77 | $2.36 | $-101.60 | $3,288.58 | ▼ -101.60 after sell → book $11,985.57; vs 09:30 mark -2.36 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 73 | $11.21 | $2.21 | — | $2,468.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.0; combo leftover $822.14; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 10 | $81.00 | $2.02 | — | $1,656.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.0; combo leftover $822.14; owner union_earn_react_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 36 | $22.46 | $2.10 | — | $845.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; combo leftover $828.01; owner union_hot_n4_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 22 | $36.76 | $2.06 | — | $34.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; combo leftover $828.01; owner union_hot_n4_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.59 | ▲ close $12,198.97 vs 09:30 $11,990.16 (session +221.78) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.59 | ▼ 09:30 equity $12,149.02 vs yday $12,198.97 (-49.95) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 930 | $1.96 | $12.17 | $+124.64 | $1,845.22 | ▲ +124.64 after sell → book $12,136.85; vs 09:30 mark -12.17 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 36 | $21.30 | $2.12 | $-45.98 | $2,609.90 | ▼ -45.98 after sell → book $12,134.73; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 22 | $39.50 | $2.08 | $+56.15 | $3,476.83 | ▲ +56.15 after sell → book $12,132.66; vs 09:30 mark -2.07 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 39 | $29.32 | $2.11 | — | $2,331.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1158.94; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 381 | $3.04 | $4.91 | — | $1,169.99 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1158.94; owner union_hot_n4_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 14 | $81.40 | $2.03 | — | $28.36 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; combo leftover $1158.94; owner union_hot_n4_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.36 | ▲ close $12,182.40 vs 09:30 $12,149.02 (session +58.79) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.36 | ▲ 09:30 equity $12,441.57 vs yday $12,182.40 (+259.17) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 548 | $3.55 | $7.18 | $+451.56 | $1,966.58 | ▲ +451.56 after sell → book $12,434.40; vs 09:30 mark -7.17 | union_hot_n4_h1: dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 121 | $41.00 | $2.41 | $+3.70 | $6,925.17 | ▲ +3.70 after sell → book $12,431.98; vs 09:30 mark -2.42 | union_earn_react_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 39 | $29.43 | $2.13 | $+0.06 | $8,070.81 | ▲ +0.06 after sell → book $12,429.86; vs 09:30 mark -2.12 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 381 | $4.00 | $4.99 | $+357.76 | $9,589.82 | ▲ +357.76 after sell → book $12,424.87; vs 09:30 mark -4.99 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 14 | $79.08 | $2.05 | $-36.56 | $10,694.89 | ▼ -36.56 after sell → book $12,422.81; vs 09:30 mark -2.06 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1082 | $2.47 | $13.96 | — | $8,008.39 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; combo leftover $2673.72; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 158 | $16.91 | $2.46 | — | $5,334.15 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $2673.72; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1620 | $1.65 | $20.90 | — | $2,640.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; combo leftover $2673.72; owner union_hot_n4_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 225 | $11.67 | $2.90 | — | $11.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; combo leftover $2673.72; owner union_hot_n4_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.60 | ▲ close $12,763.03 vs 09:30 $12,441.57 (session +380.43) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.60 | ▼ 09:30 equity $12,641.53 vs yday $12,763.03 (-121.50) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.60 | ▲ close $12,650.53 vs 09:30 $12,641.53 (session +9.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.60 | ▲ 09:30 equity $12,837.46 vs yday $12,650.53 (+186.93) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 73 | $13.82 | $2.23 | $+186.09 | $1,018.23 | ▲ +186.09 after sell → book $12,835.23; vs 09:30 mark -2.23 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 10 | $82.00 | $2.04 | $+5.94 | $1,836.19 | ▲ +5.94 after sell → book $12,833.19; vs 09:30 mark -2.04 | union_earn_react_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 158 | $16.92 | $2.51 | $-3.40 | $4,507.03 | ▼ -3.40 after sell → book $12,830.67; vs 09:30 mark -2.52 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1620 | $1.41 | $21.19 | $-430.88 | $6,770.05 | ▼ -430.88 after sell → book $12,809.49; vs 09:30 mark -21.18 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 225 | $12.80 | $2.96 | $+248.38 | $9,647.09 | ▲ +248.38 after sell → book $12,806.53; vs 09:30 mark -2.96 | union_hot_n4_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 20 | $47.57 | $2.05 | — | $8,693.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $964.71; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $7,904.51 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $964.71; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 26 | $35.74 | $2.07 | — | $6,973.21 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $964.71; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 20 | $47.15 | $2.05 | — | $6,028.16 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $964.71; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 8 | $109.67 | $2.01 | — | $5,148.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $964.71; owner union_earn_react_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 635 | $2.70 | $8.19 | — | $3,426.09 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1716.26; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 41 | $41.76 | $2.11 | — | $1,711.82 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1716.26; owner union_hot_n4_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 380 | $4.49 | $4.90 | — | $0.72 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; combo leftover $1716.26; owner union_hot_n4_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.72 | ▼ close $12,372.62 vs 09:30 $12,837.46 (session -408.52) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.72 | ▼ 09:30 equity $12,340.77 vs yday $12,372.62 (-31.85) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1082 | $2.68 | $14.16 | $+199.10 | $2,886.31 | ▲ +199.10 after sell → book $12,326.61; vs 09:30 mark -14.16 | union_hot_n4_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 41 | $36.02 | $2.13 | $-239.38 | $4,361.20 | ▼ -239.38 after sell → book $12,324.47; vs 09:30 mark -2.14 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 380 | $3.92 | $4.98 | $-224.58 | $5,847.73 | ▼ -224.58 after sell → book $12,319.49; vs 09:30 mark -4.98 | union_hot_n4_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,847.73 | ▲ close $13,743.80 vs 09:30 $12,340.77 (session +1,424.30) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,368.02 | ▲ 09:30 equity $9,997.81 vs yday $9,810.55 (+187.26) | 09:30 open · cash $1,368.02 (unchanged overnight, no fees) · equity $9,997.81 vs prior close $9,810.55 (+187.26) | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 15 | $29.76 | $2.04 | — | $919.58 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; combo leftover $456.01; owner union_hot_n4_h1 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 28 | $16.21 | $2.07 | — | $463.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $456.01; owner union_hot_n4_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 29 | $15.58 | $2.08 | — | $9.70 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; combo leftover $456.01; owner union_hot_n4_h1 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.70 | ▼ close $9,841.30 vs 09:30 $9,997.81 (session -150.32) | 16:00 close · cash $9.70 · equity $9,841.30 vs 09:30 $9,997.81 (-156.51; session marks -150.32) · 14 name(s) marked open→close (per-name table). ABVX×11 09:30 $94.87 → close $94.87 +0.00; ANAB×20 09:30 $51.70 → close $51.70 +0.00; CBRL×10 09:30 $52.39 → close $51.81 -5.80; CTAS×2 09:30 $197.68 → close $197.68 -0.00; GIS×14 09:30 $34.83 → close $34.83 +0.00; GLND×256 09:30 $6.06 → close $5.54 -133.12; KBH×10 09:30 $47.65 → close $47.65 +0.00; MLKN×55 09:30 $19.91 → close $19.91 -0.00; PAYX×4 09:30 $101.59 → close $101.59 -0.00; THO×15 09:30 $70.93 → close $70.93 +0.00; VICR×2 09:30 $276.06 → close $276.06 -0.00; TJGC×15 09:30 $29.76 → close $26.24 -52.80; SECZ×28 09:30 $16.21 → close $15.96 -7.00; USDE×29 09:30 $15.58 → close $17.25 +48.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `AMAT` | cash | leftover split 313.50 < 1 share @ 499.40 |
| 2026-08-17 | `INO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `NMAX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BRUN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `NMAX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AIRJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `AMPG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BRUN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `BZAI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DEFT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `DGXX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new long union_hot_n4_h1 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new long union_hot_n4_h1 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new long union_earn_react_h3 |
| 2026-08-21 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BABA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BILL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BULL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new long union_hot_n4_h1 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_earn_react_h3 |
| 2026-08-25 | `BEKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ROST` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `HEI` | cash | leftover split 344.47 < 1 share @ 370.00 |
| 2026-08-27 | `BMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `DY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TIGR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `FSCO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `INTU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new long union_hot_n4_h1 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_earn_react_h3 |
| 2026-09-01 | `ADSK` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_hot_n4_h1 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new long union_hot_n4_h1 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_earn_react_h3 |
| 2026-09-04 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `AI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GWRE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new long union_hot_n4_h1 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AMBA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ASAN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOCU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `GWRE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `IOT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LULU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `MAMA` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new long union_hot_n4_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_hot_n4_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `M` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NAVN` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `NB` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-10 | `SHOE` | hard_red | hard-red S=-13.28 sit; no new long union_earn_react_h3 |
| 2026-09-14 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `ORCL` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RH` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_hot_n4_h1 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_earn_react_h3 |
| 2026-09-17 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `TCOM` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | union_earn_react_h3: dropped but min-hold 2/3 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.87 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 3.87 < 1 share @ 7.23 |
| 2026-09-24 | `CBRL` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_earn_react_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_hot_n4_h1 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_earn_react_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CBRL` | 20 | 2026-09-23 @ $47.57 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-11.2; combo leftover $964.71; owner union_earn_react_h3 |
| `CTAS` | 4 | 2026-09-23 @ $196.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $964.71; owner union_earn_react_h3 |
| `GIS` | 26 | 2026-09-23 @ $35.74 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.1; combo leftover $964.71; owner union_earn_react_h3 |
| `KBH` | 20 | 2026-09-23 @ $47.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-1.9; combo leftover $964.71; owner union_earn_react_h3 |
| `PAYX` | 8 | 2026-09-23 @ $109.67 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $964.71; owner union_earn_react_h3 |
| `GLND` | 635 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; combo leftover $1716.26; owner union_hot_n4_h1 |
