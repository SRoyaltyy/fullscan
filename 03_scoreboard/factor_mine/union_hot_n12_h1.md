# Factor mine action — `union_hot_n12_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 12 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 12 by hot

Cash book **+4.82%** ($10,482) · signal-only (no cash/fees) was +34.72%. Starts YES **29/30**. Fills 373 · skips 145 · realized $-34.73.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 12 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 12.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 12.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,766.70.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $8,894.42 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $7,795.78 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,730.64 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $5,604.91 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $4,502.43 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $3,400.36 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $2,297.72 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $1,219.27 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $123.82 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.82 | ▲ close $10,195.74 vs 09:30 $10,000.00 (session +227.81) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 24 | $44.09 | $2.08 | $-49.50 | $1,179.89 | ▼ -49.50 after sell → book $10,217.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $2,254.98 | ▼ -23.55 after sell → book $10,215.39; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,414.00 | ▲ +93.88 after sell → book $10,213.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1371 | $0.93 | $17.10 | $+132.20 | $4,671.93 | ▲ +132.20 after sell → book $10,196.22; vs 09:30 mark -17.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 37 | $29.15 | $2.12 | $-26.05 | $5,748.36 | ▼ -26.05 after sell → book $10,194.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $6,911.66 | ▲ +61.23 after sell → book $10,191.80; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 50 | $23.33 | $2.16 | $+61.70 | $8,076.00 | ▲ +61.70 after sell → book $10,189.64; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 18 | $59.65 | $2.06 | $-6.81 | $9,147.64 | ▼ -6.81 after sell → book $10,187.58; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 22 | $47.27 | $2.08 | $-57.59 | $10,185.50 | ▼ -57.59 after sell → book $10,185.50; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 34 | $24.68 | $2.09 | — | $9,344.29 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 43 | $19.57 | $2.12 | — | $8,500.66 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 385 | $2.20 | $4.97 | — | $7,648.69 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 76 | $11.12 | $2.22 | — | $6,801.35 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 24 | $35.04 | $2.06 | — | $5,958.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1108 | $0.77 | $11.81 | — | $5,097.79 | — | top 12 by hot; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 19 | $44.49 | $2.05 | — | $4,250.44 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 44 | $19.17 | $2.12 | — | $3,404.83 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 17 | $48.82 | $2.04 | — | $2,572.85 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 32 | $26.25 | $2.09 | — | $1,730.93 | — | top 12 by hot; rank hot_score; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 33 | $25.21 | $2.09 | — | $896.91 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 6 | $129.48 | $2.01 | — | $118.02 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+14.3; leftover $848.79 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.02 | ▼ close $9,681.67 vs 09:30 $10,219.63 (session -466.16) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.02 | ▼ 09:30 equity $9,611.47 vs yday $9,681.67 (-70.20) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 34 | $24.83 | $2.11 | $+0.90 | $960.13 | ▲ +0.90 after sell → book $9,609.36; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 43 | $19.57 | $2.14 | $-4.26 | $1,799.50 | ▼ -4.26 after sell → book $9,607.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 385 | $2.08 | $5.04 | $-54.28 | $2,597.18 | ▼ -54.28 after sell → book $9,602.18; vs 09:30 mark -5.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 76 | $9.57 | $2.24 | $-122.26 | $3,322.26 | ▼ -122.26 after sell → book $9,599.94; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 24 | $34.03 | $2.08 | $-28.38 | $4,136.90 | ▼ -28.38 after sell → book $9,597.86; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1108 | $0.55 | $9.63 | $-258.56 | $4,738.88 | ▼ -258.56 after sell → book $9,588.22; vs 09:30 mark -9.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 19 | $42.12 | $2.07 | $-49.14 | $5,537.10 | ▼ -49.14 after sell → book $9,586.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 44 | $20.25 | $2.14 | $+43.26 | $6,425.95 | ▲ +43.26 after sell → book $9,584.01; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 17 | $47.39 | $2.06 | $-28.41 | $7,229.52 | ▼ -28.41 after sell → book $9,581.95; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 32 | $23.00 | $2.11 | $-108.03 | $7,963.42 | ▼ -108.03 after sell → book $9,579.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 33 | $24.61 | $2.11 | $-24.00 | $8,773.44 | ▼ -24.00 after sell → book $9,577.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 6 | $134.05 | $2.03 | $+23.38 | $9,575.71 | ▲ +23.38 after sell → book $9,575.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 190 | $4.19 | $2.56 | — | $8,777.05 | — | top 12 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 116 | $6.87 | $2.34 | — | $7,977.79 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 58 | $13.64 | $2.16 | — | $7,184.51 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 19 | $41.23 | $2.05 | — | $6,399.09 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 24 | $32.55 | $2.06 | — | $5,615.83 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 79 | $10.10 | $2.23 | — | $4,815.70 | — | top 12 by hot; rank hot_score; list mover_buy; ret5=+22.8; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 54 | $14.66 | $2.15 | — | $4,021.91 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 415 | $1.92 | $5.35 | — | $3,219.76 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 72 | $10.97 | $2.21 | — | $2,427.71 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 53 | $14.94 | $2.15 | — | $1,633.74 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `INDI` | 171 | $4.65 | $2.50 | — | $836.09 | — | top 12 by hot; rank hot_score; list ohlc_hot; ⚪; ret5=+16.6; leftover $797.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 146 | $5.43 | $2.43 | — | $40.88 | — | top 12 by hot; rank hot_score; list yday_gainer; ⚪; ret5=+28.8; leftover $797.98 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.88 | ▼ close $9,344.54 vs 09:30 $9,611.47 (session -200.98) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.88 | ▼ 09:30 equity $9,134.74 vs yday $9,344.54 (-209.80) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 190 | $3.94 | $2.60 | $-52.66 | $786.88 | ▼ -52.66 after sell → book $9,132.14; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 58 | $13.31 | $2.18 | $-23.49 | $1,556.68 | ▼ -23.49 after sell → book $9,129.96; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 19 | $41.50 | $2.07 | $+1.02 | $2,343.11 | ▲ +1.02 after sell → book $9,127.89; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 24 | $28.59 | $2.08 | $-99.18 | $3,027.19 | ▼ -99.18 after sell → book $9,125.81; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 79 | $10.45 | $2.25 | $+23.17 | $3,850.49 | ▲ +23.17 after sell → book $9,123.56; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 54 | $13.19 | $2.17 | $-83.70 | $4,560.57 | ▼ -83.70 after sell → book $9,121.38; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 415 | $1.70 | $5.43 | $-102.09 | $5,260.64 | ▼ -102.09 after sell → book $9,115.95; vs 09:30 mark -5.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 72 | $10.31 | $2.23 | $-51.95 | $6,000.73 | ▼ -51.95 after sell → book $9,113.72; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 53 | $14.01 | $2.17 | $-53.61 | $6,741.09 | ▼ -53.61 after sell → book $9,111.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INDI` | 171 | $4.48 | $2.54 | $-34.11 | $7,504.63 | ▼ -34.11 after sell → book $9,109.01; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 146 | $5.03 | $2.46 | $-63.29 | $8,236.55 | ▼ -63.29 after sell → book $9,106.55; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,236.55 | ▼ close $9,057.83 vs 09:30 $9,134.74 (session -48.72) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,236.55 | ▲ 09:30 equity $9,070.59 vs yday $9,057.83 (+12.76) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 116 | $7.19 | $2.37 | $+32.41 | $9,068.22 | ▲ +32.41 after sell → book $9,068.22; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,068.22 | ▲ close $9,068.22 vs 09:30 $9,070.59 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,068.22 | ▲ 09:30 equity $9,068.22 vs yday $9,068.22 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $8,315.52 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 657 | $1.15 | $8.48 | — | $7,551.49 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 63 | $11.81 | $2.18 | — | $6,804.97 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 551 | $1.37 | $7.11 | — | $6,042.99 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 84 | $8.91 | $2.24 | — | $5,292.31 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 314 | $2.40 | $4.05 | — | $4,534.66 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 114 | $6.61 | $2.33 | — | $3,779.36 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 305 | $2.47 | $3.93 | — | $3,022.07 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 6 | $109.06 | $2.01 | — | $2,365.70 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 4 | $173.90 | $2.00 | — | $1,668.10 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.2; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 37 | $20.00 | $2.10 | — | $926.00 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $755.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `EMBC` | 142 | $5.32 | $2.42 | — | $168.15 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+14.1; leftover $755.69 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.15 | ▼ close $8,954.63 vs 09:30 $9,068.22 (session -72.74) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.15 | ▲ 09:30 equity $9,127.00 vs yday $8,954.63 (+172.37) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 63 | $11.57 | $2.20 | $-19.81 | $894.86 | ▼ -19.81 after sell → book $9,124.80; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 551 | $1.46 | $7.21 | $+35.27 | $1,692.11 | ▲ +35.27 after sell → book $9,117.59; vs 09:30 mark -7.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 84 | $9.24 | $2.27 | $+23.21 | $2,466.00 | ▲ +23.21 after sell → book $9,115.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 314 | $2.28 | $4.11 | $-45.84 | $3,177.81 | ▼ -45.84 after sell → book $9,111.21; vs 09:30 mark -4.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 114 | $6.95 | $2.36 | $+34.64 | $3,967.75 | ▲ +34.64 after sell → book $9,108.85; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 305 | $2.47 | $4.00 | $-7.93 | $4,717.10 | ▼ -7.93 after sell → book $9,104.85; vs 09:30 mark -4.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 6 | $110.92 | $2.03 | $+7.12 | $5,380.59 | ▲ +7.12 after sell → book $9,102.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 4 | $174.22 | $2.02 | $-2.74 | $6,075.45 | ▼ -2.74 after sell → book $9,100.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BBNX` | 37 | $19.50 | $2.12 | $-22.72 | $6,794.83 | ▼ -22.72 after sell → book $9,098.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EMBC` | 142 | $5.43 | $2.45 | $+10.75 | $7,563.44 | ▲ +10.75 after sell → book $9,096.23; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 168 | $4.49 | $2.49 | — | $6,806.63 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 111 | $6.81 | $2.32 | — | $6,048.39 | — | top 12 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 67 | $11.13 | $2.19 | — | $5,300.49 | — | top 12 by hot; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 83 | $9.08 | $2.24 | — | $4,544.61 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 91 | $8.28 | $2.26 | — | $3,788.87 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 2572 | $0.29 | $15.28 | — | $3,017.43 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 11 | $65.60 | $2.02 | — | $2,293.80 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 614 | $1.23 | $7.92 | — | $1,530.66 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 187 | $4.04 | $2.55 | — | $772.63 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $756.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 117 | $6.42 | $2.34 | — | $19.15 | — | top 12 by hot; rank hot_score; list yday_gainer; ret5=+23.8; leftover $756.34 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.15 | ▲ close $9,425.55 vs 09:30 $9,127.00 (session +370.94) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.15 | ▲ 09:30 equity $9,939.44 vs yday $9,425.55 (+513.89) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $730.63 | ▼ -41.23 after sell → book $9,937.41; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 657 | $1.83 | $8.59 | $+429.69 | $1,924.34 | ▲ +429.69 after sell → book $9,928.82; vs 09:30 mark -8.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 168 | $4.32 | $2.53 | $-33.59 | $2,647.57 | ▼ -33.59 after sell → book $9,926.28; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 111 | $8.03 | $2.35 | $+130.75 | $3,536.55 | ▲ +130.75 after sell → book $9,923.93; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 67 | $13.33 | $2.21 | $+143.00 | $4,427.45 | ▲ +143.00 after sell → book $9,921.72; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 83 | $8.08 | $2.26 | $-87.50 | $5,095.82 | ▼ -87.50 after sell → book $9,919.46; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 91 | $8.59 | $2.29 | $+23.66 | $5,875.22 | ▲ +23.66 after sell → book $9,917.17; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 2572 | $0.38 | $18.00 | $+195.63 | $6,842.30 | ▲ +195.63 after sell → book $9,899.17; vs 09:30 mark -18.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 11 | $70.08 | $2.04 | $+45.16 | $7,611.08 | ▲ +45.16 after sell → book $9,897.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 614 | $1.19 | $8.03 | $-40.51 | $8,333.71 | ▼ -40.51 after sell → book $9,889.09; vs 09:30 mark -8.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 187 | $4.16 | $2.59 | $+17.30 | $9,109.03 | ▲ +17.30 after sell → book $9,886.50; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 117 | $6.64 | $2.37 | $+21.61 | $9,884.13 | ▲ +21.61 after sell → book $9,884.13; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,884.13 | ▲ close $9,884.13 vs 09:30 $9,939.44 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,884.13 | ▲ 09:30 equity $9,884.13 vs yday $9,884.13 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 34 | $24.11 | $2.09 | — | $9,062.30 | — | top 12 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 527 | $1.56 | $6.80 | — | $8,233.38 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 202 | $4.07 | $2.61 | — | $7,408.63 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 43 | $19.04 | $2.12 | — | $6,587.79 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 157 | $5.24 | $2.46 | — | $5,762.65 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 144 | $5.71 | $2.42 | — | $4,937.99 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 93 | $8.79 | $2.27 | — | $4,118.25 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 232 | $3.55 | $2.99 | — | $3,291.66 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+27.9; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 202 | $4.06 | $2.61 | — | $2,468.93 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+29.4; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 34 | $23.80 | $2.09 | — | $1,657.64 | — | top 12 by hot; rank hot_score; list yday_gainer; ret5=+28.9; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 87 | $9.42 | $2.25 | — | $835.85 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $823.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 43 | $19.00 | $2.12 | — | $16.73 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+11.2; leftover $823.68 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.73 | ▲ close $10,495.39 vs 09:30 $9,884.13 (session +644.09) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.73 | ▼ 09:30 equity $10,219.25 vs yday $10,495.39 (-276.14) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 34 | $26.61 | $2.11 | $+80.80 | $919.36 | ▲ +80.80 after sell → book $10,217.14; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 527 | $1.60 | $6.90 | $+7.39 | $1,755.66 | ▲ +7.39 after sell → book $10,210.24; vs 09:30 mark -6.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 43 | $20.72 | $2.14 | $+67.98 | $2,644.48 | ▲ +67.98 after sell → book $10,208.10; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 157 | $4.98 | $2.50 | $-45.78 | $3,423.85 | ▼ -45.78 after sell → book $10,205.61; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 144 | $5.97 | $2.46 | $+32.56 | $4,281.07 | ▲ +32.56 after sell → book $10,203.15; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 232 | $3.77 | $3.04 | $+45.01 | $5,152.67 | ▲ +45.01 after sell → book $10,200.11; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DFDV` | 202 | $4.35 | $2.65 | $+53.32 | $6,028.72 | ▲ +53.32 after sell → book $10,197.46; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMNR` | 34 | $24.24 | $2.11 | $+10.76 | $6,850.77 | ▲ +10.76 after sell → book $10,195.35; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 87 | $10.07 | $2.28 | $+52.02 | $7,724.58 | ▲ +52.02 after sell → book $10,193.07; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NIQ` | 43 | $19.20 | $2.14 | $+4.34 | $8,548.04 | ▲ +4.34 after sell → book $10,190.93; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 60 | $14.11 | $2.17 | — | $7,699.27 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 147 | $5.81 | $2.43 | — | $6,842.77 | — | top 12 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 73 | $11.59 | $2.21 | — | $5,994.86 | — | top 12 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 103 | $8.29 | $2.30 | — | $5,138.69 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 21 | $40.50 | $2.05 | — | $4,286.14 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.8; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 62 | $13.63 | $2.18 | — | $3,438.90 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 61 | $14.00 | $2.17 | — | $2,582.73 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+17.8; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 6 | $124.67 | $2.01 | — | $1,832.70 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.7; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRBP` | 75 | $11.36 | $2.21 | — | $978.48 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.2; leftover $854.80 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 76 | $11.22 | $2.22 | — | $123.55 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.8; leftover $854.80 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.55 | ▲ close $10,201.44 vs 09:30 $10,219.25 (session +32.45) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.55 | ▲ 09:30 equity $10,297.63 vs yday $10,201.44 (+96.19) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 93 | $9.41 | $2.29 | $+53.10 | $996.38 | ▲ +53.10 after sell → book $10,295.33; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 60 | $14.20 | $2.19 | $+1.04 | $1,846.19 | ▲ +1.04 after sell → book $10,293.14; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 147 | $6.50 | $2.47 | $+96.53 | $2,799.23 | ▲ +96.53 after sell → book $10,290.68; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 73 | $12.18 | $2.23 | $+38.99 | $3,686.14 | ▲ +38.99 after sell → book $10,288.45; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 21 | $37.42 | $2.07 | $-68.81 | $4,469.88 | ▼ -68.81 after sell → book $10,286.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 62 | $12.98 | $2.20 | $-44.67 | $5,272.45 | ▼ -44.67 after sell → book $10,284.18; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 61 | $12.56 | $2.19 | $-92.21 | $6,036.41 | ▼ -92.21 after sell → book $10,281.98; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 6 | $128.00 | $2.03 | $+15.94 | $6,802.39 | ▲ +15.94 after sell → book $10,279.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRBP` | 75 | $11.28 | $2.24 | $-10.45 | $7,646.15 | ▼ -10.45 after sell → book $10,277.72; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 76 | $11.38 | $2.24 | $+7.70 | $8,508.79 | ▲ +7.70 after sell → book $10,275.48; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 5 | $144.18 | $2.00 | — | $7,785.88 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 45 | $18.50 | $2.12 | — | $6,951.26 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 176 | $4.81 | $2.52 | — | $6,102.18 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+14.8; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 157 | $5.39 | $2.46 | — | $5,253.49 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+17.4; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `VERA` | 23 | $36.70 | $2.06 | — | $4,407.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+14.1; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 95 | $8.95 | $2.27 | — | $3,554.80 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.2; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `DJT` | 88 | $9.59 | $2.25 | — | $2,709.07 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+13.8; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `HTFL` | 17 | $48.92 | $2.04 | — | $1,875.39 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+7.5; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `NCNO` | 38 | $22.03 | $2.10 | — | $1,036.15 | — | top 12 by hot; rank hot_score; list ohlc_hot,earn_react; 🔵; ret5=+4.0; leftover $850.88 | — |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 219 | $3.87 | $2.83 | — | $185.79 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+9.8; leftover $850.88 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.79 | ▼ close $10,231.47 vs 09:30 $10,297.63 (session -21.34) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.79 | ▼ 09:30 equity $10,191.93 vs yday $10,231.47 (-39.54) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 202 | $3.69 | $2.65 | $-82.02 | $928.52 | ▼ -82.02 after sell → book $10,189.28; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 45 | $18.15 | $2.15 | $-20.02 | $1,743.13 | ▼ -20.02 after sell → book $10,187.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OABI` | 176 | $4.54 | $2.56 | $-52.60 | $2,539.61 | ▼ -52.60 after sell → book $10,184.58; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 157 | $5.11 | $2.50 | $-48.92 | $3,339.38 | ▼ -48.92 after sell → book $10,182.08; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `VERA` | 23 | $34.40 | $2.08 | $-57.04 | $4,128.50 | ▼ -57.04 after sell → book $10,180.00; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HTFL` | 17 | $48.50 | $2.06 | $-11.24 | $4,950.94 | ▼ -11.24 after sell → book $10,177.94; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 219 | $4.25 | $2.87 | $+77.52 | $5,878.82 | ▲ +77.52 after sell → book $10,175.07; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 59 | $14.00 | $2.17 | — | $5,050.65 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $839.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 5 | $146.07 | $2.00 | — | $4,318.30 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $839.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 1 | $461.85 | $1.99 | — | $3,854.45 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.8; leftover $839.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 39 | $21.49 | $2.11 | — | $3,014.24 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.3; leftover $839.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 45 | $18.36 | $2.12 | — | $2,185.91 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.8; leftover $839.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `FROG` | 8 | $103.66 | $2.01 | — | $1,354.66 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.9; leftover $839.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 313 | $2.68 | $4.04 | — | $511.78 | — | top 12 by hot; rank hot_score; list flatten,ohlc_hot; ret5=+16.3; leftover $839.83 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $511.78 | ▼ close $9,961.41 vs 09:30 $10,191.93 (session -197.21) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $511.78 | ▼ 09:30 equity $9,874.28 vs yday $9,961.41 (-87.13) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 103 | $9.50 | $2.33 | $+120.00 | $1,487.96 | ▲ +120.00 after sell → book $9,871.96; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 95 | $8.66 | $2.30 | $-32.13 | $2,308.35 | ▼ -32.13 after sell → book $9,869.65; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DJT` | 88 | $9.55 | $2.28 | $-7.61 | $3,146.48 | ▼ -7.61 after sell → book $9,867.38; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 38 | $22.66 | $2.12 | $+19.71 | $4,005.43 | ▲ +19.71 after sell → book $9,865.25; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 5 | $148.03 | $2.02 | $+5.77 | $4,743.56 | ▲ +5.77 after sell → book $9,863.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 1 | $437.95 | $2.01 | $-27.91 | $5,179.49 | ▼ -27.91 after sell → book $9,861.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 39 | $20.56 | $2.13 | $-40.50 | $5,979.21 | ▼ -40.50 after sell → book $9,859.09; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 45 | $17.77 | $2.15 | $-30.82 | $6,776.71 | ▼ -30.82 after sell → book $9,856.94; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FROG` | 8 | $98.42 | $2.03 | $-45.89 | $7,562.08 | ▼ -45.89 after sell → book $9,854.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 313 | $2.58 | $4.10 | $-39.44 | $8,365.52 | ▼ -39.44 after sell → book $9,850.81; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,365.52 | ▲ close $9,851.92 vs 09:30 $9,874.28 (session +1.11) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,365.52 | ▼ 09:30 equity $9,836.13 vs yday $9,851.92 (-15.79) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 59 | $13.04 | $2.19 | $-60.99 | $9,132.69 | ▼ -60.99 after sell → book $9,833.94; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,132.69 | ▲ close $9,904.04 vs 09:30 $9,836.13 (session +70.10) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,132.69 | ▼ 09:30 equity $9,889.69 vs yday $9,904.04 (-14.35) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,132.69 | ▼ close $9,886.74 vs 09:30 $9,889.69 (session -2.95) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,132.69 | ▼ 09:30 equity $9,862.42 vs yday $9,886.74 (-24.32) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 5 | $145.94 | $2.02 | $+4.79 | $9,860.39 | ▲ +4.79 after sell → book $9,860.39; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 461 | $1.78 | $5.95 | — | $9,033.86 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 44 | $18.40 | $2.12 | — | $8,222.14 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 59 | $13.71 | $2.17 | — | $7,411.09 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 34 | $23.88 | $2.09 | — | $6,597.07 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 604 | $1.36 | $7.79 | — | $5,767.84 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 425 | $1.93 | $5.48 | — | $4,942.11 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 6 | $127.91 | $2.01 | — | $4,172.64 | — | top 12 by hot; rank hot_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 32 | $25.62 | $2.09 | — | $3,350.56 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+13.1; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 9 | $82.76 | $2.02 | — | $2,603.70 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+17.1; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNDT` | 432 | $1.90 | $5.57 | — | $1,777.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.5; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPK` | 480 | $1.71 | $6.19 | — | $950.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $821.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `DFDV` | 146 | $5.59 | $2.43 | — | $131.04 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.0; leftover $821.70 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.04 | ▼ close $9,620.30 vs 09:30 $9,862.42 (session -194.19) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.04 | ▼ 09:30 equity $9,523.13 vs yday $9,620.30 (-97.17) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 44 | $18.15 | $2.14 | $-15.26 | $927.49 | ▼ -15.26 after sell → book $9,520.98; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 59 | $13.89 | $2.19 | $+6.27 | $1,744.82 | ▲ +6.27 after sell → book $9,518.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 34 | $23.84 | $2.11 | $-5.56 | $2,553.26 | ▼ -5.56 after sell → book $9,516.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 604 | $1.23 | $7.90 | $-94.21 | $3,288.28 | ▼ -94.21 after sell → book $9,508.78; vs 09:30 mark -7.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 425 | $1.90 | $5.56 | $-23.80 | $4,090.22 | ▼ -23.80 after sell → book $9,503.22; vs 09:30 mark -5.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 6 | $125.22 | $2.03 | $-20.18 | $4,839.51 | ▼ -20.18 after sell → book $9,501.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TARS` | 9 | $82.70 | $2.04 | $-4.59 | $5,581.77 | ▼ -4.59 after sell → book $9,499.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNDT` | 432 | $1.90 | $5.65 | $-11.23 | $6,396.92 | ▼ -11.23 after sell → book $9,493.50; vs 09:30 mark -5.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPK` | 480 | $1.59 | $6.28 | $-70.07 | $7,153.84 | ▼ -70.07 after sell → book $9,487.22; vs 09:30 mark -6.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 101 | $7.87 | $2.29 | — | $6,356.68 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 316 | $2.51 | $4.08 | — | $5,559.44 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 6 | $120.47 | $2.01 | — | $4,834.58 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 48 | $16.40 | $2.13 | — | $4,045.25 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 175 | $4.53 | $2.52 | — | $3,249.98 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 138 | $5.75 | $2.40 | — | $2,454.08 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 201 | $3.95 | $2.60 | — | $1,657.53 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+6.9; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $1,141.76 | — | top 12 by hot; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $794.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 116 | $6.84 | $2.34 | — | $345.98 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $794.87 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $345.98 | ▲ close $9,765.39 vs 09:30 $9,523.13 (session +300.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $345.98 | ▼ 09:30 equity $9,654.53 vs yday $9,765.39 (-110.86) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 461 | $1.56 | $6.03 | $-111.10 | $1,061.41 | ▼ -111.10 after sell → book $9,648.50; vs 09:30 mark -6.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 32 | $26.44 | $2.11 | $+21.89 | $1,905.38 | ▲ +21.89 after sell → book $9,646.39; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 146 | $5.81 | $2.46 | $+26.50 | $2,751.18 | ▲ +26.50 after sell → book $9,643.93; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 101 | $7.76 | $2.32 | $-15.72 | $3,532.62 | ▼ -15.72 after sell → book $9,641.61; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 316 | $2.66 | $4.14 | $+39.18 | $4,369.04 | ▲ +39.18 after sell → book $9,637.47; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HOOD` | 6 | $125.07 | $2.03 | $+23.53 | $5,117.44 | ▲ +23.53 after sell → book $9,635.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 48 | $16.74 | $2.15 | $+12.03 | $5,918.80 | ▲ +12.03 after sell → book $9,633.29; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 175 | $4.53 | $2.55 | $-5.07 | $6,709.00 | ▼ -5.07 after sell → book $9,630.74; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 138 | $5.95 | $2.44 | $+22.76 | $7,527.66 | ▲ +22.76 after sell → book $9,628.30; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 201 | $4.13 | $2.64 | $+30.94 | $8,355.15 | ▲ +30.94 after sell → book $9,625.66; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $8,874.29 | ▲ +3.36 after sell → book $9,623.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 116 | $6.46 | $2.37 | $-48.79 | $9,621.28 | ▼ -48.79 after sell → book $9,621.28; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,621.28 | ▲ close $9,621.28 vs 09:30 $9,654.53 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,621.28 | ▲ 09:30 equity $9,621.28 vs yday $9,621.28 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,621.28 | ▲ close $9,621.28 vs 09:30 $9,621.28 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,621.28 | ▲ 09:30 equity $9,621.28 vs yday $9,621.28 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,621.28 | ▲ close $9,621.28 vs 09:30 $9,621.28 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,621.28 | ▲ 09:30 equity $9,621.28 vs yday $9,621.28 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 296 | $2.70 | $3.82 | — | $8,818.26 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 163 | $4.91 | $2.48 | — | $8,015.45 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 130 | $6.16 | $2.38 | — | $7,212.27 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 256 | $3.13 | $3.30 | — | $6,407.69 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 572 | $1.40 | $7.38 | — | $5,599.51 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-17.2; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 73 | $10.95 | $2.21 | — | $4,797.95 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 14 | $54.91 | $2.03 | — | $4,027.18 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+24.3; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 9 | $84.27 | $2.02 | — | $3,266.73 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 149 | $5.38 | $2.44 | — | $2,462.68 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+19.8; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `CYPH` | 335 | $2.39 | $4.32 | — | $1,657.70 | — | top 12 by hot; rank hot_score; list yday_mover; 🔵; ret5=+31.0; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 43 | $18.30 | $2.12 | — | $868.68 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $801.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 378 | $2.12 | $4.88 | — | $62.45 | — | top 12 by hot; rank hot_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $801.77 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.45 | ▲ close $9,614.95 vs 09:30 $9,621.28 (session +33.04) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.45 | ▲ 09:30 equity $9,616.12 vs yday $9,614.95 (+1.17) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 130 | $6.02 | $2.41 | $-22.99 | $842.64 | ▼ -22.99 after sell → book $9,613.71; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 73 | $10.29 | $2.23 | $-52.62 | $1,591.58 | ▼ -52.62 after sell → book $9,611.48; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 14 | $54.75 | $2.05 | $-6.32 | $2,356.02 | ▼ -6.32 after sell → book $9,609.42; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 9 | $86.06 | $2.04 | $+12.06 | $3,128.53 | ▲ +12.06 after sell → book $9,607.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 149 | $5.57 | $2.47 | $+23.40 | $3,955.99 | ▲ +23.40 after sell → book $9,604.92; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CYPH` | 335 | $2.26 | $4.39 | $-52.26 | $4,708.70 | ▼ -52.26 after sell → book $9,600.53; vs 09:30 mark -4.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 43 | $18.28 | $2.14 | $-5.12 | $5,492.60 | ▼ -5.12 after sell → book $9,598.39; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 378 | $2.05 | $4.95 | $-36.29 | $6,262.55 | ▼ -36.29 after sell → book $9,593.44; vs 09:30 mark -4.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,262.55 | ▲ close $9,755.04 vs 09:30 $9,616.12 (session +161.60) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,262.55 | ▲ 09:30 equity $9,803.06 vs yday $9,755.04 (+48.02) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 163 | $5.11 | $2.52 | $+27.60 | $7,092.96 | ▲ +27.60 after sell → book $9,800.54; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 256 | $3.64 | $3.35 | $+123.90 | $8,021.45 | ▲ +123.90 after sell → book $9,797.19; vs 09:30 mark -3.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,021.45 | ▲ close $9,853.93 vs 09:30 $9,803.06 (session +56.74) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,021.45 | ▲ 09:30 equity $9,854.13 vs yday $9,853.93 (+0.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `GPRO` | 572 | $1.31 | $7.48 | $-66.34 | $8,763.29 | ▼ -66.34 after sell → book $9,846.65; vs 09:30 mark -7.48 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 442 | $1.80 | $5.70 | — | $7,961.98 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 34 | $23.29 | $2.09 | — | $7,168.03 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 54 | $14.62 | $2.15 | — | $6,376.40 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 50 | $15.75 | $2.14 | — | $5,586.76 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 8 | $89.38 | $2.01 | — | $4,869.71 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 15 | $52.52 | $2.04 | — | $4,079.87 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+10.7; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 28 | $28.16 | $2.07 | — | $3,289.32 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `TXG` | 10 | $74.50 | $2.02 | — | $2,542.30 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+13.4; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 40 | $19.75 | $2.11 | — | $1,750.19 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 116 | $6.83 | $2.34 | — | $955.57 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+11.2; leftover $796.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 3 | $236.92 | $2.00 | — | $242.81 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+15.5; leftover $796.66 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.81 | ▼ close $9,761.86 vs 09:30 $9,854.13 (session -58.11) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.81 | ▲ 09:30 equity $9,838.34 vs yday $9,761.86 (+76.48) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 34 | $24.09 | $2.11 | $+23.00 | $1,059.76 | ▲ +23.00 after sell → book $9,836.23; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 54 | $13.77 | $2.17 | $-50.22 | $1,801.17 | ▼ -50.22 after sell → book $9,834.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `REF` | 50 | $15.85 | $2.16 | $+0.70 | $2,591.51 | ▲ +0.70 after sell → book $9,831.90; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 8 | $86.76 | $2.03 | $-25.01 | $3,283.55 | ▼ -25.01 after sell → book $9,829.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 15 | $54.31 | $2.06 | $+22.76 | $4,096.15 | ▲ +22.76 after sell → book $9,827.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 28 | $28.59 | $2.09 | $+8.01 | $4,894.71 | ▲ +8.01 after sell → book $9,825.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TXG` | 10 | $75.38 | $2.04 | $+4.74 | $5,646.47 | ▲ +4.74 after sell → book $9,823.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 40 | $20.31 | $2.13 | $+18.16 | $6,456.74 | ▲ +18.16 after sell → book $9,821.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 116 | $6.48 | $2.37 | $-45.31 | $7,206.06 | ▼ -45.31 after sell → book $9,819.18; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRWD` | 3 | $236.04 | $2.02 | $-6.66 | $7,912.16 | ▼ -6.66 after sell → book $9,817.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 35 | $22.46 | $2.10 | — | $7,123.96 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 21 | $36.76 | $2.05 | — | $6,349.95 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 739 | $1.07 | $9.53 | — | $5,549.68 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 5 | $147.61 | $2.00 | — | $4,809.63 | — | top 12 by hot; rank hot_score; list flatten,ohlc_hot; ret5=+17.7; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 77 | $10.25 | $2.22 | — | $4,018.16 | — | top 12 by hot; rank hot_score; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 28 | $28.23 | $2.07 | — | $3,225.64 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+13.3; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 204 | $3.86 | $2.63 | — | $2,435.57 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 30 | $25.95 | $2.08 | — | $1,654.99 | — | top 12 by hot; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 329 | $2.40 | $4.24 | — | $861.15 | — | top 12 by hot; rank hot_score; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $791.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `BNC` | 157 | $5.03 | $2.46 | — | $68.98 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+7.4; leftover $791.22 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.98 | ▲ close $10,020.00 vs 09:30 $9,838.34 (session +234.24) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.98 | ▲ 09:30 equity $10,051.52 vs yday $10,020.00 (+31.52) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 442 | $1.96 | $5.79 | $+59.23 | $929.51 | ▲ +59.23 after sell → book $10,045.73; vs 09:30 mark -5.79 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 35 | $21.30 | $2.12 | $-44.81 | $1,672.90 | ▼ -44.81 after sell → book $10,043.62; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 21 | $39.50 | $2.07 | $+53.41 | $2,500.33 | ▲ +53.41 after sell → book $10,041.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IQ` | 739 | $1.12 | $9.67 | $+17.75 | $3,318.34 | ▲ +17.75 after sell → book $10,031.88; vs 09:30 mark -9.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 5 | $146.50 | $2.02 | $-9.58 | $4,048.81 | ▼ -9.58 after sell → book $10,029.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 77 | $10.12 | $2.24 | $-14.47 | $4,825.81 | ▼ -14.47 after sell → book $10,027.61; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 28 | $28.55 | $2.09 | $+4.79 | $5,623.12 | ▲ +4.79 after sell → book $10,025.52; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EMAT` | 204 | $3.97 | $2.68 | $+17.13 | $6,430.32 | ▲ +17.13 after sell → book $10,022.84; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 30 | $26.14 | $2.10 | $+1.52 | $7,212.42 | ▲ +1.52 after sell → book $10,020.74; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 329 | $2.29 | $4.31 | $-44.74 | $7,961.52 | ▼ -44.74 after sell → book $10,016.43; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 27 | $29.32 | $2.07 | — | $7,167.81 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 262 | $3.04 | $3.38 | — | $6,369.26 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 9 | $81.40 | $2.02 | — | $5,634.64 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 534 | $1.49 | $6.89 | — | $4,832.10 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 202 | $3.94 | $2.61 | — | $4,033.61 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 85 | $9.32 | $2.25 | — | $3,239.16 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `CHPT` | 79 | $10.00 | $2.23 | — | $2,446.94 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $1,786.08 | — | top 12 by hot; rank hot_score; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 69 | $11.38 | $2.20 | — | $998.66 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $796.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 7 | $108.55 | $2.01 | — | $236.80 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+21.3; leftover $796.15 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.80 | ▲ close $10,272.60 vs 09:30 $10,051.52 (session +283.81) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.80 | ▲ 09:30 equity $10,600.41 vs yday $10,272.60 (+327.81) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 296 | $3.55 | $3.88 | $+243.90 | $1,283.72 | ▲ +243.90 after sell → book $10,596.53; vs 09:30 mark -3.88 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 157 | $6.42 | $2.50 | $+212.49 | $2,288.38 | ▲ +212.49 after sell → book $10,594.03; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 27 | $29.43 | $2.09 | $-1.19 | $3,080.90 | ▼ -1.19 after sell → book $10,591.94; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 262 | $4.00 | $3.43 | $+246.02 | $4,125.47 | ▲ +246.02 after sell → book $10,588.51; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 9 | $79.08 | $2.04 | $-24.93 | $4,835.15 | ▼ -24.93 after sell → book $10,586.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 202 | $3.90 | $2.65 | $-13.34 | $5,620.30 | ▼ -13.34 after sell → book $10,583.82; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CHPT` | 79 | $10.32 | $2.25 | $+20.80 | $6,433.33 | ▲ +20.80 after sell → book $10,581.57; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 69 | $12.05 | $2.22 | $+41.81 | $7,262.56 | ▲ +41.81 after sell → book $10,579.35; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 7 | $107.57 | $2.03 | $-10.90 | $8,013.52 | ▼ -10.90 after sell → book $10,577.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 360 | $2.47 | $4.64 | — | $7,119.68 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 52 | $16.91 | $2.15 | — | $6,238.21 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 154 | $5.75 | $2.45 | — | $5,349.49 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 68 | $13.05 | $2.19 | — | $4,459.89 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 108 | $8.22 | $2.31 | — | $3,569.82 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 83 | $10.71 | $2.24 | — | $2,678.65 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 10 | $83.53 | $2.02 | — | $1,841.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+8.8; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 4 | $190.30 | $2.00 | — | $1,078.13 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+10.6; leftover $890.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 35 | $24.93 | $2.10 | — | $203.48 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+8.7; leftover $890.39 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.48 | ▲ close $10,622.51 vs 09:30 $10,600.41 (session +67.30) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.48 | ▼ 09:30 equity $10,607.19 vs yday $10,622.51 (-15.32) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 154 | $6.05 | $2.49 | $+41.26 | $1,133.47 | ▲ +41.26 after sell → book $10,604.71; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 68 | $12.99 | $2.22 | $-8.49 | $2,014.57 | ▼ -8.49 after sell → book $10,602.49; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 35 | $25.26 | $2.12 | $+7.34 | $2,896.56 | ▲ +7.34 after sell → book $10,600.38; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 28 | $9.11 | $2.07 | — | $2,639.40 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; leftover $263.32 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 36 | $7.23 | $2.10 | — | $2,377.02 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $263.32 | — |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 84 | $3.10 | $2.24 | — | $2,114.38 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $263.32 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 260 | $1.01 | $3.35 | — | $1,848.43 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+14.3; leftover $263.32 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 9 | $28.02 | $2.02 | — | $1,594.23 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $263.32 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,594.23 | ▲ close $10,612.19 vs 09:30 $10,607.19 (session +23.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,594.23 | ▲ 09:30 equity $10,745.16 vs yday $10,612.19 (+132.97) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 534 | $1.41 | $6.99 | $-56.60 | $2,340.18 | ▼ -56.60 after sell → book $10,738.17; vs 09:30 mark -6.99 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 85 | $12.80 | $2.27 | $+291.29 | $3,425.92 | ▲ +291.29 after sell → book $10,735.91; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 3 | $266.50 | $2.02 | $+136.62 | $4,223.40 | ▲ +136.62 after sell → book $10,733.89; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 52 | $16.92 | $2.17 | $-3.79 | $5,101.07 | ▼ -3.79 after sell → book $10,731.72; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FWDI` | 108 | $8.20 | $2.34 | $-6.82 | $5,984.33 | ▼ -6.82 after sell → book $10,729.38; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 83 | $10.11 | $2.26 | $-54.30 | $6,821.20 | ▼ -54.30 after sell → book $10,727.12; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 10 | $86.57 | $2.04 | $+26.34 | $7,684.86 | ▲ +26.34 after sell → book $10,725.08; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 4 | $174.50 | $2.02 | $-67.22 | $8,380.83 | ▼ -67.22 after sell → book $10,723.05; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 28 | $8.39 | $2.09 | $-24.33 | $8,613.66 | ▼ -24.33 after sell → book $10,720.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 36 | $6.83 | $2.12 | $-18.62 | $8,857.42 | ▼ -18.62 after sell → book $10,718.84; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 260 | $0.95 | $3.30 | $-22.26 | $9,101.12 | ▼ -22.26 after sell → book $10,715.54; vs 09:30 mark -3.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 9 | $25.90 | $2.04 | $-23.13 | $9,332.18 | ▼ -23.13 after sell → book $10,713.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 345 | $2.70 | $4.45 | — | $8,396.23 | — | top 12 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 22 | $41.76 | $2.06 | — | $7,475.46 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 207 | $4.49 | $2.67 | — | $6,543.35 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 94 | $9.90 | $2.27 | — | $5,610.48 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 13 | $70.84 | $2.03 | — | $4,687.53 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 1271 | $0.73 | $13.14 | — | $3,741.48 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `THM` | 326 | $2.86 | $4.21 | — | $2,804.91 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+25.5; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 717 | $1.30 | $9.25 | — | $1,863.56 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.3; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 47 | $19.70 | $2.13 | — | $935.53 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $933.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `VNET` | 132 | $7.06 | $2.39 | — | $1.23 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $933.22 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.23 | ▼ close $10,354.47 vs 09:30 $10,745.16 (session -314.44) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.23 | ▼ 09:30 equity $10,238.09 vs yday $10,354.47 (-116.38) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 360 | $2.68 | $4.71 | $+66.24 | $961.31 | ▲ +66.24 after sell → book $10,233.38; vs 09:30 mark -4.71 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 22 | $36.02 | $2.08 | $-130.30 | $1,751.79 | ▼ -130.30 after sell → book $10,231.30; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 207 | $3.92 | $2.71 | $-122.34 | $2,561.55 | ▼ -122.34 after sell → book $10,228.59; vs 09:30 mark -2.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 94 | $9.12 | $2.30 | $-77.89 | $3,416.53 | ▼ -77.89 after sell → book $10,226.29; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 13 | $70.50 | $2.05 | $-8.50 | $4,330.98 | ▼ -8.50 after sell → book $10,224.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVTL` | 1271 | $0.66 | $12.43 | $-119.12 | $5,157.92 | ▼ -119.12 after sell → book $10,211.81; vs 09:30 mark -12.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `THM` | 326 | $2.79 | $4.27 | $-31.29 | $6,063.19 | ▼ -31.29 after sell → book $10,207.54; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 717 | $1.27 | $9.38 | $-40.14 | $6,964.40 | ▼ -40.14 after sell → book $10,198.16; vs 09:30 mark -9.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMRX` | 47 | $19.29 | $2.15 | $-23.55 | $7,868.88 | ▼ -23.55 after sell → book $10,196.01; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VNET` | 132 | $6.82 | $2.42 | $-36.48 | $8,766.70 | ▼ -36.48 after sell → book $10,193.59; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,766.70 | ▲ close $10,948.45 vs 09:30 $10,238.09 (session +754.86) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,850.27 | ▲ 09:30 equity $10,598.63 vs yday $10,383.50 (+215.13) | 09:30 open · cash $6,850.27 (unchanged overnight, no fees) · equity $10,598.63 vs prior close $10,383.50 (+215.13) · 3 name(s) re-marked at the open (per-name table). GLND×303 yday $5.35 → 09:30 $6.06 +215.13; INDP×271 yday $4.00 → 09:30 $4.00 +0.00; VICR×3 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 20 | $29.76 | $2.05 | — | $6,253.02 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $622.75 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 38 | $16.21 | $2.10 | — | $5,634.94 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $622.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 39 | $15.58 | $2.11 | — | $5,025.17 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $622.75 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 5 | $123.50 | $2.00 | — | $4,405.66 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $622.75 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 155 | $4.00 | $2.46 | — | $3,782.43 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $622.75 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 61 | $10.20 | $2.17 | — | $3,158.06 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $622.75 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 161 | $3.86 | $2.47 | — | $2,534.13 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $622.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 283 | $2.20 | $3.65 | — | $1,907.87 | — | top 12 by hot; rank hot_score; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $622.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 79 | $7.85 | $2.23 | — | $1,285.50 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $622.75 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 3 | $184.00 | $2.00 | — | $731.50 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $622.75 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 20 | $29.80 | $2.05 | — | $133.45 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $622.75 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.45 | ▼ close $10,482.31 vs 09:30 $10,598.63 (session -91.02) | 16:00 close · cash $133.45 · equity $10,482.31 vs 09:30 $10,598.63 (-116.32; session marks -91.02) · 14 name(s) marked open→close (per-name table). GLND×303 09:30 $6.06 → close $5.54 -157.56; INDP×271 09:30 $4.00 → close $4.00 +0.00; VICR×3 09:30 $276.06 → close $276.06 -0.00; TJGC×20 09:30 $29.76 → close $26.24 -70.40; SECZ×38 09:30 $16.21 → close $15.96 -9.50; USDE×39 09:30 $15.58 → close $17.25 +65.09; GRAL×5 09:30 $123.50 → close $126.89 +16.95; CYPH×155 09:30 $4.00 → close $4.12 +17.83; DNA×61 09:30 $10.20 → close $10.66 +28.06; ZSQR×161 09:30 $3.86 → close $3.78 -12.88; HLP×283 09:30 $2.20 → close $2.21 +2.83; RSKD×79 09:30 $7.85 → close $7.78 -5.53; TWST×3 09:30 $184.00 → close $182.83 -3.51; QMCO×20 09:30 $29.80 → close $31.68 +37.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GSM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RBLX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UGP` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SEDG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BTDR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAFX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SION` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARM` | cash | leftover split 263.32 < 1 share @ 319.41 |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `VNET` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CTKB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RBRK` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INDP` | 84 | 2026-09-22 @ $3.10 | top 12 by hot; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $263.32 |
| `GLND` | 345 | 2026-09-23 @ $2.70 | top 12 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $933.22 |
