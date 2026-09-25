# Factor mine action — `union_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **-13.20%** ($8,680) · signal-only (no cash/fees) was -4.88%. Starts YES **0/30**. Fills 272 · skips 105 · realized $-723.91.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,276.08.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $7,550.75 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $6,283.80 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,040.27 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,797.76 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $2,553.19 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $1,314.55 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $97.53 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $2,510.63 | ▼ -29.03 after sell → book $10,173.92; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $3,926.37 | ▲ +148.79 after sell → book $10,154.67; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $5,114.71 | ▼ -55.19 after sell → book $10,152.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,426.78 | ▲ +69.56 after sell → book $10,150.25; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $7,606.44 | ▼ -64.90 after sell → book $10,148.16; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $8,819.03 | ▼ -26.05 after sell → book $10,145.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $10,143.91 | ▲ +107.86 after sell → book $10,143.91; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $8,882.01 | — | rank by cond; rank cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 117 | $10.83 | $2.34 | — | $7,612.56 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 96 | $13.18 | $2.28 | — | $6,345.01 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MNTN` | 101 | $12.50 | $2.29 | — | $5,080.21 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $3,819.39 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $2,555.71 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 217 | $5.84 | $2.80 | — | $1,285.63 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1267.99 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,285.63 | ▼ close $10,105.83 vs 09:30 $10,178.12 (session -21.58) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,285.63 | ▼ 09:30 equity $9,981.40 vs yday $10,105.83 (-124.43) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $2,387.48 | ▼ -160.05 after sell → book $9,979.25; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 117 | $11.19 | $2.37 | $+37.41 | $3,694.34 | ▲ +37.41 after sell → book $9,976.88; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 96 | $13.84 | $2.30 | $+58.78 | $5,020.67 | ▲ +58.78 after sell → book $9,974.57; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MNTN` | 101 | $12.40 | $2.32 | $-14.71 | $6,270.75 | ▼ -14.71 after sell → book $9,972.25; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $7,534.92 | ▲ +3.34 after sell → book $9,970.09; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $8,784.89 | ▼ -13.71 after sell → book $9,967.54; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 217 | $5.45 | $2.85 | $-90.27 | $9,964.70 | ▼ -90.27 after sell → book $9,964.70; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $8,722.44 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 1083 | $1.15 | $13.97 | — | $7,463.02 | — | rank by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $6,260.27 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $5,116.09 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $3,897.89 | — | rank by cond; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $2,674.75 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $1,432.03 | — | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1245.59 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 84 | $14.66 | $2.24 | — | $198.35 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1245.59 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.35 | ▼ close $9,888.83 vs 09:30 $9,981.40 (session -46.91) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.35 | ▼ 09:30 equity $9,689.87 vs yday $9,888.83 (-198.96) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $1,358.92 | ▼ -81.69 after sell → book $9,687.61; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERI` | 1083 | $1.05 | $14.16 | $-136.43 | $2,481.91 | ▼ -136.43 after sell → book $9,673.45; vs 09:30 mark -14.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $3,727.82 | ▲ +43.16 after sell → book $9,671.36; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $4,910.11 | ▲ +38.11 after sell → book $9,669.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $6,161.66 | ▲ +33.34 after sell → book $9,667.30; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $7,329.23 | ▼ -55.57 after sell → book $9,665.27; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $8,554.87 | ▼ -17.07 after sell → book $9,662.83; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 84 | $13.19 | $2.27 | $-127.99 | $9,660.57 | ▼ -127.99 after sell → book $9,660.57; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,660.57 | ▲ close $9,660.57 vs 09:30 $9,689.87 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,660.57 | ▲ 09:30 equity $9,660.57 vs yday $9,660.57 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,660.57 | ▲ close $9,660.57 vs 09:30 $9,660.57 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,660.57 | ▲ 09:30 equity $9,660.57 vs yday $9,660.57 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,466.50 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,281.35 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $6,081.48 | — | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 209 | $5.77 | $2.70 | — | $4,872.86 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $3,673.25 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,485.94 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 690 | $1.75 | $8.90 | — | $1,269.54 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1207.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $111.21 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1207.57 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.21 | ▲ close $9,863.13 vs 09:30 $9,660.57 (session +226.81) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.21 | ▲ 09:30 equity $10,123.17 vs yday $9,863.13 (+260.04) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,379.22 | ▲ +73.95 after sell → book $10,120.98; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,621.53 | ▲ +57.15 after sell → book $10,118.93; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $3,880.85 | ▲ +59.45 after sell → book $10,116.75; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 209 | $5.67 | $2.74 | $-26.34 | $5,063.14 | ▼ -26.34 after sell → book $10,114.01; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $6,352.32 | ▲ +89.57 after sell → book $10,111.82; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,636.99 | ▲ +97.36 after sell → book $10,109.69; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 690 | $1.79 | $9.03 | $+9.67 | $8,863.06 | ▲ +9.67 after sell → book $10,100.66; vs 09:30 mark -9.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,098.63 | ▲ +77.23 after sell → book $10,098.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,902.31 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 73 | $17.20 | $2.21 | — | $7,644.50 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,560.99 | — | rank by cond; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 113 | $11.13 | $2.33 | — | $5,300.97 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 511 | $2.47 | $6.59 | — | $4,032.21 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 654 | $1.93 | $8.44 | — | $2,761.55 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,505.38 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1262.33 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 956 | $1.32 | $12.33 | — | $231.13 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1262.33 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $231.13 | ▲ close $10,313.90 vs 09:30 $10,123.17 (session +253.25) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $231.13 | ▲ 09:30 equity $10,676.43 vs yday $10,313.90 (+362.53) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,434.19 | ▲ +6.74 after sell → book $10,674.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 73 | $16.57 | $2.23 | $-50.43 | $2,641.57 | ▼ -50.43 after sell → book $10,672.16; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,724.69 | ▼ -0.38 after sell → book $10,670.13; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 113 | $13.33 | $2.36 | $+243.91 | $5,228.62 | ▲ +243.91 after sell → book $10,667.77; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 511 | $2.40 | $6.69 | $-49.05 | $6,448.34 | ▼ -49.05 after sell → book $10,661.09; vs 09:30 mark -6.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 654 | $1.88 | $8.56 | $-49.69 | $7,669.30 | ▼ -49.69 after sell → book $10,652.53; vs 09:30 mark -8.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 956 | $1.83 | $12.51 | $+462.72 | $9,406.28 | ▲ +462.72 after sell → book $10,640.03; vs 09:30 mark -12.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,406.28 | ▼ close $10,604.85 vs 09:30 $10,676.43 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,406.28 | ▲ 09:30 equity $10,622.81 vs yday $10,604.85 (+17.96) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-41.72 | $10,620.73 | ▼ -41.72 after sell → book $10,620.73; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $9,314.99 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 34 | $38.01 | $2.09 | — | $8,020.56 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $6,707.31 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 111 | $11.90 | $2.32 | — | $5,384.08 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 59 | $22.41 | $2.17 | — | $4,059.73 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.9; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $2,750.22 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $1,435.34 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1327.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 147 | $8.98 | $2.43 | — | $112.85 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1327.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.85 | ▲ close $10,848.68 vs 09:30 $10,622.81 (session +245.26) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.85 | ▼ 09:30 equity $10,782.23 vs yday $10,848.68 (-66.45) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,428.61 | ▲ +10.01 after sell → book $10,780.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ERO` | 34 | $40.51 | $2.11 | $+80.79 | $2,803.83 | ▲ +80.79 after sell → book $10,778.07; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 17 | $79.34 | $2.06 | $+33.47 | $4,150.55 | ▲ +33.47 after sell → book $10,776.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CNH` | 111 | $11.54 | $2.35 | $-44.63 | $5,429.14 | ▼ -44.63 after sell → book $10,773.66; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HMY` | 59 | $22.39 | $2.19 | $-5.53 | $6,747.96 | ▼ -5.53 after sell → book $10,771.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 30 | $44.33 | $2.10 | $+12.92 | $8,075.76 | ▲ +12.92 after sell → book $10,769.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 147 | $9.03 | $2.47 | $+2.45 | $9,400.71 | ▲ +2.45 after sell → book $10,766.91; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 5 | $267.02 | $2.00 | — | $8,063.60 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1342.96 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 11 | $118.50 | $2.02 | — | $6,758.08 | — | rank by cond; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1342.96 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 33 | $40.50 | $2.09 | — | $5,419.49 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1342.96 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $4,170.77 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1342.96 | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2303 | $0.58 | $20.34 | — | $2,807.79 | — | rank by cond; rank cond; list yday_mover; 🔵; ret5=-27.5; leftover $1342.96 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 257 | $5.21 | $3.32 | — | $1,465.50 | — | rank by cond; rank cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1342.96 | — |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 233 | $5.75 | $3.01 | — | $122.74 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1342.96 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.74 | ▼ close $10,592.92 vs 09:30 $10,782.23 (session -139.19) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.74 | ▼ 09:30 equity $10,566.83 vs yday $10,592.92 (-26.09) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 5 | $267.23 | $2.03 | $-2.98 | $1,456.87 | ▼ -2.98 after sell → book $10,564.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 11 | $118.77 | $2.04 | $-1.10 | $2,761.30 | ▼ -1.10 after sell → book $10,562.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 33 | $37.42 | $2.11 | $-105.84 | $3,994.05 | ▼ -105.84 after sell → book $10,560.66; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $5,272.01 | ▲ +29.24 after sell → book $10,558.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2303 | $0.53 | $19.51 | $-161.90 | $6,473.09 | ▼ -161.90 after sell → book $10,539.11; vs 09:30 mark -19.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 257 | $5.49 | $3.37 | $+65.28 | $7,880.65 | ▲ +65.28 after sell → book $10,535.74; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 233 | $5.73 | $3.06 | $-10.72 | $9,212.68 | ▼ -10.72 after sell → book $10,532.68; vs 09:30 mark -3.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $7,904.25 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1316.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 287 | $4.57 | $3.70 | — | $6,588.95 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $1316.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $5,319.73 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-0.1; leftover $1316.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $4,350.73 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1316.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 506 | $2.60 | $6.53 | — | $3,028.60 | — | rank by cond; rank cond; list flatten,ohlc_hot; ret5=+13.0; leftover $1316.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $1,755.79 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+2.1; leftover $1316.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $518.39 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+8.5; leftover $1316.10 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $518.39 | ▼ close $10,492.50 vs 09:30 $10,566.83 (session -19.82) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $518.39 | ▼ 09:30 equity $10,488.56 vs yday $10,492.50 (-3.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $23.95 | $2.18 | $+5.57 | $1,833.46 | ▲ +5.57 after sell → book $10,486.38; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,099.72 | ▼ -42.18 after sell → book $10,484.32; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 287 | $4.67 | $3.76 | $+21.24 | $4,436.25 | ▲ +21.24 after sell → book $10,480.56; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $5,715.82 | ▲ +10.35 after sell → book $10,478.50; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $6,633.10 | ▼ -51.73 after sell → book $10,476.49; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 506 | $2.68 | $6.62 | $+27.33 | $7,982.56 | ▲ +27.33 after sell → book $10,469.87; vs 09:30 mark -6.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.97 | $2.08 | $+12.42 | $9,267.79 | ▲ +12.42 after sell → book $10,467.79; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $10,465.76 | ▼ -39.44 after sell → book $10,465.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,166.12 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,888.26 | — | rank by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,685.00 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,376.98 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,094.08 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $2,835.63 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,675.87 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1308.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $476.25 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1308.22 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $476.25 | ▼ close $10,039.06 vs 09:30 $10,488.56 (session -410.58) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $476.25 | ▲ 09:30 equity $10,091.39 vs yday $10,039.06 (+52.33) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $1,764.19 | ▼ -11.70 after sell → book $10,089.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,952.85 | ▼ -89.19 after sell → book $10,087.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,086.15 | ▼ -69.96 after sell → book $10,085.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,346.04 | ▼ -48.14 after sell → book $10,083.30; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,595.45 | ▼ -33.48 after sell → book $10,081.25; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $7,802.58 | ▼ -51.32 after sell → book $10,079.18; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $8,921.56 | ▼ -40.78 after sell → book $10,077.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $10,075.12 | ▼ -46.06 after sell → book $10,075.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,075.12 | ▲ close $10,075.12 vs 09:30 $10,091.39 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,075.12 | ▲ 09:30 equity $10,075.12 vs yday $10,075.12 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,075.12 | ▲ close $10,075.12 vs 09:30 $10,075.12 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,075.12 | ▲ 09:30 equity $10,075.12 vs yday $10,075.12 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,075.12 | ▲ close $10,075.12 vs 09:30 $10,075.12 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,075.12 | ▲ 09:30 equity $10,075.12 vs yday $10,075.12 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $8,815.15 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 652 | $1.93 | $8.41 | — | $7,548.38 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 577 | $2.18 | $7.44 | — | $6,283.08 | — | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $5,036.03 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 120 | $10.42 | $2.35 | — | $3,783.28 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $53.45 | $2.06 | — | $2,551.87 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 47 | $26.74 | $2.13 | — | $1,292.96 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1259.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $98.89 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1259.39 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.89 | ▼ close $9,847.02 vs 09:30 $10,075.12 (session -199.39) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.89 | ▼ 09:30 equity $9,819.79 vs yday $9,847.02 (-27.23) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $15.61 | $2.24 | $-91.45 | $1,267.41 | ▼ -91.45 after sell → book $9,817.56; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 577 | $2.16 | $7.55 | $-26.53 | $2,506.18 | ▼ -26.53 after sell → book $9,810.01; vs 09:30 mark -7.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 120 | $10.50 | $2.38 | $+4.87 | $3,763.80 | ▲ +4.87 after sell → book $9,807.63; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 23 | $51.80 | $2.08 | $-42.09 | $4,953.12 | ▼ -42.09 after sell → book $9,805.55; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 47 | $26.38 | $2.15 | $-21.20 | $6,190.83 | ▼ -21.20 after sell → book $9,803.40; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $7,359.06 | ▼ -25.83 after sell → book $9,801.36; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 354 | $3.46 | $4.57 | — | $6,129.65 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1226.51 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 486 | $2.52 | $6.27 | — | $4,898.66 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1226.51 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.03 | $2.06 | — | $3,699.92 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1226.51 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 182 | $6.71 | $2.54 | — | $2,476.16 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1226.51 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $1,420.72 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1226.51 | — |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $28.00 | $2.12 | — | $214.60 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1226.51 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $214.60 | ▲ close $9,816.18 vs 09:30 $9,819.79 (session +34.37) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $214.60 | ▼ 09:30 equity $9,778.34 vs yday $9,816.18 (-37.84) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 652 | $2.00 | $8.53 | $+28.70 | $1,510.07 | ▲ +28.70 after sell → book $9,769.81; vs 09:30 mark -8.53 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,731.77 | ▼ -25.34 after sell → book $9,767.71; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 354 | $3.43 | $4.64 | $-19.82 | $3,941.36 | ▼ -19.82 after sell → book $9,763.08; vs 09:30 mark -4.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 486 | $2.38 | $6.36 | $-80.67 | $5,091.68 | ▼ -80.67 after sell → book $9,756.72; vs 09:30 mark -6.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+48.30 | $6,338.73 | ▲ +48.30 after sell → book $9,754.64; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 182 | $6.57 | $2.58 | $-30.59 | $7,531.89 | ▼ -30.59 after sell → book $9,752.06; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $8,544.75 | ▼ -42.58 after sell → book $9,750.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 43 | $28.03 | $2.14 | $-2.97 | $9,747.90 | ▼ -2.97 after sell → book $9,747.90; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,747.90 | ▲ close $9,747.90 vs 09:30 $9,778.34 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,747.90 | ▲ 09:30 equity $9,747.90 vs yday $9,747.90 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,747.90 | ▲ close $9,747.90 vs 09:30 $9,747.90 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,747.90 | ▲ 09:30 equity $9,747.90 vs yday $9,747.90 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,747.90 | ▲ close $9,747.90 vs 09:30 $9,747.90 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,747.90 | ▲ 09:30 equity $9,747.90 vs yday $9,747.90 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 23 | $52.55 | $2.06 | — | $8,537.19 | — | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $7,384.17 | — | rank by cond; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 120 | $10.11 | $2.35 | — | $6,168.62 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $4,986.81 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $3,856.44 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 66 | $18.30 | $2.19 | — | $2,646.45 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 102 | $11.88 | $2.30 | — | $1,432.39 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+20.7; leftover $1218.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $222.32 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1218.49 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.32 | ▲ close $9,840.33 vs 09:30 $9,747.90 (session +109.44) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.32 | ▼ 09:30 equity $9,676.38 vs yday $9,840.33 (-163.95) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 23 | $56.90 | $2.08 | $+95.91 | $1,528.94 | ▲ +95.91 after sell → book $9,674.30; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $2,516.85 | ▼ -165.11 after sell → book $9,672.27; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 120 | $10.00 | $2.38 | $-17.93 | $3,714.47 | ▼ -17.93 after sell → book $9,669.89; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 14 | $86.06 | $2.05 | $+20.98 | $4,917.26 | ▲ +20.98 after sell → book $9,667.84; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `QRVO` | 10 | $114.11 | $2.04 | $+8.69 | $6,056.32 | ▲ +8.69 after sell → book $9,665.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 66 | $18.28 | $2.21 | $-5.72 | $7,260.59 | ▼ -5.72 after sell → book $9,663.59; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `APPS` | 102 | $11.75 | $2.32 | $-17.88 | $8,456.76 | ▼ -17.88 after sell → book $9,661.26; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 22 | $54.75 | $2.08 | $-7.65 | $9,659.19 | ▼ -7.65 after sell → book $9,659.19; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.19 | ▲ close $9,659.19 vs 09:30 $9,676.38 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.19 | ▲ 09:30 equity $9,659.19 vs yday $9,659.19 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.19 | ▲ close $9,659.19 vs 09:30 $9,659.19 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.19 | ▲ 09:30 equity $9,659.19 vs yday $9,659.19 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 10 | $118.18 | $2.02 | — | $8,475.37 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $7,311.40 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 21 | $55.66 | $2.05 | — | $6,140.49 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 45 | $26.27 | $2.12 | — | $4,956.21 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 44 | $27.09 | $2.12 | — | $3,762.13 | — | rank by cond; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 84 | $14.31 | $2.24 | — | $2,557.85 | — | rank by cond; rank cond; list flatten; ret5=+4.8; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 5 | $236.92 | $2.00 | — | $1,371.24 | — | rank by cond; rank cond; list ohlc_hot; ret5=+15.5; leftover $1207.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $246.79 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+5.3; leftover $1207.40 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.79 | ▲ close $9,662.42 vs 09:30 $9,659.19 (session +19.83) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.79 | ▲ 09:30 equity $9,729.07 vs yday $9,662.42 (+66.65) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $-36.86 | $1,393.75 | ▼ -36.86 after sell → book $9,727.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $2,519.58 | ▼ -38.14 after sell → book $9,724.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 21 | $57.96 | $2.07 | $+44.17 | $3,734.67 | ▲ +44.17 after sell → book $9,722.91; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 45 | $26.51 | $2.15 | $+6.53 | $4,925.47 | ▲ +6.53 after sell → book $9,720.76; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 84 | $14.33 | $2.27 | $-2.83 | $6,126.92 | ▼ -2.83 after sell → book $9,718.49; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRWD` | 5 | $236.04 | $2.02 | $-8.43 | $7,305.10 | ▼ -8.43 after sell → book $9,716.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ILMN` | 5 | $233.85 | $2.02 | $+42.77 | $8,472.32 | ▲ +42.77 after sell → book $9,714.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 152 | $7.95 | $2.45 | — | $7,261.48 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1210.33 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 67 | $18.04 | $2.19 | — | $6,050.94 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1210.33 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 504 | $2.40 | $6.50 | — | $4,834.84 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1210.33 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $3,636.88 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1210.33 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 65 | $18.56 | $2.19 | — | $2,428.29 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+4.8; leftover $1210.33 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 46 | $25.95 | $2.13 | — | $1,232.47 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1210.33 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 17 | $67.91 | $2.04 | — | $75.96 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1210.33 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.96 | ▼ close $9,612.78 vs 09:30 $9,729.07 (session -82.16) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.96 | ▲ 09:30 equity $9,713.91 vs yday $9,612.78 (+101.13) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 44 | $28.55 | $2.14 | $+59.98 | $1,330.01 | ▲ +59.98 after sell → book $9,711.76; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 152 | $7.85 | $2.48 | $-20.13 | $2,520.73 | ▼ -20.13 after sell → book $9,709.28; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 67 | $17.80 | $2.21 | $-20.15 | $3,711.12 | ▼ -20.15 after sell → book $9,707.07; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 504 | $2.29 | $6.60 | $-68.54 | $4,858.69 | ▼ -68.54 after sell → book $9,700.48; vs 09:30 mark -6.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $6,132.96 | ▲ +76.32 after sell → book $9,698.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 65 | $18.12 | $2.21 | $-32.99 | $7,308.56 | ▼ -32.99 after sell → book $9,696.24; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 46 | $26.14 | $2.15 | $+4.46 | $8,508.85 | ▲ +4.46 after sell → book $9,694.09; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 17 | $69.72 | $2.06 | $+26.67 | $9,692.03 | ▲ +26.67 after sell → book $9,692.03; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 4 | $246.98 | $2.00 | — | $8,702.11 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 35 | $34.44 | $2.10 | — | $7,494.61 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `ATRC` | 20 | $58.51 | $2.05 | — | $6,322.36 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $5,130.33 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 151 | $7.98 | $2.44 | — | $3,922.91 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $2,726.83 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+21.3; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 307 | $3.94 | $3.96 | — | $1,513.29 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1211.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 57 | $20.91 | $2.16 | — | $319.26 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1211.50 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $319.26 | ▼ close $9,480.63 vs 09:30 $9,713.91 (session -192.63) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $319.26 | ▲ 09:30 equity $9,523.42 vs yday $9,480.63 (+42.79) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 4 | $231.62 | $2.02 | $-65.46 | $1,243.72 | ▼ -65.46 after sell → book $9,521.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 35 | $33.00 | $2.12 | $-54.61 | $2,396.61 | ▼ -54.61 after sell → book $9,519.29; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 20 | $58.23 | $2.07 | $-9.72 | $3,559.14 | ▼ -9.72 after sell → book $9,517.22; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $4,716.70 | ▼ -34.46 after sell → book $9,515.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 151 | $7.84 | $2.48 | $-26.06 | $5,898.07 | ▼ -26.06 after sell → book $9,512.69; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $7,079.29 | ▼ -14.85 after sell → book $9,510.64; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 307 | $3.90 | $4.02 | $-20.26 | $8,272.57 | ▼ -20.26 after sell → book $9,506.62; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 57 | $21.65 | $2.18 | $+37.84 | $9,504.44 | ▲ +37.84 after sell → book $9,504.44; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $8,351.19 | — | rank by cond; rank cond; list ohlc_hot; ret5=+12.5; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 3 | $326.48 | $2.00 | — | $7,369.75 | — | rank by cond; rank cond; list ohlc_hot; ret5=+3.9; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 9 | $123.00 | $2.02 | — | $6,260.73 | — | rank by cond; rank cond; list ohlc_hot; ret5=+3.0; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $5,116.92 | — | rank by cond; rank cond; list ohlc_hot; ret5=+10.6; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 47 | $24.93 | $2.13 | — | $3,943.08 | — | rank by cond; rank cond; list ohlc_hot; ret5=+8.7; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 110 | $10.71 | $2.32 | — | $2,762.66 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMD` | 2 | $583.88 | $2.00 | — | $1,592.90 | — | rank by cond; rank cond; list ohlc_hot; ret5=+8.5; leftover $1188.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 4 | $294.36 | $2.00 | — | $413.46 | — | rank by cond; rank cond; list ohlc_hot; ret5=+4.1; leftover $1188.06 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $413.46 | ▼ close $9,486.38 vs 09:30 $9,523.42 (session -1.58) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $413.46 | ▼ 09:30 equity $9,412.84 vs yday $9,486.38 (-73.54) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 3 | $310.29 | $2.02 | $-52.59 | $1,342.31 | ▼ -52.59 after sell → book $9,410.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 47 | $25.26 | $2.15 | $+11.23 | $2,527.38 | ▲ +11.23 after sell → book $9,408.67; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 2 | $606.57 | $2.02 | $+41.37 | $3,738.51 | ▲ +41.37 after sell → book $9,406.66; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 920 | $0.58 | $8.10 | — | $3,196.81 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $534.07 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 19 | $28.02 | $2.05 | — | $2,662.38 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $534.07 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,662.38 | ▼ close $9,368.81 vs 09:30 $9,412.84 (session -27.70) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,662.38 | ▲ 09:30 equity $9,630.71 vs yday $9,368.81 (+261.90) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 5 | $266.50 | $2.03 | $+177.22 | $3,992.86 | ▲ +177.22 after sell → book $9,628.69; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FORM` | 9 | $125.39 | $2.04 | $+17.46 | $5,119.33 | ▲ +17.46 after sell → book $9,626.65; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 6 | $174.50 | $2.03 | $-98.84 | $6,164.30 | ▼ -98.84 after sell → book $9,624.62; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 110 | $10.11 | $2.35 | $-70.67 | $7,274.05 | ▼ -70.67 after sell → book $9,622.27; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 4 | $331.78 | $2.02 | $+145.66 | $8,599.15 | ▲ +145.66 after sell → book $9,620.25; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 920 | $0.57 | $8.21 | $-20.91 | $9,119.94 | ▼ -20.91 after sell → book $9,612.04; vs 09:30 mark -8.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 19 | $25.90 | $2.07 | $-44.39 | $9,609.97 | ▼ -44.39 after sell → book $9,609.97; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $8,442.18 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 121 | $9.90 | $2.35 | — | $7,241.93 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $6,076.40 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $4,905.88 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 58 | $20.65 | $2.16 | — | $3,706.02 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 122 | $9.81 | $2.36 | — | $2,506.84 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 60 | $19.70 | $2.17 | — | $1,322.67 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1201.25 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 43 | $27.79 | $2.12 | — | $125.58 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1201.25 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.58 | ▼ close $9,341.37 vs 09:30 $9,630.71 (session -251.38) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.58 | ▼ 09:30 equity $9,293.49 vs yday $9,341.37 (-47.88) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $-22.17 | $1,271.20 | ▼ -22.17 after sell → book $9,291.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 121 | $9.12 | $2.38 | $-99.12 | $2,372.34 | ▼ -99.12 after sell → book $9,289.07; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-27.80 | $3,510.06 | ▼ -27.80 after sell → book $9,287.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $4,630.22 | ▼ -50.36 after sell → book $9,284.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 58 | $20.52 | $2.18 | $-11.89 | $5,818.20 | ▼ -11.89 after sell → book $9,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 122 | $9.67 | $2.39 | $-21.82 | $6,995.55 | ▼ -21.82 after sell → book $9,280.41; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMRX` | 60 | $19.29 | $2.19 | $-28.96 | $8,150.76 | ▼ -28.96 after sell → book $9,278.22; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 43 | $26.22 | $2.14 | $-71.77 | $9,276.08 | ▼ -71.77 after sell → book $9,276.08; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,276.08 | ▲ close $9,276.08 vs 09:30 $9,293.49 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,638.93 | ▲ 09:30 equity $8,638.93 vs yday $8,638.93 (+0.00) | 09:30 open · cash $8,638.93 · no holdings · equity $8,638.93 vs prior close $8,638.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 137 | $7.85 | $2.40 | — | $7,561.08 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 3 | $324.97 | $2.00 | — | $6,584.17 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 269 | $4.00 | $3.47 | — | $5,503.35 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DUOT` | 112 | $9.59 | $2.33 | — | $4,426.95 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $3,436.93 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $2,396.68 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 687 | $1.57 | $8.86 | — | $1,309.23 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PRGO` | 72 | $14.81 | $2.21 | — | $240.70 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.9; leftover $1079.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.70 | ▲ close $8,680.31 vs 09:30 $8,638.93 (session +66.68) | 16:00 close · cash $240.70 · equity $8,680.31 vs 09:30 $8,638.93 (+41.38; session marks +66.68) · 8 name(s) marked open→close (per-name table). RSKD×137 09:30 $7.85 → close $7.78 -9.59; CDNS×3 09:30 $324.97 → close $326.13 +3.48; CYPH×269 09:30 $4.00 → close $4.12 +30.94; DUOT×112 09:30 $9.59 → close $9.14 -50.40; GRAL×8 09:30 $123.50 → close $126.89 +27.12; HALO×9 09:30 $115.36 → close $113.90 -13.14; PACB×687 09:30 $1.57 → close $1.62 +34.35; PRGO×72 09:30 $14.81 → close $15.42 +43.92 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1267.99 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DNN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AGRO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NOK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `APA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CHRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASTH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHEF` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KGS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `META` | cash | leftover split 534.07 < 1 share @ 731.40 |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AKAM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
