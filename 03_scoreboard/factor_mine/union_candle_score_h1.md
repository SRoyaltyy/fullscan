# Factor mine action — `union_candle_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `candle_score` · size `leftover` · sell `list` · S-boost `none` · rank by candle_score

Cash book **-6.32%** ($9,368) · signal-only (no cash/fees) was +8.19%. Starts YES **0/30**. Fills 257 · skips 104 · realized $-563.45.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how clean the prior candles looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how clean the prior candles looked and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `candle_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,929.69.

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
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $7,544.34 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $6,293.15 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,049.62 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $1,349.89 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $107.38 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | ▼ -26.05 after sell → book $10,310.53; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $2,644.85 | ▲ +107.86 after sell → book $10,308.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $3,867.01 | ▼ -29.03 after sell → book $10,306.31; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $5,055.35 | ▼ -55.19 after sell → book $10,304.22; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | ▲ +148.79 after sell → book $10,284.98; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | ▲ +69.58 after sell → book $10,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $8,966.33 | ▼ -7.12 after sell → book $10,280.73; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $10,278.39 | ▲ +69.56 after sell → book $10,278.39; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $9,136.38 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $7,873.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `SATL` | 214 | $5.98 | $2.76 | — | $6,591.26 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.9; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRZE` | 42 | $30.00 | $2.12 | — | $5,329.15 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.2; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 94 | $13.55 | $2.27 | — | $4,053.18 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 129 | $9.89 | $2.38 | — | $2,774.34 | — | rank by candle_score; rank candle_score; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `GLOB` | 33 | $38.21 | $2.09 | — | $1,511.32 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+10.0; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $224.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.74 | ▼ close $10,166.44 vs 09:30 $10,312.70 (session -94.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.74 | ▲ 09:30 equity $10,259.17 vs yday $10,166.44 (+92.73) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $1,352.97 | ▼ -13.79 after sell → book $10,257.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $2,581.31 | ▼ -34.30 after sell → book $10,254.99; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SATL` | 214 | $5.81 | $2.81 | $-41.95 | $3,821.84 | ▼ -41.95 after sell → book $10,252.18; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRZE` | 42 | $28.44 | $2.14 | $-69.77 | $5,014.18 | ▼ -69.77 after sell → book $10,250.04; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 94 | $13.16 | $2.30 | $-41.23 | $6,248.93 | ▼ -41.23 after sell → book $10,247.75; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GLOB` | 33 | $37.18 | $2.11 | $-38.19 | $7,473.76 | ▼ -38.19 after sell → book $10,245.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,828.29 | ▲ +67.96 after sell → book $10,243.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 656 | $1.92 | $8.46 | — | $7,560.31 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1261.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `JBIO` | 51 | $24.60 | $2.14 | — | $6,303.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.5; leftover $1261.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,064.59 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1261.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 124 | $10.10 | $2.36 | — | $3,809.83 | — | rank by candle_score; rank candle_score; list mover_buy; ret5=+22.8; leftover $1261.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 92 | $13.64 | $2.27 | — | $2,552.68 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1261.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 77 | $16.25 | $2.22 | — | $1,299.21 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1261.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 274 | $4.59 | $3.53 | — | $38.01 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1261.18 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.01 | ▼ close $10,059.68 vs 09:30 $10,259.17 (session -160.67) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.01 | ▼ 09:30 equity $9,975.83 vs yday $10,059.68 (-83.85) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 129 | $10.31 | $2.41 | $+48.75 | $1,365.60 | ▲ +48.75 after sell → book $9,973.43; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 656 | $1.70 | $8.58 | $-161.36 | $2,472.21 | ▼ -161.36 after sell → book $9,964.84; vs 09:30 mark -8.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `JBIO` | 51 | $23.07 | $2.16 | $-82.34 | $3,646.62 | ▼ -82.34 after sell → book $9,962.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $4,889.52 | ▲ +3.92 after sell → book $9,960.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 124 | $10.45 | $2.39 | $+38.64 | $6,182.93 | ▲ +38.64 after sell → book $9,958.19; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 92 | $13.31 | $2.29 | $-34.92 | $7,405.16 | ▼ -34.92 after sell → book $9,955.90; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 77 | $16.90 | $2.24 | $+45.58 | $8,704.21 | ▲ +45.58 after sell → book $9,953.65; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 274 | $4.56 | $3.59 | $-15.34 | $9,950.06 | ▼ -15.34 after sell → book $9,950.06; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.06 | ▲ close $9,950.06 vs 09:30 $9,975.83 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.06 | ▲ 09:30 equity $9,950.06 vs yday $9,950.06 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,950.06 | ▲ close $9,950.06 vs 09:30 $9,950.06 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.06 | ▲ 09:30 equity $9,950.06 vs yday $9,950.06 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 18 | $65.60 | $2.04 | — | $8,767.22 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `NBP` | 631 | $1.97 | $8.14 | — | $7,516.01 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ret5=+5.9; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 95 | $12.98 | $2.27 | — | $6,280.63 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 105 | $11.81 | $2.31 | — | $5,037.75 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,834.62 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 252 | $4.92 | $3.25 | — | $2,591.53 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1081 | $1.15 | $13.94 | — | $1,334.43 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1243.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `GENB` | 74 | $16.76 | $2.21 | — | $91.98 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.5; leftover $1243.76 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.98 | ▼ close $9,738.35 vs 09:30 $9,950.06 (session -175.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.98 | ▲ 09:30 equity $10,006.22 vs yday $9,738.35 (+267.87) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 18 | $68.41 | $2.06 | $+46.47 | $1,321.30 | ▲ +46.47 after sell → book $10,004.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NBP` | 631 | $1.91 | $8.25 | $-54.25 | $2,518.25 | ▼ -54.25 after sell → book $9,995.90; vs 09:30 mark -8.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 95 | $13.36 | $2.30 | $+31.52 | $3,785.15 | ▲ +31.52 after sell → book $9,993.60; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 105 | $11.57 | $2.33 | $-30.36 | $4,997.67 | ▼ -30.36 after sell → book $9,991.27; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $6,060.52 | ▼ -140.29 after sell → book $9,989.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 252 | $5.20 | $3.30 | $+64.01 | $7,367.61 | ▲ +64.01 after sell → book $9,985.93; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `GENB` | 74 | $16.10 | $2.23 | $-53.29 | $8,556.78 | ▼ -53.29 after sell → book $9,983.70; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 32 | $37.81 | $2.09 | — | $7,344.77 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $1222.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 134 | $9.08 | $2.39 | — | $6,125.66 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1222.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARIS` | 58 | $20.90 | $2.16 | — | $4,911.30 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1222.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $3,695.81 | — | rank by candle_score; rank candle_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1222.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 35 | $34.89 | $2.10 | — | $2,472.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1222.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 5 | $212.40 | $2.00 | — | $1,408.56 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1222.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $325.06 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1222.40 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $325.06 | ▲ close $10,219.61 vs 09:30 $10,006.22 (session +250.98) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $325.06 | ▲ 09:30 equity $10,550.89 vs yday $10,219.61 (+331.27) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1081 | $1.83 | $14.14 | $+706.99 | $2,289.14 | ▲ +706.99 after sell → book $10,536.74; vs 09:30 mark -14.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 32 | $36.61 | $2.11 | $-42.59 | $3,458.56 | ▼ -42.59 after sell → book $10,534.64; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 134 | $8.08 | $2.42 | $-138.82 | $4,538.85 | ▼ -138.82 after sell → book $10,532.21; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARIS` | 58 | $20.98 | $2.18 | $+0.29 | $5,753.51 | ▲ +0.29 after sell → book $10,530.03; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $7,204.13 | ▲ +235.14 after sell → book $10,527.68; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 35 | $33.10 | $2.12 | $-66.86 | $8,360.52 | ▼ -66.86 after sell → book $10,525.57; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 5 | $215.98 | $2.02 | $+13.87 | $9,438.39 | ▲ +13.87 after sell → book $10,523.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $10,521.52 | ▼ -0.38 after sell → book $10,521.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.52 | ▲ close $10,521.52 vs 09:30 $10,550.89 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.52 | ▲ 09:30 equity $10,521.52 vs yday $10,521.52 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 36 | $36.52 | $2.10 | — | $9,204.70 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+7.9; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 149 | $8.79 | $2.44 | — | $7,892.55 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 15 | $83.15 | $2.04 | — | $6,643.27 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+14.5; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 250 | $5.24 | $3.23 | — | $5,330.04 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `B` | 27 | $47.52 | $2.07 | — | $4,044.93 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.3; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 55 | $23.80 | $2.15 | — | $2,733.78 | — | rank by candle_score; rank candle_score; list yday_gainer; ret5=+28.9; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `BRZE` | 42 | $30.69 | $2.12 | — | $1,442.68 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.5; leftover $1315.19 | — |
| 2026-08-25 09:30 ET | **BUY** | `CELH` | 37 | $35.23 | $2.10 | — | $137.07 | — | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+17.0; leftover $1315.19 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.07 | ▲ close $10,673.50 vs 09:30 $10,521.52 (session +170.22) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.07 | ▼ 09:30 equity $10,526.49 vs yday $10,673.50 (-147.01) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ANRO` | 36 | $35.80 | $2.12 | $-30.14 | $1,423.75 | ▼ -30.14 after sell → book $10,524.37; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WIX` | 15 | $84.02 | $2.06 | $+8.96 | $2,682.00 | ▲ +8.96 after sell → book $10,522.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 250 | $4.98 | $3.28 | $-71.50 | $3,923.72 | ▼ -71.50 after sell → book $10,519.04; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `B` | 27 | $48.18 | $2.09 | $+13.66 | $5,222.49 | ▲ +13.66 after sell → book $10,516.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMNR` | 55 | $24.24 | $2.18 | $+19.87 | $6,553.51 | ▲ +19.87 after sell → book $10,514.77; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BRZE` | 42 | $29.95 | $2.14 | $-35.33 | $7,809.28 | ▼ -35.33 after sell → book $10,512.64; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CELH` | 37 | $35.25 | $2.12 | $-3.48 | $9,111.41 | ▼ -3.48 after sell → book $10,510.52; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 92 | $14.00 | $2.27 | — | $7,821.14 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+17.8; leftover $1301.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 112 | $11.59 | $2.33 | — | $6,521.29 | — | rank by candle_score; rank candle_score; list overnight; 🔵; ret5=+64.9; leftover $1301.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `VIR` | 122 | $10.60 | $2.36 | — | $5,225.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.9; leftover $1301.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `NEM` | 9 | $132.64 | $2.02 | — | $4,029.96 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.5; leftover $1301.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `AQST` | 256 | $5.08 | $3.30 | — | $2,726.18 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+17.6; leftover $1301.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 116 | $11.22 | $2.34 | — | $1,422.32 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.8; leftover $1301.63 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 591 | $2.20 | $7.62 | — | $114.50 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+17.8; leftover $1301.63 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.50 | ▲ close $10,500.25 vs 09:30 $10,526.49 (session +11.96) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.50 | ▲ 09:30 equity $10,551.66 vs yday $10,500.25 (+51.41) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 149 | $9.41 | $2.47 | $+87.47 | $1,514.11 | ▲ +87.47 after sell → book $10,549.18; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 92 | $12.56 | $2.29 | $-137.04 | $2,667.34 | ▼ -137.04 after sell → book $10,546.89; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 112 | $12.18 | $2.36 | $+61.96 | $4,029.15 | ▲ +61.96 after sell → book $10,544.54; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIR` | 122 | $11.00 | $2.39 | $+44.06 | $5,368.76 | ▲ +44.06 after sell → book $10,542.15; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEM` | 9 | $131.02 | $2.04 | $-18.63 | $6,545.90 | ▼ -18.63 after sell → book $10,540.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 116 | $11.38 | $2.37 | $+13.85 | $7,863.62 | ▲ +13.85 after sell → book $10,537.75; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 591 | $2.19 | $7.73 | $-21.27 | $9,150.17 | ▼ -21.27 after sell → book $10,530.01; vs 09:30 mark -7.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GALT` | 314 | $4.15 | $4.05 | — | $7,843.02 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.1; leftover $1307.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 70 | $18.50 | $2.20 | — | $6,545.82 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $1307.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 5 | $235.94 | $2.00 | — | $5,364.12 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+7.6; leftover $1307.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `DJT` | 136 | $9.59 | $2.40 | — | $4,058.16 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.8; leftover $1307.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `PD` | 104 | $12.45 | $2.30 | — | $2,761.06 | — | rank by candle_score; rank candle_score; list overnight; 🔵; ret5=+0.2; leftover $1307.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 21 | $60.00 | $2.05 | — | $1,499.00 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+6.2; leftover $1307.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `PRGO` | 89 | $14.63 | $2.26 | — | $194.68 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+5.7; leftover $1307.17 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.68 | ▼ close $10,400.68 vs 09:30 $10,551.66 (session -112.07) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.68 | ▲ 09:30 equity $10,411.19 vs yday $10,400.68 (+10.51) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 256 | $5.11 | $3.36 | $+1.02 | $1,499.48 | ▲ +1.02 after sell → book $10,407.83; vs 09:30 mark -3.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GALT` | 314 | $4.14 | $4.11 | $-11.30 | $2,795.33 | ▼ -11.30 after sell → book $10,403.72; vs 09:30 mark -4.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 70 | $18.15 | $2.22 | $-28.92 | $4,063.61 | ▼ -28.92 after sell → book $10,401.50; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 5 | $233.37 | $2.02 | $-16.88 | $5,228.43 | ▼ -16.88 after sell → book $10,399.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DJT` | 136 | $9.72 | $2.43 | $+13.53 | $6,547.92 | ▲ +13.53 after sell → book $10,397.04; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PD` | 104 | $13.09 | $2.33 | $+61.93 | $7,906.95 | ▲ +61.93 after sell → book $10,394.71; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 21 | $58.75 | $2.07 | $-30.38 | $9,138.63 | ▼ -30.38 after sell → book $10,392.64; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PRGO` | 89 | $14.09 | $2.28 | $-52.60 | $10,390.36 | ▼ -52.60 after sell → book $10,390.36; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 12 | $106.99 | $2.03 | — | $9,104.45 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+10.5; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIG` | 43 | $30.18 | $2.12 | — | $7,804.59 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 10 | $129.46 | $2.02 | — | $6,507.97 | — | rank by candle_score; rank candle_score; list overnight; ret5=+2.1; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $5,421.97 | — | rank by candle_score; rank candle_score; list earn_react; ret5=+4.8; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 31 | $41.74 | $2.08 | — | $4,125.95 | — | rank by candle_score; rank candle_score; list flatten; ret5=+2.4; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `BRZE` | 38 | $34.06 | $2.10 | — | $2,829.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.0; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `CXM` | 164 | $7.88 | $2.48 | — | $1,534.77 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+10.3; leftover $1298.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 70 | $18.36 | $2.20 | — | $247.37 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.8; leftover $1298.79 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.37 | ▼ close $10,221.05 vs 09:30 $10,411.19 (session -152.28) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.37 | ▲ 09:30 equity $10,291.59 vs yday $10,221.05 (+70.54) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `EL` | 12 | $102.70 | $2.05 | $-55.55 | $1,477.72 | ▼ -55.55 after sell → book $10,289.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIG` | 43 | $27.60 | $2.14 | $-115.20 | $2,662.38 | ▼ -115.20 after sell → book $10,287.40; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAIC` | 10 | $140.39 | $2.04 | $+105.24 | $4,064.24 | ▲ +105.24 after sell → book $10,285.36; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $5,104.42 | ▼ -45.81 after sell → book $10,283.34; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 31 | $42.00 | $2.10 | $+3.87 | $6,404.32 | ▲ +3.87 after sell → book $10,281.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BRZE` | 38 | $34.03 | $2.12 | $-5.37 | $7,695.34 | ▼ -5.37 after sell → book $10,279.12; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CXM` | 164 | $8.17 | $2.52 | $+42.56 | $9,032.70 | ▲ +42.56 after sell → book $10,276.60; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 70 | $17.77 | $2.22 | $-45.72 | $10,274.37 | ▼ -45.72 after sell → book $10,274.37; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,274.37 | ▲ close $10,274.37 vs 09:30 $10,291.59 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,274.37 | ▲ 09:30 equity $10,274.37 vs yday $10,274.37 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,274.37 | ▲ close $10,274.37 vs 09:30 $10,274.37 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,274.37 | ▲ 09:30 equity $10,274.37 vs yday $10,274.37 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,274.37 | ▲ close $10,274.37 vs 09:30 $10,274.37 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,274.37 | ▲ 09:30 equity $10,274.37 vs yday $10,274.37 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 14 | $90.24 | $2.03 | — | $9,008.98 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 192 | $6.68 | $2.57 | — | $7,723.86 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.4; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $6,442.74 | — | rank by candle_score; rank candle_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 50 | $25.62 | $2.14 | — | $5,159.35 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.1; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 80 | $15.87 | $2.23 | — | $3,887.52 | — | rank by candle_score; rank candle_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 175 | $7.31 | $2.52 | — | $2,605.75 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+18.5; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $1,329.01 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1284.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 346 | $3.71 | $4.46 | — | $40.89 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+12.3; leftover $1284.30 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.89 | ▼ close $10,160.63 vs 09:30 $10,274.37 (session -93.56) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.89 | ▼ 09:30 equity $9,981.07 vs yday $10,160.63 (-179.56) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 14 | $87.64 | $2.05 | $-40.48 | $1,265.80 | ▼ -40.48 after sell → book $9,979.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 192 | $6.84 | $2.61 | $+25.55 | $2,576.47 | ▲ +25.55 after sell → book $9,976.41; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 10 | $125.22 | $2.04 | $-30.96 | $3,826.63 | ▼ -30.96 after sell → book $9,974.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SION` | 175 | $6.68 | $2.55 | $-115.32 | $4,993.08 | ▼ -115.32 after sell → book $9,971.82; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $15.61 | $2.24 | $-92.62 | $6,177.20 | ▼ -92.62 after sell → book $9,969.58; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 346 | $3.53 | $4.53 | $-71.27 | $7,394.04 | ▼ -71.27 after sell → book $9,965.04; vs 09:30 mark -4.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 212 | $5.79 | $2.73 | — | $6,163.83 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1232.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `PAGS` | 123 | $9.96 | $2.36 | — | $4,936.39 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.5; leftover $1232.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `TTD` | 81 | $15.18 | $2.23 | — | $3,704.58 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.4; leftover $1232.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.70 | $2.03 | — | $2,544.75 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1232.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 32 | $37.44 | $2.09 | — | $1,344.58 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1232.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 37 | $32.65 | $2.10 | — | $134.43 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+8.1; leftover $1232.34 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.43 | ▲ close $10,047.72 vs 09:30 $9,981.07 (session +96.22) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.43 | ▼ 09:30 equity $10,019.54 vs yday $10,047.72 (-28.18) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 50 | $26.44 | $2.16 | $+36.45 | $1,454.27 | ▲ +36.45 after sell → book $10,017.38; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 80 | $16.74 | $2.25 | $+65.12 | $2,791.21 | ▲ +65.12 after sell → book $10,015.12; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 212 | $5.81 | $2.78 | $-1.27 | $4,020.15 | ▼ -1.27 after sell → book $10,012.34; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PAGS` | 123 | $9.91 | $2.39 | $-10.90 | $5,236.69 | ▼ -10.90 after sell → book $10,009.95; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TTD` | 81 | $14.32 | $2.26 | $-74.15 | $6,394.36 | ▼ -74.15 after sell → book $10,007.70; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+93.50 | $7,647.69 | ▲ +93.50 after sell → book $10,005.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 32 | $37.75 | $2.11 | $+5.73 | $8,853.58 | ▲ +5.73 after sell → book $10,003.54; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 37 | $31.08 | $2.12 | $-62.31 | $10,001.42 | ▼ -62.31 after sell → book $10,001.42; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,001.42 | ▲ close $10,001.42 vs 09:30 $10,019.54 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,001.42 | ▲ 09:30 equity $10,001.42 vs yday $10,001.42 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,001.42 | ▲ close $10,001.42 vs 09:30 $10,001.42 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,001.42 | ▲ 09:30 equity $10,001.42 vs yday $10,001.42 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,001.42 | ▲ close $10,001.42 vs 09:30 $10,001.42 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,001.42 | ▲ 09:30 equity $10,001.42 vs yday $10,001.42 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 232 | $5.38 | $2.99 | — | $8,750.27 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+19.8; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `SION` | 160 | $7.79 | $2.47 | — | $7,501.40 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+4.2; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `MYGN` | 370 | $3.37 | $4.77 | — | $6,249.72 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+4.0; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `PUMP` | 108 | $11.57 | $2.31 | — | $4,997.85 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+5.9; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 399 | $3.13 | $5.15 | — | $3,743.83 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `HAFN` | 134 | $9.32 | $2.39 | — | $2,492.56 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+5.4; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 68 | $18.30 | $2.19 | — | $1,245.97 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1250.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 11 | $112.83 | $2.02 | — | $2.76 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1250.18 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.76 | ▲ close $10,218.32 vs 09:30 $10,001.42 (session +241.21) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.76 | ▼ 09:30 equity $10,176.50 vs yday $10,218.32 (-41.82) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 232 | $5.57 | $3.04 | $+38.05 | $1,291.96 | ▲ +38.05 after sell → book $10,173.46; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SION` | 160 | $7.84 | $2.51 | $+3.02 | $2,543.85 | ▲ +3.02 after sell → book $10,170.95; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `HAFN` | 134 | $9.35 | $2.42 | $-0.80 | $3,794.33 | ▼ -0.80 after sell → book $10,168.53; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 68 | $18.28 | $2.22 | $-5.77 | $5,035.15 | ▼ -5.77 after sell → book $10,166.31; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `QRVO` | 11 | $114.11 | $2.04 | $+9.96 | $6,288.32 | ▲ +9.96 after sell → book $10,164.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,288.32 | ▲ close $10,276.98 vs 09:30 $10,176.50 (session +112.71) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,288.32 | ▲ 09:30 equity $10,287.16 vs yday $10,276.98 (+10.18) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `MYGN` | 370 | $3.80 | $4.85 | $+149.48 | $7,689.47 | ▲ +149.48 after sell → book $10,282.31; vs 09:30 mark -4.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 399 | $3.64 | $5.22 | $+193.12 | $9,136.61 | ▲ +193.12 after sell → book $10,277.09; vs 09:30 mark -5.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,136.61 | ▼ close $10,260.89 vs 09:30 $10,287.16 (session -16.20) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,136.61 | ▼ 09:30 equity $10,259.81 vs yday $10,260.89 (-1.08) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 46 | $28.16 | $2.13 | — | $7,839.12 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1305.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `INDP` | 356 | $3.66 | $4.59 | — | $6,531.57 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+96.8; leftover $1305.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `TXG` | 17 | $74.50 | $2.04 | — | $5,263.03 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.4; leftover $1305.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 48 | $27.09 | $2.13 | — | $3,960.57 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1305.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 11 | $118.18 | $2.02 | — | $2,658.57 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1305.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $1,405.22 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1305.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 24 | $52.52 | $2.06 | — | $142.67 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+10.7; leftover $1305.23 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.67 | ▼ close $9,989.50 vs 09:30 $10,259.81 (session -253.29) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.67 | ▲ 09:30 equity $10,164.80 vs yday $9,989.50 (+175.30) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 46 | $28.59 | $2.15 | $+15.73 | $1,455.90 | ▲ +15.73 after sell → book $10,162.66; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 356 | $3.30 | $4.66 | $-137.41 | $2,626.03 | ▼ -137.41 after sell → book $10,157.99; vs 09:30 mark -4.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TXG` | 17 | $75.38 | $2.06 | $+10.86 | $3,905.43 | ▲ +10.86 after sell → book $10,155.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 11 | $114.90 | $2.04 | $-40.15 | $5,167.29 | ▼ -40.15 after sell → book $10,153.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $-40.76 | $6,379.88 | ▼ -40.76 after sell → book $10,151.84; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 24 | $54.31 | $2.08 | $+38.82 | $7,681.24 | ▲ +38.82 after sell → book $10,149.76; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 124 | $10.25 | $2.36 | — | $6,407.87 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1280.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 1196 | $1.07 | $15.43 | — | $5,112.73 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $1280.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $3,914.76 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1280.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 876 | $1.46 | $11.30 | — | $2,624.50 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+4.4; leftover $1280.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `HLP` | 609 | $2.10 | $7.86 | — | $1,337.75 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+60.5; leftover $1280.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $154.85 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ret5=+17.7; leftover $1280.21 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.85 | ▼ close $10,090.76 vs 09:30 $10,164.80 (session -18.02) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.85 | ▲ 09:30 equity $10,128.60 vs yday $10,090.76 (+37.84) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 108 | $10.48 | $2.34 | $-122.38 | $1,284.35 | ▼ -122.38 after sell → book $10,126.26; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 48 | $28.55 | $2.15 | $+65.79 | $2,652.60 | ▲ +65.79 after sell → book $10,124.11; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 124 | $10.12 | $2.39 | $-20.87 | $3,905.08 | ▼ -20.87 after sell → book $10,121.71; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IQ` | 1196 | $1.12 | $15.64 | $+28.73 | $5,228.97 | ▲ +28.73 after sell → book $10,106.08; vs 09:30 mark -15.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $6,503.25 | ▲ +76.32 after sell → book $10,104.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AIB` | 876 | $1.41 | $11.46 | $-66.56 | $7,726.95 | ▼ -66.56 after sell → book $10,092.59; vs 09:30 mark -11.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 609 | $1.96 | $7.97 | $-101.08 | $8,912.62 | ▼ -101.08 after sell → book $10,084.62; vs 09:30 mark -7.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $10,082.59 | ▼ -12.93 after sell → book $10,082.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 5 | $246.98 | $2.00 | — | $8,845.68 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `NEO` | 63 | $19.91 | $2.18 | — | $7,589.17 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.9; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $6,355.62 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `INDP` | 327 | $3.85 | $4.22 | — | $5,092.45 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+53.5; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 15 | $81.40 | $2.04 | — | $3,869.41 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $2,677.38 | — | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 55 | $22.90 | $2.15 | — | $1,415.73 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1260.32 | — |
| 2026-09-18 09:30 ET | **BUY** | `ATRC` | 21 | $58.51 | $2.05 | — | $184.96 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $1260.32 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $184.96 | ▼ close $9,829.43 vs 09:30 $10,128.60 (session -234.36) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.96 | ▼ 09:30 equity $9,816.48 vs yday $9,829.43 (-12.95) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-80.83 | $1,341.04 | ▼ -80.83 after sell → book $9,814.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `NEO` | 63 | $19.92 | $2.20 | $-3.75 | $2,593.80 | ▼ -3.75 after sell → book $9,812.26; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+0.37 | $3,827.72 | ▲ +0.37 after sell → book $9,810.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 327 | $3.55 | $4.28 | $-106.60 | $4,984.29 | ▼ -106.60 after sell → book $9,805.84; vs 09:30 mark -4.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 15 | $79.08 | $2.06 | $-38.89 | $6,168.44 | ▼ -38.89 after sell → book $9,803.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $7,326.00 | ▼ -34.46 after sell → book $9,801.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 55 | $22.78 | $2.17 | $-10.93 | $8,576.73 | ▼ -10.93 after sell → book $9,799.56; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 21 | $58.23 | $2.07 | $-10.01 | $9,797.49 | ▼ -10.01 after sell → book $9,797.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,690.39 | — | rank by candle_score; rank candle_score; list flatten; ret5=+6.5; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $7,533.57 | — | rank by candle_score; rank candle_score; list flatten; ret5=+7.6; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 14 | $83.53 | $2.03 | — | $6,362.11 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+8.8; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `TRMD` | 32 | $37.47 | $2.09 | — | $5,160.99 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+9.0; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 49 | $24.93 | $2.14 | — | $3,937.28 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+8.7; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `PUMP` | 117 | $10.40 | $2.34 | — | $2,718.14 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+5.9; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 495 | $2.47 | $6.39 | — | $1,489.11 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $1224.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 72 | $16.91 | $2.21 | — | $269.38 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $1224.69 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.38 | ▲ close $9,881.88 vs 09:30 $9,816.48 (session +105.62) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.38 | ▼ 09:30 equity $9,873.55 vs yday $9,881.88 (-8.33) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 49 | $25.26 | $2.16 | $+11.88 | $1,504.96 | ▲ +11.88 after sell → book $9,871.39; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 43 | $4.30 | $1.98 | — | $1,318.08 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+16.9; leftover $188.12 | — |
| 2026-09-22 09:30 ET | **BUY** | `SECZ` | 14 | $12.96 | $1.86 | — | $1,134.79 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+64.4; leftover $188.12 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,134.79 | ▼ close $9,864.68 vs 09:30 $9,873.55 (session -2.88) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,134.79 | ▲ 09:30 equity $10,000.05 vs yday $9,864.68 (+135.37) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 14 | $86.57 | $2.05 | $+38.48 | $2,344.72 | ▲ +38.48 after sell → book $9,998.00; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TRMD` | 32 | $34.83 | $2.11 | $-88.67 | $3,457.17 | ▼ -88.67 after sell → book $9,995.89; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `PUMP` | 117 | $10.10 | $2.37 | $-39.81 | $4,636.50 | ▼ -39.81 after sell → book $9,993.52; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 72 | $16.92 | $2.23 | $-3.71 | $5,852.51 | ▼ -3.71 after sell → book $9,991.29; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 14 | $12.80 | $1.85 | $-5.95 | $6,029.86 | ▼ -5.95 after sell → book $9,989.44; vs 09:30 mark -1.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 81 | $18.57 | $2.23 | — | $4,523.05 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.2; leftover $1507.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 335 | $4.49 | $4.32 | — | $3,014.58 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $1507.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 21 | $70.84 | $2.05 | — | $1,524.88 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1507.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `HYLN` | 335 | $4.49 | $4.32 | — | $16.41 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.6; leftover $1507.46 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.41 | ▼ close $9,555.31 vs 09:30 $10,000.05 (session -421.19) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.41 | ▼ 09:30 equity $9,451.52 vs yday $9,555.31 (-103.79) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,162.03 | ▲ +38.52 after sell → book $9,449.49; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $2,299.76 | ▼ -19.09 after sell → book $9,447.44; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 495 | $2.68 | $6.48 | $+91.09 | $3,619.88 | ▲ +91.09 after sell → book $9,440.97; vs 09:30 mark -6.47 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 43 | $4.12 | $1.92 | $-11.64 | $3,795.12 | ▼ -11.64 after sell → book $9,439.04; vs 09:30 mark -1.93 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 335 | $3.92 | $4.39 | $-197.98 | $5,105.61 | ▼ -197.98 after sell → book $9,434.66; vs 09:30 mark -4.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 21 | $70.50 | $2.07 | $-11.27 | $6,584.03 | ▼ -11.27 after sell → book $9,432.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HYLN` | 335 | $4.03 | $4.39 | $-162.81 | $7,929.69 | ▼ -162.81 after sell → book $9,428.19; vs 09:30 mark -4.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,929.69 | ▲ close $9,433.86 vs 09:30 $9,451.52 (session +5.67) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,510.39 | ▲ 09:30 equity $9,510.39 vs yday $9,510.39 (+0.00) | 09:30 open · cash $9,510.39 · no holdings · equity $9,510.39 vs prior close $9,510.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `AMD` | 1 | $634.53 | $1.99 | — | $8,873.86 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.4; leftover $1188.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `A` | 6 | $171.98 | $2.01 | — | $7,839.97 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+10.6; leftover $1188.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NTRA` | 2 | $410.00 | $2.00 | — | $7,017.98 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $1188.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CLS` | 3 | $380.51 | $2.00 | — | $5,874.45 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $1188.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DGXX` | 248 | $4.78 | $3.20 | — | $4,685.81 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+18.0; leftover $1188.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 115 | $10.28 | $2.33 | — | $3,501.27 | — | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+9.7; leftover $1188.80 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 540 | $2.20 | $6.97 | — | $2,306.31 | — | rank by candle_score; rank candle_score; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1188.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CURI` | 382 | $3.11 | $4.93 | — | $1,113.36 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ret5=+11.6; leftover $1188.80 | join🟢 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,113.36 | ▼ close $9,368.09 vs 09:30 $9,510.39 (session -116.87) | 16:00 close · cash $1,113.36 · equity $9,368.09 vs 09:30 $9,510.39 (-142.30; session marks -116.87) · 8 name(s) marked open→close (per-name table). AMD×1 09:30 $634.53 → close $630.63 -3.90; A×6 09:30 $171.98 → close $172.79 +4.86; NTRA×2 09:30 $410.00 → close $412.56 +5.12; CLS×3 09:30 $380.51 → close $365.44 -45.21; DGXX×248 09:30 $4.78 → close $4.59 -47.12; SENS×115 09:30 $10.28 → close $10.00 -32.20; HLP×540 09:30 $2.20 → close $2.21 +5.40; CURI×382 09:30 $3.11 → close $3.10 -3.82 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CERS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYMR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSKY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RDZN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TEAM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KVYO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZETA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LPG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PUMP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLMT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SWKS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INDP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INDP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TRMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PUMP` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARM` | cash | leftover split 188.12 < 1 share @ 319.41 |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `RBRK` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RBRK` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `HLP` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NTSK` | 81 | 2026-09-23 @ $18.57 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.2; leftover $1507.46 |
