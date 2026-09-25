# Factor mine action — `union_w_hot_candle_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_candle` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_candle

Cash book **+4.85%** ($10,485) · signal-only (no cash/fees) was +319.57%. Starts YES **29/30**. Fills 194 · skips 289 · realized $+901.34.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: a mix of tape-heat and prior candles.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by a mix of tape-heat and prior candles and keep the top 8.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_candle` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,128.75.

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
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,517.83 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $5,049.62 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $1,305.43 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.80 | ▲ close $10,514.44 vs 09:30 $10,312.70 (session +202.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.80 | ▼ 09:30 equity $10,487.00 vs yday $10,514.44 (-27.44) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $74.33 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $10.35 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 1 | $10.10 | $0.10 | — | $64.12 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $10.35 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 5 | $1.92 | $0.11 | — | $54.41 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $10.35 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $47.47 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $10.35 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.47 | ▲ close $10,588.08 vs 09:30 $10,487.00 (session +101.45) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.47 | ▼ 09:30 equity $10,444.16 vs yday $10,588.08 (-143.92) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,219.78 | ▼ -66.33 after sell → book $10,441.99; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,393.81 | ▼ -69.50 after sell → book $10,439.90; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,634.21 | ▲ +23.38 after sell → book $10,437.82; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $4,801.77 | ▼ -83.63 after sell → book $10,435.68; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $6,540.62 | ▲ +471.89 after sell → book $10,415.51; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $7,816.36 | ▲ +41.02 after sell → book $10,413.33; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $9,155.98 | ▲ +97.12 after sell → book $10,410.99; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,353.91 | ▼ -0.12 after sell → book $10,408.92; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,353.91 | ▲ close $10,409.90 vs 09:30 $10,444.16 (session +0.98) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,353.91 | ▲ 09:30 equity $10,410.11 vs yday $10,409.90 (+0.21) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,365.82 | ▼ -1.45 after sell → book $10,409.96; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,374.80 | ▼ -2.25 after sell → book $10,409.84; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,374.80 | ▲ close $10,410.51 vs 09:30 $10,410.11 (session +0.67) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,374.80 | ▼ 09:30 equity $10,409.58 vs yday $10,410.51 (-0.93) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,382.89 | ▼ -0.38 after sell → book $10,409.47; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `SMJF` | 1 | $10.72 | $0.13 | $+0.39 | $10,393.48 | ▲ +0.39 after sell → book $10,409.34; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 5 | $1.64 | $0.12 | $-1.63 | $10,401.57 | ▼ -1.63 after sell → book $10,409.23; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,409.13 | ▲ +0.62 after sell → book $10,409.13; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,205.99 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,890.75 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,588.78 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 146 | $8.91 | $2.43 | — | $5,285.49 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 542 | $2.40 | $6.99 | — | $3,977.70 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 196 | $6.61 | $2.58 | — | $2,680.54 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 100 | $12.98 | $2.29 | — | $1,380.25 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 65 | $20.00 | $2.19 | — | $78.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1301.14 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.07 | ▼ close $10,151.66 vs 09:30 $10,409.58 (session -222.07) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.07 | ▲ 09:30 equity $10,459.07 vs yday $10,151.66 (+307.41) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 2 | $4.49 | $0.10 | — | $68.99 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $13.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $57.75 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $48.58 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $13.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 3 | $4.04 | $0.13 | — | $36.33 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $13.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 1 | $8.28 | $0.09 | — | $27.96 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $13.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 2 | $6.42 | $0.13 | — | $14.99 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $13.01 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.99 | ▲ close $10,732.92 vs 09:30 $10,459.07 (session +274.50) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.99 | ▲ 09:30 equity $11,115.42 vs yday $10,732.92 (+382.50) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.99 | ▼ close $10,849.54 vs 09:30 $11,115.42 (session -265.88) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.99 | ▼ 09:30 equity $10,802.94 vs yday $10,849.54 (-46.60) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $1,160.95 | ▼ -57.17 after sell → book $10,800.90; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $11.00 | $2.35 | $-94.32 | $2,368.60 | ▼ -94.32 after sell → book $10,798.55; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 146 | $9.36 | $2.46 | $+60.81 | $3,732.70 | ▲ +60.81 after sell → book $10,796.09; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALEC` | 542 | $2.32 | $7.09 | $-57.44 | $4,983.05 | ▼ -57.44 after sell → book $10,789.00; vs 09:30 mark -7.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 196 | $6.75 | $2.62 | $+23.22 | $6,303.43 | ▲ +23.22 after sell → book $10,786.38; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IMMX` | 100 | $13.60 | $2.32 | $+57.39 | $7,661.11 | ▲ +57.39 after sell → book $10,784.06; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BBNX` | 65 | $19.92 | $2.21 | $-9.59 | $8,953.70 | ▼ -9.59 after sell → book $10,781.85; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 61 | $24.11 | $2.17 | — | $7,480.82 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+891.7; leftover $1492.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 78 | $19.04 | $2.22 | — | $5,993.48 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+49.5; leftover $1492.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 284 | $5.24 | $3.66 | — | $4,501.65 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1492.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 169 | $8.79 | $2.50 | — | $3,013.65 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1492.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 62 | $23.80 | $2.18 | — | $1,535.87 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+28.9; leftover $1492.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 420 | $3.55 | $5.42 | — | $39.45 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+27.9; leftover $1492.28 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.45 | ▲ close $11,539.83 vs 09:30 $10,802.94 (session +776.13) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.45 | ▼ 09:30 equity $11,241.86 vs yday $11,539.83 (-297.97) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,834.26 | ▲ +479.57 after sell → book $11,227.07; vs 09:30 mark -14.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,849.43 | ▲ +3.93 after sell → book $11,226.89; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $1,857.67 | ▼ -0.94 after sell → book $11,226.79; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `DFDV` | 3 | $4.35 | $0.16 | $+0.64 | $1,870.56 | ▲ +0.64 after sell → book $11,226.63; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MRVI` | 1 | $8.85 | $0.11 | $+0.37 | $1,879.30 | ▲ +0.37 after sell → book $11,226.52; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `XXI` | 2 | $6.36 | $0.15 | $-0.41 | $1,891.86 | ▼ -0.41 after sell → book $11,226.36; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 22 | $14.11 | $2.06 | — | $1,579.39 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.4; leftover $315.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 54 | $5.81 | $2.15 | — | $1,263.49 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $315.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 27 | $11.59 | $2.07 | — | $948.63 | — | rank by w_hot_candle; rank w_hot_candle; list overnight; 🔵; ret5=+64.9; leftover $315.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 22 | $14.00 | $2.06 | — | $638.57 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.8; leftover $315.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 7 | $40.50 | $2.01 | — | $353.06 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.8; leftover $315.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 28 | $11.22 | $2.07 | — | $36.83 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+16.8; leftover $315.31 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.83 | ▼ close $11,178.92 vs 09:30 $11,241.86 (session -35.03) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.83 | ▲ 09:30 equity $11,316.15 vs yday $11,178.92 (+137.23) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.83 | ▼ close $11,311.67 vs 09:30 $11,316.15 (session -4.48) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.83 | ▼ 09:30 equity $11,171.86 vs yday $11,311.67 (-139.81) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 2 | $3.69 | $0.10 | $-1.80 | $44.11 | ▼ -1.80 after sell → book $11,171.76; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 61 | $23.40 | $2.19 | $-47.68 | $1,469.31 | ▼ -47.68 after sell → book $11,169.57; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 78 | $22.50 | $2.25 | $+265.41 | $3,222.06 | ▲ +265.41 after sell → book $11,167.32; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 284 | $4.84 | $3.72 | $-120.99 | $4,592.90 | ▼ -120.99 after sell → book $11,163.60; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 169 | $9.08 | $2.54 | $+43.98 | $6,124.88 | ▲ +43.98 after sell → book $11,161.06; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 62 | $25.10 | $2.20 | $+76.23 | $7,678.88 | ▲ +76.23 after sell → book $11,158.86; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 420 | $3.80 | $5.50 | $+94.08 | $9,269.38 | ▲ +94.08 after sell → book $11,153.36; vs 09:30 mark -5.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 136 | $9.73 | $2.40 | — | $7,943.71 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+47.1; leftover $1324.20 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $6,627.06 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1324.20 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 72 | $18.36 | $2.21 | — | $5,302.93 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.8; leftover $1324.20 | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 12 | $106.99 | $2.03 | — | $4,017.03 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+10.5; leftover $1324.20 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 145 | $9.13 | $2.42 | — | $2,690.75 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+20.0; leftover $1324.20 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIG` | 43 | $30.18 | $2.12 | — | $1,390.89 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1324.20 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 56 | $23.30 | $2.16 | — | $83.94 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+14.5; leftover $1324.20 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.94 | ▼ close $10,915.90 vs 09:30 $11,171.86 (session -222.12) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.94 | ▼ 09:30 equity $10,791.09 vs yday $10,915.90 (-124.81) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `PURR` | 27 | $12.19 | $2.09 | $+12.04 | $410.84 | ▲ +12.04 after sell → book $10,789.00; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 22 | $12.77 | $2.08 | $-31.19 | $689.70 | ▼ -31.19 after sell → book $10,786.92; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 7 | $35.77 | $2.03 | $-37.15 | $938.06 | ▼ -37.15 after sell → book $10,784.89; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 28 | $11.80 | $2.09 | $+12.07 | $1,266.37 | ▲ +12.07 after sell → book $10,782.80; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,266.37 | ▲ close $10,853.32 vs 09:30 $10,791.09 (session +70.52) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,266.37 | ▼ 09:30 equity $10,831.63 vs yday $10,853.32 (-21.69) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 22 | $13.04 | $2.08 | $-27.67 | $1,551.17 | ▼ -27.67 after sell → book $10,829.55; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `USDE` | 54 | $8.15 | $2.17 | $+122.04 | $1,989.10 | ▲ +122.04 after sell → book $10,827.38; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,989.10 | ▼ close $10,758.68 vs 09:30 $10,831.63 (session -68.70) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,989.10 | ▼ 09:30 equity $10,728.86 vs yday $10,758.68 (-29.82) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 136 | $10.07 | $2.43 | $+41.41 | $3,356.19 | ▲ +41.41 after sell → book $10,726.43; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $139.65 | $2.04 | $-61.83 | $4,611.00 | ▼ -61.83 after sell → book $10,724.39; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 72 | $17.40 | $2.23 | $-73.55 | $5,861.57 | ▼ -73.55 after sell → book $10,722.16; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EL` | 12 | $100.00 | $2.05 | $-87.95 | $7,059.53 | ▼ -87.95 after sell → book $10,720.12; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 145 | $8.73 | $2.46 | $-62.88 | $8,322.92 | ▼ -62.88 after sell → book $10,717.66; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIG` | 43 | $26.78 | $2.14 | $-150.46 | $9,472.32 | ▼ -150.46 after sell → book $10,715.52; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 56 | $22.20 | $2.18 | $-65.94 | $10,713.34 | ▼ -65.94 after sell → book $10,713.34; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.34 | ▲ close $10,713.34 vs 09:30 $10,728.86 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.34 | ▲ 09:30 equity $10,713.34 vs yday $10,713.34 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 752 | $1.78 | $9.70 | — | $9,365.08 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+183.1; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 72 | $18.40 | $2.21 | — | $8,038.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=-32.2; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $6,756.95 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 52 | $25.62 | $2.15 | — | $5,422.31 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.1; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 984 | $1.36 | $12.69 | — | $4,071.37 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 16 | $82.76 | $2.04 | — | $2,745.18 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.1; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNDT` | 704 | $1.90 | $9.08 | — | $1,398.50 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.5; leftover $1339.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 200 | $6.68 | $2.59 | — | $59.91 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+11.4; leftover $1339.17 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.91 | ▼ close $10,370.51 vs 09:30 $10,713.34 (session -300.36) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.91 | ▼ 09:30 equity $10,280.35 vs yday $10,370.51 (-90.16) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 2 | $5.79 | $0.12 | — | $48.20 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $11.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 1 | $7.87 | $0.08 | — | $40.25 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+8.7; leftover $11.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 2 | $5.75 | $0.12 | — | $28.63 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $11.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `PAGS` | 1 | $9.96 | $0.10 | — | $18.57 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+11.5; leftover $11.98 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.57 | ▲ close $10,748.53 vs 09:30 $10,280.35 (session +468.61) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.57 | ▼ 09:30 equity $10,648.84 vs yday $10,748.53 (-99.69) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.57 | ▼ close $10,337.47 vs 09:30 $10,648.84 (session -311.37) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.57 | ▲ 09:30 equity $10,407.77 vs yday $10,337.47 (+70.30) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 752 | $1.45 | $9.84 | $-267.70 | $1,099.13 | ▼ -267.70 after sell → book $10,397.93; vs 09:30 mark -9.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `REAX` | 72 | $20.70 | $2.23 | $+161.16 | $2,587.30 | ▲ +161.16 after sell → book $10,395.70; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AGCO` | 10 | $127.69 | $2.04 | $-6.26 | $3,862.16 | ▼ -6.26 after sell → book $10,393.66; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ASST` | 52 | $28.00 | $2.17 | $+119.19 | $5,315.99 | ▲ +119.19 after sell → book $10,391.49; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SID` | 984 | $1.28 | $12.87 | $-104.28 | $6,562.65 | ▼ -104.28 after sell → book $10,378.63; vs 09:30 mark -12.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `TARS` | 16 | $86.31 | $2.06 | $+52.70 | $7,941.55 | ▲ +52.70 after sell → book $10,376.57; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNDT` | 704 | $1.66 | $9.21 | $-187.25 | $9,100.98 | ▼ -187.25 after sell → book $10,367.36; vs 09:30 mark -9.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RSKD` | 200 | $6.13 | $2.63 | $-115.22 | $10,324.35 | ▼ -115.22 after sell → book $10,364.73; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.35 | ▼ close $10,361.90 vs 09:30 $10,407.77 (session -2.83) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.35 | ▼ 09:30 equity $10,360.77 vs yday $10,361.90 (-1.13) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 2 | $5.22 | $0.13 | $-1.39 | $10,334.66 | ▼ -1.39 after sell → book $10,360.64; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `USDE` | 1 | $6.73 | $0.09 | $-1.31 | $10,341.30 | ▼ -1.31 after sell → book $10,360.55; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 2 | $4.85 | $0.12 | $-2.04 | $10,350.87 | ▼ -2.04 after sell → book $10,360.42; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `PAGS` | 1 | $9.55 | $0.12 | $-0.63 | $10,360.31 | ▼ -0.63 after sell → book $10,360.31; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,360.31 | ▲ close $10,360.31 vs 09:30 $10,360.77 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,360.31 | ▲ 09:30 equity $10,360.31 vs yday $10,360.31 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 479 | $2.70 | $6.18 | — | $9,060.83 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 413 | $3.13 | $5.33 | — | $7,762.81 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+24.2; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 210 | $6.16 | $2.71 | — | $6,466.50 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+36.4; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 263 | $4.91 | $3.39 | — | $5,171.78 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 240 | $5.38 | $3.10 | — | $3,877.48 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+19.8; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 15 | $84.27 | $2.04 | — | $2,611.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 70 | $18.30 | $2.20 | — | $1,328.20 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1295.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 118 | $10.95 | $2.34 | — | $33.75 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1295.04 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.75 | ▲ close $10,488.54 vs 09:30 $10,360.31 (session +155.52) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.75 | ▲ 09:30 equity $10,533.19 vs yday $10,488.54 (+44.65) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.75 | ▲ close $10,749.24 vs 09:30 $10,533.19 (session +216.05) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.75 | ▲ 09:30 equity $10,803.80 vs yday $10,749.24 (+54.56) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.75 | ▲ close $10,914.24 vs 09:30 $10,803.80 (session +110.44) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.75 | ▼ 09:30 equity $10,827.20 vs yday $10,914.24 (-87.04) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 413 | $3.48 | $5.41 | $+133.81 | $1,465.58 | ▲ +133.81 after sell → book $10,821.79; vs 09:30 mark -5.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `IRD` | 210 | $5.80 | $2.75 | $-81.06 | $2,680.83 | ▼ -81.06 after sell → book $10,819.04; vs 09:30 mark -2.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 263 | $4.77 | $3.45 | $-43.66 | $3,931.89 | ▼ -43.66 after sell → book $10,815.59; vs 09:30 mark -3.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ANGX` | 240 | $5.30 | $3.15 | $-25.44 | $5,200.75 | ▼ -25.44 after sell → book $10,812.45; vs 09:30 mark -3.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 70 | $17.73 | $2.22 | $-44.32 | $6,439.63 | ▼ -44.32 after sell → book $10,810.23; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 118 | $10.82 | $2.37 | $-20.06 | $7,714.01 | ▼ -20.06 after sell → book $10,807.85; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 714 | $1.80 | $9.21 | — | $6,419.60 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1285.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 45 | $28.16 | $2.12 | — | $5,150.28 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1285.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `TXG` | 17 | $74.50 | $2.04 | — | $3,881.74 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.4; leftover $1285.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 87 | $14.62 | $2.25 | — | $2,607.54 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.6; leftover $1285.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 55 | $23.29 | $2.15 | — | $1,324.44 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+16.1; leftover $1285.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 24 | $52.52 | $2.06 | — | $61.90 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+10.7; leftover $1285.67 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.90 | ▼ close $10,713.79 vs 09:30 $10,827.20 (session -74.22) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.90 | ▲ 09:30 equity $10,838.01 vs yday $10,713.79 (+124.22) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 15 | $86.76 | $2.06 | $+33.26 | $1,361.24 | ▲ +33.26 after sell → book $10,835.96; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 8 | $28.23 | $2.01 | — | $1,133.39 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.3; leftover $226.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 10 | $22.46 | $2.02 | — | $906.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $226.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 212 | $1.07 | $2.73 | — | $677.19 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $226.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 22 | $10.25 | $2.06 | — | $449.64 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $226.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $300.55 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,ohlc_hot; ret5=+17.7; leftover $226.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `PUMP` | 22 | $10.31 | $2.06 | — | $71.67 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+5.9; leftover $226.87 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.67 | ▲ close $11,559.60 vs 09:30 $10,838.01 (session +736.01) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.67 | ▼ 09:30 equity $11,329.80 vs yday $11,559.60 (-229.80) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 3 | $3.04 | $0.10 | — | $62.47 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $11.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 1 | $11.38 | $0.12 | — | $50.97 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $11.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 1 | $7.98 | $0.08 | — | $42.91 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $11.95 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.91 | ▼ close $11,152.74 vs 09:30 $11,329.80 (session -176.77) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.91 | ▲ 09:30 equity $11,255.06 vs yday $11,152.74 (+102.32) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 479 | $3.55 | $6.27 | $+394.70 | $1,737.09 | ▲ +394.70 after sell → book $11,248.79; vs 09:30 mark -6.27 | dropped from list after 6 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HLP` | 714 | $2.08 | $9.34 | $+181.37 | $3,212.86 | ▲ +181.37 after sell → book $11,239.44; vs 09:30 mark -9.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 45 | $30.23 | $2.15 | $+88.88 | $4,571.07 | ▲ +88.88 after sell → book $11,237.30; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TXG` | 17 | $79.15 | $2.06 | $+74.95 | $5,914.56 | ▲ +74.95 after sell → book $11,235.24; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SSL` | 87 | $13.87 | $2.28 | $-69.78 | $7,118.97 | ▼ -69.78 after sell → book $11,232.96; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 55 | $29.43 | $2.18 | $+333.37 | $8,735.44 | ▲ +333.37 after sell → book $11,230.78; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FRO` | 24 | $49.83 | $2.08 | $-68.70 | $9,929.28 | ▼ -68.70 after sell → book $11,228.70; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 502 | $2.47 | $6.48 | — | $8,682.87 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+73.6; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 73 | $16.91 | $2.21 | — | $7,446.23 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+50.5; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 106 | $11.67 | $2.31 | — | $6,206.90 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+31.3; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 752 | $1.65 | $9.70 | — | $4,956.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+43.0; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 14 | $83.53 | $2.03 | — | $3,784.95 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+8.8; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 215 | $5.75 | $2.77 | — | $2,544.85 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 95 | $13.05 | $2.27 | — | $1,302.82 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1241.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 49 | $24.93 | $2.14 | — | $79.12 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+8.7; leftover $1241.16 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.12 | ▲ close $11,402.44 vs 09:30 $11,255.06 (session +203.65) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.12 | ▼ 09:30 equity $11,388.00 vs yday $11,402.44 (-14.44) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 22 | $10.18 | $2.08 | $-5.67 | $301.00 | ▼ -5.67 after sell → book $11,385.92; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 4 | $9.11 | $0.38 | — | $264.18 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+44.4; leftover $43.00 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 9 | $4.30 | $0.41 | — | $225.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $43.00 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 5 | $7.23 | $0.38 | — | $188.54 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+36.6; leftover $43.00 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.54 | ▲ close $11,440.05 vs 09:30 $11,388.00 (session +55.29) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.54 | ▲ 09:30 equity $11,455.78 vs yday $11,440.05 (+15.73) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 8 | $27.74 | $2.03 | $-7.97 | $408.43 | ▼ -7.97 after sell → book $11,453.75; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 10 | $23.00 | $2.04 | $+1.34 | $636.39 | ▲ +1.34 after sell → book $11,451.71; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `IQ` | 212 | $1.03 | $2.78 | $-13.99 | $851.97 | ▼ -13.99 after sell → book $11,448.93; vs 09:30 mark -2.78 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $992.92 | ▼ -8.14 after sell → book $11,447.48; vs 09:30 mark -1.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PUMP` | 22 | $10.10 | $2.08 | $-8.75 | $1,213.05 | ▼ -8.75 after sell → book $11,445.41; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CYPH` | 3 | $3.82 | $0.14 | $+2.11 | $1,224.36 | ▲ +2.11 after sell → book $11,445.26; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `VITL` | 1 | $11.17 | $0.13 | $-0.46 | $1,235.40 | ▼ -0.46 after sell → book $11,445.13; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 1 | $7.95 | $0.10 | $-0.22 | $1,243.24 | ▼ -0.22 after sell → book $11,445.02; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 65 | $2.70 | $1.95 | — | $1,065.79 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+109.2; leftover $177.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 39 | $4.49 | $1.87 | — | $888.82 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+26.4; leftover $177.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 2 | $70.84 | $1.42 | — | $745.71 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $177.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 17 | $9.90 | $1.73 | — | $575.68 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $177.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 136 | $1.30 | $2.18 | — | $396.70 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.3; leftover $177.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 4 | $41.76 | $1.68 | — | $227.98 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $177.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 9 | $18.57 | $1.70 | — | $59.11 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+8.2; leftover $177.61 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.11 | ▲ close $11,711.29 vs 09:30 $11,455.78 (session +278.80) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.11 | ▼ 09:30 equity $11,706.96 vs yday $11,711.29 (-4.33) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 502 | $2.68 | $6.57 | $+92.37 | $1,397.90 | ▲ +92.37 after sell → book $11,700.39; vs 09:30 mark -6.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `LVWR` | 752 | $1.32 | $9.84 | $-267.70 | $2,380.70 | ▼ -267.70 after sell → book $11,690.56; vs 09:30 mark -9.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MXL` | 14 | $82.53 | $2.05 | $-18.01 | $3,534.14 | ▼ -18.01 after sell → book $11,688.50; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 215 | $5.62 | $2.82 | $-34.62 | $4,739.62 | ▼ -34.62 after sell → book $11,685.69; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 95 | $12.76 | $2.30 | $-32.13 | $5,949.52 | ▼ -32.13 after sell → book $11,683.38; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `UMC` | 49 | $24.11 | $2.16 | $-44.47 | $7,128.75 | ▼ -44.47 after sell → book $11,681.23; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,128.75 | ▲ close $12,348.59 vs 09:30 $11,706.96 (session +667.37) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,280.59 | ▲ 09:30 equity $10,496.84 vs yday $10,380.69 (+116.15) | 09:30 open · cash $8,280.59 (unchanged overnight, no fees) · equity $10,496.84 vs prior close $10,380.69 (+116.15) · 4 name(s) re-marked at the open (per-name table). GLND×1 yday $5.35 → 09:30 $6.06 +0.71; SVIA×1 yday $3.96 → 09:30 $3.96 +0.00; TJGC×74 yday $28.20 → 09:30 $29.76 +115.44; VERI×3 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 85 | $16.21 | $2.25 | — | $6,900.49 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1380.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 88 | $15.58 | $2.25 | — | $5,527.10 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+84.4; leftover $1380.10 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 11 | $123.50 | $2.02 | — | $4,166.58 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1380.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 135 | $10.20 | $2.40 | — | $2,787.19 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1380.10 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 344 | $4.00 | $4.44 | — | $1,405.03 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1380.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 627 | $2.20 | $8.09 | — | $17.54 | — | rank by w_hot_candle; rank w_hot_candle; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1380.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.54 | ▲ close $10,485.23 vs 09:30 $10,496.84 (session +9.83) | 16:00 close · cash $17.54 · equity $10,485.23 vs 09:30 $10,496.84 (-11.61; session marks +9.83) · 10 name(s) marked open→close (per-name table). GLND×1 09:30 $6.06 → close $5.54 -0.52; SVIA×1 09:30 $3.96 → close $3.96 +0.00; TJGC×74 09:30 $29.76 → close $26.24 -260.48; VERI×3 09:30 $1.33 → close $1.33 +0.00; SECZ×85 09:30 $16.21 → close $15.96 -21.25; USDE×88 09:30 $15.58 → close $17.25 +146.86; GRAL×11 09:30 $123.50 → close $126.89 +37.29; DNA×135 09:30 $10.20 → close $10.66 +62.10; CYPH×344 09:30 $4.00 → close $4.12 +39.56; HLP×627 09:30 $2.20 → close $2.21 +6.27 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 13.42 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 13.42 < 1 share @ 19.57 |
| 2026-08-14 | `LIFE` | cash | leftover split 13.42 < 1 share @ 35.04 |
| 2026-08-14 | `BETA` | cash | leftover split 13.42 < 1 share @ 25.21 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-14 | `VOYG` | cash | leftover split 13.42 < 1 share @ 44.49 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 10.35 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 10.35 < 1 share @ 41.23 |
| 2026-08-17 | `NMAX` | cash | leftover split 10.35 < 1 share @ 10.97 |
| 2026-08-17 | `UMAC` | cash | leftover split 10.35 < 1 share @ 32.55 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IMMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IMMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `XXI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `XXI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PURR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BZ` | cash | leftover split 5.26 < 1 share @ 18.50 |
| 2026-08-27 | `CAPR` | cash | leftover split 5.26 < 1 share @ 9.19 |
| 2026-08-27 | `AQST` | cash | leftover split 5.26 < 1 share @ 5.39 |
| 2026-08-27 | `MRNA` | cash | leftover split 5.26 < 1 share @ 144.18 |
| 2026-08-27 | `VERA` | cash | leftover split 5.26 < 1 share @ 36.70 |
| 2026-08-27 | `DJT` | cash | leftover split 5.26 < 1 share @ 9.59 |
| 2026-08-27 | `SRRK` | cash | leftover split 5.26 < 1 share @ 60.00 |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PURR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KVYO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNDT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RSKD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRNM` | cash | leftover split 11.98 < 1 share @ 16.40 |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `TARS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNDT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RSKD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SION` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TXG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TXG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PUMP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TEM` | cash | leftover split 11.95 < 1 share @ 81.40 |
| 2026-09-18 | `CRWD` | cash | leftover split 11.95 < 1 share @ 246.98 |
| 2026-09-18 | `NEO` | cash | leftover split 11.95 < 1 share @ 19.91 |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PUMP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ADPT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BBNX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `IQ` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PUMP` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MXL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ARM` | cash | leftover split 43.00 < 1 share @ 319.41 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MXL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `DGXX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DGXX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NTSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 73 | 2026-09-21 @ $16.91 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+50.5; leftover $1241.16 |
| `SECZ` | 106 | 2026-09-21 @ $11.67 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+31.3; leftover $1241.16 |
| `CRML` | 4 | 2026-09-22 @ $9.11 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+44.4; leftover $43.00 |
| `DGXX` | 9 | 2026-09-22 @ $4.30 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $43.00 |
| `NUAI` | 5 | 2026-09-22 @ $7.23 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+36.6; leftover $43.00 |
| `GLND` | 65 | 2026-09-23 @ $2.70 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+109.2; leftover $177.61 |
| `SVIA` | 39 | 2026-09-23 @ $4.49 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+26.4; leftover $177.61 |
| `INOD` | 2 | 2026-09-23 @ $70.84 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $177.61 |
| `BFLY` | 17 | 2026-09-23 @ $9.90 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $177.61 |
| `VERI` | 136 | 2026-09-23 @ $1.30 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.3; leftover $177.61 |
| `VKTX` | 4 | 2026-09-23 @ $41.76 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $177.61 |
| `NTSK` | 9 | 2026-09-23 @ $18.57 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+8.2; leftover $177.61 |
