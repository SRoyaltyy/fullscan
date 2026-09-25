# Factor mine action — `union_w_hot_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_candle` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_candle

Cash book **+9.18%** ($10,918) · signal-only (no cash/fees) was +36.45%. Starts YES **29/30**. Fills 258 · skips 97 · realized $+14.68.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_candle` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,704.31.

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
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | ▼ -26.05 after sell → book $10,310.53; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,508.31 | ▼ -55.19 after sell → book $10,308.44; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $5,055.35 | ▼ -29.03 after sell → book $10,304.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | ▲ +148.79 after sell → book $10,284.98; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | ▲ +69.58 after sell → book $10,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $9,087.46 | ▲ +69.56 after sell → book $10,280.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $7,702.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $6,421.63 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $5,147.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $2,621.22 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $1,334.64 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $86.84 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.84 | ▼ close $10,010.26 vs 09:30 $10,312.70 (session -245.44) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.84 | ▼ 09:30 equity $9,957.85 vs yday $10,010.26 (-52.41) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,375.84 | ▲ +3.49 after sell → book $9,955.68; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,583.76 | ▼ -82.19 after sell → book $9,948.05; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $3,681.95 | ▼ -182.95 after sell → book $9,945.69; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $4,951.79 | ▼ -4.39 after sell → book $9,943.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,174.76 | ▼ -40.58 after sell → book $9,941.37; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $7,403.10 | ▼ -34.30 after sell → book $9,939.21; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,757.63 | ▲ +67.96 after sell → book $9,936.99; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $9,934.90 | ▼ -70.53 after sell → book $9,934.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 296 | $4.19 | $3.82 | — | $8,690.84 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 91 | $13.64 | $2.26 | — | $7,447.34 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $6,208.36 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+46.0; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 122 | $10.10 | $2.36 | — | $4,973.80 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 646 | $1.92 | $8.33 | — | $3,725.15 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 113 | $10.97 | $2.33 | — | $2,483.21 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 180 | $6.87 | $2.53 | — | $1,244.08 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $1241.86 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $5.07 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1241.86 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.07 | ▼ close $9,681.70 vs 09:30 $9,957.85 (session -227.38) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.07 | ▼ 09:30 equity $9,602.07 vs yday $9,681.70 (-79.63) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 296 | $3.94 | $3.88 | $-81.70 | $1,167.44 | ▼ -81.70 after sell → book $9,598.20; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 91 | $13.31 | $2.29 | $-34.58 | $2,376.36 | ▼ -34.58 after sell → book $9,595.91; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,619.26 | ▲ +3.92 after sell → book $9,593.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 122 | $10.45 | $2.39 | $+37.96 | $4,891.77 | ▲ +37.96 after sell → book $9,591.42; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 646 | $1.70 | $8.45 | $-158.90 | $5,981.52 | ▼ -158.90 after sell → book $9,582.97; vs 09:30 mark -8.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 113 | $10.31 | $2.36 | $-79.27 | $7,144.19 | ▼ -79.27 after sell → book $9,580.61; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $8,228.49 | ▼ -154.71 after sell → book $9,578.49; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,228.49 | ▼ close $9,502.89 vs 09:30 $9,602.07 (session -75.60) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,228.49 | ▲ 09:30 equity $9,522.69 vs yday $9,502.89 (+19.80) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 180 | $7.19 | $2.57 | $+52.50 | $9,520.12 | ▲ +52.50 after sell → book $9,520.12; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,520.12 | ▲ close $9,520.12 vs 09:30 $9,522.69 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,520.12 | ▲ 09:30 equity $9,520.12 vs yday $9,520.12 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,467.13 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1034 | $1.15 | $13.34 | — | $7,264.69 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $6,080.90 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $4,893.48 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 495 | $2.40 | $6.39 | — | $3,699.10 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 180 | $6.61 | $2.53 | — | $2,507.67 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 91 | $12.98 | $2.26 | — | $1,324.22 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 59 | $20.00 | $2.17 | — | $142.06 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1190.02 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.06 | ▼ close $9,289.40 vs 09:30 $9,520.12 (session -197.35) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.06 | ▲ 09:30 equity $9,570.49 vs yday $9,289.40 (+281.09) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $1,296.74 | ▼ -29.11 after sell → book $9,568.17; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $2,523.24 | ▲ +39.08 after sell → book $9,565.75; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 495 | $2.28 | $6.48 | $-72.26 | $3,645.36 | ▼ -72.26 after sell → book $9,559.27; vs 09:30 mark -6.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 180 | $6.95 | $2.57 | $+57.00 | $4,893.79 | ▲ +57.00 after sell → book $9,556.70; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 91 | $13.36 | $2.29 | $+30.03 | $6,107.26 | ▲ +30.03 after sell → book $9,554.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BBNX` | 59 | $19.50 | $2.19 | $-33.85 | $7,255.58 | ▼ -33.85 after sell → book $9,552.23; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 269 | $4.49 | $3.47 | — | $6,044.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $1209.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 108 | $11.13 | $2.31 | — | $4,839.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1209.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 133 | $9.08 | $2.39 | — | $3,629.91 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1209.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 299 | $4.04 | $3.86 | — | $2,418.10 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $1209.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 146 | $8.28 | $2.43 | — | $1,206.79 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1209.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 187 | $6.42 | $2.55 | — | $3.70 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $1209.26 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.70 | ▲ close $9,882.48 vs 09:30 $9,570.49 (session +347.26) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.70 | ▲ 09:30 equity $10,311.77 vs yday $9,882.48 (+429.29) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,000.57 | ▼ -56.12 after sell → book $10,309.74; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1034 | $1.83 | $13.53 | $+676.26 | $2,879.26 | ▲ +676.26 after sell → book $10,296.22; vs 09:30 mark -13.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 269 | $4.32 | $3.52 | $-52.72 | $4,037.82 | ▼ -52.72 after sell → book $10,292.69; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 108 | $13.33 | $2.34 | $+232.94 | $5,475.11 | ▲ +232.94 after sell → book $10,290.35; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 133 | $8.08 | $2.42 | $-137.81 | $6,547.33 | ▼ -137.81 after sell → book $10,287.93; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 299 | $4.16 | $3.92 | $+28.11 | $7,787.25 | ▲ +28.11 after sell → book $10,284.01; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 146 | $8.59 | $2.46 | $+40.37 | $9,038.93 | ▲ +40.37 after sell → book $10,281.55; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 187 | $6.64 | $2.59 | $+36.93 | $10,278.95 | ▲ +36.93 after sell → book $10,278.95; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,278.95 | ▲ close $10,278.95 vs 09:30 $10,311.77 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,278.95 | ▲ 09:30 equity $10,278.95 vs yday $10,278.95 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 53 | $24.11 | $2.15 | — | $8,998.98 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+891.7; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 823 | $1.56 | $10.62 | — | $7,704.48 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 315 | $4.07 | $4.06 | — | $6,418.37 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+4.9; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 67 | $19.04 | $2.19 | — | $5,140.49 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+49.5; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 245 | $5.24 | $3.16 | — | $3,853.53 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 146 | $8.79 | $2.43 | — | $2,567.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 53 | $23.80 | $2.15 | — | $1,304.22 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+28.9; leftover $1284.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 361 | $3.55 | $4.66 | — | $18.01 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+27.9; leftover $1284.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.01 | ▲ close $10,885.91 vs 09:30 $10,278.95 (session +638.37) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.01 | ▼ 09:30 equity $10,570.26 vs yday $10,885.91 (-315.65) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 53 | $26.61 | $2.17 | $+128.18 | $1,426.17 | ▲ +128.18 after sell → book $10,568.09; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 823 | $1.60 | $10.76 | $+11.54 | $2,732.21 | ▲ +11.54 after sell → book $10,557.33; vs 09:30 mark -10.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 67 | $20.72 | $2.21 | $+108.16 | $4,118.23 | ▲ +108.16 after sell → book $10,555.11; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 245 | $4.98 | $3.21 | $-70.07 | $5,335.12 | ▼ -70.07 after sell → book $10,551.90; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMNR` | 53 | $24.24 | $2.17 | $+19.00 | $6,617.67 | ▲ +19.00 after sell → book $10,549.73; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 361 | $3.77 | $4.73 | $+70.04 | $7,973.91 | ▲ +70.04 after sell → book $10,545.00; vs 09:30 mark -4.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 94 | $14.11 | $2.27 | — | $6,645.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.4; leftover $1328.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 228 | $5.81 | $2.94 | — | $5,317.68 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1328.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 114 | $11.59 | $2.33 | — | $3,994.66 | — | rank by w_hot_candle; rank w_hot_candle; list overnight; 🔵; ret5=+64.9; leftover $1328.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 94 | $14.00 | $2.27 | — | $2,676.39 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.8; leftover $1328.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 32 | $40.50 | $2.09 | — | $1,378.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.8; leftover $1328.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 118 | $11.22 | $2.34 | — | $52.00 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+16.8; leftover $1328.99 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.00 | ▼ close $10,450.56 vs 09:30 $10,570.26 (session -80.20) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.00 | ▲ 09:30 equity $10,631.00 vs yday $10,450.56 (+180.44) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 146 | $9.41 | $2.46 | $+85.63 | $1,423.39 | ▲ +85.63 after sell → book $10,628.53; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 94 | $14.20 | $2.30 | $+3.89 | $2,755.90 | ▲ +3.89 after sell → book $10,626.24; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 228 | $6.50 | $2.99 | $+151.39 | $4,234.90 | ▲ +151.39 after sell → book $10,623.24; vs 09:30 mark -3.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 114 | $12.18 | $2.36 | $+63.14 | $5,621.06 | ▲ +63.14 after sell → book $10,620.88; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 94 | $12.56 | $2.30 | $-139.93 | $6,799.41 | ▼ -139.93 after sell → book $10,618.59; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 32 | $37.42 | $2.11 | $-102.75 | $7,994.74 | ▼ -102.75 after sell → book $10,616.48; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 118 | $11.38 | $2.37 | $+14.16 | $9,335.21 | ▲ +14.16 after sell → book $10,614.11; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 72 | $18.50 | $2.21 | — | $8,001.00 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+17.2; leftover $1333.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 145 | $9.19 | $2.42 | — | $6,666.02 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1333.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 247 | $5.39 | $3.19 | — | $5,331.51 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.4; leftover $1333.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 9 | $144.18 | $2.02 | — | $4,031.87 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=-14.2; leftover $1333.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `VERA` | 36 | $36.70 | $2.10 | — | $2,708.57 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+14.1; leftover $1333.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `DJT` | 139 | $9.59 | $2.41 | — | $1,373.85 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.8; leftover $1333.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 22 | $60.00 | $2.06 | — | $51.79 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+6.2; leftover $1333.60 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.79 | ▼ close $10,470.29 vs 09:30 $10,631.00 (session -127.42) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.79 | ▼ 09:30 equity $10,310.65 vs yday $10,470.29 (-159.64) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 315 | $3.69 | $4.13 | $-127.89 | $1,210.02 | ▼ -127.89 after sell → book $10,306.53; vs 09:30 mark -4.12 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 72 | $18.15 | $2.23 | $-29.63 | $2,514.59 | ▼ -29.63 after sell → book $10,304.30; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 247 | $5.11 | $3.24 | $-75.58 | $3,773.52 | ▼ -75.58 after sell → book $10,301.06; vs 09:30 mark -3.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRNA` | 9 | $137.19 | $2.04 | $-66.96 | $5,006.20 | ▼ -66.96 after sell → book $10,299.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `VERA` | 36 | $34.40 | $2.12 | $-87.02 | $6,242.48 | ▼ -87.02 after sell → book $10,296.91; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DJT` | 139 | $9.72 | $2.44 | $+13.92 | $7,591.12 | ▲ +13.92 after sell → book $10,294.47; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 22 | $58.75 | $2.08 | $-31.63 | $8,881.54 | ▼ -31.63 after sell → book $10,292.39; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 90 | $14.00 | $2.26 | — | $7,619.28 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=-3.3; leftover $1268.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,448.71 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1268.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 69 | $18.36 | $2.20 | — | $5,179.67 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.8; leftover $1268.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 11 | $106.99 | $2.02 | — | $4,000.76 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+10.5; leftover $1268.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 138 | $9.13 | $2.40 | — | $2,738.41 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+20.0; leftover $1268.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIG` | 42 | $30.18 | $2.12 | — | $1,468.74 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1268.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 54 | $23.30 | $2.15 | — | $208.38 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+14.5; leftover $1268.79 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.38 | ▼ close $10,079.97 vs 09:30 $10,310.65 (session -197.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.38 | ▼ 09:30 equity $9,946.77 vs yday $10,079.97 (-133.20) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 145 | $9.50 | $2.46 | $+40.06 | $1,583.42 | ▲ +40.06 after sell → book $9,944.31; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $2,765.63 | ▲ +11.63 after sell → book $9,942.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 69 | $17.77 | $2.22 | $-45.13 | $3,989.54 | ▼ -45.13 after sell → book $9,940.06; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EL` | 11 | $102.70 | $2.04 | $-51.26 | $5,117.20 | ▼ -51.26 after sell → book $9,938.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 138 | $8.66 | $2.44 | $-69.70 | $6,309.84 | ▼ -69.70 after sell → book $9,935.58; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIG` | 42 | $27.60 | $2.14 | $-112.61 | $7,466.91 | ▼ -112.61 after sell → book $9,933.45; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 54 | $22.66 | $2.17 | $-38.88 | $8,688.37 | ▼ -38.88 after sell → book $9,931.27; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,688.37 | ▼ close $9,885.37 vs 09:30 $9,946.77 (session -45.90) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,688.37 | ▼ 09:30 equity $9,861.97 vs yday $9,885.37 (-23.40) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 90 | $13.04 | $2.28 | $-90.94 | $9,859.69 | ▼ -90.94 after sell → book $9,859.69; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,859.69 | ▲ close $9,859.69 vs 09:30 $9,861.97 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,859.69 | ▲ 09:30 equity $9,859.69 vs yday $9,859.69 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,859.69 | ▲ close $9,859.69 vs 09:30 $9,859.69 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,859.69 | ▲ 09:30 equity $9,859.69 vs yday $9,859.69 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 692 | $1.78 | $8.93 | — | $8,619.00 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+183.1; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 66 | $18.40 | $2.19 | — | $7,402.41 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=-32.2; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $6,249.21 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 48 | $25.62 | $2.13 | — | $5,017.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.1; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 906 | $1.36 | $11.69 | — | $3,773.23 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 14 | $82.76 | $2.03 | — | $2,612.55 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.1; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNDT` | 648 | $1.90 | $8.36 | — | $1,373.00 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.5; leftover $1232.46 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 184 | $6.68 | $2.54 | — | $141.33 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+11.4; leftover $1232.46 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.33 | ▼ close $9,543.62 vs 09:30 $9,859.69 (session -276.18) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.33 | ▼ 09:30 equity $9,460.95 vs yday $9,543.62 (-82.67) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 66 | $18.15 | $2.21 | $-20.90 | $1,337.02 | ▼ -20.90 after sell → book $9,458.74; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $2,461.97 | ▼ -28.26 after sell → book $9,456.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 906 | $1.23 | $11.85 | $-141.32 | $3,564.50 | ▼ -141.32 after sell → book $9,444.86; vs 09:30 mark -11.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNDT` | 648 | $1.90 | $8.48 | $-16.84 | $4,787.22 | ▼ -16.84 after sell → book $9,436.38; vs 09:30 mark -8.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 184 | $6.84 | $2.58 | $+24.32 | $6,043.20 | ▲ +24.32 after sell → book $9,433.80; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 208 | $5.79 | $2.68 | — | $4,836.20 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1208.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 153 | $7.87 | $2.45 | — | $3,629.64 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+8.7; leftover $1208.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 73 | $16.40 | $2.21 | — | $2,430.23 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1208.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 210 | $5.75 | $2.71 | — | $1,220.02 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1208.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `PAGS` | 121 | $9.96 | $2.35 | — | $12.51 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+11.5; leftover $1208.64 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.51 | ▲ close $9,816.36 vs 09:30 $9,460.95 (session +394.96) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.51 | ▼ 09:30 equity $9,686.38 vs yday $9,816.36 (-129.98) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 692 | $1.56 | $9.05 | $-166.76 | $1,086.44 | ▼ -166.76 after sell → book $9,677.33; vs 09:30 mark -9.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 48 | $26.44 | $2.15 | $+34.83 | $2,353.40 | ▲ +34.83 after sell → book $9,675.17; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+92.66 | $3,606.73 | ▲ +92.66 after sell → book $9,673.12; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 208 | $5.81 | $2.73 | $-1.25 | $4,812.48 | ▼ -1.25 after sell → book $9,670.39; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 153 | $7.76 | $2.48 | $-21.76 | $5,997.28 | ▼ -21.76 after sell → book $9,667.91; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 73 | $16.74 | $2.23 | $+20.38 | $7,217.07 | ▲ +20.38 after sell → book $9,665.68; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 210 | $5.95 | $2.75 | $+36.54 | $8,463.81 | ▲ +36.54 after sell → book $9,662.92; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PAGS` | 121 | $9.91 | $2.38 | $-10.79 | $9,660.54 | ▼ -10.79 after sell → book $9,660.54; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,660.54 | ▲ close $9,660.54 vs 09:30 $9,686.38 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,660.54 | ▲ 09:30 equity $9,660.54 vs yday $9,660.54 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,660.54 | ▲ close $9,660.54 vs 09:30 $9,660.54 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,660.54 | ▲ 09:30 equity $9,660.54 vs yday $9,660.54 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,660.54 | ▲ close $9,660.54 vs 09:30 $9,660.54 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,660.54 | ▲ 09:30 equity $9,660.54 vs yday $9,660.54 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 447 | $2.70 | $5.77 | — | $8,447.87 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 385 | $3.13 | $4.97 | — | $7,237.86 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+24.2; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 196 | $6.16 | $2.58 | — | $6,027.92 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+36.4; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 245 | $4.91 | $3.16 | — | $4,821.81 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 224 | $5.38 | $2.89 | — | $3,613.80 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+19.8; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $2,431.99 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 65 | $18.30 | $2.19 | — | $1,240.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1207.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 110 | $10.95 | $2.32 | — | $33.48 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1207.57 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.48 | ▲ close $9,779.69 vs 09:30 $9,660.54 (session +145.05) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.48 | ▲ 09:30 equity $9,821.32 vs yday $9,779.69 (+41.63) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 196 | $6.02 | $2.62 | $-32.64 | $1,210.78 | ▼ -32.64 after sell → book $9,818.70; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 245 | $5.03 | $3.21 | $+23.03 | $2,439.92 | ▲ +23.03 after sell → book $9,815.49; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 224 | $5.57 | $2.94 | $+36.73 | $3,684.66 | ▲ +36.73 after sell → book $9,812.55; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 14 | $86.06 | $2.05 | $+20.98 | $4,887.45 | ▲ +20.98 after sell → book $9,810.50; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 65 | $18.28 | $2.21 | $-5.69 | $6,073.44 | ▼ -5.69 after sell → book $9,808.29; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 110 | $10.29 | $2.35 | $-77.27 | $7,203.00 | ▼ -77.27 after sell → book $9,805.95; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,203.00 | ▲ close $10,007.98 vs 09:30 $9,821.32 (session +202.03) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,203.00 | ▲ 09:30 equity $10,124.20 vs yday $10,007.98 (+116.22) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 385 | $3.64 | $5.04 | $+186.34 | $8,599.35 | ▲ +186.34 after sell → book $10,119.15; vs 09:30 mark -5.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,599.35 | ▲ close $10,226.43 vs 09:30 $10,124.20 (session +107.28) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,599.35 | ▲ 09:30 equity $10,235.37 vs yday $10,226.43 (+8.94) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 682 | $1.80 | $8.80 | — | $7,362.96 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1228.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 43 | $28.16 | $2.12 | — | $6,149.96 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1228.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $4,985.99 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1228.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `TXG` | 16 | $74.50 | $2.04 | — | $3,791.95 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.4; leftover $1228.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 84 | $14.62 | $2.24 | — | $2,561.63 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.6; leftover $1228.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $1,348.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+16.1; leftover $1228.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 23 | $52.52 | $2.06 | — | $138.38 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+10.7; leftover $1228.48 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.38 | ▼ close $10,152.70 vs 09:30 $10,235.37 (session -61.24) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.38 | ▲ 09:30 equity $10,267.72 vs yday $10,152.70 (+115.02) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 43 | $28.59 | $2.14 | $+14.45 | $1,365.83 | ▲ +14.45 after sell → book $10,265.58; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $2,491.66 | ▼ -38.14 after sell → book $10,263.53; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TXG` | 16 | $75.38 | $2.06 | $+9.98 | $3,695.68 | ▲ +9.98 after sell → book $10,261.47; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 84 | $13.77 | $2.27 | $-75.91 | $4,850.10 | ▼ -75.91 after sell → book $10,259.21; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 52 | $24.09 | $2.17 | $+37.29 | $6,100.61 | ▲ +37.29 after sell → book $10,257.04; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 23 | $54.31 | $2.08 | $+37.03 | $7,347.66 | ▲ +37.03 after sell → book $10,254.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 43 | $28.23 | $2.12 | — | $6,131.65 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.3; leftover $1224.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 54 | $22.46 | $2.15 | — | $4,916.66 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1224.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 1144 | $1.07 | $14.76 | — | $3,677.82 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $1224.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 119 | $10.25 | $2.35 | — | $2,455.73 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1224.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $1,272.83 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,ohlc_hot; ret5=+17.7; leftover $1224.61 | — |
| 2026-09-17 09:30 ET | **BUY** | `PUMP` | 118 | $10.31 | $2.34 | — | $53.91 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+5.9; leftover $1224.61 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.91 | ▲ close $10,432.03 vs 09:30 $10,267.72 (session +202.80) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.91 | ▼ 09:30 equity $10,383.63 vs yday $10,432.03 (-48.40) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 682 | $1.96 | $8.92 | $+91.40 | $1,381.71 | ▲ +91.40 after sell → book $10,374.71; vs 09:30 mark -8.92 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 43 | $28.55 | $2.14 | $+9.50 | $2,607.22 | ▲ +9.50 after sell → book $10,372.57; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 54 | $21.30 | $2.17 | $-66.96 | $3,755.25 | ▼ -66.96 after sell → book $10,370.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IQ` | 1144 | $1.12 | $14.96 | $+27.48 | $5,021.57 | ▲ +27.48 after sell → book $10,355.44; vs 09:30 mark -14.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 119 | $10.12 | $2.38 | $-20.19 | $6,223.47 | ▼ -20.19 after sell → book $10,353.06; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $7,393.44 | ▼ -12.93 after sell → book $10,351.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 118 | $10.48 | $2.37 | $+15.34 | $8,627.70 | ▲ +15.34 after sell → book $10,348.65; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $7,394.15 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1232.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 15 | $81.40 | $2.04 | — | $6,171.11 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1232.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 406 | $3.04 | $5.24 | — | $4,933.66 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1232.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 4 | $246.98 | $2.00 | — | $3,943.74 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1232.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 108 | $11.38 | $2.31 | — | $2,712.39 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $1232.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 154 | $7.98 | $2.45 | — | $1,481.02 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1232.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `NEO` | 61 | $19.91 | $2.17 | — | $264.33 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+12.9; leftover $1232.53 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $264.33 | ▲ close $10,357.37 vs 09:30 $10,383.63 (session +27.05) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $264.33 | ▲ 09:30 equity $10,547.80 vs yday $10,357.37 (+190.43) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 447 | $3.55 | $5.85 | $+368.33 | $1,845.33 | ▲ +368.33 after sell → book $10,541.95; vs 09:30 mark -5.85 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+0.37 | $3,079.25 | ▲ +0.37 after sell → book $10,539.81; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 15 | $79.08 | $2.06 | $-38.89 | $4,263.40 | ▼ -38.89 after sell → book $10,537.76; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 406 | $4.00 | $5.32 | $+381.23 | $5,882.08 | ▲ +381.23 after sell → book $10,532.44; vs 09:30 mark -5.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 4 | $231.62 | $2.02 | $-65.46 | $6,806.54 | ▼ -65.46 after sell → book $10,530.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 108 | $12.05 | $2.34 | $+67.70 | $8,105.60 | ▲ +67.70 after sell → book $10,528.08; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 154 | $7.84 | $2.49 | $-26.50 | $9,310.47 | ▼ -26.50 after sell → book $10,525.59; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `NEO` | 61 | $19.92 | $2.19 | $-3.76 | $10,523.40 | ▼ -3.76 after sell → book $10,523.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 532 | $2.47 | $6.86 | — | $9,202.49 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+73.6; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 77 | $16.91 | $2.22 | — | $7,898.20 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+50.5; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 112 | $11.67 | $2.33 | — | $6,588.84 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+31.3; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 797 | $1.65 | $10.28 | — | $5,263.51 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+43.0; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 15 | $83.53 | $2.04 | — | $4,008.52 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+8.8; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 228 | $5.75 | $2.94 | — | $2,693.44 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 100 | $13.05 | $2.29 | — | $1,386.15 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1315.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 52 | $24.93 | $2.15 | — | $87.64 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+8.7; leftover $1315.42 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.64 | ▲ close $10,729.85 vs 09:30 $10,547.80 (session +237.56) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.64 | ▼ 09:30 equity $10,714.35 vs yday $10,729.85 (-15.50) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 228 | $6.05 | $2.99 | $+62.47 | $1,465.19 | ▲ +62.47 after sell → book $10,711.36; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 100 | $12.99 | $2.32 | $-10.61 | $2,761.88 | ▼ -10.61 after sell → book $10,709.05; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 52 | $25.26 | $2.17 | $+12.85 | $4,073.23 | ▲ +12.85 after sell → book $10,706.88; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $3,751.83 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+35.1; leftover $581.89 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 63 | $9.11 | $2.18 | — | $3,175.72 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+44.4; leftover $581.89 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 135 | $4.30 | $2.40 | — | $2,592.82 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $581.89 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 80 | $7.23 | $2.23 | — | $2,012.19 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+36.6; leftover $581.89 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,012.19 | ▼ close $10,649.24 vs 09:30 $10,714.35 (session -48.84) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,012.19 | ▲ 09:30 equity $10,711.64 vs yday $10,649.24 (+62.40) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 77 | $16.92 | $2.24 | $-3.70 | $3,312.79 | ▼ -3.70 after sell → book $10,709.40; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 112 | $12.80 | $2.36 | $+121.88 | $4,744.03 | ▲ +121.88 after sell → book $10,707.04; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 797 | $1.41 | $10.42 | $-211.98 | $5,857.38 | ▼ -211.98 after sell → book $10,696.62; vs 09:30 mark -10.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 15 | $86.57 | $2.06 | $+41.51 | $7,153.87 | ▲ +41.51 after sell → book $10,694.56; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $7,483.64 | ▲ +8.36 after sell → book $10,692.55; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 63 | $8.39 | $2.20 | $-49.74 | $8,010.01 | ▼ -49.74 after sell → book $10,690.35; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DGXX` | 135 | $4.30 | $2.43 | $-4.82 | $8,588.08 | ▼ -4.82 after sell → book $10,687.92; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 80 | $6.83 | $2.25 | $-36.48 | $9,132.23 | ▼ -36.48 after sell → book $10,685.67; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 483 | $2.70 | $6.23 | — | $7,821.90 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+109.2; leftover $1304.60 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 290 | $4.49 | $3.74 | — | $6,516.06 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+26.4; leftover $1304.60 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 18 | $70.84 | $2.04 | — | $5,238.89 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1304.60 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 131 | $9.90 | $2.38 | — | $3,939.61 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1304.60 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1003 | $1.30 | $12.94 | — | $2,622.77 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.3; leftover $1304.60 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 31 | $41.76 | $2.08 | — | $1,326.13 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1304.60 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 70 | $18.57 | $2.20 | — | $23.68 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+8.2; leftover $1304.60 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.68 | ▼ close $10,388.33 vs 09:30 $10,711.64 (session -265.72) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.68 | ▼ 09:30 equity $10,291.29 vs yday $10,388.33 (-97.04) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 532 | $2.68 | $6.96 | $+97.89 | $1,442.48 | ▲ +97.89 after sell → book $10,284.33; vs 09:30 mark -6.96 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 290 | $3.92 | $3.80 | $-171.39 | $2,576.93 | ▼ -171.39 after sell → book $10,280.53; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 18 | $70.50 | $2.06 | $-10.23 | $3,843.86 | ▼ -10.23 after sell → book $10,278.46; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 131 | $9.12 | $2.41 | $-106.98 | $5,036.17 | ▼ -106.98 after sell → book $10,276.05; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1003 | $1.27 | $13.12 | $-56.14 | $6,296.86 | ▼ -56.14 after sell → book $10,262.93; vs 09:30 mark -13.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 31 | $36.02 | $2.10 | $-181.97 | $7,411.54 | ▼ -181.97 after sell → book $10,260.83; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NTSK` | 70 | $18.50 | $2.22 | $-9.67 | $8,704.31 | ▼ -9.67 after sell → book $10,258.61; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,704.31 | ▲ close $11,288.36 vs 09:30 $10,291.29 (session +1,029.76) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,177.04 | ▲ 09:30 equity $11,099.18 vs yday $10,769.03 (+330.15) | 09:30 open · cash $7,177.04 (unchanged overnight, no fees) · equity $11,099.18 vs prior close $10,769.03 (+330.15) · 2 name(s) re-marked at the open (per-name table). GLND×465 yday $5.35 → 09:30 $6.06 +330.15; VICR×4 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 34 | $29.76 | $2.09 | — | $6,163.11 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1025.29 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 63 | $16.21 | $2.18 | — | $5,139.70 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1025.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 65 | $15.58 | $2.19 | — | $4,124.74 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+84.4; leftover $1025.29 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $3,134.73 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1025.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 100 | $10.20 | $2.29 | — | $2,112.44 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1025.29 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 256 | $4.00 | $3.30 | — | $1,083.86 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1025.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 466 | $2.20 | $6.01 | — | $52.64 | — | rank by w_hot_candle; rank w_hot_candle; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1025.29 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.64 | ▼ close $10,917.57 vs 09:30 $11,099.18 (session -161.53) | 16:00 close · cash $52.64 · equity $10,917.57 vs 09:30 $11,099.18 (-181.61; session marks -161.53) · 9 name(s) marked open→close (per-name table). GLND×465 09:30 $6.06 → close $5.54 -241.80; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×34 09:30 $29.76 → close $26.24 -119.68; SECZ×63 09:30 $16.21 → close $15.96 -15.75; USDE×65 09:30 $15.58 → close $17.25 +108.48; GRAL×8 09:30 $123.50 → close $126.89 +27.12; DNA×100 09:30 $10.20 → close $10.66 +46.00; CYPH×256 09:30 $4.00 → close $4.12 +29.44; HLP×466 09:30 $2.20 → close $2.21 +4.66 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
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
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SION` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 483 | 2026-09-23 @ $2.70 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+109.2; leftover $1304.60 |
