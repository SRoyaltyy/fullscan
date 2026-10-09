# Factor mine action — `union_w_hot_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **+26.48%** ($12,648) · signal-only (no cash/fees) was +31.34%. Starts YES **29/30**. Fills 350 · skips 96 · realized $-9.27.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: a mix of tape-heat and green cameras.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by a mix of tape-heat and green cameras and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,513.55.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▼ -55.19 after sell → book $10,310.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▼ -26.05 after sell → book $10,308.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | ▲ +148.79 after sell → book $10,287.11; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▼ -29.03 after sell → book $10,284.98; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ +69.56 after sell → book $10,282.64; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ +69.58 after sell → book $10,280.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $7,702.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $6,428.53 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $3,844.94 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $2,583.04 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $1,319.50 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $32.92 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.92 | ▼ close $9,605.00 vs 09:30 $10,312.70 (session -634.90) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.92 | ▼ 09:30 equity $9,523.77 vs yday $9,605.00 (-81.23) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,321.92 | ▲ +3.49 after sell → book $9,521.61; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,529.84 | ▼ -82.19 after sell → book $9,513.98; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,799.69 | ▼ -4.39 after sell → book $9,511.77; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,897.87 | ▼ -182.95 after sell → book $9,509.41; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $5,809.00 | ▼ -391.33 after sell → book $9,494.83; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $6,910.85 | ▼ -160.05 after sell → book $9,492.68; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $8,133.81 | ▼ -40.58 after sell → book $9,490.56; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,488.35 | ▲ +67.96 after sell → book $9,488.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 283 | $4.19 | $3.65 | — | $8,298.93 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 86 | $13.64 | $2.25 | — | $7,123.64 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 172 | $6.87 | $2.51 | — | $5,939.49 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $4,765.59 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 28 | $41.23 | $2.07 | — | $3,609.08 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 80 | $14.66 | $2.23 | — | $2,434.05 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 79 | $14.94 | $2.23 | — | $1,251.56 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1186.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 617 | $1.92 | $7.96 | — | $58.96 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1186.04 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.96 | ▼ close $9,196.28 vs 09:30 $9,523.77 (session -267.07) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.96 | ▼ 09:30 equity $9,010.77 vs yday $9,196.28 (-185.51) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 283 | $3.94 | $3.71 | $-78.11 | $1,170.28 | ▼ -78.11 after sell → book $9,007.07; vs 09:30 mark -3.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 86 | $13.31 | $2.27 | $-32.90 | $2,312.66 | ▼ -32.90 after sell → book $9,004.79; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $3,339.79 | ▼ -146.78 after sell → book $9,002.68; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 28 | $41.50 | $2.09 | $+3.39 | $4,499.69 | ▲ +3.39 after sell → book $9,000.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 80 | $13.19 | $2.25 | $-122.08 | $5,552.64 | ▼ -122.08 after sell → book $8,998.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 79 | $14.01 | $2.25 | $-77.95 | $6,657.18 | ▼ -77.95 after sell → book $8,996.08; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 617 | $1.70 | $8.07 | $-151.77 | $7,698.01 | ▼ -151.77 after sell → book $8,988.01; vs 09:30 mark -8.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,698.01 | ▼ close $8,915.77 vs 09:30 $9,010.77 (session -72.24) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,698.01 | ▲ 09:30 equity $8,934.69 vs yday $8,915.77 (+18.92) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 172 | $7.19 | $2.54 | $+49.99 | $8,932.14 | ▲ +49.99 after sell → book $8,932.14; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,932.14 | ▲ close $8,932.14 vs 09:30 $8,934.69 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,932.14 | ▲ 09:30 equity $8,932.14 vs yday $8,932.14 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $7,879.15 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 970 | $1.15 | $12.51 | — | $6,751.14 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 94 | $11.81 | $2.27 | — | $5,638.26 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 125 | $8.91 | $2.37 | — | $4,522.14 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 452 | $2.47 | $5.83 | — | $3,399.87 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $2,284.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,271.10 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1116.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $169.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1116.52 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.66 | ▲ close $8,958.65 vs 09:30 $8,932.14 (session +57.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.66 | ▲ 09:30 equity $9,190.07 vs yday $8,958.65 (+231.42) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 94 | $11.57 | $2.30 | $-27.60 | $1,254.94 | ▼ -27.60 after sell → book $9,187.77; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 125 | $9.24 | $2.40 | $+36.49 | $2,407.54 | ▲ +36.49 after sell → book $9,185.37; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 452 | $2.47 | $5.92 | $-11.75 | $3,518.07 | ▼ -11.75 after sell → book $9,179.46; vs 09:30 mark -5.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEM` | 18 | $65.60 | $2.06 | $+63.75 | $4,696.80 | ▲ +63.75 after sell → book $9,177.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $5,777.67 | ▲ +67.08 after sell → book $9,175.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $6,961.02 | ▲ +81.90 after sell → book $9,173.19; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,799.27 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.17 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $4,639.45 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.17 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $3,483.91 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.17 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 140 | $8.28 | $2.41 | — | $2,322.30 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1160.17 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $1,245.42 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1160.17 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $85.22 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $1160.17 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.22 | ▲ close $9,437.82 vs 09:30 $9,190.07 (session +279.56) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.22 | ▲ 09:30 equity $10,038.55 vs yday $9,437.82 (+600.73) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,082.09 | ▼ -56.12 after sell → book $10,036.52; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 970 | $1.83 | $12.69 | $+634.40 | $2,844.50 | ▲ +634.40 after sell → book $10,023.83; vs 09:30 mark -12.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.32 | $3.38 | $-50.57 | $3,955.68 | ▼ -50.57 after sell → book $10,020.45; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.33 | $2.33 | $+224.17 | $5,339.67 | ▲ +224.17 after sell → book $10,018.12; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.08 | $2.40 | $-131.77 | $6,363.42 | ▼ -131.77 after sell → book $10,015.71; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 140 | $8.59 | $2.44 | $+38.55 | $7,563.58 | ▲ +38.55 after sell → book $10,013.27; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $8,646.13 | ▲ +5.67 after sell → book $10,011.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.03 | $2.54 | $+202.36 | $10,008.70 | ▲ +202.36 after sell → book $10,008.70; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,008.70 | ▲ close $10,008.70 vs 09:30 $10,038.55 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,008.70 | ▲ 09:30 equity $10,008.70 vs yday $10,008.70 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 51 | $24.11 | $2.14 | — | $8,776.94 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+891.7; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 801 | $1.56 | $10.33 | — | $7,517.05 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 307 | $4.07 | $3.96 | — | $6,263.60 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 238 | $5.24 | $3.07 | — | $5,013.41 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 65 | $19.04 | $2.19 | — | $3,773.62 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+49.5; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 352 | $3.55 | $4.54 | — | $2,519.48 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+27.9; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 35 | $35.05 | $2.10 | — | $1,290.64 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1251.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 767 | $1.63 | $9.89 | — | $30.53 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1251.09 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.53 | ▲ close $10,542.69 vs 09:30 $10,008.70 (session +572.22) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.53 | ▼ 09:30 equity $10,293.58 vs yday $10,542.69 (-249.11) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 51 | $26.61 | $2.16 | $+123.19 | $1,385.48 | ▲ +123.19 after sell → book $10,291.42; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 801 | $1.60 | $10.48 | $+11.23 | $2,656.60 | ▲ +11.23 after sell → book $10,280.94; vs 09:30 mark -10.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 238 | $4.98 | $3.12 | $-68.07 | $3,838.72 | ▼ -68.07 after sell → book $10,277.82; vs 09:30 mark -3.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 65 | $20.72 | $2.21 | $+104.81 | $5,183.32 | ▲ +104.81 after sell → book $10,275.61; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 352 | $3.77 | $4.61 | $+68.29 | $6,505.75 | ▲ +68.29 after sell → book $10,271.00; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 35 | $35.70 | $2.12 | $+18.54 | $7,753.13 | ▲ +18.54 after sell → book $10,268.89; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 767 | $1.75 | $10.03 | $+75.95 | $9,089.19 | ▲ +75.95 after sell → book $10,258.86; vs 09:30 mark -10.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 92 | $14.11 | $2.27 | — | $7,788.80 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+11.4; leftover $1298.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 223 | $5.81 | $2.88 | — | $6,490.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1298.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 112 | $11.59 | $2.33 | — | $5,190.45 | — | rank by w_hot_cond; rank w_hot_cond; list overnight; 🔵; ret5=+64.9; leftover $1298.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 32 | $40.50 | $2.09 | — | $3,892.36 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1298.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $2,643.64 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1298.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 249 | $5.21 | $3.21 | — | $1,343.14 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1298.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $273.06 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1298.46 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.06 | ▲ close $10,347.72 vs 09:30 $10,293.58 (session +105.65) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.06 | ▲ 09:30 equity $10,552.91 vs yday $10,347.72 (+205.19) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 92 | $14.20 | $2.29 | $+3.72 | $1,577.16 | ▲ +3.72 after sell → book $10,550.61; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 223 | $6.50 | $2.93 | $+148.07 | $3,023.74 | ▲ +148.07 after sell → book $10,547.69; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 112 | $12.18 | $2.36 | $+61.96 | $4,385.54 | ▲ +61.96 after sell → book $10,545.33; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 32 | $37.42 | $2.11 | $-102.75 | $5,580.88 | ▼ -102.75 after sell → book $10,543.23; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $6,858.84 | ▲ +29.24 after sell → book $10,541.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 249 | $5.49 | $3.26 | $+63.24 | $8,222.58 | ▲ +63.24 after sell → book $10,537.92; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $9,289.48 | ▼ -3.18 after sell → book $10,535.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 144 | $9.19 | $2.42 | — | $7,963.70 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1327.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 71 | $18.50 | $2.20 | — | $6,648.00 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+17.2; leftover $1327.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 510 | $2.60 | $6.58 | — | $5,315.42 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot; ret5=+13.0; leftover $1327.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `DJT` | 138 | $9.59 | $2.40 | — | $3,990.28 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+13.8; leftover $1327.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 275 | $4.81 | $3.55 | — | $2,663.99 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+14.8; leftover $1327.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 57 | $22.93 | $2.16 | — | $1,354.82 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+9.5; leftover $1327.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 9 | $144.18 | $2.02 | — | $55.18 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=-14.2; leftover $1327.07 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.18 | ▲ close $10,521.58 vs 09:30 $10,552.91 (session +7.01) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.18 | ▼ 09:30 equity $10,392.12 vs yday $10,521.58 (-129.46) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 307 | $3.69 | $4.02 | $-124.64 | $1,183.99 | ▼ -124.64 after sell → book $10,388.10; vs 09:30 mark -4.02 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 71 | $18.15 | $2.23 | $-29.28 | $2,470.41 | ▼ -29.28 after sell → book $10,385.87; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 510 | $2.68 | $6.67 | $+27.55 | $3,830.54 | ▲ +27.55 after sell → book $10,379.20; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DJT` | 138 | $9.72 | $2.44 | $+13.79 | $5,169.46 | ▲ +13.79 after sell → book $10,376.76; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OABI` | 275 | $4.54 | $3.60 | $-81.40 | $6,414.36 | ▼ -81.40 after sell → book $10,373.16; vs 09:30 mark -3.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 57 | $23.21 | $2.18 | $+11.62 | $7,735.14 | ▲ +11.62 after sell → book $10,370.97; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRNA` | 9 | $137.19 | $2.04 | $-66.96 | $8,967.82 | ▼ -66.96 after sell → book $10,368.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 91 | $14.00 | $2.26 | — | $7,691.55 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=-3.3; leftover $1281.12 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,520.98 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1281.12 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 54 | $23.30 | $2.15 | — | $5,260.63 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+14.5; leftover $1281.12 | — |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 72 | $17.78 | $2.21 | — | $3,978.26 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1281.12 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 140 | $9.13 | $2.41 | — | $2,697.65 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+20.0; leftover $1281.12 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,419.80 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1281.12 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $494.10 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.8; leftover $1281.12 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $494.10 | ▼ close $10,171.21 vs 09:30 $10,392.12 (session -182.67) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $494.10 | ▼ 09:30 equity $10,112.49 vs yday $10,171.21 (-58.72) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 144 | $9.50 | $2.46 | $+39.76 | $1,859.64 | ▲ +39.76 after sell → book $10,110.03; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,041.85 | ▲ +11.63 after sell → book $10,108.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 54 | $22.66 | $2.17 | $-38.88 | $4,263.32 | ▼ -38.88 after sell → book $10,105.83; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 72 | $18.15 | $2.23 | $+22.21 | $5,567.89 | ▲ +22.21 after sell → book $10,103.60; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 140 | $8.66 | $2.44 | $-70.65 | $6,777.85 | ▼ -70.65 after sell → book $10,101.16; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $7,966.51 | ▼ -89.19 after sell → book $10,099.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $8,840.39 | ▼ -51.81 after sell → book $10,097.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,840.39 | ▼ close $10,050.69 vs 09:30 $10,112.49 (session -46.41) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,840.39 | ▼ 09:30 equity $10,027.03 vs yday $10,050.69 (-23.66) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 91 | $13.04 | $2.29 | $-91.91 | $10,024.74 | ▼ -91.91 after sell → book $10,024.74; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,024.74 | ▲ close $10,024.74 vs 09:30 $10,027.03 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,024.74 | ▲ 09:30 equity $10,024.74 vs yday $10,024.74 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,024.74 | ▲ close $10,024.74 vs 09:30 $10,024.74 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,024.74 | ▲ 09:30 equity $10,024.74 vs yday $10,024.74 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 703 | $1.78 | $9.07 | — | $8,764.34 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+183.1; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 68 | $18.40 | $2.19 | — | $7,510.94 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-32.2; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 649 | $1.93 | $8.37 | — | $6,250.00 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $5,006.09 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $3,852.89 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $2,609.69 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 120 | $10.42 | $2.35 | — | $1,356.94 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1253.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 156 | $8.03 | $2.46 | — | $101.81 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1253.09 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.81 | ▼ close $9,578.95 vs 09:30 $10,024.74 (session -414.98) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.81 | ▲ 09:30 equity $9,625.31 vs yday $9,578.95 (+46.36) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 68 | $18.15 | $2.22 | $-21.41 | $1,333.79 | ▼ -21.41 after sell → book $9,623.09; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 649 | $1.90 | $8.49 | $-36.33 | $2,558.40 | ▼ -36.33 after sell → book $9,614.60; vs 09:30 mark -8.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $3,795.92 | ▼ -6.39 after sell → book $9,612.44; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $4,920.86 | ▼ -28.26 after sell → book $9,610.40; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $6,073.76 | ▼ -90.29 after sell → book $9,608.16; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 120 | $10.50 | $2.38 | $+4.87 | $7,331.38 | ▲ +4.87 after sell → book $9,605.78; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 156 | $7.91 | $2.49 | $-23.67 | $8,562.85 | ▼ -23.67 after sell → book $9,603.29; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 211 | $5.79 | $2.72 | — | $7,338.44 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1223.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 74 | $16.40 | $2.21 | — | $6,122.63 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1223.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.70 | $2.03 | — | $4,962.79 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1223.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 487 | $2.51 | $6.28 | — | $3,734.14 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1223.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 270 | $4.53 | $3.48 | — | $2,507.56 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1223.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 212 | $5.75 | $2.73 | — | $1,285.82 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1223.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 48 | $25.18 | $2.13 | — | $75.05 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.0; leftover $1223.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.05 | ▲ close $10,109.14 vs 09:30 $9,625.31 (session +527.45) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.05 | ▼ 09:30 equity $9,944.34 vs yday $10,109.14 (-164.80) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 703 | $1.56 | $9.20 | $-169.41 | $1,166.05 | ▼ -169.41 after sell → book $9,935.14; vs 09:30 mark -9.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 211 | $5.81 | $2.77 | $-1.27 | $2,389.19 | ▼ -1.27 after sell → book $9,932.37; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 74 | $16.74 | $2.23 | $+20.71 | $3,625.72 | ▲ +20.71 after sell → book $9,930.14; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+93.50 | $4,879.05 | ▲ +93.50 after sell → book $9,928.09; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 487 | $2.66 | $6.37 | $+60.39 | $6,168.09 | ▲ +60.39 after sell → book $9,921.71; vs 09:30 mark -6.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 270 | $4.53 | $3.54 | $-7.02 | $7,387.66 | ▼ -7.02 after sell → book $9,918.18; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 212 | $5.95 | $2.78 | $+36.89 | $8,646.28 | ▲ +36.89 after sell → book $9,915.40; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 48 | $26.44 | $2.15 | $+56.19 | $9,913.24 | ▲ +56.19 after sell → book $9,913.24; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,913.24 | ▲ close $9,913.24 vs 09:30 $9,944.34 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,913.24 | ▲ 09:30 equity $9,913.24 vs yday $9,913.24 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,913.24 | ▲ close $9,913.24 vs 09:30 $9,913.24 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,913.24 | ▲ 09:30 equity $9,913.24 vs yday $9,913.24 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,913.24 | ▲ close $9,913.24 vs 09:30 $9,913.24 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,913.24 | ▲ 09:30 equity $9,913.24 vs yday $9,913.24 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 458 | $2.70 | $5.91 | — | $8,670.73 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 252 | $4.91 | $3.25 | — | $7,430.16 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 395 | $3.13 | $5.10 | — | $6,188.72 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+24.2; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 885 | $1.40 | $11.42 | — | $4,938.30 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-17.2; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $3,756.49 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 113 | $10.95 | $2.33 | — | $2,516.81 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $1,306.73 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1239.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 67 | $18.30 | $2.19 | — | $78.44 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1239.16 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.44 | ▲ close $10,026.39 vs 09:30 $9,913.24 (session +147.42) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.44 | ▼ 09:30 equity $10,024.17 vs yday $10,026.39 (-2.22) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `GPRO` | 885 | $1.37 | $11.57 | $-49.54 | $1,279.32 | ▼ -49.54 after sell → book $10,012.60; vs 09:30 mark -11.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 14 | $86.06 | $2.05 | $+20.98 | $2,482.11 | ▲ +20.98 after sell → book $10,010.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 113 | $10.29 | $2.36 | $-79.27 | $3,642.52 | ▼ -79.27 after sell → book $10,008.19; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 22 | $54.75 | $2.08 | $-7.65 | $4,844.94 | ▼ -7.65 after sell → book $10,006.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 67 | $18.28 | $2.21 | $-5.74 | $6,067.49 | ▼ -5.74 after sell → book $10,003.90; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,067.49 | ▲ close $10,271.45 vs 09:30 $10,024.17 (session +267.55) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,067.49 | ▲ 09:30 equity $10,350.21 vs yday $10,271.45 (+78.76) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 252 | $5.11 | $3.30 | $+43.85 | $7,351.91 | ▲ +43.85 after sell → book $10,346.91; vs 09:30 mark -3.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 395 | $3.64 | $5.17 | $+191.18 | $8,784.54 | ▲ +191.18 after sell → book $10,341.74; vs 09:30 mark -5.17 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,784.54 | ▲ close $10,451.66 vs 09:30 $10,350.21 (session +109.92) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,784.54 | ▲ 09:30 equity $10,460.82 vs yday $10,451.66 (+9.16) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $7,531.18 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1254.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 53 | $23.29 | $2.15 | — | $6,294.66 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+16.1; leftover $1254.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 10 | $118.18 | $2.02 | — | $5,110.84 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1254.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 22 | $55.66 | $2.06 | — | $3,884.27 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $1254.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 5 | $236.92 | $2.00 | — | $2,697.66 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.5; leftover $1254.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $1,568.61 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1254.93 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 46 | $27.09 | $2.13 | — | $320.34 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1254.93 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.34 | ▼ close $10,299.31 vs 09:30 $10,460.82 (session -147.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.34 | ▲ 09:30 equity $10,406.93 vs yday $10,299.31 (+107.62) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $-40.76 | $1,532.93 | ▼ -40.76 after sell → book $10,404.88; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 53 | $24.09 | $2.17 | $+38.08 | $2,807.53 | ▲ +38.08 after sell → book $10,402.71; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $-36.86 | $3,954.49 | ▼ -36.86 after sell → book $10,400.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 22 | $57.96 | $2.08 | $+46.47 | $5,227.53 | ▲ +46.47 after sell → book $10,398.59; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRWD` | 5 | $236.04 | $2.02 | $-8.43 | $6,405.71 | ▼ -8.43 after sell → book $10,396.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `HLP` | 610 | $2.10 | $7.87 | — | $5,116.84 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+60.5; leftover $1281.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 57 | $22.46 | $2.16 | — | $3,834.46 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1281.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 533 | $2.40 | $6.88 | — | $2,548.38 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1281.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 49 | $25.95 | $2.14 | — | $1,274.70 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1281.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $103.44 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot; ret5=+11.7; leftover $1281.14 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.44 | ▲ close $10,603.23 vs 09:30 $10,406.93 (session +227.71) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.44 | ▼ 09:30 equity $10,508.82 vs yday $10,603.23 (-94.41) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $+40.91 | $1,273.41 | ▲ +40.91 after sell → book $10,506.79; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 46 | $28.55 | $2.15 | $+62.88 | $2,584.56 | ▲ +62.88 after sell → book $10,504.64; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 610 | $1.96 | $7.98 | $-101.25 | $3,772.18 | ▼ -101.25 after sell → book $10,496.66; vs 09:30 mark -7.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 57 | $21.30 | $2.18 | $-70.46 | $4,984.10 | ▼ -70.46 after sell → book $10,494.48; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 533 | $2.29 | $6.97 | $-72.48 | $6,197.69 | ▼ -72.48 after sell → book $10,487.50; vs 09:30 mark -6.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 49 | $26.14 | $2.16 | $+5.02 | $7,476.40 | ▲ +5.02 after sell → book $10,485.35; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $8,720.02 | ▲ +72.37 after sell → book $10,483.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $7,486.46 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1245.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 410 | $3.04 | $5.29 | — | $6,236.83 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1245.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 15 | $81.40 | $2.04 | — | $5,013.79 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1245.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 316 | $3.94 | $4.08 | — | $3,764.67 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1245.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 5 | $246.98 | $2.00 | — | $2,527.77 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1245.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $1,331.70 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+21.3; leftover $1245.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 156 | $7.98 | $2.46 | — | $84.36 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1245.72 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.36 | ▼ close $10,349.24 vs 09:30 $10,508.82 (session -114.08) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.36 | ▲ 09:30 equity $10,569.33 vs yday $10,349.24 (+220.09) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 458 | $3.55 | $6.00 | $+377.39 | $1,704.26 | ▲ +377.39 after sell → book $10,563.33; vs 09:30 mark -6.00 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+0.37 | $2,938.19 | ▲ +0.37 after sell → book $10,561.20; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 410 | $4.00 | $5.37 | $+384.99 | $4,572.81 | ▲ +384.99 after sell → book $10,555.82; vs 09:30 mark -5.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 15 | $79.08 | $2.06 | $-38.89 | $5,756.96 | ▼ -38.89 after sell → book $10,553.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 316 | $3.90 | $4.14 | $-20.86 | $6,985.22 | ▼ -20.86 after sell → book $10,549.63; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-80.83 | $8,141.30 | ▼ -80.83 after sell → book $10,547.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $9,322.52 | ▼ -14.85 after sell → book $10,545.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 156 | $7.84 | $2.49 | $-26.79 | $10,543.07 | ▼ -26.79 after sell → book $10,543.07; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 533 | $2.47 | $6.88 | — | $9,219.68 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+73.6; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 77 | $16.91 | $2.22 | — | $7,915.39 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+50.5; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 112 | $11.67 | $2.33 | — | $6,606.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+31.3; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $5,452.77 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.5; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 100 | $13.05 | $2.29 | — | $4,145.48 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $3,001.67 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+10.6; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 52 | $24.93 | $2.15 | — | $1,703.17 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+8.7; leftover $1317.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 123 | $10.71 | $2.36 | — | $383.48 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1317.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $383.48 | ▲ close $10,603.96 vs 09:30 $10,569.33 (session +83.12) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $383.48 | ▼ 09:30 equity $10,573.64 vs yday $10,603.96 (-30.32) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 100 | $12.99 | $2.32 | $-10.61 | $1,680.16 | ▼ -10.61 after sell → book $10,571.32; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 52 | $25.26 | $2.17 | $+12.85 | $2,991.51 | ▲ +12.85 after sell → book $10,569.15; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $2,670.11 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+35.1; leftover $427.36 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 46 | $9.11 | $2.13 | — | $2,248.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+44.4; leftover $427.36 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 59 | $7.23 | $2.17 | — | $1,820.19 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+36.6; leftover $427.36 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 15 | $28.02 | $2.04 | — | $1,397.85 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $427.36 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,397.85 | ▼ close $10,508.74 vs 09:30 $10,573.64 (session -52.10) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,397.85 | ▲ 09:30 equity $10,822.87 vs yday $10,508.74 (+314.13) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 77 | $16.92 | $2.24 | $-3.70 | $2,698.45 | ▼ -3.70 after sell → book $10,820.63; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 112 | $12.80 | $2.36 | $+121.88 | $4,129.69 | ▲ +121.88 after sell → book $10,818.27; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 6 | $174.50 | $2.03 | $-98.84 | $5,174.66 | ▼ -98.84 after sell → book $10,816.24; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 123 | $10.11 | $2.39 | $-78.55 | $6,415.80 | ▼ -78.55 after sell → book $10,813.85; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $6,745.57 | ▲ +8.36 after sell → book $10,811.84; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 46 | $8.39 | $2.15 | $-37.40 | $7,129.36 | ▼ -37.40 after sell → book $10,809.69; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 59 | $6.83 | $2.19 | $-27.95 | $7,530.15 | ▼ -27.95 after sell → book $10,807.51; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 15 | $25.90 | $2.06 | $-35.89 | $7,916.59 | ▼ -35.89 after sell → book $10,805.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 488 | $2.70 | $6.30 | — | $6,592.70 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+109.2; leftover $1319.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 133 | $9.90 | $2.39 | — | $5,273.61 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1319.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 31 | $41.76 | $2.08 | — | $3,976.96 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1319.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 18 | $70.84 | $2.04 | — | $2,699.80 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1319.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 66 | $19.70 | $2.19 | — | $1,397.41 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1319.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 1797 | $0.73 | $18.58 | — | $59.83 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $1319.43 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.83 | ▼ close $10,616.53 vs 09:30 $10,822.87 (session -155.34) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.83 | ▼ 09:30 equity $10,490.32 vs yday $10,616.53 (-126.21) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 533 | $2.68 | $6.98 | $+98.08 | $1,481.30 | ▲ +98.08 after sell → book $10,483.35; vs 09:30 mark -6.97 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 133 | $9.12 | $2.42 | $-108.55 | $2,691.84 | ▼ -108.55 after sell → book $10,480.92; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 31 | $36.02 | $2.10 | $-181.97 | $3,806.51 | ▼ -181.97 after sell → book $10,478.82; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 18 | $70.50 | $2.06 | $-10.23 | $5,073.44 | ▼ -10.23 after sell → book $10,476.76; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMRX` | 66 | $19.29 | $2.21 | $-31.46 | $6,344.38 | ▼ -31.46 after sell → book $10,474.55; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVTL` | 1797 | $0.66 | $17.57 | $-168.41 | $7,513.55 | ▼ -168.41 after sell → book $10,456.98; vs 09:30 mark -17.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,513.55 | ▲ close $11,504.65 vs 09:30 $10,490.32 (session +1,047.67) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,507.55 | ▲ 09:30 equity $11,544.83 vs yday $11,201.19 (+343.64) | 09:30 open · cash $7,507.55 (unchanged overnight, no fees) · equity $11,544.83 vs prior close $11,201.19 (+343.64) · 2 name(s) re-marked at the open (per-name table). GLND×484 yday $5.35 → 09:30 $6.06 +343.64; VICR×4 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 36 | $29.76 | $2.10 | — | $6,434.09 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1072.51 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 66 | $16.21 | $2.19 | — | $5,362.04 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1072.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $4,372.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1072.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 68 | $15.58 | $2.19 | — | $3,310.32 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+84.4; leftover $1072.51 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 267 | $4.00 | $3.44 | — | $2,237.54 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1072.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 277 | $3.86 | $3.57 | — | $1,164.75 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1072.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 136 | $7.85 | $2.40 | — | $94.75 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $1072.51 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.75 | ▼ close $11,271.65 vs 09:30 $11,544.83 (session -255.26) | 16:00 close · cash $94.75 · equity $11,271.65 vs 09:30 $11,544.83 (-273.18; session marks -255.26) · 9 name(s) marked open→close (per-name table). GLND×484 09:30 $6.06 → close $5.54 -251.68; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×36 09:30 $29.76 → close $26.24 -126.72; SECZ×66 09:30 $16.21 → close $15.96 -16.50; GRAL×8 09:30 $123.50 → close $126.89 +27.12; USDE×68 09:30 $15.58 → close $17.25 +113.49; CYPH×267 09:30 $4.00 → close $4.12 +30.71; ZSQR×277 09:30 $3.86 → close $3.78 -22.16; RSKD×136 09:30 $7.85 → close $7.78 -9.52 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.75 | ▼ 09:30 equity $11,042.81 vs yday $11,271.65 (-228.84) | 09:30 open · cash $94.75 (unchanged overnight, no fees) · equity $11,042.81 vs prior close $11,271.65 (-228.84) · 9 name(s) re-marked at the open (per-name table). CYPH×267 yday $4.12 → 09:30 $4.03 -23.36; GLND×484 yday $5.54 → 09:30 $5.14 -194.57; GRAL×8 yday $126.89 → 09:30 $128.90 +16.08; RSKD×136 yday $7.78 → 09:30 $7.72 -8.16; SECZ×66 yday $15.96 → 09:30 $16.00 +2.64; TJGC×36 yday $26.24 → 09:30 $26.50 +9.36; USDE×68 yday $17.25 → 09:30 $16.85 -27.20; VICR×4 yday $276.06 → 09:30 $280.00 +15.76; ZSQR×277 yday $3.78 → 09:30 $3.71 -19.39 | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 267 | $4.03 | $3.50 | $+0.40 | $1,167.93 | ▲ +0.40 after sell → book $11,039.31; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `GLND` | 484 | $5.14 | $6.34 | $+1167.40 | $3,648.38 | ▲ +1,167.40 after sell → book $11,032.97; vs 09:30 mark -6.34 | dropped from list after 3 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `GRAL` | 8 | $128.90 | $2.03 | $+39.15 | $4,677.54 | ▲ +39.15 after sell → book $11,030.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `RSKD` | 136 | $7.72 | $2.43 | $-22.51 | $5,725.03 | ▼ -22.51 after sell → book $11,028.50; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SECZ` | 66 | $16.00 | $2.21 | $-18.26 | $6,778.82 | ▼ -18.26 after sell → book $11,026.29; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TJGC` | 36 | $26.50 | $2.12 | $-121.58 | $7,730.71 | ▼ -121.58 after sell → book $11,024.18; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `USDE` | 68 | $16.85 | $2.22 | $+81.88 | $8,874.29 | ▲ +81.88 after sell → book $11,021.96; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `VICR` | 4 | $280.00 | $2.02 | $+49.98 | $9,992.27 | ▲ +49.98 after sell → book $11,019.94; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 277 | $3.71 | $3.63 | $-48.75 | $11,016.31 | ▼ -48.75 after sell → book $11,016.31; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,016.31 | ▲ close $11,016.31 vs 09:30 $11,042.81 (session +0.00) | 16:00 close · cash $11,016.31 · no lots left · equity $11,016.31. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,016.31 | ▲ 09:30 equity $11,016.31 vs yday $11,016.31 (+0.00) | 09:30 open · cash $11,016.31 · no holdings · equity $11,016.31 vs prior close $11,016.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `KOD` | 15 | $87.50 | $2.04 | — | $9,701.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+177.4; leftover $1377.04 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GLND` | 345 | $3.98 | $4.45 | — | $8,324.22 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+55.9; leftover $1377.04 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SRFM` | 1157 | $1.19 | $14.93 | — | $6,932.47 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; ret5=+77.6; leftover $1377.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ZSQR` | 308 | $4.47 | $3.97 | — | $5,551.74 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+72.0; leftover $1377.04 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GRML` | 131 | $10.50 | $2.38 | — | $4,173.85 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+25.8; leftover $1377.04 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TJGC` | 49 | $27.68 | $2.14 | — | $2,815.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+62.3; leftover $1377.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AMC` | 414 | $3.32 | $5.34 | — | $1,435.58 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.8; leftover $1377.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `LTRX` | 187 | $7.34 | $2.55 | — | $60.44 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.8; leftover $1377.04 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.44 | ▲ close $11,421.93 vs 09:30 $11,016.31 (session +443.42) | 16:00 close · cash $60.44 · equity $11,421.93 vs 09:30 $11,016.31 (+405.62; session marks +443.42) · 8 name(s) marked open→close (per-name table). KOD×15 09:30 $87.50 → close $91.12 +54.30; GLND×345 09:30 $3.98 → close $4.75 +265.65; SRFM×1157 09:30 $1.19 → close $1.20 +11.57; ZSQR×308 09:30 $4.47 → close $4.15 -98.56; GRML×131 09:30 $10.50 → close $10.38 -16.38; TJGC×49 09:30 $27.68 → close $34.80 +348.64; AMC×414 09:30 $3.32 → close $3.08 -99.36; LTRX×187 09:30 $7.34 → close $7.22 -22.44 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.44 | ▲ 09:30 equity $11,421.93 vs yday $11,421.93 (+0.00) | 09:30 open · cash $60.44 (unchanged overnight, no fees) · equity $11,421.93 vs prior close $11,421.93 (+0.00) · 8 name(s) re-marked at the open (per-name table). AMC×414 yday $3.08 → 09:30 $3.08 +0.00; GLND×345 yday $4.75 → 09:30 $4.75 +0.00; GRML×131 yday $10.38 → 09:30 $10.38 +0.00; KOD×15 yday $91.12 → 09:30 $91.12 +0.00; LTRX×187 yday $7.22 → 09:30 $7.22 +0.00; SRFM×1157 yday $1.20 → 09:30 $1.20 +0.00; TJGC×49 yday $34.80 → 09:30 $34.80 +0.00; ZSQR×308 yday $4.15 → 09:30 $4.15 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.44 | ▲ close $11,421.93 vs 09:30 $11,421.93 (session +0.00) | 16:00 close · cash $60.44 · equity $11,421.93 vs 09:30 $11,421.93 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). AMC×414 09:30 $3.08 → close $3.08 +0.00; GLND×345 09:30 $4.75 → close $4.75 +0.00; GRML×131 09:30 $10.38 → close $10.38 +0.00; KOD×15 09:30 $91.12 → close $91.12 +0.00; LTRX×187 09:30 $7.22 → close $7.22 +0.00; SRFM×1157 09:30 $1.20 → close $1.20 +0.00; TJGC×49 09:30 $34.80 → close $34.80 +0.00; ZSQR×308 09:30 $4.15 → close $4.15 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.44 | ▼ 09:30 equity $11,061.50 vs yday $11,421.93 (-360.43) | 09:30 open · cash $60.44 (unchanged overnight, no fees) · equity $11,061.50 vs prior close $11,421.93 (-360.43) · 8 name(s) re-marked at the open (per-name table). AMC×414 yday $3.08 → 09:30 $2.98 -41.40; GLND×345 yday $4.75 → 09:30 $5.06 +106.95; GRML×131 yday $10.38 → 09:30 $10.04 -43.89; KOD×15 yday $91.12 → 09:30 $95.41 +64.35; LTRX×187 yday $7.22 → 09:30 $7.25 +5.61; SRFM×1157 yday $1.20 → 09:30 $1.15 -57.85; TJGC×49 yday $34.80 → 09:30 $26.31 -415.77; ZSQR×308 yday $4.15 → 09:30 $4.22 +21.56 | — |
| 2026-10-01 09:30 ET | **SELL** | `AMC` | 414 | $2.98 | $5.42 | $-151.52 | $1,288.74 | ▼ -151.52 after sell → book $11,056.08; vs 09:30 mark -5.42 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GLND` | 345 | $5.06 | $4.52 | $+363.63 | $3,029.92 | ▲ +363.63 after sell → book $11,051.56; vs 09:30 mark -4.52 | exit unpriced hold on first bar after 2 sess | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `GRML` | 131 | $10.04 | $2.42 | $-65.06 | $4,342.74 | ▼ -65.06 after sell → book $11,049.14; vs 09:30 mark -2.42 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `KOD` | 15 | $95.41 | $2.06 | $+114.56 | $5,771.84 | ▲ +114.56 after sell → book $11,047.09; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `LTRX` | 187 | $7.25 | $2.59 | $-21.97 | $7,124.99 | ▼ -21.97 after sell → book $11,044.49; vs 09:30 mark -2.60 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SRFM` | 1157 | $1.15 | $15.13 | $-76.33 | $8,440.42 | ▼ -76.33 after sell → book $11,029.37; vs 09:30 mark -15.12 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `TJGC` | 49 | $26.31 | $2.16 | $-71.42 | $9,727.45 | ▼ -71.42 after sell → book $11,027.21; vs 09:30 mark -2.16 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `ZSQR` | 308 | $4.22 | $4.03 | $-85.01 | $11,023.17 | ▼ -85.01 after sell → book $11,023.17; vs 09:30 mark -4.04 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `PACB` | 785 | $2.34 | $10.13 | — | $9,176.15 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+68.1; leftover $1837.20 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `QSI` | 1530 | $1.20 | $19.74 | — | $7,320.41 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; ret5=+41.0; leftover $1837.20 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PAYS` | 136 | $13.50 | $2.40 | — | $5,482.01 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+6.4; leftover $1837.20 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `FORM` | 12 | $149.45 | $2.03 | — | $3,686.59 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; ret5=+13.8; leftover $1837.20 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 128 | $14.27 | $2.37 | — | $1,857.65 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $1837.20 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `PDFS` | 35 | $51.41 | $2.10 | — | $56.21 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+5.4; leftover $1837.20 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.21 | ▲ close $11,619.59 vs 09:30 $11,061.50 (session +635.17) | 16:00 close · cash $56.21 · equity $11,619.59 vs 09:30 $11,061.50 (+558.09; session marks +635.17) · 6 name(s) marked open→close (per-name table). PACB×785 09:30 $2.34 → close $2.51 +133.45; QSI×1530 09:30 $1.20 → close $1.43 +351.90; PAYS×136 09:30 $13.50 → close $14.36 +116.96; FORM×12 09:30 $149.45 → close $148.44 -12.12; AVPT×128 09:30 $14.27 → close $14.08 -24.32; PDFS×35 09:30 $51.41 → close $53.39 +69.30 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.21 | ▼ 09:30 equity $11,619.51 vs yday $11,619.59 (-0.08) | 09:30 open · cash $56.21 (unchanged overnight, no fees) · equity $11,619.51 vs prior close $11,619.59 (-0.08) · 6 name(s) re-marked at the open (per-name table). AVPT×128 yday $14.08 → 09:30 $14.22 +17.92; FORM×12 yday $148.44 → 09:30 $150.50 +24.72; PACB×785 yday $2.51 → 09:30 $2.50 -7.85; PAYS×136 yday $14.36 → 09:30 $14.19 -23.12; PDFS×35 yday $53.39 → 09:30 $55.24 +64.75; QSI×1530 yday $1.43 → 09:30 $1.38 -76.50 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 128 | $14.22 | $2.41 | $-11.18 | $1,873.96 | ▼ -11.18 after sell → book $11,617.10; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `FORM` | 12 | $150.50 | $2.05 | $+8.52 | $3,677.91 | ▲ +8.52 after sell → book $11,615.05; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PACB` | 785 | $2.50 | $10.27 | $+105.20 | $5,630.14 | ▲ +105.20 after sell → book $11,604.78; vs 09:30 mark -10.27 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PAYS` | 136 | $14.19 | $2.44 | $+89.01 | $7,557.54 | ▲ +89.01 after sell → book $11,602.34; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PDFS` | 35 | $55.24 | $2.12 | $+129.83 | $9,488.82 | ▲ +129.83 after sell → book $11,600.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SDEV` | 267 | $5.06 | $3.44 | — | $8,134.36 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+183.7; leftover $1355.55 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `NAUT` | 814 | $1.67 | $10.50 | — | $6,768.55 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+58.4; leftover $1355.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SES` | 1576 | $0.86 | $18.28 | — | $5,394.90 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+54.3; leftover $1355.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SNPS` | 2 | $497.86 | $2.00 | — | $4,397.20 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; leftover $1355.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `GLND` | 318 | $4.25 | $4.10 | — | $3,041.60 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover,ohlc_hot; ret5=-17.8; leftover $1355.55 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `FOSL` | 195 | $6.95 | $2.58 | — | $1,683.77 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.2; leftover $1355.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 20 | $66.33 | $2.05 | — | $355.12 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1355.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $355.12 | ▲ close $12,548.45 vs 09:30 $11,619.51 (session +991.18) | 16:00 close · cash $355.12 · equity $12,548.45 vs 09:30 $11,619.51 (+928.94; session marks +991.18) · 8 name(s) marked open→close (per-name table). QSI×1530 09:30 $1.38 → close $1.55 +260.10; SDEV×267 09:30 $5.06 → close $7.48 +646.14; NAUT×814 09:30 $1.67 → close $1.96 +240.13; SES×1576 09:30 $0.86 → close $0.88 +38.93; SNPS×2 09:30 $497.86 → close $489.90 -15.91; GLND×318 09:30 $4.25 → close $3.73 -165.36; FOSL×195 09:30 $6.95 → close $6.80 -29.25; CDNA×20 09:30 $66.33 → close $67.15 +16.40 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $355.12 | ▲ 09:30 equity $13,390.21 vs yday $12,548.45 (+841.76) | 09:30 open · cash $355.12 (unchanged overnight, no fees) · equity $13,390.21 vs prior close $12,548.45 (+841.76) · 8 name(s) re-marked at the open (per-name table). CDNA×20 yday $67.15 → 09:30 $66.90 -5.00; FOSL×195 yday $6.80 → 09:30 $6.86 +11.70; GLND×318 yday $3.73 → 09:30 $3.92 +62.01; NAUT×814 yday $1.96 → 09:30 $2.18 +179.08; QSI×1530 yday $1.55 → 09:30 $1.52 -38.25; SDEV×267 yday $7.48 → 09:30 $9.71 +595.41; SES×1576 yday $0.88 → 09:30 $0.90 +24.11; SNPS×2 yday $489.90 → 09:30 $496.25 +12.70 | — |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 20 | $66.90 | $2.07 | $+7.28 | $1,691.05 | ▲ +7.28 after sell → book $13,388.14; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `FOSL` | 195 | $6.86 | $2.62 | $-22.74 | $3,026.13 | ▼ -22.74 after sell → book $13,385.52; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `GLND` | 318 | $3.92 | $4.17 | $-111.62 | $4,270.12 | ▼ -111.62 after sell → book $13,381.36; vs 09:30 mark -4.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `NAUT` | 814 | $2.18 | $10.65 | $+398.06 | $6,033.99 | ▲ +398.06 after sell → book $13,370.71; vs 09:30 mark -10.65 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `QSI` | 1530 | $1.52 | $20.01 | $+457.50 | $8,347.23 | ▲ +457.50 after sell → book $13,350.70; vs 09:30 mark -20.01 | dropped from list after 2 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SDEV` | 267 | $9.71 | $3.51 | $+1234.60 | $10,936.29 | ▲ +1,234.60 after sell → book $13,347.19; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `SES` | 1576 | $0.90 | $19.18 | $+25.57 | $12,335.50 | ▲ +25.57 after sell → book $13,328.00; vs 09:30 mark -19.19 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SNPS` | 2 | $496.25 | $2.02 | $-7.22 | $13,325.99 | ▼ -7.22 after sell → book $13,325.99; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `NTAP` | 7 | $225.47 | $2.01 | — | $11,745.69 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+12.5; leftover $1665.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VECO` | 29 | $56.94 | $2.08 | — | $10,092.35 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.5; leftover $1665.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `FEAM` | 429 | $3.88 | $5.53 | — | $8,422.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+38.6; leftover $1665.75 | join🟡 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RXO` | 57 | $29.00 | $2.16 | — | $6,767.13 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; ret5=+18.1; leftover $1665.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `NNBR` | 362 | $4.59 | $4.67 | — | $5,100.88 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+22.9; leftover $1665.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `WOLF` | 48 | $34.04 | $2.13 | — | $3,464.83 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+28.0; leftover $1665.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SYNA` | 13 | $120.98 | $2.03 | — | $1,890.06 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+17.9; leftover $1665.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VIAV` | 35 | $47.39 | $2.10 | — | $229.32 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.8; leftover $1665.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.32 | ▼ close $13,086.09 vs 09:30 $13,390.21 (session -217.19) | 16:00 close · cash $229.32 · equity $13,086.09 vs 09:30 $13,390.21 (-304.12; session marks -217.19) · 8 name(s) marked open→close (per-name table). NTAP×7 09:30 $225.47 → close $223.77 -11.90; VECO×29 09:30 $56.94 → close $56.31 -18.27; FEAM×429 09:30 $3.88 → close $3.92 +17.16; RXO×57 09:30 $29.00 → close $28.65 -19.95; NNBR×362 09:30 $4.59 → close $4.24 -126.70; WOLF×48 09:30 $34.04 → close $33.53 -24.48; SYNA×13 09:30 $120.98 → close $119.73 -16.25; VIAV×35 09:30 $47.39 → close $46.91 -16.80 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $229.32 | ▲ 09:30 equity $13,194.09 vs yday $13,086.09 (+108.00) | 09:30 open · cash $229.32 (unchanged overnight, no fees) · equity $13,194.09 vs prior close $13,086.09 (+108.00) · 8 name(s) re-marked at the open (per-name table). FEAM×429 yday $3.92 → 09:30 $3.96 +17.16; NNBR×362 yday $4.24 → 09:30 $4.24 +0.00; NTAP×7 yday $223.77 → 09:30 $224.80 +7.21; RXO×57 yday $28.65 → 09:30 $28.73 +4.56; SYNA×13 yday $119.73 → 09:30 $120.65 +11.96; VECO×29 yday $56.31 → 09:30 $57.24 +27.05; VIAV×35 yday $46.91 → 09:30 $47.41 +17.50; WOLF×48 yday $33.53 → 09:30 $34.00 +22.56 | — |
| 2026-10-06 09:30 ET | **SELL** | `FEAM` | 429 | $3.96 | $5.62 | $+23.17 | $1,922.54 | ▲ +23.17 after sell → book $13,188.48; vs 09:30 mark -5.61 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `NNBR` | 362 | $4.24 | $4.74 | $-136.11 | $3,452.68 | ▼ -136.11 after sell → book $13,183.73; vs 09:30 mark -4.75 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `NTAP` | 7 | $224.80 | $2.03 | $-8.73 | $5,024.25 | ▼ -8.73 after sell → book $13,181.70; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `RXO` | 57 | $28.73 | $2.18 | $-19.74 | $6,659.67 | ▼ -19.74 after sell → book $13,179.52; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `SYNA` | 13 | $120.65 | $2.05 | $-8.37 | $8,226.07 | ▼ -8.37 after sell → book $13,177.46; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `VECO` | 29 | $57.24 | $2.10 | $+4.61 | $9,884.01 | ▲ +4.61 after sell → book $13,175.36; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `VIAV` | 35 | $47.41 | $2.12 | $-3.51 | $11,541.25 | ▼ -3.51 after sell → book $13,173.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `WOLF` | 48 | $34.00 | $2.16 | $-6.21 | $13,171.09 | ▼ -6.21 after sell → book $13,171.09; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **BUY** | `SDEV` | 473 | $3.48 | $6.10 | — | $11,518.95 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+151.0; leftover $1646.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `QTEX` | 965 | $1.71 | $12.45 | — | $9,861.17 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+123.8; leftover $1646.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PTC` | 8 | $193.00 | $2.01 | — | $8,315.16 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+37.6; leftover $1646.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `DNA` | 109 | $15.08 | $2.32 | — | $6,669.12 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+43.2; leftover $1646.39 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PAGS` | 150 | $10.96 | $2.44 | — | $5,022.68 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+23.0; leftover $1646.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `GWRE` | 9 | $165.00 | $2.02 | — | $3,535.66 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $1646.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XP` | 56 | $29.20 | $2.16 | — | $1,898.31 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+39.1; leftover $1646.39 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `AVPT` | 111 | $14.77 | $2.32 | — | $256.51 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+7.2; leftover $1646.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $256.51 | ▼ close $12,597.85 vs 09:30 $13,194.09 (session -541.41) | 16:00 close · cash $256.51 · equity $12,597.85 vs 09:30 $13,194.09 (-596.24; session marks -541.41) · 8 name(s) marked open→close (per-name table). SDEV×473 09:30 $3.48 → close $3.25 -108.79; QTEX×965 09:30 $1.71 → close $1.59 -110.97; PTC×8 09:30 $193.00 → close $193.00 +0.00; DNA×109 09:30 $15.08 → close $12.41 -291.03; PAGS×150 09:30 $10.96 → close $10.72 -36.00; GWRE×9 09:30 $165.00 → close $164.52 -4.32; XP×56 09:30 $29.20 → close $29.73 +29.68; AVPT×111 09:30 $14.77 → close $14.59 -19.98 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $256.51 | ▼ 09:30 equity $12,275.78 vs yday $12,597.85 (-322.07) | 09:30 open · cash $256.51 (unchanged overnight, no fees) · equity $12,275.78 vs prior close $12,597.85 (-322.07) · 8 name(s) re-marked at the open (per-name table). AVPT×111 yday $14.59 → 09:30 $14.51 -8.88; DNA×109 yday $12.41 → 09:30 $11.80 -65.95; GWRE×9 yday $164.52 → 09:30 $162.01 -22.59; PAGS×150 yday $10.72 → 09:30 $10.64 -12.00; PTC×8 yday $193.00 → 09:30 $193.20 +1.60; QTEX×965 yday $1.59 → 09:30 $1.38 -207.48; SDEV×473 yday $3.25 → 09:30 $3.27 +9.46; XP×56 yday $29.73 → 09:30 $29.44 -16.24 | — |
| 2026-10-07 09:30 ET | **SELL** | `AVPT` | 111 | $14.51 | $2.35 | $-33.54 | $1,864.77 | ▼ -33.54 after sell → book $12,273.43; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `DNA` | 109 | $11.80 | $2.35 | $-361.64 | $3,149.17 | ▼ -361.64 after sell → book $12,271.08; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-07 09:30 ET | **SELL** | `GWRE` | 9 | $162.01 | $2.04 | $-30.97 | $4,605.22 | ▼ -30.97 after sell → book $12,269.04; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `PAGS` | 150 | $10.64 | $2.48 | $-52.92 | $6,198.74 | ▼ -52.92 after sell → book $12,266.56; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `PTC` | 8 | $193.20 | $2.04 | $-2.45 | $7,742.30 | ▼ -2.45 after sell → book $12,264.53; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `QTEX` | 965 | $1.38 | $12.62 | $-343.52 | $9,056.56 | ▼ -343.52 after sell → book $12,251.91; vs 09:30 mark -12.62 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `SDEV` | 473 | $3.27 | $6.19 | $-111.62 | $10,597.08 | ▼ -111.62 after sell → book $12,245.72; vs 09:30 mark -6.19 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-07 09:30 ET | **SELL** | `XP` | 56 | $29.44 | $2.18 | $+9.10 | $12,243.53 | ▲ +9.10 after sell → book $12,243.53; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,243.53 | ▲ close $12,243.53 vs 09:30 $12,275.78 (session +0.00) | 16:00 close · cash $12,243.53 · no lots left · equity $12,243.53. | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,243.53 | ▲ 09:30 equity $12,243.53 vs yday $12,243.53 (+0.00) | 09:30 open · cash $12,243.53 · no holdings · equity $12,243.53 vs prior close $12,243.53 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,243.53 | ▲ close $12,243.53 vs 09:30 $12,243.53 (session +0.00) | 16:00 close · cash $12,243.53 · no lots left · equity $12,243.53. | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,243.53 | ▲ 09:30 equity $12,243.53 vs yday $12,243.53 (+0.00) | 09:30 open · cash $12,243.53 · no holdings · equity $12,243.53 vs prior close $12,243.53 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-09 09:30 ET | **BUY** | `PCRX` | 42 | $36.33 | $2.12 | — | $10,715.55 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+45.5; leftover $1530.44 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ADCT` | 968 | $1.58 | $12.49 | — | $9,173.63 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+47.2; leftover $1530.44 | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `KOD` | 16 | $94.64 | $2.04 | — | $7,657.35 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-4.3; leftover $1530.44 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `NAUT` | 800 | $1.91 | $10.32 | — | $6,118.23 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.9; leftover $1530.44 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `BYRN` | 325 | $4.70 | $4.19 | — | $4,586.54 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+35.9; leftover $1530.44 | join🔴 sector🟡 gen🔴 news🟡 digest🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `HAE` | 12 | $119.81 | $2.03 | — | $3,146.79 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+14.5; leftover $1530.44 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ECO` | 16 | $93.64 | $2.04 | — | $1,646.51 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; ret5=+10.1; leftover $1530.44 | join🟢 sector🟡 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `HAFN` | 144 | $10.56 | $2.42 | — | $123.45 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+7.7; leftover $1530.44 | join🟢 sector🟡 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.45 | ▲ close $12,647.75 vs 09:30 $12,243.53 (session +441.86) | 16:00 close · cash $123.45 · equity $12,647.75 vs 09:30 $12,243.53 (+404.22; session marks +441.86) · 8 name(s) marked open→close (per-name table). PCRX×42 09:30 $36.33 → close $36.31 -0.84; ADCT×968 09:30 $1.58 → close $1.73 +145.20; KOD×16 09:30 $94.64 → close $99.87 +83.68; NAUT×800 09:30 $1.91 → close $2.25 +271.20; BYRN×325 09:30 $4.70 → close $4.52 -58.50; HAE×12 09:30 $119.81 → close $118.69 -13.44; ECO×16 09:30 $93.64 → close $93.11 -8.48; HAFN×144 09:30 $10.56 → close $10.72 +23.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `WFRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EBS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIMO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `META` | cash | leftover split 427.36 < 1 share @ 731.40 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 5 | 2026-09-21 @ $230.25 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.5; leftover $1317.88 |
| `GLND` | 488 | 2026-09-23 @ $2.70 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+109.2; leftover $1319.43 |
