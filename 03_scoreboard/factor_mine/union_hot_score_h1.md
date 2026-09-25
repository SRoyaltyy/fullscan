# Factor mine action — `union_hot_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · rank by hot_score

Cash book **+13.07%** ($11,307) · signal-only (no cash/fees) was +50.67%. Starts YES **29/30**. Fills 249 · skips 90 · realized $+477.92.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Sort the keepers by how hot the prior tape looked and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,509.90.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $2,581.40 | — | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $1,333.60 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $47.02 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.02 | ▼ close $9,721.90 vs 09:30 $10,312.70 (session -518.06) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.02 | ▼ 09:30 equity $9,613.23 vs yday $9,721.90 (-108.67) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,336.02 | ▲ +3.49 after sell → book $9,611.07; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,605.86 | ▼ -4.39 after sell → book $9,608.86; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,813.79 | ▼ -82.19 after sell → book $9,601.23; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,911.97 | ▼ -182.95 after sell → book $9,598.87; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,134.94 | ▼ -40.58 after sell → book $9,596.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $7,046.06 | ▼ -391.33 after sell → book $9,582.17; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $8,223.33 | ▼ -70.53 after sell → book $9,580.08; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,577.87 | ▲ +67.96 after sell → book $9,577.87; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 285 | $4.19 | $3.68 | — | $8,380.04 | — | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,182.15 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 87 | $13.64 | $2.25 | — | $5,993.22 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,795.47 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,621.57 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 118 | $10.10 | $2.34 | — | $2,427.43 | — | rank by hot_score; rank hot_score; list mover_buy; ret5=+22.8; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $1,237.74 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1197.23 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 623 | $1.92 | $8.04 | — | $33.54 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1197.23 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.54 | ▼ close $9,336.96 vs 09:30 $9,613.23 (session -215.67) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▼ 09:30 equity $9,212.74 vs yday $9,336.96 (-124.22) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 285 | $3.94 | $3.73 | $-78.66 | $1,152.71 | ▼ -78.66 after sell → book $9,209.01; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 87 | $13.31 | $2.28 | $-33.24 | $2,308.40 | ▼ -33.24 after sell → book $9,206.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,509.80 | ▲ +3.66 after sell → book $9,204.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,536.93 | ▼ -146.78 after sell → book $9,202.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 118 | $10.45 | $2.37 | $+36.58 | $5,767.65 | ▲ +36.58 after sell → book $9,200.14; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $6,833.79 | ▼ -123.56 after sell → book $9,197.89; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 623 | $1.70 | $8.15 | $-153.25 | $7,884.73 | ▼ -153.25 after sell → book $9,189.73; vs 09:30 mark -8.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,884.73 | ▼ close $9,116.65 vs 09:30 $9,212.74 (session -73.08) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,884.73 | ▲ 09:30 equity $9,135.79 vs yday $9,116.65 (+19.14) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,133.24 | ▲ +50.62 after sell → book $9,133.24; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,133.24 | ▲ close $9,133.24 vs 09:30 $9,135.79 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,133.24 | ▲ 09:30 equity $9,133.24 vs yday $9,133.24 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,080.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 992 | $1.15 | $12.80 | — | $6,926.66 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 96 | $11.81 | $2.28 | — | $5,790.14 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 833 | $1.37 | $10.75 | — | $4,638.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 128 | $8.91 | $2.37 | — | $3,495.33 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 475 | $2.40 | $6.13 | — | $2,349.20 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 172 | $6.61 | $2.51 | — | $1,210.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1141.66 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 462 | $2.47 | $5.96 | — | $63.54 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1141.66 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.54 | ▼ close $8,961.68 vs 09:30 $9,133.24 (session -126.77) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.54 | ▲ 09:30 equity $9,233.91 vs yday $8,961.68 (+272.23) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 96 | $11.57 | $2.30 | $-28.10 | $1,171.95 | ▼ -28.10 after sell → book $9,231.60; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 833 | $1.46 | $10.89 | $+53.33 | $2,377.24 | ▲ +53.33 after sell → book $9,220.71; vs 09:30 mark -10.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 128 | $9.24 | $2.41 | $+37.46 | $3,557.55 | ▲ +37.46 after sell → book $9,218.30; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 475 | $2.28 | $6.22 | $-69.34 | $4,634.34 | ▼ -69.34 after sell → book $9,212.09; vs 09:30 mark -6.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 172 | $6.95 | $2.54 | $+54.29 | $5,827.19 | ▲ +54.29 after sell → book $9,209.54; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 462 | $2.47 | $6.05 | $-12.01 | $6,962.28 | ▼ -12.01 after sell → book $9,203.49; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,800.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $4,640.34 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $1160.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $3,480.51 | — | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $2,324.98 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 140 | $8.28 | $2.41 | — | $1,163.37 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1160.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3877 | $0.29 | $23.03 | — | $0.51 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.38 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.51 | ▲ close $9,669.70 vs 09:30 $9,233.91 (session +502.15) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.51 | ▲ 09:30 equity $10,394.40 vs yday $9,669.70 (+724.70) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.38 | ▼ -56.12 after sell → book $10,392.37; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 992 | $1.83 | $12.98 | $+648.79 | $2,799.76 | ▲ +648.79 after sell → book $10,379.39; vs 09:30 mark -12.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.32 | $3.38 | $-50.57 | $3,910.94 | ▼ -50.57 after sell → book $10,376.01; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.03 | $2.54 | $+202.36 | $5,273.50 | ▲ +202.36 after sell → book $10,373.47; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.33 | $2.33 | $+224.17 | $6,657.49 | ▲ +224.17 after sell → book $10,371.14; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.08 | $2.40 | $-131.77 | $7,681.25 | ▼ -131.77 after sell → book $10,368.74; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 140 | $8.59 | $2.44 | $+38.55 | $8,881.40 | ▲ +38.55 after sell → book $10,366.29; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3877 | $0.38 | $27.14 | $+294.89 | $10,339.16 | ▲ +294.89 after sell → book $10,339.16; vs 09:30 mark -27.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,339.16 | ▲ close $10,339.16 vs 09:30 $10,394.40 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,339.16 | ▲ 09:30 equity $10,339.16 vs yday $10,339.16 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 53 | $24.11 | $2.15 | — | $9,059.18 | — | rank by hot_score; rank hot_score; list yday_mover; ret5=+891.7; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 828 | $1.56 | $10.68 | — | $7,756.82 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 317 | $4.07 | $4.09 | — | $6,462.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 67 | $19.04 | $2.19 | — | $5,184.67 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 246 | $5.24 | $3.17 | — | $3,892.45 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 226 | $5.71 | $2.92 | — | $2,599.08 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 147 | $8.79 | $2.43 | — | $1,304.52 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1292.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 364 | $3.55 | $4.70 | — | $7.62 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+27.9; leftover $1292.39 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.62 | ▲ close $10,969.59 vs 09:30 $10,339.16 (session +662.76) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.62 | ▼ 09:30 equity $10,665.67 vs yday $10,969.59 (-303.92) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 53 | $26.61 | $2.17 | $+128.18 | $1,415.78 | ▲ +128.18 after sell → book $10,663.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 828 | $1.60 | $10.83 | $+11.61 | $2,729.75 | ▲ +11.61 after sell → book $10,652.67; vs 09:30 mark -10.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 67 | $20.72 | $2.21 | $+108.16 | $4,115.78 | ▲ +108.16 after sell → book $10,650.46; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 246 | $4.98 | $3.22 | $-70.36 | $5,337.64 | ▼ -70.36 after sell → book $10,647.24; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 226 | $5.97 | $2.96 | $+52.88 | $6,683.89 | ▲ +52.88 after sell → book $10,644.27; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 364 | $3.77 | $4.77 | $+70.62 | $8,051.41 | ▲ +70.62 after sell → book $10,639.51; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 95 | $14.11 | $2.27 | — | $6,708.68 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $1341.90 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 230 | $5.81 | $2.97 | — | $5,369.41 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1341.90 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 115 | $11.59 | $2.33 | — | $4,034.80 | — | rank by hot_score; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $1341.90 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 161 | $8.29 | $2.47 | — | $2,697.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1341.90 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 33 | $40.50 | $2.09 | — | $1,359.05 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.8; leftover $1341.90 | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 98 | $13.63 | $2.28 | — | $21.03 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1341.90 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.03 | ▲ close $10,764.76 vs 09:30 $10,665.67 (session +139.67) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.03 | ▲ 09:30 equity $10,922.51 vs yday $10,764.76 (+157.75) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 147 | $9.41 | $2.47 | $+86.24 | $1,401.83 | ▲ +86.24 after sell → book $10,920.04; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 95 | $14.20 | $2.30 | $+3.97 | $2,748.53 | ▲ +3.97 after sell → book $10,917.74; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 230 | $6.50 | $3.02 | $+152.72 | $4,240.51 | ▲ +152.72 after sell → book $10,914.72; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 115 | $12.18 | $2.37 | $+63.72 | $5,638.85 | ▲ +63.72 after sell → book $10,912.36; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 33 | $37.42 | $2.11 | $-105.84 | $6,871.60 | ▼ -105.84 after sell → book $10,910.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 98 | $12.98 | $2.31 | $-68.29 | $8,141.33 | ▼ -68.29 after sell → book $10,907.94; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 9 | $144.18 | $2.02 | — | $6,841.69 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $1356.89 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 73 | $18.50 | $2.21 | — | $5,488.98 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $1356.89 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 282 | $4.81 | $3.64 | — | $4,128.92 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.8; leftover $1356.89 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 251 | $5.39 | $3.24 | — | $2,772.80 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+17.4; leftover $1356.89 | — |
| 2026-08-27 09:30 ET | **BUY** | `VERA` | 36 | $36.70 | $2.10 | — | $1,449.50 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.1; leftover $1356.89 | — |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 151 | $8.95 | $2.44 | — | $95.60 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.2; leftover $1356.89 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.60 | ▼ close $10,745.75 vs 09:30 $10,922.51 (session -146.54) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.60 | ▼ 09:30 equity $10,571.44 vs yday $10,745.75 (-174.31) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 317 | $3.69 | $4.15 | $-128.70 | $1,261.18 | ▼ -128.70 after sell → book $10,567.29; vs 09:30 mark -4.15 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 73 | $18.15 | $2.23 | $-29.99 | $2,583.90 | ▼ -29.99 after sell → book $10,565.06; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OABI` | 282 | $4.54 | $3.69 | $-83.47 | $3,860.49 | ▼ -83.47 after sell → book $10,561.37; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 251 | $5.11 | $3.29 | $-76.81 | $5,139.81 | ▼ -76.81 after sell → book $10,558.08; vs 09:30 mark -3.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `VERA` | 36 | $34.40 | $2.12 | $-87.02 | $6,376.09 | ▼ -87.02 after sell → book $10,555.96; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 91 | $14.00 | $2.26 | — | $5,099.83 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $1275.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $3,929.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1275.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $3,003.56 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.8; leftover $1275.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 59 | $21.49 | $2.17 | — | $1,733.48 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.3; leftover $1275.22 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 69 | $18.36 | $2.20 | — | $464.44 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.8; leftover $1275.22 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $464.44 | ▼ close $10,386.15 vs 09:30 $10,571.44 (session -159.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $464.44 | ▼ 09:30 equity $10,264.52 vs yday $10,386.15 (-121.63) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 161 | $9.50 | $2.51 | $+189.83 | $1,991.43 | ▲ +189.83 after sell → book $10,262.01; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 151 | $8.66 | $2.48 | $-48.71 | $3,296.61 | ▼ -48.71 after sell → book $10,259.53; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $4,478.82 | ▲ +11.63 after sell → book $10,257.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $5,352.70 | ▼ -51.81 after sell → book $10,255.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 59 | $20.56 | $2.19 | $-59.22 | $6,563.55 | ▼ -59.22 after sell → book $10,253.29; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 69 | $17.77 | $2.22 | $-45.13 | $7,787.47 | ▼ -45.13 after sell → book $10,251.08; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,787.47 | ▲ close $10,260.83 vs 09:30 $10,264.52 (session +9.75) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,787.47 | ▼ 09:30 equity $10,236.36 vs yday $10,260.83 (-24.47) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 91 | $13.04 | $2.29 | $-91.91 | $8,971.82 | ▼ -91.91 after sell → book $10,234.07; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.82 | ▲ close $10,360.25 vs 09:30 $10,236.36 (session +126.18) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,971.82 | ▼ 09:30 equity $10,334.42 vs yday $10,360.25 (-25.83) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.82 | ▼ close $10,329.11 vs 09:30 $10,334.42 (session -5.31) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,971.82 | ▼ 09:30 equity $10,285.32 vs yday $10,329.11 (-43.79) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 9 | $145.94 | $2.04 | $+11.83 | $10,283.28 | ▲ +11.83 after sell → book $10,283.28; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 722 | $1.78 | $9.31 | — | $8,988.81 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 69 | $18.40 | $2.20 | — | $7,717.01 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 93 | $13.71 | $2.27 | — | $6,439.72 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $23.88 | $2.15 | — | $5,171.93 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 945 | $1.36 | $12.19 | — | $3,874.54 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 666 | $1.93 | $8.59 | — | $2,580.56 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $1,299.44 | — | rank by hot_score; rank hot_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1285.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 50 | $25.62 | $2.14 | — | $16.05 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.1; leftover $1285.41 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▼ close $9,901.83 vs 09:30 $10,285.32 (session -340.58) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▼ 09:30 equity $9,831.20 vs yday $9,901.83 (-70.63) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 69 | $18.15 | $2.22 | $-21.67 | $1,266.19 | ▼ -21.67 after sell → book $9,828.99; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 93 | $13.89 | $2.29 | $+12.18 | $2,555.66 | ▲ +12.18 after sell → book $9,826.69; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.84 | $2.17 | $-6.44 | $3,817.01 | ▼ -6.44 after sell → book $9,824.52; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 945 | $1.23 | $12.36 | $-147.40 | $4,967.00 | ▼ -147.40 after sell → book $9,812.16; vs 09:30 mark -12.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 666 | $1.90 | $8.71 | $-37.28 | $6,223.69 | ▼ -37.28 after sell → book $9,803.45; vs 09:30 mark -8.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 10 | $125.22 | $2.04 | $-30.96 | $7,473.85 | ▼ -30.96 after sell → book $9,801.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 158 | $7.87 | $2.46 | — | $6,227.93 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $1245.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 215 | $5.79 | $2.77 | — | $4,980.30 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1245.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 496 | $2.51 | $6.40 | — | $3,728.95 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1245.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 10 | $120.47 | $2.02 | — | $2,522.18 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $1245.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 75 | $16.40 | $2.21 | — | $1,289.96 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1245.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 274 | $4.53 | $3.53 | — | $45.21 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1245.64 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.21 | ▲ close $10,187.89 vs 09:30 $9,831.20 (session +405.88) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.21 | ▼ 09:30 equity $10,039.15 vs yday $10,187.89 (-148.74) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 722 | $1.56 | $9.44 | $-173.99 | $1,165.69 | ▼ -173.99 after sell → book $10,029.70; vs 09:30 mark -9.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 50 | $26.44 | $2.16 | $+36.45 | $2,485.53 | ▲ +36.45 after sell → book $10,027.54; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 158 | $7.76 | $2.50 | $-22.34 | $3,709.11 | ▼ -22.34 after sell → book $10,025.04; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 215 | $5.81 | $2.82 | $-1.29 | $4,955.44 | ▼ -1.29 after sell → book $10,022.22; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 496 | $2.66 | $6.49 | $+61.51 | $6,268.31 | ▲ +61.51 after sell → book $10,015.73; vs 09:30 mark -6.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HOOD` | 10 | $125.07 | $2.04 | $+41.89 | $7,516.97 | ▲ +41.89 after sell → book $10,013.69; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 75 | $16.74 | $2.24 | $+21.05 | $8,770.23 | ▲ +21.05 after sell → book $10,011.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 274 | $4.53 | $3.59 | $-7.12 | $10,007.86 | ▼ -7.12 after sell → book $10,007.86; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.86 | ▲ close $10,007.86 vs 09:30 $10,039.15 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.86 | ▲ 09:30 equity $10,007.86 vs yday $10,007.86 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.86 | ▲ close $10,007.86 vs 09:30 $10,007.86 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.86 | ▲ 09:30 equity $10,007.86 vs yday $10,007.86 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.86 | ▲ close $10,007.86 vs 09:30 $10,007.86 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.86 | ▲ 09:30 equity $10,007.86 vs yday $10,007.86 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 463 | $2.70 | $5.97 | — | $8,751.79 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 254 | $4.91 | $3.28 | — | $7,501.37 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 203 | $6.16 | $2.62 | — | $6,248.28 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 399 | $3.13 | $5.15 | — | $4,994.26 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 893 | $1.40 | $11.52 | — | $3,732.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=-17.2; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 114 | $10.95 | $2.33 | — | $2,481.91 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $1,271.83 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+24.3; leftover $1250.98 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $90.02 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1250.98 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.02 | ▲ close $10,086.82 vs 09:30 $10,007.86 (session +113.91) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.02 | ▲ 09:30 equity $10,092.40 vs yday $10,086.82 (+5.58) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 203 | $6.02 | $2.66 | $-33.70 | $1,309.42 | ▼ -33.70 after sell → book $10,089.74; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 114 | $10.29 | $2.36 | $-79.93 | $2,480.12 | ▼ -79.93 after sell → book $10,087.38; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 22 | $54.75 | $2.08 | $-7.65 | $3,682.54 | ▼ -7.65 after sell → book $10,085.30; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 14 | $86.06 | $2.05 | $+20.98 | $4,885.33 | ▲ +20.98 after sell → book $10,083.25; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,885.33 | ▲ close $10,335.64 vs 09:30 $10,092.40 (session +252.39) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,885.33 | ▲ 09:30 equity $10,410.91 vs yday $10,335.64 (+75.27) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 254 | $5.11 | $3.33 | $+44.19 | $6,179.94 | ▲ +44.19 after sell → book $10,407.58; vs 09:30 mark -3.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 399 | $3.64 | $5.22 | $+193.12 | $7,627.07 | ▲ +193.12 after sell → book $10,402.36; vs 09:30 mark -5.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,627.07 | ▲ close $10,491.15 vs 09:30 $10,410.91 (session +88.80) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,627.07 | ▲ 09:30 equity $10,491.48 vs yday $10,491.15 (+0.33) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `GPRO` | 893 | $1.31 | $11.68 | $-103.57 | $8,785.23 | ▼ -103.57 after sell → book $10,479.81; vs 09:30 mark -11.67 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 697 | $1.80 | $8.99 | — | $7,521.63 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1255.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 53 | $23.29 | $2.15 | — | $6,285.12 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $1255.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 85 | $14.62 | $2.25 | — | $5,040.17 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $1255.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 79 | $15.75 | $2.23 | — | $3,793.69 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1255.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $2,540.34 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1255.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 23 | $52.52 | $2.06 | — | $1,330.32 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+10.7; leftover $1255.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 44 | $28.16 | $2.12 | — | $89.16 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1255.03 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.16 | ▼ close $10,417.70 vs 09:30 $10,491.48 (session -40.28) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.16 | ▲ 09:30 equity $10,502.08 vs yday $10,417.70 (+84.38) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 53 | $24.09 | $2.17 | $+38.08 | $1,363.76 | ▲ +38.08 after sell → book $10,499.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 85 | $13.77 | $2.27 | $-76.76 | $2,531.94 | ▼ -76.76 after sell → book $10,497.64; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `REF` | 79 | $15.85 | $2.25 | $+3.42 | $3,781.84 | ▲ +3.42 after sell → book $10,495.39; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $-40.76 | $4,994.43 | ▼ -40.76 after sell → book $10,493.34; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FRO` | 23 | $54.31 | $2.08 | $+37.03 | $6,241.48 | ▲ +37.03 after sell → book $10,491.26; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 44 | $28.59 | $2.14 | $+14.88 | $7,497.52 | ▲ +14.88 after sell → book $10,489.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 55 | $22.46 | $2.15 | — | $6,260.06 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1249.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 33 | $36.76 | $2.09 | — | $5,044.90 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $1249.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 1167 | $1.07 | $15.05 | — | $3,781.15 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $1249.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $2,598.26 | — | rank by hot_score; rank hot_score; list flatten,ohlc_hot; ret5=+17.7; leftover $1249.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 121 | $10.25 | $2.35 | — | $1,355.65 | — | rank by hot_score; rank hot_score; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1249.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 44 | $28.23 | $2.12 | — | $111.41 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.3; leftover $1249.59 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.41 | ▲ close $10,699.73 vs 09:30 $10,502.08 (session +236.40) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.41 | ▼ 09:30 equity $10,694.84 vs yday $10,699.73 (-4.89) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 697 | $1.96 | $9.12 | $+93.41 | $1,468.41 | ▲ +93.41 after sell → book $10,685.72; vs 09:30 mark -9.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 55 | $21.30 | $2.17 | $-68.13 | $2,637.74 | ▼ -68.13 after sell → book $10,683.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 33 | $39.50 | $2.11 | $+86.22 | $3,939.13 | ▲ +86.22 after sell → book $10,681.44; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IQ` | 1167 | $1.12 | $15.26 | $+28.04 | $5,230.91 | ▲ +28.04 after sell → book $10,666.18; vs 09:30 mark -15.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $6,400.88 | ▼ -12.93 after sell → book $10,664.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 121 | $10.12 | $2.38 | $-20.47 | $7,623.01 | ▼ -20.47 after sell → book $10,661.76; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 44 | $28.55 | $2.14 | $+9.82 | $8,877.07 | ▲ +9.82 after sell → book $10,659.62; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 43 | $29.32 | $2.12 | — | $7,614.19 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1268.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 417 | $3.04 | $5.38 | — | $6,343.22 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1268.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 15 | $81.40 | $2.04 | — | $5,120.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1268.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 851 | $1.49 | $10.98 | — | $3,841.22 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1268.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 321 | $3.94 | $4.14 | — | $2,572.33 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1268.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 136 | $9.32 | $2.40 | — | $1,302.42 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $1268.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `CHPT` | 126 | $10.00 | $2.37 | — | $40.05 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $1268.15 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.05 | ▲ close $10,984.57 vs 09:30 $10,694.84 (session +354.36) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.05 | ▲ 09:30 equity $11,346.88 vs yday $10,984.57 (+362.31) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 463 | $3.55 | $6.06 | $+381.51 | $1,677.64 | ▲ +381.51 after sell → book $11,340.82; vs 09:30 mark -6.06 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 43 | $29.43 | $2.14 | $+0.47 | $2,940.99 | ▲ +0.47 after sell → book $11,338.68; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 417 | $4.00 | $5.46 | $+391.56 | $4,603.53 | ▲ +391.56 after sell → book $11,333.22; vs 09:30 mark -5.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 15 | $79.08 | $2.06 | $-38.89 | $5,787.67 | ▼ -38.89 after sell → book $11,331.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 321 | $3.90 | $4.20 | $-21.19 | $7,035.37 | ▼ -21.19 after sell → book $11,326.96; vs 09:30 mark -4.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CHPT` | 126 | $10.32 | $2.40 | $+35.55 | $8,333.29 | ▲ +35.55 after sell → book $11,324.56; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 562 | $2.47 | $7.25 | — | $6,937.90 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $1388.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 82 | $16.91 | $2.24 | — | $5,549.04 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $1388.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 241 | $5.75 | $3.11 | — | $4,158.98 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1388.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 106 | $13.05 | $2.31 | — | $2,773.37 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1388.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 168 | $8.22 | $2.49 | — | $1,389.92 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1388.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 129 | $10.71 | $2.38 | — | $5.95 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1388.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.95 | ▲ close $11,459.75 vs 09:30 $11,346.88 (session +154.97) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.95 | ▼ 09:30 equity $11,443.31 vs yday $11,459.75 (-16.44) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 241 | $6.05 | $3.16 | $+66.03 | $1,462.04 | ▲ +66.03 after sell → book $11,440.15; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 106 | $12.99 | $2.34 | $-11.00 | $2,836.65 | ▼ -11.00 after sell → book $11,437.82; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 44 | $9.11 | $2.12 | — | $2,433.68 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; leftover $405.24 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 56 | $7.23 | $2.16 | — | $2,026.65 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $405.24 | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $1,705.24 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+35.1; leftover $405.24 | — |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 130 | $3.10 | $2.38 | — | $1,299.86 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $405.24 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 401 | $1.01 | $5.17 | — | $889.68 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+14.3; leftover $405.24 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $889.68 | ▲ close $11,501.11 vs 09:30 $11,443.31 (session +77.12) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $889.68 | ▲ 09:30 equity $11,515.93 vs yday $11,501.11 (+14.82) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 851 | $1.41 | $11.13 | $-90.19 | $2,078.46 | ▼ -90.19 after sell → book $11,504.80; vs 09:30 mark -11.13 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 136 | $12.80 | $2.43 | $+468.45 | $3,816.83 | ▲ +468.45 after sell → book $11,502.37; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 82 | $16.92 | $2.26 | $-3.68 | $5,202.01 | ▼ -3.68 after sell → book $11,500.11; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FWDI` | 168 | $8.20 | $2.53 | $-8.39 | $6,577.07 | ▼ -8.39 after sell → book $11,497.57; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 129 | $10.11 | $2.41 | $-82.19 | $7,878.85 | ▼ -82.19 after sell → book $11,495.16; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 44 | $8.39 | $2.14 | $-35.94 | $8,245.87 | ▼ -35.94 after sell → book $11,493.02; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 56 | $6.83 | $2.18 | $-26.74 | $8,626.17 | ▼ -26.74 after sell → book $11,490.84; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $8,955.94 | ▲ +8.36 after sell → book $11,488.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 401 | $0.95 | $5.09 | $-34.32 | $9,331.80 | ▼ -34.32 after sell → book $11,483.74; vs 09:30 mark -5.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 576 | $2.70 | $7.43 | — | $7,769.17 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $1555.30 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 37 | $41.76 | $2.10 | — | $6,221.95 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1555.30 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 346 | $4.49 | $4.46 | — | $4,663.95 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $1555.30 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 157 | $9.90 | $2.46 | — | $3,107.19 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1555.30 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 21 | $70.84 | $2.05 | — | $1,617.49 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1555.30 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 2118 | $0.73 | $21.90 | — | $40.98 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $1555.30 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.98 | ▼ close $11,029.13 vs 09:30 $11,515.93 (session -414.20) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.98 | ▼ 09:30 equity $10,892.85 vs yday $11,029.13 (-136.28) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 562 | $2.68 | $7.36 | $+103.42 | $1,539.79 | ▲ +103.42 after sell → book $10,885.50; vs 09:30 mark -7.35 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 37 | $36.02 | $2.12 | $-216.42 | $2,870.59 | ▼ -216.42 after sell → book $10,883.37; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 346 | $3.92 | $4.53 | $-204.49 | $4,224.11 | ▼ -204.49 after sell → book $10,878.84; vs 09:30 mark -4.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 157 | $9.12 | $2.50 | $-127.42 | $5,653.45 | ▼ -127.42 after sell → book $10,876.34; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 21 | $70.50 | $2.07 | $-11.27 | $7,131.87 | ▼ -11.27 after sell → book $10,874.27; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVTL` | 2118 | $0.66 | $20.70 | $-198.49 | $8,509.90 | ▼ -198.49 after sell → book $10,853.57; vs 09:30 mark -20.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,509.90 | ▲ close $12,111.50 vs 09:30 $10,892.85 (session +1,257.93) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,533.37 | ▲ 09:30 equity $11,516.11 vs yday $11,178.86 (+337.25) | 09:30 open · cash $7,533.37 (unchanged overnight, no fees) · equity $11,516.11 vs prior close $11,178.86 (+337.25) · 2 name(s) re-marked at the open (per-name table). GLND×475 yday $5.35 → 09:30 $6.06 +337.25; VICR×4 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 36 | $29.76 | $2.10 | — | $6,459.91 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1076.20 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 66 | $16.21 | $2.19 | — | $5,387.86 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1076.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 69 | $15.58 | $2.20 | — | $4,310.57 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $1076.20 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $3,320.56 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1076.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 268 | $4.00 | $3.46 | — | $2,243.76 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1076.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 105 | $10.20 | $2.31 | — | $1,170.45 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1076.20 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 278 | $3.86 | $3.59 | — | $93.79 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1076.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.79 | ▼ close $11,307.20 vs 09:30 $11,516.11 (session -191.07) | 16:00 close · cash $93.79 · equity $11,307.20 vs 09:30 $11,516.11 (-208.91; session marks -191.07) · 9 name(s) marked open→close (per-name table). GLND×475 09:30 $6.06 → close $5.54 -247.00; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×36 09:30 $29.76 → close $26.24 -126.72; SECZ×66 09:30 $16.21 → close $15.96 -16.50; USDE×69 09:30 $15.58 → close $17.25 +115.15; GRAL×8 09:30 $123.50 → close $126.89 +27.12; CYPH×268 09:30 $4.00 → close $4.12 +30.82; DNA×105 09:30 $10.20 → close $10.66 +48.30; ZSQR×278 09:30 $3.86 → close $3.78 -22.24 | — |

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
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SEDG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INDP` | 130 | 2026-09-22 @ $3.10 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $405.24 |
| `GLND` | 576 | 2026-09-23 @ $2.70 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $1555.30 |
