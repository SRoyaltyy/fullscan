# Factor mine action — `union_candle_score_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `candle_score` · size `leftover` · sell `list` · S-boost `none` · rank by candle_score

Cash book **-9.28%** ($9,072) · signal-only (no cash/fees) was +43.18%. Starts YES **21/30**. Fills 190 · skips 288 · realized $+91.50.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `candle_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,731.29.

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
| 2026-08-14 09:30 ET | **BUY** | `SATL` | 2 | $5.98 | $0.13 | — | $95.30 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.9; leftover $13.42 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 1 | $9.89 | $0.10 | — | $85.30 | — | rank by candle_score; rank candle_score; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $13.42 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.30 | ▲ close $10,517.00 vs 09:30 $10,312.70 (session +204.52) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.30 | ▼ 09:30 equity $10,490.01 vs yday $10,517.00 (-26.99) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 6 | $1.92 | $0.13 | — | $73.65 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $12.19 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 1 | $10.10 | $0.10 | — | $63.44 | — | rank by candle_score; rank candle_score; list mover_buy; ret5=+22.8; leftover $12.19 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 2 | $4.59 | $0.10 | — | $54.16 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $12.19 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.16 | ▲ close $10,590.50 vs 09:30 $10,490.01 (session +100.82) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.16 | ▼ 09:30 equity $10,446.92 vs yday $10,590.50 (-143.58) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,226.48 | ▼ -66.33 after sell → book $10,444.76; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $2,466.87 | ▲ +23.38 after sell → book $10,442.67; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $3,634.44 | ▼ -83.63 after sell → book $10,440.54; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $4,808.47 | ▼ -69.50 after sell → book $10,438.45; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $6,547.31 | ▲ +471.89 after sell → book $10,418.27; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $7,823.05 | ▲ +41.02 after sell → book $10,416.09; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $9,020.98 | ▼ -0.12 after sell → book $10,414.02; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $10,360.61 | ▲ +97.12 after sell → book $10,411.69; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,360.61 | ▲ close $10,413.16 vs 09:30 $10,446.92 (session +1.47) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,360.61 | ▲ 09:30 equity $10,413.68 vs yday $10,413.16 (+0.52) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `SATL` | 2 | $5.82 | $0.14 | $-0.59 | $10,372.10 | ▼ -0.59 after sell → book $10,413.53; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,372.10 | ▼ close $10,412.55 vs 09:30 $10,413.68 (session -0.98) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.10 | ▼ 09:30 equity $10,412.47 vs yday $10,412.55 (-0.08) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `NMAX` | 1 | $10.89 | $0.13 | $+0.76 | $10,382.86 | ▲ +0.76 after sell → book $10,412.34; vs 09:30 mark -0.13 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 6 | $1.64 | $0.14 | $-1.95 | $10,392.57 | ▼ -1.95 after sell → book $10,412.21; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `SMJF` | 1 | $10.72 | $0.13 | $+0.39 | $10,403.16 | ▲ +0.39 after sell → book $10,412.08; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 2 | $4.46 | $0.12 | $-0.47 | $10,411.96 | ▼ -0.47 after sell → book $10,411.96; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 19 | $65.60 | $2.05 | — | $9,163.51 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `NBP` | 660 | $1.97 | $8.51 | — | $7,854.80 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ret5=+5.9; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 100 | $12.98 | $2.29 | — | $6,554.51 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $5,252.54 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $4,049.41 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 264 | $4.92 | $3.41 | — | $2,747.12 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $1,431.88 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1301.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `GENB` | 77 | $16.76 | $2.22 | — | $139.14 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.5; leftover $1301.50 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.14 | ▼ close $10,198.03 vs 09:30 $10,412.47 (session -176.53) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.14 | ▲ 09:30 equity $10,478.53 vs yday $10,198.03 (+280.50) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 2 | $9.08 | $0.19 | — | $120.79 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $19.88 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $109.55 | — | rank by candle_score; rank candle_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.88 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.55 | ▲ close $10,760.39 vs 09:30 $10,478.53 (session +282.16) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.55 | ▲ 09:30 equity $11,206.65 vs yday $10,760.39 (+446.26) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.55 | ▼ close $10,985.40 vs 09:30 $11,206.65 (session -221.25) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.55 | ▼ 09:30 equity $10,927.34 vs yday $10,985.40 (-58.06) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 19 | $69.00 | $2.07 | $+60.49 | $1,418.48 | ▲ +60.49 after sell → book $10,925.27; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NBP` | 660 | $1.89 | $8.63 | $-69.95 | $2,657.25 | ▼ -69.95 after sell → book $10,916.64; vs 09:30 mark -8.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IMMX` | 100 | $13.60 | $2.32 | $+57.39 | $4,014.93 | ▲ +57.39 after sell → book $10,914.32; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $11.00 | $2.35 | $-94.32 | $5,222.58 | ▼ -94.32 after sell → book $10,911.97; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $6,368.55 | ▼ -57.17 after sell → book $10,909.94; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 264 | $5.25 | $3.46 | $+80.25 | $7,751.09 | ▲ +80.25 after sell → book $10,906.48; vs 09:30 mark -3.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CYPH` | 1131 | $1.56 | $14.79 | $+434.33 | $9,500.65 | ▲ +434.33 after sell → book $10,891.68; vs 09:30 mark -14.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `GENB` | 77 | $17.67 | $2.24 | $+65.60 | $10,859.00 | ▲ +65.60 after sell → book $10,889.44; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 37 | $36.52 | $2.10 | — | $9,505.66 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+7.9; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 154 | $8.79 | $2.45 | — | $8,149.55 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 16 | $83.15 | $2.04 | — | $6,817.11 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+14.5; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 259 | $5.24 | $3.34 | — | $5,456.61 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `B` | 28 | $47.52 | $2.07 | — | $4,123.97 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.3; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 57 | $23.80 | $2.16 | — | $2,765.21 | — | rank by candle_score; rank candle_score; list yday_gainer; ret5=+28.9; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `BRZE` | 44 | $30.69 | $2.12 | — | $1,412.73 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.5; leftover $1357.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `CELH` | 38 | $35.23 | $2.10 | — | $71.89 | — | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+17.0; leftover $1357.37 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.89 | ▲ close $11,050.55 vs 09:30 $10,927.34 (session +179.50) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.89 | ▼ 09:30 equity $10,896.74 vs yday $11,050.55 (-153.81) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `IOVA` | 2 | $8.34 | $0.19 | $-1.86 | $88.37 | ▼ -1.86 after sell → book $10,896.54; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $103.55 | ▲ +3.93 after sell → book $10,896.37; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 1 | $14.00 | $0.14 | — | $89.40 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+17.8; leftover $14.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 1 | $11.59 | $0.12 | — | $77.70 | — | rank by candle_score; rank candle_score; list overnight; 🔵; ret5=+64.9; leftover $14.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `VIR` | 1 | $10.60 | $0.11 | — | $66.99 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+12.9; leftover $14.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `AQST` | 2 | $5.08 | $0.11 | — | $56.72 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+17.6; leftover $14.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 1 | $11.22 | $0.12 | — | $45.39 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.8; leftover $14.79 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 6 | $2.20 | $0.15 | — | $32.04 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+17.8; leftover $14.79 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.04 | ▲ close $10,926.47 vs 09:30 $10,896.74 (session +30.84) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.04 | ▲ 09:30 equity $10,975.37 vs yday $10,926.47 (+48.90) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `GALT` | 1 | $4.15 | $0.04 | — | $27.84 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.1; leftover $4.58 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.84 | ▲ close $11,007.31 vs 09:30 $10,975.37 (session +31.99) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.84 | ▼ 09:30 equity $10,918.98 vs yday $11,007.31 (-88.33) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ANRO` | 37 | $35.00 | $2.12 | $-60.46 | $1,320.72 | ▼ -60.46 after sell → book $10,916.86; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 154 | $9.08 | $2.49 | $+39.72 | $2,716.55 | ▲ +39.72 after sell → book $10,914.37; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `WIX` | 16 | $85.32 | $2.06 | $+30.62 | $4,079.61 | ▲ +30.62 after sell → book $10,912.31; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 259 | $4.84 | $3.39 | $-110.34 | $5,329.78 | ▼ -110.34 after sell → book $10,908.92; vs 09:30 mark -3.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `B` | 28 | $47.50 | $2.09 | $-4.73 | $6,657.69 | ▼ -4.73 after sell → book $10,906.82; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 57 | $25.10 | $2.18 | $+69.76 | $8,086.20 | ▲ +69.76 after sell → book $10,904.64; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CELH` | 38 | $32.77 | $2.12 | $-97.71 | $9,329.34 | ▼ -97.71 after sell → book $10,902.51; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 12 | $106.99 | $2.03 | — | $8,043.43 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+10.5; leftover $1332.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `FIG` | 44 | $30.18 | $2.12 | — | $6,713.39 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1332.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 10 | $129.46 | $2.02 | — | $5,416.77 | — | rank by candle_score; rank candle_score; list overnight; ret5=+2.1; leftover $1332.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $4,330.78 | — | rank by candle_score; rank candle_score; list earn_react; ret5=+4.8; leftover $1332.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 31 | $41.74 | $2.08 | — | $3,034.75 | — | rank by candle_score; rank candle_score; list flatten; ret5=+2.4; leftover $1332.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `CXM` | 169 | $7.88 | $2.50 | — | $1,700.54 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+10.3; leftover $1332.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 72 | $18.36 | $2.21 | — | $376.41 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.8; leftover $1332.76 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $376.41 | ▼ close $10,737.59 vs 09:30 $10,918.98 (session -149.98) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $376.41 | ▲ 09:30 equity $10,803.67 vs yday $10,737.59 (+66.08) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `BRZE` | 44 | $34.03 | $2.14 | $+142.69 | $1,871.59 | ▲ +142.69 after sell → book $10,801.53; vs 09:30 mark -2.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 1 | $12.77 | $0.15 | $-1.52 | $1,884.21 | ▼ -1.52 after sell → book $10,801.38; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `PURR` | 1 | $12.19 | $0.14 | $+0.34 | $1,896.25 | ▲ +0.34 after sell → book $10,801.24; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `VIR` | 1 | $10.77 | $0.13 | $-0.07 | $1,906.88 | ▼ -0.07 after sell → book $10,801.10; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AQST` | 2 | $4.97 | $0.13 | $-0.44 | $1,916.71 | ▼ -0.44 after sell → book $10,800.98; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 1 | $11.80 | $0.14 | $+0.32 | $1,928.37 | ▲ +0.32 after sell → book $10,800.84; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BRR` | 6 | $2.23 | $0.17 | $-0.14 | $1,941.58 | ▼ -0.14 after sell → book $10,800.67; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,941.58 | ▼ close $10,697.02 vs 09:30 $10,803.67 (session -103.65) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,941.58 | ▼ 09:30 equity $10,615.65 vs yday $10,697.02 (-81.37) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `GALT` | 1 | $3.91 | $0.06 | $-0.35 | $1,945.42 | ▼ -0.35 after sell → book $10,615.58; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,945.42 | ▼ close $10,556.54 vs 09:30 $10,615.65 (session -59.04) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,945.42 | ▼ 09:30 equity $10,484.60 vs yday $10,556.54 (-71.94) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `EL` | 12 | $100.00 | $2.05 | $-87.95 | $3,143.38 | ▼ -87.95 after sell → book $10,482.56; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIG` | 44 | $26.78 | $2.14 | $-153.86 | $4,319.56 | ▼ -153.86 after sell → book $10,480.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SAIC` | 10 | $126.10 | $2.04 | $-37.66 | $5,578.52 | ▼ -37.66 after sell → book $10,478.38; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 2 | $545.68 | $2.02 | $+3.35 | $6,667.86 | ▲ +3.35 after sell → book $10,476.36; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 31 | $42.10 | $2.10 | $+6.97 | $7,970.86 | ▲ +6.97 after sell → book $10,474.26; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CXM` | 169 | $7.40 | $2.54 | $-86.15 | $9,218.92 | ▼ -86.15 after sell → book $10,471.72; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 72 | $17.40 | $2.23 | $-73.55 | $10,469.49 | ▼ -73.55 after sell → book $10,469.49; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,469.49 | ▲ close $10,469.49 vs 09:30 $10,484.60 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,469.49 | ▲ 09:30 equity $10,469.49 vs yday $10,469.49 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 14 | $90.24 | $2.03 | — | $9,204.10 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 195 | $6.68 | $2.58 | — | $7,898.93 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.4; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $6,617.81 | — | rank by candle_score; rank candle_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 51 | $25.62 | $2.14 | — | $5,308.79 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.1; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 82 | $15.87 | $2.24 | — | $4,005.21 | — | rank by candle_score; rank candle_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 179 | $7.31 | $2.53 | — | $2,694.20 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+18.5; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $1,383.91 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1308.69 | — |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 352 | $3.71 | $4.54 | — | $73.45 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+12.3; leftover $1308.69 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.45 | ▼ close $10,354.11 vs 09:30 $10,469.49 (session -95.08) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.45 | ▼ 09:30 equity $10,171.25 vs yday $10,354.11 (-182.86) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 2 | $5.79 | $0.12 | — | $61.75 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $12.24 | — |
| 2026-09-04 09:30 ET | **BUY** | `PAGS` | 1 | $9.96 | $0.10 | — | $51.69 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.5; leftover $12.24 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.69 | ▲ close $10,516.67 vs 09:30 $10,171.25 (session +345.64) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.69 | ▼ 09:30 equity $10,511.87 vs yday $10,516.67 (-4.80) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.69 | ▼ close $10,341.78 vs 09:30 $10,511.87 (session -170.09) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.69 | ▼ 09:30 equity $10,224.18 vs yday $10,341.78 (-117.60) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CTVA` | 14 | $86.40 | $2.05 | $-57.84 | $1,259.23 | ▼ -57.84 after sell → book $10,222.12; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RSKD` | 195 | $6.13 | $2.62 | $-112.44 | $2,451.97 | ▼ -112.44 after sell → book $10,219.51; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AGCO` | 10 | $127.69 | $2.04 | $-6.26 | $3,726.83 | ▼ -6.26 after sell → book $10,217.47; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ASST` | 51 | $28.00 | $2.16 | $+116.82 | $5,152.66 | ▲ +116.82 after sell → book $10,215.30; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 82 | $15.96 | $2.26 | $+2.88 | $6,459.12 | ▲ +2.88 after sell → book $10,213.04; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 179 | $7.27 | $2.57 | $-12.25 | $7,757.89 | ▼ -12.25 after sell → book $10,210.48; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 78 | $15.46 | $2.25 | $-106.65 | $8,961.52 | ▼ -106.65 after sell → book $10,208.23; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PYXS` | 352 | $3.48 | $4.61 | $-90.11 | $10,181.87 | ▼ -90.11 after sell → book $10,203.62; vs 09:30 mark -4.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,181.87 | ▼ close $10,202.36 vs 09:30 $10,224.18 (session -1.26) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,181.87 | ▼ 09:30 equity $10,201.86 vs yday $10,202.36 (-0.50) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 2 | $5.22 | $0.13 | $-1.39 | $10,192.18 | ▼ -1.39 after sell → book $10,201.73; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `PAGS` | 1 | $9.55 | $0.12 | $-0.63 | $10,201.61 | ▼ -0.63 after sell → book $10,201.61; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,201.61 | ▲ close $10,201.61 vs 09:30 $10,201.86 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,201.61 | ▲ 09:30 equity $10,201.61 vs yday $10,201.61 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 237 | $5.38 | $3.06 | — | $8,923.49 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+19.8; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `SION` | 163 | $7.79 | $2.48 | — | $7,651.24 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+4.2; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `MYGN` | 378 | $3.37 | $4.88 | — | $6,372.51 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+4.0; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `PUMP` | 110 | $11.57 | $2.32 | — | $5,097.49 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+5.9; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 407 | $3.13 | $5.25 | — | $3,818.33 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `HAFN` | 136 | $9.32 | $2.40 | — | $2,548.41 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+5.4; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 69 | $18.30 | $2.20 | — | $1,283.51 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1275.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 11 | $112.83 | $2.02 | — | $40.30 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1275.20 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.30 | ▲ close $10,422.18 vs 09:30 $10,201.61 (session +245.17) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.30 | ▼ 09:30 equity $10,380.25 vs yday $10,422.18 (-41.93) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.30 | ▲ close $10,500.12 vs 09:30 $10,380.25 (session +119.87) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.30 | ▼ 09:30 equity $10,422.96 vs yday $10,500.12 (-77.16) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.30 | ▼ close $10,297.05 vs 09:30 $10,422.96 (session -125.91) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.30 | ▼ 09:30 equity $10,234.70 vs yday $10,297.05 (-62.35) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ANGX` | 237 | $5.30 | $3.11 | $-25.12 | $1,293.30 | ▼ -25.12 after sell → book $10,231.60; vs 09:30 mark -3.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SION` | 163 | $6.95 | $2.52 | $-141.92 | $2,423.63 | ▼ -141.92 after sell → book $10,229.08; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `MYGN` | 378 | $3.75 | $4.95 | $+133.81 | $3,836.18 | ▲ +133.81 after sell → book $10,224.13; vs 09:30 mark -4.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 407 | $3.48 | $5.33 | $+131.87 | $5,247.21 | ▲ +131.87 after sell → book $10,218.80; vs 09:30 mark -5.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `HAFN` | 136 | $9.59 | $2.43 | $+31.89 | $6,549.02 | ▲ +31.89 after sell → book $10,216.37; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 69 | $17.73 | $2.22 | $-43.75 | $7,770.17 | ▼ -43.75 after sell → book $10,214.15; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 45 | $28.16 | $2.12 | — | $6,500.85 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1295.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `INDP` | 353 | $3.66 | $4.55 | — | $5,204.31 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+96.8; leftover $1295.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `TXG` | 17 | $74.50 | $2.04 | — | $3,935.77 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.4; leftover $1295.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 47 | $27.09 | $2.13 | — | $2,660.41 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1295.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $1,407.06 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1295.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 24 | $52.52 | $2.06 | — | $144.52 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+10.7; leftover $1295.03 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.52 | ▼ close $9,946.26 vs 09:30 $10,234.70 (session -252.95) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.52 | ▲ 09:30 equity $10,120.54 vs yday $9,946.26 (+174.28) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 11 | $114.90 | $2.04 | $+18.65 | $1,406.37 | ▲ +18.65 after sell → book $10,118.50; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 22 | $10.25 | $2.06 | — | $1,178.82 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $234.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 219 | $1.07 | $2.83 | — | $941.66 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $234.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $769.10 | — | rank by candle_score; rank candle_score; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $234.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 160 | $1.46 | $2.47 | — | $533.03 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+4.4; leftover $234.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `HLP` | 111 | $2.10 | $2.32 | — | $297.61 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+60.5; leftover $234.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $148.52 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ret5=+17.7; leftover $234.40 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.52 | ▲ close $10,537.83 vs 09:30 $10,120.54 (session +432.20) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.52 | ▼ 09:30 equity $10,442.32 vs yday $10,537.83 (-95.51) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 110 | $10.48 | $2.35 | $-124.57 | $1,298.97 | ▼ -124.57 after sell → book $10,439.97; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `NEO` | 9 | $19.91 | $1.82 | — | $1,117.96 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.9; leftover $185.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 6 | $29.32 | $1.78 | — | $940.27 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $185.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 2 | $81.40 | $1.63 | — | $775.83 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $185.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 2 | $85.00 | $1.71 | — | $604.13 | — | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+18.3; leftover $185.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 8 | $22.90 | $1.86 | — | $419.07 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $185.57 | — |
| 2026-09-18 09:30 ET | **BUY** | `ATRC` | 3 | $58.51 | $1.76 | — | $241.78 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $185.57 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.78 | ▼ close $10,234.89 vs 09:30 $10,442.32 (session -194.53) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $241.78 | ▲ 09:30 equity $10,285.39 vs yday $10,234.89 (+50.50) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 45 | $30.23 | $2.15 | $+88.88 | $1,599.98 | ▲ +88.88 after sell → book $10,283.24; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 353 | $3.55 | $4.62 | $-48.01 | $2,848.51 | ▼ -48.01 after sell → book $10,278.62; vs 09:30 mark -4.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TXG` | 17 | $79.15 | $2.06 | $+74.95 | $4,192.00 | ▲ +74.95 after sell → book $10,276.56; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 47 | $28.69 | $2.15 | $+70.92 | $5,538.27 | ▲ +70.92 after sell → book $10,274.40; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 14 | $89.66 | $2.05 | $-0.16 | $6,791.46 | ▼ -0.16 after sell → book $10,272.35; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FRO` | 24 | $49.83 | $2.08 | $-68.70 | $7,985.30 | ▼ -68.70 after sell → book $10,270.27; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,036.07 | — | rank by candle_score; rank candle_score; list flatten; ret5=+6.5; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 11 | $88.83 | $2.02 | — | $6,056.92 | — | rank by candle_score; rank candle_score; list flatten; ret5=+7.6; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 11 | $83.53 | $2.02 | — | $5,136.07 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+8.8; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `TRMD` | 26 | $37.47 | $2.07 | — | $4,159.78 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+9.0; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 40 | $24.93 | $2.11 | — | $3,160.47 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+8.7; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `PUMP` | 95 | $10.40 | $2.27 | — | $2,170.19 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+5.9; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 404 | $2.47 | $5.21 | — | $1,167.10 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $998.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 59 | $16.91 | $2.17 | — | $167.24 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $998.16 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.24 | ▲ close $10,307.14 vs 09:30 $10,285.39 (session +56.76) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.24 | ▼ 09:30 equity $10,305.96 vs yday $10,307.14 (-1.18) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 22 | $10.18 | $2.08 | $-5.67 | $389.13 | ▼ -5.67 after sell → book $10,303.89; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 11 | $4.30 | $0.51 | — | $341.32 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+16.9; leftover $48.64 | — |
| 2026-09-22 09:30 ET | **BUY** | `SECZ` | 3 | $12.96 | $0.40 | — | $302.04 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+64.4; leftover $48.64 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $302.04 | ▲ close $10,325.26 vs 09:30 $10,305.96 (session +22.28) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $302.04 | ▲ 09:30 equity $10,387.60 vs yday $10,325.26 (+62.34) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQ` | 219 | $1.03 | $2.87 | $-14.46 | $524.74 | ▼ -14.46 after sell → book $10,384.73; vs 09:30 mark -2.87 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 1 | $174.50 | $1.77 | $+0.17 | $697.47 | ▲ +0.17 after sell → book $10,382.96; vs 09:30 mark -1.77 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AIB` | 160 | $1.37 | $2.51 | $-19.38 | $914.17 | ▼ -19.38 after sell → book $10,380.46; vs 09:30 mark -2.50 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `HLP` | 111 | $2.01 | $2.35 | $-14.66 | $1,134.93 | ▼ -14.66 after sell → book $10,378.11; vs 09:30 mark -2.35 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $1,275.88 | ▼ -8.14 after sell → book $10,376.66; vs 09:30 mark -1.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `NEO` | 9 | $18.69 | $1.73 | $-14.53 | $1,442.36 | ▼ -14.53 after sell → book $10,374.93; vs 09:30 mark -1.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SDGR` | 6 | $30.05 | $1.84 | $+0.76 | $1,620.82 | ▲ +0.76 after sell → book $10,373.09; vs 09:30 mark -1.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 2 | $76.47 | $1.56 | $-13.05 | $1,772.20 | ▼ -13.05 after sell → book $10,371.53; vs 09:30 mark -1.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ECO` | 2 | $77.55 | $1.58 | $-18.18 | $1,925.73 | ▼ -18.18 after sell → book $10,369.96; vs 09:30 mark -1.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GME` | 8 | $23.94 | $1.96 | $+4.50 | $2,115.29 | ▲ +4.50 after sell → book $10,368.00; vs 09:30 mark -1.96 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ATRC` | 3 | $58.92 | $1.80 | $-2.33 | $2,290.25 | ▼ -2.33 after sell → book $10,366.20; vs 09:30 mark -1.80 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 30 | $18.57 | $2.08 | — | $1,730.92 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.2; leftover $572.56 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 127 | $4.49 | $2.37 | — | $1,158.32 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $572.56 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 8 | $70.84 | $2.01 | — | $589.59 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $572.56 | — |
| 2026-09-23 09:30 ET | **BUY** | `HYLN` | 127 | $4.49 | $2.37 | — | $16.99 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.6; leftover $572.56 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.99 | ▲ close $10,414.77 vs 09:30 $10,387.60 (session +57.40) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.99 | ▼ 09:30 equity $10,385.60 vs yday $10,414.77 (-29.17) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $998.66 | ▲ +32.44 after sell → book $10,383.57; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 11 | $87.67 | $2.04 | $-16.77 | $1,961.04 | ▼ -16.77 after sell → book $10,381.53; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MXL` | 11 | $82.53 | $2.04 | $-15.01 | $2,866.88 | ▼ -15.01 after sell → book $10,379.49; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `TRMD` | 26 | $34.25 | $2.09 | $-87.88 | $3,755.29 | ▼ -87.88 after sell → book $10,377.40; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `UMC` | 40 | $24.11 | $2.13 | $-37.04 | $4,717.56 | ▼ -37.04 after sell → book $10,375.27; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `PUMP` | 95 | $9.88 | $2.30 | $-53.98 | $5,653.86 | ▼ -53.98 after sell → book $10,372.97; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 404 | $2.68 | $5.29 | $+74.34 | $6,731.29 | ▲ +74.34 after sell → book $10,367.68; vs 09:30 mark -5.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,731.29 | ▲ close $10,662.89 vs 09:30 $10,385.60 (session +295.22) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,488.52 | ▲ 09:30 equity $9,167.93 vs yday $9,162.43 (+5.50) | 09:30 open · cash $8,488.52 (unchanged overnight, no fees) · equity $9,167.93 vs prior close $9,162.43 (+5.50) · 9 name(s) re-marked at the open (per-name table). DGXX×22 yday $4.53 → 09:30 $4.78 +5.50; GLBE×1 yday $40.88 → 09:30 $40.88 +0.00; GRPN×4 yday $20.89 → 09:30 $20.89 +0.00; NEOG×7 yday $13.66 → 09:30 $13.66 +0.00; NTSK×5 yday $18.57 → 09:30 $18.57 +0.00; OPTU×38 yday $1.04 → 09:30 $1.04 +0.00; RXRX×24 yday $3.89 → 09:30 $3.89 +0.00; SVIA×9 yday $3.96 → 09:30 $3.96 +0.00; XXI×14 yday $6.63 → 09:30 $6.63 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `AMD` | 1 | $634.53 | $1.99 | — | $7,851.99 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.4; leftover $1212.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `A` | 7 | $171.98 | $2.01 | — | $6,646.12 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+10.6; leftover $1212.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NTRA` | 2 | $410.00 | $2.00 | — | $5,824.12 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $1212.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CLS` | 3 | $380.51 | $2.00 | — | $4,680.60 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $1212.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 117 | $10.28 | $2.34 | — | $3,475.49 | — | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+9.7; leftover $1212.65 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 551 | $2.20 | $7.11 | — | $2,256.19 | — | rank by candle_score; rank candle_score; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1212.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CURI` | 389 | $3.11 | $5.02 | — | $1,041.38 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; 🔵; ret5=+11.6; leftover $1212.65 | join🟢 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,041.38 | ▼ close $9,071.82 vs 09:30 $9,167.93 (session -73.64) | 16:00 close · cash $1,041.38 · equity $9,071.82 vs 09:30 $9,167.93 (-96.11; session marks -73.64) · 16 name(s) marked open→close (per-name table). DGXX×22 09:30 $4.78 → close $4.59 -4.18; GLBE×1 09:30 $40.88 → close $40.88 +0.00; GRPN×4 09:30 $20.89 → close $20.89 -0.00; NEOG×7 09:30 $13.66 → close $13.66 -0.00; NTSK×5 09:30 $18.57 → close $18.57 -0.00; OPTU×38 09:30 $1.04 → close $1.04 -0.00; RXRX×24 09:30 $3.89 → close $3.89 +0.00; SVIA×9 09:30 $3.96 → close $3.96 +0.00; XXI×14 09:30 $6.63 → close $6.63 +0.00; AMD×1 09:30 $634.53 → close $630.63 -3.90; A×7 09:30 $171.98 → close $172.79 +5.67; NTRA×2 09:30 $410.00 → close $412.56 +5.12; CLS×3 09:30 $380.51 → close $365.44 -45.21; SENS×117 09:30 $10.28 → close $10.00 -32.76; HLP×551 09:30 $2.20 → close $2.21 +5.51; CURI×389 09:30 $3.11 → close $3.10 -3.89 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ZS` | cash | leftover split 13.42 < 1 share @ 190.00 |
| 2026-08-14 | `BETA` | cash | leftover split 13.42 < 1 share @ 25.21 |
| 2026-08-14 | `BRZE` | cash | leftover split 13.42 < 1 share @ 30.00 |
| 2026-08-14 | `MH` | cash | leftover split 13.42 < 1 share @ 13.55 |
| 2026-08-14 | `GLOB` | cash | leftover split 13.42 < 1 share @ 38.21 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `JBIO` | cash | leftover split 12.19 < 1 share @ 24.60 |
| 2026-08-17 | `HTFL` | cash | leftover split 12.19 < 1 share @ 41.23 |
| 2026-08-17 | `STDN` | cash | leftover split 12.19 < 1 share @ 13.64 |
| 2026-08-17 | `CLYM` | cash | leftover split 12.19 < 1 share @ 16.25 |
| 2026-08-18 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CERS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYMR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSKY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RDZN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `IOND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IMMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SM` | cash | leftover split 19.88 < 1 share @ 37.81 |
| 2026-08-21 | `ARIS` | cash | leftover split 19.88 < 1 share @ 20.90 |
| 2026-08-21 | `DXYZ` | cash | leftover split 19.88 < 1 share @ 34.89 |
| 2026-08-21 | `ILMN` | cash | leftover split 19.88 < 1 share @ 212.40 |
| 2026-08-21 | `AEM` | cash | leftover split 19.88 < 1 share @ 216.30 |
| 2026-08-24 | `IOND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IMMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `WIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `B` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BRZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CELH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NEM` | cash | leftover split 14.79 < 1 share @ 132.64 |
| 2026-08-27 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `WIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `B` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BRZE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CELH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PURR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BZ` | cash | leftover split 4.58 < 1 share @ 18.50 |
| 2026-08-27 | `DASH` | cash | leftover split 4.58 < 1 share @ 235.94 |
| 2026-08-27 | `DJT` | cash | leftover split 4.58 < 1 share @ 9.59 |
| 2026-08-27 | `PD` | cash | leftover split 4.58 < 1 share @ 12.45 |
| 2026-08-27 | `SRRK` | cash | leftover split 4.58 < 1 share @ 60.00 |
| 2026-08-27 | `PRGO` | cash | leftover split 4.58 < 1 share @ 14.63 |
| 2026-08-28 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PURR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GALT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GALT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAIC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ULTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CXM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TEAM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HUBS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAIC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ULTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CXM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `CTVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RSKD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `TTD` | cash | leftover split 12.24 < 1 share @ 15.18 |
| 2026-09-04 | `TARS` | cash | leftover split 12.24 < 1 share @ 82.70 |
| 2026-09-04 | `TDS` | cash | leftover split 12.24 < 1 share @ 37.44 |
| 2026-09-04 | `ZETA` | cash | leftover split 12.24 < 1 share @ 32.65 |
| 2026-09-08 | `CTVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RSKD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LPG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INDP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INDP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TXG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TXG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AIB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CRWD` | cash | leftover split 185.57 < 1 share @ 246.98 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AIB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `IQ` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AIB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HLP` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MXL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PUMP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ARM` | cash | leftover split 48.64 < 1 share @ 319.41 |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `RBRK` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-23 | `MXL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `PUMP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DGXX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RBRK` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `HLP` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 59 | 2026-09-21 @ $16.91 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $998.16 |
| `DGXX` | 11 | 2026-09-22 @ $4.30 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+16.9; leftover $48.64 |
| `SECZ` | 3 | 2026-09-22 @ $12.96 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+64.4; leftover $48.64 |
| `NTSK` | 30 | 2026-09-23 @ $18.57 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.2; leftover $572.56 |
| `SVIA` | 127 | 2026-09-23 @ $4.49 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $572.56 |
| `INOD` | 8 | 2026-09-23 @ $70.84 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $572.56 |
| `HYLN` | 127 | 2026-09-23 @ $4.49 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.6; leftover $572.56 |
