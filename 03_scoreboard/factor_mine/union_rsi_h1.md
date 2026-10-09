# Factor mine action — `union_rsi_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `rsi` · size `leftover` · sell `list` · S-boost `none` · rank by rsi

Cash book **-37.57%** ($6,243) · signal-only (no cash/fees) was -22.60%. Starts YES **0/30**. Fills 338 · skips 98 · realized $-2571.92.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how oversold the prior RSI is (lower first).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how oversold the prior RSI is (lower first) and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `rsi` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,213.79.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $8,733.04 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $7,534.99 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $6,290.43 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $5,039.23 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,796.72 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $2,553.19 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,318.47 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $101.46 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.46 | ▲ close $10,235.40 vs 09:30 $10,000.00 (session +267.35) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.46 | ▲ 09:30 equity $10,273.77 vs yday $10,235.40 (+38.37) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $1,517.20 | ▲ +148.79 after sell → book $10,254.52; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $2,708.13 | ▼ -7.12 after sell → book $10,252.45; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $3,887.79 | ▼ -64.90 after sell → book $10,250.36; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $5,109.96 | ▼ -29.03 after sell → book $10,248.23; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,422.02 | ▲ +69.56 after sell → book $10,245.89; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $7,610.36 | ▼ -55.19 after sell → book $10,243.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $8,914.66 | ▲ +69.58 after sell → book $10,241.62; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $10,239.54 | ▲ +107.86 after sell → book $10,239.54; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 175 | $7.29 | $2.52 | — | $8,961.28 | — | rank by rsi; rank rsi; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 118 | $10.83 | $2.34 | — | $7,680.99 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 127 | $10.06 | $2.37 | — | $6,401.00 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 475 | $2.69 | $6.13 | — | $5,117.12 | — | rank by rsi; rank rsi; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `STUB` | 167 | $7.66 | $2.49 | — | $3,835.41 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-13.5; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `ENHA` | 554 | $2.31 | $7.15 | — | $2,548.53 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=-5.3; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 142 | $9.01 | $2.42 | — | $1,266.69 | — | rank by rsi; rank rsi; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1279.94 | — |
| 2026-08-14 09:30 ET | **BUY** | `STNE` | 127 | $9.89 | $2.37 | — | $8.29 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=-7.7; leftover $1279.94 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.29 | ▲ close $10,304.87 vs 09:30 $10,273.77 (session +93.11) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.29 | ▼ 09:30 equity $10,208.19 vs yday $10,304.87 (-96.68) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 175 | $7.24 | $2.55 | $-13.82 | $1,272.73 | ▼ -13.82 after sell → book $10,205.63; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 118 | $11.19 | $2.37 | $+37.76 | $2,590.78 | ▲ +37.76 after sell → book $10,203.26; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 127 | $10.36 | $2.40 | $+33.33 | $3,904.10 | ▲ +33.33 after sell → book $10,200.86; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 475 | $2.80 | $6.22 | $+39.91 | $5,227.88 | ▲ +39.91 after sell → book $10,194.64; vs 09:30 mark -6.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `STUB` | 167 | $7.91 | $2.53 | $+36.73 | $6,546.32 | ▲ +36.73 after sell → book $10,192.11; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 142 | $9.22 | $2.45 | $+24.95 | $7,853.11 | ▲ +24.95 after sell → book $10,189.66; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `STNE` | 127 | $9.63 | $2.40 | $-37.79 | $9,073.72 | ▼ -37.79 after sell → book $10,187.26; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 494 | $2.62 | $6.37 | — | $7,773.07 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1296.25 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 800 | $1.62 | $10.32 | — | $6,466.75 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1296.25 | — |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 518 | $2.50 | $6.68 | — | $5,165.06 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $1296.25 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 32 | $39.85 | $2.09 | — | $3,887.78 | — | rank by rsi; rank rsi; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1296.25 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 188 | $6.87 | $2.55 | — | $2,593.66 | — | rank by rsi; rank rsi; list yday_gainer,yday_mover; ret5=+62.6; leftover $1296.25 | — |
| 2026-08-17 09:30 ET | **BUY** | `ZNTL` | 364 | $3.56 | $4.70 | — | $1,293.13 | — | rank by rsi; rank rsi; list yday_mover; ret5=-15.6; leftover $1296.25 | — |
| 2026-08-17 09:30 ET | **BUY** | `AMPG` | 315 | $4.09 | $4.06 | — | $0.71 | — | rank by rsi; rank rsi; list yday_mover; ⚪; ret5=-31.1; leftover $1296.25 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.71 | ▼ close $9,796.13 vs 09:30 $10,208.19 (session -354.35) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.71 | ▼ 09:30 equity $9,779.88 vs yday $9,796.13 (-16.25) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ENHA` | 554 | $1.70 | $7.25 | $-352.34 | $935.27 | ▼ -352.34 after sell → book $9,772.64; vs 09:30 mark -7.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 494 | $2.52 | $6.46 | $-62.24 | $2,173.68 | ▼ -62.24 after sell → book $9,766.17; vs 09:30 mark -6.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 800 | $1.32 | $10.46 | $-256.78 | $3,223.22 | ▼ -256.78 after sell → book $9,755.71; vs 09:30 mark -10.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CSAN` | 518 | $2.51 | $6.78 | $-8.28 | $4,516.62 | ▼ -8.28 after sell → book $9,748.93; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 32 | $41.57 | $2.11 | $+50.85 | $5,844.75 | ▲ +50.85 after sell → book $9,746.82; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ZNTL` | 364 | $3.75 | $4.77 | $+59.70 | $7,204.99 | ▲ +59.70 after sell → book $9,742.06; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AMPG` | 315 | $3.58 | $4.13 | $-169.47 | $8,327.93 | ▼ -169.47 after sell → book $9,737.93; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,327.93 | ▼ close $9,658.97 vs 09:30 $9,779.88 (session -78.96) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,327.93 | ▲ 09:30 equity $9,679.65 vs yday $9,658.97 (+20.68) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 188 | $7.19 | $2.60 | $+55.01 | $9,677.05 | ▲ +55.01 after sell → book $9,677.05; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,677.05 | ▲ close $9,677.05 vs 09:30 $9,679.65 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,677.05 | ▲ 09:30 equity $9,677.05 vs yday $9,677.05 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 35 | $33.61 | $2.10 | — | $8,498.61 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-17.4; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 273 | $4.43 | $3.52 | — | $7,285.70 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-23.1; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 56 | $21.40 | $2.16 | — | $6,085.14 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.2; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3417 | $0.35 | $22.35 | — | $4,853.18 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-29.4; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 5 | $229.55 | $2.00 | — | $3,703.42 | — | rank by rsi; rank rsi; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 28 | $42.60 | $2.07 | — | $2,508.55 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-4.6; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `DE` | 1 | $611.12 | $1.99 | — | $1,895.43 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-6.3; leftover $1209.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4032 | $0.30 | $24.19 | — | $661.64 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-3.2; leftover $1209.63 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $661.64 | ▲ close $9,679.03 vs 09:30 $9,677.05 (session +62.35) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $661.64 | ▲ 09:30 equity $9,817.19 vs yday $9,679.03 (+138.16) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 35 | $33.63 | $2.12 | $-3.51 | $1,836.58 | ▼ -3.51 after sell → book $9,815.08; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 273 | $4.68 | $3.58 | $+61.15 | $3,110.64 | ▲ +61.15 after sell → book $9,811.50; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 56 | $21.54 | $2.18 | $+3.50 | $4,314.70 | ▲ +3.50 after sell → book $9,809.32; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAFX` | 3417 | $0.35 | $22.79 | $-58.80 | $5,487.86 | ▼ -58.80 after sell → book $9,786.53; vs 09:30 mark -22.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DE` | 1 | $623.26 | $2.01 | $+8.13 | $6,109.11 | ▲ +8.13 after sell → book $9,784.52; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4032 | $0.31 | $25.27 | $-9.15 | $7,333.76 | ▼ -9.15 after sell → book $9,759.25; vs 09:30 mark -25.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 28 | $42.41 | $2.07 | — | $6,144.20 | — | rank by rsi; rank rsi; list yday_mover; ret5=-26.1; leftover $1222.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 11 | $103.69 | $2.02 | — | $5,001.59 | — | rank by rsi; rank rsi; list yday_mover; ret5=-10.3; leftover $1222.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `EYPT` | 223 | $5.48 | $2.88 | — | $3,776.67 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-59.9; leftover $1222.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 714 | $1.71 | $9.21 | — | $2,546.52 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1222.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `EOSE` | 345 | $3.54 | $4.45 | — | $1,320.77 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-16.8; leftover $1222.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 179 | $6.81 | $2.53 | — | $99.25 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=+62.5; leftover $1222.29 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.25 | ▼ close $9,708.87 vs 09:30 $9,817.19 (session -27.21) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.25 | ▲ 09:30 equity $9,984.44 vs yday $9,708.87 (+275.57) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `ROST` | 5 | $238.08 | $2.02 | $+38.62 | $1,287.63 | ▲ +38.62 after sell → book $9,982.42; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 28 | $44.22 | $2.09 | $+41.19 | $2,523.69 | ▲ +41.19 after sell → book $9,980.32; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 28 | $43.05 | $2.09 | $+13.75 | $3,727.00 | ▲ +13.75 after sell → book $9,978.23; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 11 | $104.14 | $2.04 | $+0.88 | $4,870.50 | ▲ +0.88 after sell → book $9,976.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EYPT` | 223 | $5.17 | $2.92 | $-74.93 | $6,020.48 | ▼ -74.93 after sell → book $9,973.26; vs 09:30 mark -2.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 714 | $1.74 | $9.34 | $+2.87 | $7,253.51 | ▲ +2.87 after sell → book $9,963.93; vs 09:30 mark -9.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOSE` | 345 | $3.69 | $4.52 | $+42.78 | $8,522.04 | ▲ +42.78 after sell → book $9,959.41; vs 09:30 mark -4.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 179 | $8.03 | $2.57 | $+213.28 | $9,956.84 | ▲ +213.28 after sell → book $9,956.84; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,956.84 | ▲ close $9,956.84 vs 09:30 $9,984.44 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,956.84 | ▲ 09:30 equity $9,956.84 vs yday $9,956.84 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 209 | $5.93 | $2.70 | — | $8,714.77 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-17.5; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 112 | $11.09 | $2.33 | — | $7,470.37 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-8.0; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 8 | $142.36 | $2.01 | — | $6,329.47 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-8.6; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `RGNX` | 152 | $8.14 | $2.45 | — | $5,089.75 | — | rank by rsi; rank rsi; list yday_mover; ret5=-28.9; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $3,862.67 | — | rank by rsi; rank rsi; list earn_react; ret5=-7.0; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $2,690.01 | — | rank by rsi; rank rsi; list overnight; ret5=-12.0; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `INDP` | 1082 | $1.15 | $13.96 | — | $1,431.75 | — | rank by rsi; rank rsi; list yday_mover; ret5=+16.0; leftover $1244.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 171 | $7.25 | $2.50 | — | $189.50 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1244.60 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.50 | ▲ close $10,012.09 vs 09:30 $9,956.84 (session +85.20) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.50 | ▼ 09:30 equity $9,745.25 vs yday $10,012.09 (-266.84) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `RGNX` | 152 | $8.85 | $2.48 | $+102.99 | $1,532.21 | ▲ +102.99 after sell → book $9,742.76; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $-16.57 | $2,742.72 | ▼ -16.57 after sell → book $9,740.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DY` | 3 | $326.91 | $2.02 | $-193.95 | $3,721.43 | ▼ -193.95 after sell → book $9,738.71; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INDP` | 1082 | $1.09 | $14.15 | $-93.03 | $4,886.67 | ▼ -93.03 after sell → book $9,724.57; vs 09:30 mark -14.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 44 | $27.59 | $2.12 | — | $3,670.58 | — | rank by rsi; rank rsi; list yday_gainer; ret5=+2.0; leftover $1221.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `OSUR` | 324 | $3.77 | $4.18 | — | $2,444.92 | — | rank by rsi; rank rsi; list yday_mover; ret5=+0.8; leftover $1221.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 75 | $16.22 | $2.21 | — | $1,226.21 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-2.5; leftover $1221.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `OKTA` | 9 | $128.00 | $2.02 | — | $72.19 | — | rank by rsi; rank rsi; list overnight; ret5=-9.2; leftover $1221.67 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.19 | ▲ close $9,909.64 vs 09:30 $9,745.25 (session +195.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.19 | ▲ 09:30 equity $10,237.58 vs yday $9,909.64 (+327.94) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 171 | $9.19 | $2.54 | $+326.69 | $1,641.14 | ▲ +326.69 after sell → book $10,235.04; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BILI` | 75 | $16.18 | $2.24 | $-7.45 | $2,852.40 | ▼ -7.45 after sell → book $10,232.80; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OKTA` | 9 | $166.15 | $2.04 | $+339.29 | $4,345.71 | ▲ +339.29 after sell → book $10,230.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 316 | $4.57 | $4.08 | — | $2,897.52 | — | rank by rsi; rank rsi; list mover_buy; 🔵; ret5=+1.1; leftover $1448.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 96 | $14.96 | $2.28 | — | $1,459.08 | — | rank by rsi; rank rsi; list overnight; ret5=+3.0; leftover $1448.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `MNSO` | 133 | $10.89 | $2.39 | — | $8.32 | — | rank by rsi; rank rsi; list overnight; ret5=+2.7; leftover $1448.57 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.32 | ▼ close $10,157.32 vs 09:30 $10,237.58 (session -64.70) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.32 | ▼ 09:30 equity $10,112.46 vs yday $10,157.32 (-44.86) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 209 | $6.27 | $2.74 | $+65.62 | $1,316.01 | ▲ +65.62 after sell → book $10,109.72; vs 09:30 mark -2.74 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 8 | $132.80 | $2.03 | $-80.53 | $2,376.37 | ▼ -80.53 after sell → book $10,107.68; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MAIR` | 44 | $27.36 | $2.14 | $-14.38 | $3,578.07 | ▼ -14.38 after sell → book $10,105.54; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OSUR` | 324 | $3.70 | $4.24 | $-31.10 | $4,772.63 | ▼ -31.10 after sell → book $10,101.30; vs 09:30 mark -4.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 316 | $4.67 | $4.14 | $+23.38 | $6,244.21 | ▲ +23.38 after sell → book $10,097.16; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MNSO` | 133 | $10.43 | $2.42 | $-65.99 | $7,628.98 | ▼ -65.99 after sell → book $10,094.74; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 1096 | $1.16 | $14.14 | — | $6,343.48 | — | rank by rsi; rank rsi; list overnight; ret5=-13.8; leftover $1271.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $5,116.12 | — | rank by rsi; rank rsi; list yday_mover; ret5=-23.0; leftover $1271.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 327 | $3.88 | $4.22 | — | $3,843.14 | — | rank by rsi; rank rsi; list earn_react; ret5=-8.6; leftover $1271.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 95 | $13.37 | $2.27 | — | $2,570.71 | — | rank by rsi; rank rsi; list yday_mover; ret5=-14.9; leftover $1271.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 13 | $91.75 | $2.03 | — | $1,375.93 | — | rank by rsi; rank rsi; list yday_mover; ret5=-13.2; leftover $1271.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `QBTS` | 72 | $17.56 | $2.21 | — | $109.41 | — | rank by rsi; rank rsi; list yday_mover; ret5=-4.8; leftover $1271.50 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.41 | ▼ close $9,750.83 vs 09:30 $10,112.46 (session -317.04) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.41 | ▼ 09:30 equity $9,566.39 vs yday $9,750.83 (-184.44) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 112 | $8.70 | $2.35 | $-272.36 | $1,081.45 | ▼ -272.36 after sell → book $9,564.03; vs 09:30 mark -2.36 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 96 | $14.88 | $2.31 | $-12.26 | $2,507.63 | ▼ -12.26 after sell → book $9,561.73; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $3,697.64 | ▼ -37.34 after sell → book $9,559.70; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 327 | $3.39 | $4.28 | $-168.73 | $4,801.89 | ▼ -168.73 after sell → book $9,555.42; vs 09:30 mark -4.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 95 | $13.54 | $2.30 | $+11.57 | $6,085.89 | ▲ +11.57 after sell → book $9,553.12; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 13 | $89.15 | $2.05 | $-37.88 | $7,242.79 | ▼ -37.88 after sell → book $9,551.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QBTS` | 72 | $16.68 | $2.23 | $-67.43 | $8,441.88 | ▼ -67.43 after sell → book $9,548.84; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,441.88 | ▲ close $9,614.60 vs 09:30 $9,566.39 (session +65.76) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,441.88 | ▼ 09:30 equity $9,548.84 vs yday $9,614.60 (-65.76) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `LX` | 1096 | $1.01 | $14.33 | $-192.87 | $9,534.51 | ▼ -192.87 after sell → book $9,534.51; vs 09:30 mark -14.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,534.51 | ▲ close $9,534.51 vs 09:30 $9,548.84 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,534.51 | ▲ 09:30 equity $9,534.51 vs yday $9,534.51 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,534.51 | ▲ close $9,534.51 vs 09:30 $9,534.51 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,534.51 | ▲ 09:30 equity $9,534.51 vs yday $9,534.51 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1441 | $0.83 | $16.24 | — | $8,326.57 | — | rank by rsi; rank rsi; list yday_mover; ret5=-30.4; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 114 | $10.38 | $2.33 | — | $7,141.48 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-56.2; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1862 | $0.64 | $17.50 | — | $5,932.30 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.0; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 54 | $22.00 | $2.15 | — | $4,742.15 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-17.3; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 163 | $7.31 | $2.48 | — | $3,548.14 | — | rank by rsi; rank rsi; list yday_gainer; 🔵; ret5=+18.5; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 93 | $12.78 | $2.27 | — | $2,357.33 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-4.4; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 419 | $2.84 | $5.41 | — | $1,161.97 | — | rank by rsi; rank rsi; list yday_mover; ret5=-26.9; leftover $1191.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 58 | $19.86 | $2.16 | — | $7.92 | — | rank by rsi; rank rsi; list overnight; ret5=-5.5; leftover $1191.81 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.92 | ▼ close $9,320.95 vs 09:30 $9,534.51 (session -163.01) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.92 | ▲ 09:30 equity $9,558.62 vs yday $9,320.95 (+237.67) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 1441 | $0.86 | $16.94 | $+11.49 | $1,227.36 | ▲ +11.49 after sell → book $9,541.68; vs 09:30 mark -16.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 114 | $11.23 | $2.36 | $+92.78 | $2,505.22 | ▲ +92.78 after sell → book $9,539.32; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 1862 | $0.60 | $17.08 | $-109.06 | $3,605.35 | ▼ -109.06 after sell → book $9,522.25; vs 09:30 mark -17.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 54 | $22.27 | $2.17 | $+10.26 | $4,805.75 | ▲ +10.26 after sell → book $9,520.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FJET` | 419 | $2.80 | $5.48 | $-27.65 | $5,973.47 | ▼ -27.65 after sell → book $9,514.59; vs 09:30 mark -5.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 682 | $1.75 | $8.80 | — | $4,771.17 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.6; leftover $1194.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRDO` | 7 | $162.10 | $2.01 | — | $3,634.46 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-31.7; leftover $1194.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `FCEL` | 82 | $14.52 | $2.24 | — | $2,441.59 | — | rank by rsi; rank rsi; list yday_mover; ret5=-24.1; leftover $1194.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 76 | $15.70 | $2.22 | — | $1,246.17 | — | rank by rsi; rank rsi; list earn_react; ret5=-0.4; leftover $1194.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 27 | $43.80 | $2.07 | — | $61.50 | — | rank by rsi; rank rsi; list overnight; ret5=-7.7; leftover $1194.69 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.50 | ▼ close $9,494.61 vs 09:30 $9,558.62 (session -2.65) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.50 | ▼ 09:30 equity $9,471.28 vs yday $9,494.61 (-23.33) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 163 | $7.13 | $2.52 | $-34.34 | $1,221.17 | ▼ -34.34 after sell → book $9,468.76; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SWBI` | 93 | $12.51 | $2.29 | $-29.67 | $2,382.31 | ▼ -29.67 after sell → book $9,466.47; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PL` | 58 | $17.85 | $2.18 | $-120.93 | $3,415.42 | ▼ -120.93 after sell → book $9,464.29; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AIIO` | 682 | $1.81 | $8.92 | $+23.20 | $4,640.92 | ▲ +23.20 after sell → book $9,455.37; vs 09:30 mark -8.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRDO` | 7 | $170.54 | $2.03 | $+55.07 | $5,832.70 | ▲ +55.07 after sell → book $9,453.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FCEL` | 82 | $15.18 | $2.26 | $+49.62 | $7,075.20 | ▲ +49.62 after sell → book $9,451.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 76 | $15.20 | $2.24 | $-42.46 | $8,228.16 | ▼ -42.46 after sell → book $9,448.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,228.16 | ▼ close $9,441.27 vs 09:30 $9,471.28 (session -7.56) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,228.16 | ▲ 09:30 equity $9,456.66 vs yday $9,441.27 (+15.39) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `UNFI` | 27 | $45.50 | $2.09 | $+41.74 | $9,454.57 | ▲ +41.74 after sell → book $9,454.57; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,454.57 | ▲ close $9,454.57 vs 09:30 $9,456.66 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,454.57 | ▲ 09:30 equity $9,454.57 vs yday $9,454.57 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,454.57 | ▲ close $9,454.57 vs 09:30 $9,454.57 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,454.57 | ▲ 09:30 equity $9,454.57 vs yday $9,454.57 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 21 | $54.66 | $2.05 | — | $8,304.66 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.3; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 335 | $3.52 | $4.32 | — | $7,121.14 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-19.2; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 57 | $20.61 | $2.16 | — | $5,944.21 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.7; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `SION` | 151 | $7.79 | $2.44 | — | $4,765.47 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+4.2; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 8 | $135.71 | $2.01 | — | $3,677.78 | — | rank by rsi; rank rsi; list earn_react; ret5=-9.2; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 80 | $14.71 | $2.23 | — | $2,498.75 | — | rank by rsi; rank rsi; list yday_mover; ret5=-12.8; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `AXGN` | 27 | $42.48 | $2.07 | — | $1,349.72 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-13.4; leftover $1181.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `SLBT` | 565 | $2.09 | $7.29 | — | $161.58 | — | rank by rsi; rank rsi; list yday_mover; ret5=-36.6; leftover $1181.82 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.58 | ▲ close $9,442.81 vs 09:30 $9,454.57 (session +12.82) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.58 | ▼ 09:30 equity $9,383.40 vs yday $9,442.81 (-59.41) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 21 | $54.78 | $2.07 | $-1.61 | $1,309.89 | ▼ -1.61 after sell → book $9,381.33; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RWT` | 335 | $3.53 | $4.39 | $-5.36 | $2,488.05 | ▼ -5.36 after sell → book $9,376.94; vs 09:30 mark -4.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 57 | $21.10 | $2.18 | $+23.59 | $3,688.57 | ▲ +23.59 after sell → book $9,374.76; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 8 | $131.40 | $2.03 | $-38.53 | $4,737.74 | ▼ -38.53 after sell → book $9,372.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 80 | $14.85 | $2.25 | $+6.72 | $5,923.48 | ▲ +6.72 after sell → book $9,370.47; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AXGN` | 27 | $41.55 | $2.09 | $-29.27 | $7,043.24 | ▼ -29.27 after sell → book $9,368.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SLBT` | 565 | $2.02 | $7.39 | $-54.23 | $8,177.15 | ▼ -54.23 after sell → book $9,360.99; vs 09:30 mark -7.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,177.15 | ▲ close $9,424.41 vs 09:30 $9,383.40 (session +63.42) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,177.15 | ▼ 09:30 equity $9,365.52 vs yday $9,424.41 (-58.89) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,177.15 | ▼ close $9,216.03 vs 09:30 $9,365.52 (session -149.49) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,177.15 | ▲ 09:30 equity $9,226.60 vs yday $9,216.03 (+10.57) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SION` | 151 | $6.95 | $2.48 | $-131.76 | $9,224.12 | ▼ -131.76 after sell → book $9,224.12; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1366 | $0.84 | $15.63 | — | $8,055.59 | — | rank by rsi; rank rsi; list yday_mover; ret5=-33.5; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 111 | $10.30 | $2.32 | — | $6,909.97 | — | rank by rsi; rank rsi; list yday_mover; ret5=-23.0; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 168 | $6.86 | $2.49 | — | $5,754.99 | — | rank by rsi; rank rsi; list yday_mover; ret5=-34.7; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 7206 | $0.16 | $33.15 | — | $4,568.89 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-23.8; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 315 | $3.66 | $4.06 | — | $3,411.92 | — | rank by rsi; rank rsi; list yday_mover; ret5=-19.7; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 28 | $40.93 | $2.07 | — | $2,263.81 | — | rank by rsi; rank rsi; list earn_react; ret5=-3.1; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 423 | $2.72 | $5.46 | — | $1,107.79 | — | rank by rsi; rank rsi; list yday_mover; ret5=-26.3; leftover $1153.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 470 | $2.34 | $6.06 | — | $1.93 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.2; leftover $1153.02 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.93 | ▼ close $9,006.10 vs 09:30 $9,226.60 (session -146.78) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.93 | ▲ 09:30 equity $9,032.75 vs yday $9,006.10 (+26.65) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `NMRA` | 1366 | $0.78 | $14.99 | $-118.04 | $1,052.42 | ▼ -118.04 after sell → book $9,017.76; vs 09:30 mark -14.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRBP` | 168 | $7.26 | $2.53 | $+62.17 | $2,269.57 | ▲ +62.17 after sell → book $9,015.23; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 315 | $3.57 | $4.13 | $-36.54 | $3,389.99 | ▼ -36.54 after sell → book $9,011.10; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 28 | $40.79 | $2.09 | $-8.09 | $4,530.02 | ▼ -8.09 after sell → book $9,009.01; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CTMX` | 423 | $2.83 | $5.54 | $+35.54 | $5,721.57 | ▲ +35.54 after sell → book $9,003.47; vs 09:30 mark -5.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 470 | $2.35 | $6.15 | $-7.51 | $6,819.92 | ▼ -7.51 after sell → book $8,997.32; vs 09:30 mark -6.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `MRLN` | 500 | $2.27 | $6.45 | — | $5,678.47 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-29.6; leftover $1136.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $4,722.07 | — | rank by rsi; rank rsi; list yday_mover; ret5=-11.6; leftover $1136.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `RCAT` | 153 | $7.39 | $2.45 | — | $3,588.18 | — | rank by rsi; rank rsi; list yday_mover; ret5=-12.7; leftover $1136.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `PALI` | 649 | $1.75 | $8.37 | — | $2,444.06 | — | rank by rsi; rank rsi; list yday_mover; ret5=-17.6; leftover $1136.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `CRDO` | 6 | $168.65 | $2.01 | — | $1,430.15 | — | rank by rsi; rank rsi; list yday_gainer; ret5=-3.8; leftover $1136.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `PUMP` | 110 | $10.31 | $2.32 | — | $293.73 | — | rank by rsi; rank rsi; list ohlc_hot; ret5=+5.9; leftover $1136.65 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.73 | ▼ close $8,738.53 vs 09:30 $9,032.75 (session -235.19) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.73 | ▲ 09:30 equity $8,832.75 vs yday $8,738.53 (+94.22) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 111 | $8.68 | $2.35 | $-184.49 | $1,254.86 | ▼ -184.49 after sell → book $8,830.40; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7206 | $0.17 | $35.07 | $+3.84 | $2,444.81 | ▲ +3.84 after sell → book $8,795.33; vs 09:30 mark -35.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MRLN` | 500 | $2.07 | $6.54 | $-112.99 | $3,473.26 | ▼ -112.99 after sell → book $8,788.78; vs 09:30 mark -6.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $4,418.44 | ▼ -11.22 after sell → book $8,786.76; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RCAT` | 153 | $7.18 | $2.48 | $-37.83 | $5,514.50 | ▼ -37.83 after sell → book $8,784.28; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PALI` | 649 | $1.68 | $8.49 | $-62.29 | $6,596.33 | ▼ -62.29 after sell → book $8,775.79; vs 09:30 mark -8.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CRDO` | 6 | $171.11 | $2.03 | $+10.72 | $7,620.96 | ▲ +10.72 after sell → book $8,773.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 110 | $10.48 | $2.35 | $+14.03 | $8,771.41 | ▲ +14.03 after sell → book $8,771.41; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 145 | $7.54 | $2.42 | — | $7,676.41 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-20.9; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 74 | $14.79 | $2.21 | — | $6,579.74 | — | rank by rsi; rank rsi; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 277 | $3.95 | $3.57 | — | $5,482.02 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 3097 | $0.35 | $20.25 | — | $4,365.42 | — | rank by rsi; rank rsi; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `ALMU` | 94 | $11.64 | $2.27 | — | $3,268.99 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-12.8; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `AKBA` | 1231 | $0.89 | $14.65 | — | $2,158.75 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+12.1; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `XE` | 67 | $16.28 | $2.19 | — | $1,065.80 | — | rank by rsi; rank rsi; list yday_gainer; 🔵; ret5=+3.1; leftover $1096.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 75 | $14.07 | $2.21 | — | $8.34 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1096.43 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.34 | ▼ close $8,023.66 vs 09:30 $8,832.75 (session -697.95) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.34 | ▲ 09:30 equity $8,173.95 vs yday $8,023.66 (+150.29) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 145 | $7.36 | $2.46 | $-30.26 | $1,073.08 | ▼ -30.26 after sell → book $8,171.49; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 74 | $14.58 | $2.23 | $-19.99 | $2,149.76 | ▼ -19.99 after sell → book $8,169.26; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 277 | $3.87 | $3.63 | $-29.36 | $3,218.12 | ▼ -29.36 after sell → book $8,165.63; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DCX` | 3097 | $0.14 | $14.18 | $-694.10 | $3,640.62 | ▼ -694.10 after sell → book $8,151.45; vs 09:30 mark -14.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 94 | $13.12 | $2.30 | $+135.02 | $4,872.07 | ▲ +135.02 after sell → book $8,149.15; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `AKBA` | 1231 | $0.93 | $15.32 | $+15.58 | $5,997.89 | ▲ +15.58 after sell → book $8,133.83; vs 09:30 mark -15.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `XE` | 67 | $16.32 | $2.21 | $-1.72 | $7,089.12 | ▼ -1.72 after sell → book $8,131.62; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 75 | $13.90 | $2.24 | $-17.20 | $8,129.38 | ▼ -17.20 after sell → book $8,129.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 25 | $40.00 | $2.06 | — | $7,127.32 | — | rank by rsi; rank rsi; list yday_mover; ret5=-32.2; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 169 | $6.00 | $2.50 | — | $6,110.82 | — | rank by rsi; rank rsi; list yday_mover; ret5=-24.1; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 14 | $68.39 | $2.03 | — | $5,151.33 | — | rank by rsi; rank rsi; list overnight; ret5=-7.0; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `KDK` | 297 | $3.42 | $3.83 | — | $4,131.76 | — | rank by rsi; rank rsi; list yday_mover; ret5=-12.3; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 48 | $20.85 | $2.13 | — | $3,128.82 | — | rank by rsi; rank rsi; list overnight; ret5=-2.5; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 9 | $105.72 | $2.02 | — | $2,175.32 | — | rank by rsi; rank rsi; list overnight; ret5=-11.5; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKV` | 44 | $22.68 | $2.12 | — | $1,175.28 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+10.8; leftover $1016.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `PUMP` | 97 | $10.40 | $2.28 | — | $164.20 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+5.9; leftover $1016.17 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.20 | ▼ close $8,005.03 vs 09:30 $8,173.95 (session -105.37) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.20 | ▼ 09:30 equity $8,003.34 vs yday $8,005.03 (-1.69) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 169 | $5.99 | $2.54 | $-6.72 | $1,173.98 | ▼ -6.72 after sell → book $8,000.81; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 2 | $93.97 | $1.89 | — | $984.15 | — | rank by rsi; rank rsi; list flatten; ret5=-0.6; leftover $195.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `KBH` | 3 | $49.39 | $1.49 | — | $834.49 | — | rank by rsi; rank rsi; list overnight; ret5=-3.4; leftover $195.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `GIS` | 5 | $35.96 | $1.81 | — | $652.88 | — | rank by rsi; rank rsi; list overnight; ret5=-2.9; leftover $195.66 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $652.88 | ▼ close $7,990.59 vs 09:30 $8,003.34 (session -5.03) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $652.88 | ▼ 09:30 equity $7,933.53 vs yday $7,990.59 (-57.06) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `XENE` | 25 | $39.51 | $2.08 | $-16.40 | $1,638.54 | ▼ -16.40 after sell → book $7,931.44; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `THO` | 14 | $71.41 | $2.05 | $+38.20 | $2,636.23 | ▲ +38.20 after sell → book $7,929.39; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `KDK` | 297 | $3.19 | $3.89 | $-76.03 | $3,579.77 | ▼ -76.03 after sell → book $7,925.50; vs 09:30 mark -3.89 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MLKN` | 48 | $19.76 | $2.15 | $-56.61 | $4,526.10 | ▼ -56.61 after sell → book $7,923.35; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABVX` | 9 | $98.30 | $2.04 | $-70.83 | $5,408.76 | ▼ -70.83 after sell → book $7,921.31; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKV` | 44 | $23.29 | $2.14 | $+22.58 | $6,431.38 | ▲ +22.58 after sell → book $7,919.17; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `PUMP` | 97 | $10.10 | $2.31 | $-33.69 | $7,408.77 | ▼ -33.69 after sell → book $7,916.86; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 2 | $93.97 | $1.91 | $-3.79 | $7,594.80 | ▼ -3.79 after sell → book $7,914.95; vs 09:30 mark -1.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `XNDU` | 211 | $5.99 | $2.72 | — | $6,328.19 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.9; leftover $1265.80 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1037 | $1.22 | $13.38 | — | $5,049.68 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-33.0; leftover $1265.80 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1648 | $0.77 | $17.60 | — | $3,766.41 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1265.80 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVER` | 65 | $19.46 | $2.19 | — | $2,499.33 | — | rank by rsi; rank rsi; list yday_mover; ret5=-5.0; leftover $1265.80 | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 26 | $47.57 | $2.07 | — | $1,260.44 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-11.2; leftover $1265.80 | — |
| 2026-09-23 09:30 ET | **BUY** | `FUL` | 24 | $50.51 | $2.06 | — | $46.14 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-2.6; leftover $1265.80 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.14 | ▼ close $7,495.80 vs 09:30 $7,933.53 (session -379.14) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.14 | ▼ 09:30 equity $7,438.16 vs yday $7,495.80 (-57.64) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 3 | $47.14 | $1.44 | $-9.68 | $186.11 | ▼ -9.68 after sell → book $7,436.72; vs 09:30 mark -1.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 5 | $35.96 | $1.83 | $-3.65 | $364.08 | ▼ -3.65 after sell → book $7,434.89; vs 09:30 mark -1.83 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `XNDU` | 211 | $5.12 | $2.77 | $-189.06 | $1,441.63 | ▼ -189.06 after sell → book $7,432.12; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1037 | $1.17 | $13.56 | $-78.79 | $2,641.36 | ▼ -78.79 after sell → book $7,418.56; vs 09:30 mark -13.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1648 | $0.75 | $17.52 | $-71.38 | $3,853.25 | ▼ -71.38 after sell → book $7,401.04; vs 09:30 mark -17.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVER` | 65 | $17.63 | $2.21 | $-123.34 | $4,996.99 | ▼ -123.34 after sell → book $7,398.83; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 26 | $46.88 | $2.09 | $-22.10 | $6,213.79 | ▼ -22.10 after sell → book $7,396.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,213.79 | ▲ close $7,413.79 vs 09:30 $7,438.16 (session +17.04) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,537.96 | ▲ 09:30 equity $7,487.96 vs yday $7,487.96 (+0.00) | 09:30 open · cash $6,537.96 (unchanged overnight, no fees) · equity $7,487.96 vs prior close $7,487.96 (+0.00) · 1 name(s) re-marked at the open (per-name table). FUL×19 yday $50.00 → 09:30 $50.00 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 36 | $22.21 | $2.10 | — | $5,736.30 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-19.2; leftover $817.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GEN` | 35 | $22.91 | $2.10 | — | $4,932.36 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-23.6; leftover $817.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 371 | $2.20 | $4.79 | — | $4,111.37 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.1; leftover $817.25 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NEOV` | 341 | $2.39 | $4.40 | — | $3,291.98 | — | rank by rsi; rank rsi; list yday_mover; ret5=-31.4; leftover $817.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `LRMR` | 246 | $3.32 | $3.17 | — | $2,472.09 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-12.6; leftover $817.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RZLT` | 204 | $3.99 | $2.63 | — | $1,655.50 | — | rank by rsi; rank rsi; list earn_react; ret5=-0.3; leftover $817.25 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SGMT` | 89 | $9.11 | $2.26 | — | $842.45 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $817.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SMWB` | 108 | $7.50 | $2.31 | — | $30.14 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-9.7; leftover $817.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.14 | ▼ close $7,220.66 vs 09:30 $7,487.96 (session -243.55) | 16:00 close · cash $30.14 · equity $7,220.66 vs 09:30 $7,487.96 (-267.30; session marks -243.55) · 9 name(s) marked open→close (per-name table). FUL×19 09:30 $50.00 → close $50.00 +0.00; ACAD×36 09:30 $22.21 → close $20.68 -55.08; GEN×35 09:30 $22.91 → close $21.62 -45.15; SFIX×371 09:30 $2.20 → close $2.15 -16.70; NEOV×341 09:30 $2.39 → close $2.19 -68.20; LRMR×246 09:30 $3.32 → close $3.08 -60.27; RZLT×204 09:30 $3.99 → close $3.90 -18.36; SGMT×89 09:30 $9.11 → close $9.24 +11.57; SMWB×108 09:30 $7.50 → close $7.58 +8.64 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.14 | ▼ 09:30 equity $7,188.35 vs yday $7,220.66 (-32.31) | 09:30 open · cash $30.14 (unchanged overnight, no fees) · equity $7,188.35 vs prior close $7,220.66 (-32.31) · 9 name(s) re-marked at the open (per-name table). ACAD×36 yday $20.68 → 09:30 $20.53 -5.40; FUL×19 yday $50.00 → 09:30 $49.06 -17.86; GEN×35 yday $21.62 → 09:30 $21.44 -6.30; LRMR×246 yday $3.08 → 09:30 $3.23 +38.33; NEOV×341 yday $2.19 → 09:30 $2.20 +3.41; RZLT×204 yday $3.90 → 09:30 $3.85 -10.20; SFIX×371 yday $2.15 → 09:30 $2.12 -12.24; SGMT×89 yday $9.24 → 09:30 $9.15 -8.01; SMWB×108 yday $7.58 → 09:30 $7.45 -14.04 | — |
| 2026-09-28 09:30 ET | **SELL** | `ACAD` | 36 | $20.53 | $2.12 | $-64.70 | $767.10 | ▼ -64.70 after sell → book $7,186.23; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `FUL` | 19 | $49.06 | $2.07 | $-31.66 | $1,697.18 | ▼ -31.66 after sell → book $7,184.16; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `GEN` | 35 | $21.44 | $2.12 | $-55.66 | $2,445.46 | ▼ -55.66 after sell → book $7,182.05; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `LRMR` | 246 | $3.23 | $3.22 | $-28.34 | $3,237.01 | ▼ -28.34 after sell → book $7,178.82; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `NEOV` | 341 | $2.20 | $4.47 | $-73.65 | $3,982.75 | ▼ -73.65 after sell → book $7,174.36; vs 09:30 mark -4.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `RZLT` | 204 | $3.85 | $2.68 | $-33.87 | $4,765.47 | ▼ -33.87 after sell → book $7,171.68; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SFIX` | 371 | $2.12 | $4.86 | $-38.58 | $5,547.88 | ▼ -38.58 after sell → book $7,166.83; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SGMT` | 89 | $9.15 | $2.28 | $-0.98 | $6,359.94 | ▼ -0.98 after sell → book $7,164.54; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SMWB` | 108 | $7.45 | $2.34 | $-10.06 | $7,162.20 | ▼ -10.06 after sell → book $7,162.20; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,162.20 | ▲ close $7,162.20 vs 09:30 $7,188.35 (session +0.00) | 16:00 close · cash $7,162.20 · no lots left · equity $7,162.20. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,162.20 | ▲ 09:30 equity $7,162.20 vs yday $7,162.20 (+0.00) | 09:30 open · cash $7,162.20 · no holdings · equity $7,162.20 vs prior close $7,162.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `OCUL` | 115 | $7.76 | $2.33 | — | $6,267.47 | — | rank by rsi; rank rsi; list yday_mover; ret5=-23.4; leftover $895.27 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SWMR` | 48 | $18.59 | $2.13 | — | $5,373.01 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.5; leftover $895.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TDTH` | 1005 | $0.89 | $11.96 | — | $4,466.60 | — | rank by rsi; rank rsi; list yday_mover; ret5=-36.6; leftover $895.27 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MRLN` | 511 | $1.75 | $6.59 | — | $3,565.76 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-5.9; leftover $895.27 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GFI` | 24 | $36.38 | $2.06 | — | $2,690.58 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-16.2; leftover $895.27 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AUR` | 168 | $5.32 | $2.49 | — | $1,794.32 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-18.7; leftover $895.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `UEC` | 90 | $9.91 | $2.26 | — | $900.16 | — | rank by rsi; rank rsi; list earn_react; ret5=-8.6; leftover $895.27 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `JEF` | 19 | $46.08 | $2.05 | — | $22.60 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-1.5; leftover $895.27 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.60 | ▲ close $7,276.91 vs 09:30 $7,162.20 (session +146.59) | 16:00 close · cash $22.60 · equity $7,276.91 vs 09:30 $7,162.20 (+114.71; session marks +146.59) · 8 name(s) marked open→close (per-name table). OCUL×115 09:30 $7.76 → close $7.87 +12.65; SWMR×48 09:30 $18.59 → close $16.37 -106.56; TDTH×1005 09:30 $0.89 → close $1.14 +251.25; MRLN×511 09:30 $1.75 → close $1.78 +15.33; GFI×24 09:30 $36.38 → close $36.78 +9.60; AUR×168 09:30 $5.32 → close $5.39 +11.76; UEC×90 09:30 $9.91 → close $9.29 -55.80; JEF×19 09:30 $46.08 → close $46.52 +8.36 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.60 | ▲ 09:30 equity $7,276.91 vs yday $7,276.91 (+0.00) | 09:30 open · cash $22.60 (unchanged overnight, no fees) · equity $7,276.91 vs prior close $7,276.91 (+0.00) · 8 name(s) re-marked at the open (per-name table). AUR×168 yday $5.39 → 09:30 $5.39 +0.00; GFI×24 yday $36.78 → 09:30 $36.78 +0.00; JEF×19 yday $46.52 → 09:30 $46.52 +0.00; MRLN×511 yday $1.78 → 09:30 $1.78 +0.00; OCUL×115 yday $7.87 → 09:30 $7.87 +0.00; SWMR×48 yday $16.37 → 09:30 $16.37 +0.00; TDTH×1005 yday $1.14 → 09:30 $1.14 +0.00; UEC×90 yday $9.29 → 09:30 $9.29 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.60 | ▲ close $7,276.91 vs 09:30 $7,276.91 (session +0.00) | 16:00 close · cash $22.60 · equity $7,276.91 vs 09:30 $7,276.91 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). AUR×168 09:30 $5.39 → close $5.39 +0.00; GFI×24 09:30 $36.78 → close $36.78 +0.00; JEF×19 09:30 $46.52 → close $46.52 +0.00; MRLN×511 09:30 $1.78 → close $1.78 +0.00; OCUL×115 09:30 $7.87 → close $7.87 +0.00; SWMR×48 09:30 $16.37 → close $16.37 +0.00; TDTH×1005 09:30 $1.14 → close $1.14 +0.00; UEC×90 09:30 $9.29 → close $9.29 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.60 | ▼ 09:30 equity $7,224.15 vs yday $7,276.91 (-52.76) | 09:30 open · cash $22.60 (unchanged overnight, no fees) · equity $7,224.15 vs prior close $7,276.91 (-52.76) · 8 name(s) re-marked at the open (per-name table). AUR×168 yday $5.39 → 09:30 $5.58 +31.92; GFI×24 yday $36.78 → 09:30 $36.18 -14.40; JEF×19 yday $46.52 → 09:30 $45.41 -21.09; MRLN×511 yday $1.78 → 09:30 $1.85 +35.77; OCUL×115 yday $7.87 → 09:30 $7.63 -27.60; SWMR×48 yday $16.37 → 09:30 $17.50 +54.24; TDTH×1005 yday $1.14 → 09:30 $1.02 -120.60; UEC×90 yday $9.29 → 09:30 $9.39 +9.00 | — |
| 2026-10-01 09:30 ET | **SELL** | `AUR` | 168 | $5.58 | $2.53 | $+38.65 | $957.51 | ▲ +38.65 after sell → book $7,221.62; vs 09:30 mark -2.53 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GFI` | 24 | $36.18 | $2.08 | $-8.94 | $1,823.75 | ▼ -8.94 after sell → book $7,219.54; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `JEF` | 19 | $45.41 | $2.07 | $-16.84 | $2,684.47 | ▼ -16.84 after sell → book $7,217.47; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MRLN` | 511 | $1.85 | $6.69 | $+37.82 | $3,623.13 | ▲ +37.82 after sell → book $7,210.78; vs 09:30 mark -6.69 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `OCUL` | 115 | $7.63 | $2.36 | $-19.65 | $4,498.22 | ▼ -19.65 after sell → book $7,208.42; vs 09:30 mark -2.36 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SWMR` | 48 | $17.50 | $2.15 | $-56.61 | $5,336.06 | ▼ -56.61 after sell → book $7,206.26; vs 09:30 mark -2.16 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TDTH` | 1005 | $1.02 | $13.14 | $+105.55 | $6,348.02 | ▲ +105.55 after sell → book $7,193.12; vs 09:30 mark -13.14 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `UEC` | 90 | $9.39 | $2.28 | $-51.34 | $7,190.84 | ▼ -51.34 after sell → book $7,190.84; vs 09:30 mark -2.28 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `GLAS` | 182 | $5.62 | $2.54 | — | $6,165.46 | — | rank by rsi; rank rsi; list yday_mover; ret5=-28.5; leftover $1027.26 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `LQDA` | 42 | $23.96 | $2.12 | — | $5,157.03 | — | rank by rsi; rank rsi; list yday_mover; ret5=-55.2; leftover $1027.26 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `QTTB` | 122 | $8.36 | $2.36 | — | $4,134.75 | — | rank by rsi; rank rsi; list yday_mover; ret5=-13.5; leftover $1027.26 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVXL` | 513 | $2.00 | $6.62 | — | $3,102.13 | — | rank by rsi; rank rsi; list yday_mover; ret5=-14.0; leftover $1027.26 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MKC` | 21 | $46.80 | $2.05 | — | $2,117.28 | — | rank by rsi; rank rsi; list earn_react; ret5=-5.5; leftover $1027.26 | join🟢 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PYXS` | 444 | $2.31 | $5.73 | — | $1,085.91 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-27.6; leftover $1027.26 | join🟢 sector🔴 gen🔴 news🔴 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `NKE` | 28 | $35.45 | $2.07 | — | $91.24 | — | rank by rsi; rank rsi; list overnight,overnight_mega; ret5=-1.8; leftover $1027.26 | join🔴 sector🔴 gen🔴 news🔴 digest🔴 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.24 | ▼ close $7,094.63 vs 09:30 $7,224.15 (session -72.73) | 16:00 close · cash $91.24 · equity $7,094.63 vs 09:30 $7,224.15 (-129.52; session marks -72.73) · 7 name(s) marked open→close (per-name table). GLAS×182 09:30 $5.62 → close $5.32 -54.60; LQDA×42 09:30 $23.96 → close $27.99 +169.26; QTTB×122 09:30 $8.36 → close $8.25 -13.42; AVXL×513 09:30 $2.00 → close $1.89 -56.43; MKC×21 09:30 $46.80 → close $44.14 -55.86; PYXS×444 09:30 $2.31 → close $2.19 -53.28; NKE×28 09:30 $35.45 → close $35.15 -8.40 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.24 | ▼ 09:30 equity $6,966.60 vs yday $7,094.63 (-128.03) | 09:30 open · cash $91.24 (unchanged overnight, no fees) · equity $6,966.60 vs prior close $7,094.63 (-128.03) · 7 name(s) re-marked at the open (per-name table). AVXL×513 yday $1.89 → 09:30 $1.88 -5.13; GLAS×182 yday $5.32 → 09:30 $5.30 -3.64; LQDA×42 yday $27.99 → 09:30 $26.69 -54.60; MKC×21 yday $44.14 → 09:30 $43.52 -13.02; NKE×28 yday $35.15 → 09:30 $32.55 -72.72; PYXS×444 yday $2.19 → 09:30 $2.21 +8.88; QTTB×122 yday $8.25 → 09:30 $8.35 +12.20 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVXL` | 513 | $1.88 | $6.71 | $-74.89 | $1,048.97 | ▼ -74.89 after sell → book $6,959.89; vs 09:30 mark -6.71 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `GLAS` | 182 | $5.30 | $2.58 | $-63.35 | $2,010.99 | ▼ -63.35 after sell → book $6,957.31; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `LQDA` | 42 | $26.69 | $2.14 | $+110.41 | $3,129.83 | ▲ +110.41 after sell → book $6,955.18; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `MKC` | 21 | $43.52 | $2.07 | $-73.01 | $4,041.68 | ▼ -73.01 after sell → book $6,953.11; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `NKE` | 28 | $32.55 | $2.09 | $-85.28 | $4,951.07 | ▼ -85.28 after sell → book $6,951.01; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `PYXS` | 444 | $2.21 | $5.81 | $-55.94 | $5,926.50 | ▼ -55.94 after sell → book $6,945.20; vs 09:30 mark -5.81 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `QTTB` | 122 | $8.35 | $2.39 | $-5.96 | $6,942.81 | ▼ -5.96 after sell → book $6,942.81; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `CTVA` | 70 | $12.38 | $2.20 | — | $6,073.66 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-84.2; leftover $867.85 | join🟢 sector🔴 gen🟢 news🟡 digest🔴 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CMPX` | 923 | $0.94 | $11.45 | — | $5,194.51 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-20.6; leftover $867.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `KDK` | 525 | $1.65 | $6.77 | — | $4,321.48 | — | rank by rsi; rank rsi; list yday_mover; ret5=-39.9; leftover $867.85 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `EVMN` | 119 | $7.25 | $2.35 | — | $3,456.39 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-18.1; leftover $867.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `NKTR` | 18 | $47.11 | $2.04 | — | $2,606.36 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-20.9; leftover $867.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `LUCD` | 1367 | $0.63 | $12.78 | — | $1,725.81 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.2; leftover $867.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `VIVO` | 267 | $3.24 | $3.44 | — | $857.29 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.5; leftover $867.85 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ACRS` | 189 | $4.50 | $2.56 | — | $4.23 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-18.0; leftover $867.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.23 | ▼ close $6,682.63 vs 09:30 $6,966.60 (session -216.59) | 16:00 close · cash $4.23 · equity $6,682.63 vs 09:30 $6,966.60 (-283.97; session marks -216.59) · 8 name(s) marked open→close (per-name table). CTVA×70 09:30 $12.38 → close $11.92 -32.55; CMPX×923 09:30 $0.94 → close $0.86 -76.98; KDK×525 09:30 $1.65 → close $1.64 -5.25; EVMN×119 09:30 $7.25 → close $6.92 -39.27; NKTR×18 09:30 $47.11 → close $43.58 -63.54; LUCD×1367 09:30 $0.63 → close $0.63 -10.25; VIVO×267 09:30 $3.24 → close $3.36 +32.04; ACRS×189 09:30 $4.50 → close $4.39 -20.79 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.23 | ▲ 09:30 equity $6,698.96 vs yday $6,682.63 (+16.33) | 09:30 open · cash $4.23 (unchanged overnight, no fees) · equity $6,698.96 vs prior close $6,682.63 (+16.33) · 8 name(s) re-marked at the open (per-name table). ACRS×189 yday $4.39 → 09:30 $4.34 -9.45; CMPX×923 yday $0.86 → 09:30 $0.90 +39.97; CTVA×70 yday $11.92 → 09:30 $11.81 -7.70; EVMN×119 yday $6.92 → 09:30 $6.86 -7.14; KDK×525 yday $1.64 → 09:30 $1.66 +10.50; LUCD×1367 yday $0.63 → 09:30 $0.62 -5.74; NKTR×18 yday $43.58 → 09:30 $43.50 -1.44; VIVO×267 yday $3.36 → 09:30 $3.35 -2.67 | — |
| 2026-10-05 09:30 ET | **SELL** | `ACRS` | 189 | $4.34 | $2.60 | $-35.40 | $821.89 | ▼ -35.40 after sell → book $6,696.36; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CMPX` | 923 | $0.90 | $11.24 | $-59.70 | $1,641.35 | ▼ -59.70 after sell → book $6,685.12; vs 09:30 mark -11.24 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CTVA` | 70 | $11.81 | $2.22 | $-44.67 | $2,465.83 | ▼ -44.67 after sell → book $6,682.90; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `EVMN` | 119 | $6.86 | $2.38 | $-51.13 | $3,279.79 | ▼ -51.13 after sell → book $6,680.52; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `KDK` | 525 | $1.66 | $6.87 | $-8.39 | $4,144.42 | ▼ -8.39 after sell → book $6,673.65; vs 09:30 mark -6.87 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `LUCD` | 1367 | $0.62 | $12.86 | $-41.63 | $4,983.35 | ▼ -41.63 after sell → book $6,660.80; vs 09:30 mark -12.85 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `NKTR` | 18 | $43.50 | $2.06 | $-69.09 | $5,764.28 | ▼ -69.09 after sell → book $6,658.73; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `VIVO` | 267 | $3.35 | $3.50 | $+22.43 | $6,655.23 | ▲ +22.43 after sell → book $6,655.23; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `IART` | 65 | $12.65 | $2.19 | — | $5,830.80 | — | rank by rsi; rank rsi; list yday_mover; ret5=-21.3; leftover $831.90 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `ECX` | 943 | $0.88 | $11.15 | — | $4,988.02 | — | rank by rsi; rank rsi; list yday_mover; ret5=-21.2; leftover $831.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `LU` | 784 | $1.06 | $10.11 | — | $4,146.87 | — | rank by rsi; rank rsi; list yday_mover; ret5=-17.1; leftover $831.90 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EXE` | 9 | $84.70 | $2.02 | — | $3,382.55 | — | rank by rsi; rank rsi; list flatten; ret5=-1.5; leftover $831.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 81 | $10.26 | $2.23 | — | $2,549.26 | — | rank by rsi; rank rsi; list flatten; ret5=-2.7; leftover $831.90 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SGML` | 89 | $9.33 | $2.26 | — | $1,716.63 | — | rank by rsi; rank rsi; list yday_gainer; ret5=-5.1; leftover $831.90 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VET` | 71 | $11.62 | $2.20 | — | $889.41 | — | rank by rsi; rank rsi; list flatten; ret5=+0.7; leftover $831.90 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 21 | $38.10 | $2.05 | — | $87.26 | — | rank by rsi; rank rsi; list flatten; ret5=-1.0; leftover $831.90 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.26 | ▲ close $6,667.88 vs 09:30 $6,698.96 (session +46.85) | 16:00 close · cash $87.26 · equity $6,667.88 vs 09:30 $6,698.96 (-31.08; session marks +46.85) · 8 name(s) marked open→close (per-name table). IART×65 09:30 $12.65 → close $13.29 +41.60; ECX×943 09:30 $0.88 → close $0.83 -46.49; LU×784 09:30 $1.06 → close $1.03 -23.52; EXE×9 09:30 $84.70 → close $86.43 +15.57; OBE×81 09:30 $10.26 → close $10.50 +19.44; SGML×89 09:30 $9.33 → close $9.56 +20.47; VET×71 09:30 $11.62 → close $11.73 +7.81; RRC×21 09:30 $38.10 → close $38.67 +11.97 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.26 | ▼ 09:30 equity $6,659.11 vs yday $6,667.88 (-8.77) | 09:30 open · cash $87.26 (unchanged overnight, no fees) · equity $6,659.11 vs prior close $6,667.88 (-8.77) · 8 name(s) re-marked at the open (per-name table). ECX×943 yday $0.83 → 09:30 $0.83 +0.00; EXE×9 yday $86.43 → 09:30 $86.38 -0.45; IART×65 yday $13.29 → 09:30 $13.41 +7.80; LU×784 yday $1.03 → 09:30 $1.02 -7.84; OBE×81 yday $10.50 → 09:30 $10.44 -4.86; RRC×21 yday $38.67 → 09:30 $38.71 +0.84; SGML×89 yday $9.56 → 09:30 $9.56 +0.00; VET×71 yday $11.73 → 09:30 $11.67 -4.26 | — |
| 2026-10-06 09:30 ET | **SELL** | `ECX` | 943 | $0.83 | $10.85 | $-68.48 | $861.55 | ▼ -68.48 after sell → book $6,648.26; vs 09:30 mark -10.85 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `EXE` | 9 | $86.38 | $2.04 | $+11.07 | $1,636.94 | ▲ +11.07 after sell → book $6,646.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `IART` | 65 | $13.41 | $2.21 | $+45.01 | $2,506.38 | ▲ +45.01 after sell → book $6,644.02; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `LU` | 784 | $1.02 | $10.25 | $-51.73 | $3,295.81 | ▼ -51.73 after sell → book $6,633.77; vs 09:30 mark -10.25 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `OBE` | 81 | $10.44 | $2.26 | $+10.09 | $4,139.19 | ▲ +10.09 after sell → book $6,631.51; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `RRC` | 21 | $38.71 | $2.07 | $+8.68 | $4,950.03 | ▲ +8.68 after sell → book $6,629.44; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `SGML` | 89 | $9.56 | $2.28 | $+15.93 | $5,798.59 | ▲ +15.93 after sell → book $6,627.16; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `VET` | 71 | $11.67 | $2.22 | $-0.88 | $6,624.93 | ▼ -0.88 after sell → book $6,624.93; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **BUY** | `NXH` | 405 | $2.04 | $5.22 | — | $5,793.51 | — | rank by rsi; rank rsi; list yday_mover; ret5=-33.3; leftover $828.12 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `STZ` | 7 | $113.83 | $2.01 | — | $4,994.69 | — | rank by rsi; rank rsi; list overnight; ret5=+0.3; leftover $828.12 | join🔴 sector🔴 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `RPM` | 8 | $96.25 | $2.01 | — | $4,222.67 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-4.9; leftover $828.12 | join🔴 sector🟢 gen🟡 news🟡 digest🔴 ab🟢 peer🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ZNTL` | 294 | $2.81 | $3.79 | — | $3,392.74 | — | rank by rsi; rank rsi; list probable; ret5=+6.9; leftover $828.12 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `VELO` | 84 | $9.77 | $2.24 | — | $2,569.82 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-12.8; leftover $828.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TOST` | 27 | $30.07 | $2.07 | — | $1,755.86 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=-1.1; leftover $828.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PCOR` | 15 | $54.22 | $2.04 | — | $940.52 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+10.4; leftover $828.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 56 | $14.70 | $2.16 | — | $115.16 | — | rank by rsi; rank rsi; list probable,yday_gainer; ret5=+1.4; leftover $828.12 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.16 | ▼ close $6,401.94 vs 09:30 $6,659.11 (session -201.44) | 16:00 close · cash $115.16 · equity $6,401.94 vs 09:30 $6,659.11 (-257.17; session marks -201.44) · 8 name(s) marked open→close (per-name table). NXH×405 09:30 $2.04 → close $1.83 -85.05; STZ×7 09:30 $113.83 → close $115.67 +12.88; RPM×8 09:30 $96.25 → close $98.31 +16.48; ZNTL×294 09:30 $2.81 → close $2.67 -41.16; VELO×84 09:30 $9.77 → close $9.57 -16.80; TOST×27 09:30 $30.07 → close $30.25 +4.86; PCOR×15 09:30 $54.22 → close $54.39 +2.55; ARCT×56 09:30 $14.70 → close $13.00 -95.20 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.16 | ▼ 09:30 equity $6,308.56 vs yday $6,401.94 (-93.38) | 09:30 open · cash $115.16 (unchanged overnight, no fees) · equity $6,308.56 vs prior close $6,401.94 (-93.38) · 8 name(s) re-marked at the open (per-name table). ARCT×56 yday $13.00 → 09:30 $12.71 -16.24; NXH×405 yday $1.83 → 09:30 $1.81 -8.10; PCOR×15 yday $54.39 → 09:30 $54.04 -5.25; RPM×8 yday $98.31 → 09:30 $98.10 -1.68; STZ×7 yday $115.67 → 09:30 $114.34 -9.31; TOST×27 yday $30.25 → 09:30 $30.13 -3.24; VELO×84 yday $9.57 → 09:30 $9.12 -37.80; ZNTL×294 yday $2.67 → 09:30 $2.63 -11.76 | — |
| 2026-10-07 09:30 ET | **SELL** | `ARCT` | 56 | $12.71 | $2.18 | $-115.78 | $824.74 | ▼ -115.78 after sell → book $6,306.38; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `NXH` | 405 | $1.81 | $5.30 | $-103.68 | $1,552.49 | ▼ -103.68 after sell → book $6,301.08; vs 09:30 mark -5.30 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `PCOR` | 15 | $54.04 | $2.06 | $-6.79 | $2,361.04 | ▼ -6.79 after sell → book $6,299.03; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `RPM` | 8 | $98.10 | $2.03 | $+10.75 | $3,143.80 | ▲ +10.75 after sell → book $6,296.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `TOST` | 27 | $30.13 | $2.09 | $-2.54 | $3,955.22 | ▼ -2.54 after sell → book $6,294.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `VELO` | 84 | $9.12 | $2.27 | $-59.11 | $4,719.03 | ▼ -59.11 after sell → book $6,292.63; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `ZNTL` | 294 | $2.63 | $3.85 | $-60.56 | $5,488.40 | ▼ -60.56 after sell → book $6,288.78; vs 09:30 mark -3.85 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,488.40 | ▲ close $6,317.13 vs 09:30 $6,308.56 (session +28.35) | 16:00 close · cash $5,488.40 · equity $6,317.13 vs 09:30 $6,308.56 (+8.57; session marks +28.35) · 1 name(s) marked open→close (per-name table). STZ×7 09:30 $114.34 → close $118.39 +28.35 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,488.40 | ▲ 09:30 equity $6,317.13 vs yday $6,317.13 (-0.00) | 09:30 open · cash $5,488.40 (unchanged overnight, no fees) · equity $6,317.13 vs prior close $6,317.13 (-0.00) · 1 name(s) re-marked at the open (per-name table). STZ×7 yday $118.39 → 09:30 $118.39 +0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,488.40 | ▲ close $6,317.13 vs 09:30 $6,317.13 (session +0.00) | 16:00 close · cash $5,488.40 · equity $6,317.13 vs 09:30 $6,317.13 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). STZ×7 09:30 $118.39 → close $118.39 +0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,488.40 | ▲ 09:30 equity $6,351.15 vs yday $6,317.13 (+34.02) | 09:30 open · cash $5,488.40 (unchanged overnight, no fees) · equity $6,351.15 vs prior close $6,317.13 (+34.02) · 1 name(s) re-marked at the open (per-name table). STZ×7 yday $118.39 → 09:30 $123.25 +34.02 | — |
| 2026-10-09 09:30 ET | **SELL** | `STZ` | 7 | $123.25 | $2.03 | $+61.90 | $6,349.12 | ▲ +61.90 after sell → book $6,349.12; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 3 sess | join🟢 sector🟢 gen🔴 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `GLAS` | 180 | $4.39 | $2.53 | — | $5,556.39 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-17.1; leftover $793.64 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ANGO` | 71 | $11.06 | $2.20 | — | $4,768.93 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.7; leftover $793.64 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ACRS` | 211 | $3.76 | $2.72 | — | $3,972.84 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=-14.1; leftover $793.64 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `HOVR` | 489 | $1.62 | $6.31 | — | $3,174.36 | — | rank by rsi; rank rsi; list earn_react; ret5=-8.4; leftover $793.64 | join🔴 sector🟡 gen🔴 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `NUAI` | 161 | $4.91 | $2.47 | — | $2,382.18 | — | rank by rsi; rank rsi; list yday_mover; ret5=-24.2; leftover $793.64 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `IPSC` | 543 | $1.46 | $7.00 | — | $1,582.39 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.3; leftover $793.64 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `EQPT` | 50 | $15.81 | $2.14 | — | $789.75 | — | rank by rsi; rank rsi; list yday_mover; ret5=-10.4; leftover $793.64 | join🔴 sector🟡 gen🔴 news🔴 digest🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `CLVT` | 486 | $1.61 | $6.27 | — | $1.02 | — | rank by rsi; rank rsi; list probable,yday_gainer; ret5=+1.9; leftover $793.64 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.02 | ▼ close $6,242.64 vs 09:30 $6,351.15 (session -74.83) | 16:00 close · cash $1.02 · equity $6,242.64 vs 09:30 $6,351.15 (-108.51; session marks -74.83) · 8 name(s) marked open→close (per-name table). GLAS×180 09:30 $4.39 → close $4.26 -23.40; ANGO×71 09:30 $11.06 → close $11.52 +32.66; ACRS×211 09:30 $3.76 → close $3.83 +14.77; HOVR×489 09:30 $1.62 → close $1.55 -34.23; NUAI×161 09:30 $4.91 → close $4.70 -33.81; IPSC×543 09:30 $1.46 → close $1.47 +5.43; EQPT×50 09:30 $15.81 → close $15.09 -36.25; CLVT×486 09:30 $1.61 → close $1.61 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `SSTK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BYND` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `XLAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KRMN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SIGA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MYGN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PVH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRLN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TTI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AIAI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EMR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `EYPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BRZE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ANAB` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `EQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `EU` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARQQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CNTB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `XENE` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `KDK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKV` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PUMP` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-24 | `LXEO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ALKT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CCOI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BYND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PBLS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FUL` | 24 | 2026-09-23 @ $50.51 | rank by rsi; rank rsi; list overnight; 🔵; ret5=-2.6; leftover $1265.80 |
