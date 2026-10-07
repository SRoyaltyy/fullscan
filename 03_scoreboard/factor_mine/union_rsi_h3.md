# Factor mine action — `union_rsi_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `rsi` · size `leftover` · sell `list` · S-boost `none` · rank by rsi

Cash book **-20.61%** ($7,939) · signal-only (no cash/fees) was -34.58%. Starts YES **0/30**. Fills 231 · skips 269 · realized $-1699.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `rsi` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,271.38.

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
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 1 | $7.29 | $0.08 | — | $94.09 | — | rank by rsi; rank rsi; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 1 | $10.83 | $0.11 | — | $83.15 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 1 | $10.06 | $0.10 | — | $72.99 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 4 | $2.69 | $0.12 | — | $62.11 | — | rank by rsi; rank rsi; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `STUB` | 1 | $7.66 | $0.08 | — | $54.37 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-13.5; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `ENHA` | 5 | $2.31 | $0.13 | — | $42.69 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=-5.3; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $33.58 | — | rank by rsi; rank rsi; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `STNE` | 1 | $9.89 | $0.10 | — | $23.59 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=-7.7; leftover $12.68 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.59 | ▲ close $10,524.62 vs 09:30 $10,273.77 (session +251.67) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.59 | ▼ 09:30 equity $10,508.51 vs yday $10,524.62 (-16.11) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 1 | $2.62 | $0.03 | — | $20.94 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $3.37 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 2 | $1.62 | $0.04 | — | $17.66 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $3.37 | — |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 1 | $2.50 | $0.03 | — | $15.13 | — | rank by rsi; rank rsi; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $3.37 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.13 | ▲ close $10,633.81 vs 09:30 $10,508.51 (session +125.40) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.13 | ▼ 09:30 equity $10,494.51 vs yday $10,633.81 (-139.30) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $1,753.98 | ▲ +471.89 after sell → book $10,474.34; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $2,951.91 | ▼ -0.12 after sell → book $10,472.27; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,181.82 | ▼ -14.65 after sell → book $10,470.18; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $5,349.39 | ▼ -83.63 after sell → book $10,468.05; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,689.01 | ▲ +97.12 after sell → book $10,465.71; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $7,863.04 | ▼ -69.50 after sell → book $10,463.62; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $9,138.78 | ▲ +41.02 after sell → book $10,461.44; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $10,379.18 | ▲ +23.38 after sell → book $10,459.36; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,379.18 | ▼ close $10,458.55 vs 09:30 $10,494.51 (session -0.81) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,379.18 | ▲ 09:30 equity $10,459.06 vs yday $10,458.55 (+0.51) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 1 | $6.74 | $0.09 | $-0.72 | $10,385.83 | ▼ -0.72 after sell → book $10,458.97; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 1 | $10.85 | $0.13 | $-0.22 | $10,396.55 | ▼ -0.22 after sell → book $10,458.83; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `YSS` | 1 | $10.32 | $0.13 | $+0.03 | $10,406.74 | ▲ +0.03 after sell → book $10,458.71; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 4 | $2.56 | $0.13 | $-0.77 | $10,416.85 | ▼ -0.77 after sell → book $10,458.57; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `STUB` | 1 | $7.12 | $0.09 | $-0.71 | $10,423.87 | ▼ -0.71 after sell → book $10,458.48; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ENHA` | 5 | $1.70 | $0.12 | $-3.30 | $10,432.25 | ▼ -3.30 after sell → book $10,458.36; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,441.05 | ▼ -0.31 after sell → book $10,458.25; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `STNE` | 1 | $9.26 | $0.12 | $-0.85 | $10,450.20 | ▼ -0.85 after sell → book $10,458.13; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,450.20 | ▲ close $10,458.73 vs 09:30 $10,459.06 (session +0.60) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,450.20 | ▲ 09:30 equity $10,458.75 vs yday $10,458.73 (+0.02) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `KLC` | 1 | $2.88 | $0.05 | $+0.18 | $10,453.03 | ▲ +0.18 after sell → book $10,458.70; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 2 | $1.55 | $0.06 | $-0.24 | $10,456.07 | ▼ -0.24 after sell → book $10,458.64; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CSAN` | 1 | $2.57 | $0.05 | $-0.01 | $10,458.59 | ▼ -0.01 after sell → book $10,458.59; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 38 | $33.61 | $2.10 | — | $9,179.31 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-17.4; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 295 | $4.43 | $3.81 | — | $7,868.65 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-23.1; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 61 | $21.40 | $2.17 | — | $6,561.08 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.2; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3693 | $0.35 | $24.15 | — | $5,229.60 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-29.4; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 5 | $229.55 | $2.00 | — | $4,079.85 | — | rank by rsi; rank rsi; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 30 | $42.60 | $2.08 | — | $2,799.77 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-4.6; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `DE` | 2 | $611.12 | $2.00 | — | $1,575.53 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-6.3; leftover $1307.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4357 | $0.30 | $26.14 | — | $242.29 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-3.2; leftover $1307.32 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.29 | ▲ close $10,470.64 vs 09:30 $10,458.75 (session +76.51) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.29 | ▲ 09:30 equity $10,616.16 vs yday $10,470.64 (+145.52) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `EYPT` | 7 | $5.48 | $0.40 | — | $203.53 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-59.9; leftover $40.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 23 | $1.71 | $0.46 | — | $163.73 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $40.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `EOSE` | 11 | $3.54 | $0.42 | — | $124.37 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-16.8; leftover $40.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 5 | $6.81 | $0.36 | — | $89.97 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=+62.5; leftover $40.38 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.97 | ▼ close $10,605.83 vs 09:30 $10,616.16 (session -8.68) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.97 | ▼ 09:30 equity $10,552.44 vs yday $10,605.83 (-53.39) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.97 | ▲ close $10,557.26 vs 09:30 $10,552.44 (session +4.82) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.97 | ▲ 09:30 equity $10,564.70 vs yday $10,557.26 (+7.44) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `LZB` | 38 | $32.33 | $2.12 | $-52.87 | $1,316.38 | ▼ -52.87 after sell → book $10,562.58; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 295 | $4.42 | $3.86 | $-10.62 | $2,616.42 | ▼ -10.62 after sell → book $10,558.71; vs 09:30 mark -3.87 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WYFI` | 61 | $20.90 | $2.19 | $-34.87 | $3,889.12 | ▼ -34.87 after sell → book $10,556.52; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SAFX` | 3693 | $0.36 | $24.92 | $-34.30 | $5,186.29 | ▼ -34.30 after sell → book $10,531.59; vs 09:30 mark -24.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ROST` | 5 | $241.50 | $2.02 | $+55.72 | $6,391.77 | ▲ +55.72 after sell → book $10,529.57; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BKE` | 30 | $44.50 | $2.10 | $+52.82 | $7,724.67 | ▲ +52.82 after sell → book $10,527.47; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DE` | 2 | $652.07 | $2.02 | $+77.89 | $9,026.79 | ▲ +77.89 after sell → book $10,525.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 4357 | $0.31 | $27.31 | $-9.88 | $10,350.15 | ▼ -9.88 after sell → book $10,498.14; vs 09:30 mark -27.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 249 | $5.93 | $3.21 | — | $8,870.37 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-17.5; leftover $1478.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 133 | $11.09 | $2.39 | — | $7,393.01 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-8.0; leftover $1478.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 10 | $142.36 | $2.02 | — | $5,967.39 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-8.6; leftover $1478.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `RGNX` | 181 | $8.14 | $2.53 | — | $4,491.52 | — | rank by rsi; rank rsi; list yday_mover; ret5=-28.9; leftover $1478.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $175.01 | $2.01 | — | $3,089.42 | — | rank by rsi; rank rsi; list earn_react; ret5=-7.0; leftover $1478.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $1,916.76 | — | rank by rsi; rank rsi; list overnight; ret5=-12.0; leftover $1478.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `INDP` | 1285 | $1.15 | $16.58 | — | $422.44 | — | rank by rsi; rank rsi; list yday_mover; ret5=+16.0; leftover $1478.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $422.44 | ▼ close $10,376.07 vs 09:30 $10,564.70 (session -91.32) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $422.44 | ▼ 09:30 equity $10,071.83 vs yday $10,376.07 (-304.24) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `EYPT` | 7 | $5.03 | $0.39 | $-3.95 | $457.25 | ▼ -3.95 after sell → book $10,071.44; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ENHA` | 23 | $1.63 | $0.46 | $-2.77 | $494.28 | ▼ -2.77 after sell → book $10,070.97; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOSE` | 11 | $3.50 | $0.44 | $-1.35 | $532.29 | ▼ -1.35 after sell → book $10,070.54; vs 09:30 mark -0.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 4 | $27.59 | $1.12 | — | $420.81 | — | rank by rsi; rank rsi; list yday_gainer; ret5=+2.0; leftover $133.07 | — |
| 2026-08-26 09:30 ET | **BUY** | `OSUR` | 35 | $3.77 | $1.42 | — | $287.44 | — | rank by rsi; rank rsi; list yday_mover; ret5=+0.8; leftover $133.07 | — |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 8 | $16.22 | $1.32 | — | $156.35 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-2.5; leftover $133.07 | — |
| 2026-08-26 09:30 ET | **BUY** | `OKTA` | 1 | $128.00 | $1.28 | — | $27.07 | — | rank by rsi; rank rsi; list overnight; ret5=-9.2; leftover $133.07 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.07 | ▲ close $10,119.04 vs 09:30 $10,071.83 (session +53.65) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.07 | ▲ 09:30 equity $10,202.78 vs yday $10,119.04 (+83.74) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 5 | $9.19 | $0.49 | $+11.05 | $72.53 | ▲ +11.05 after sell → book $10,202.29; vs 09:30 mark -0.49 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 5 | $4.57 | $0.24 | — | $49.43 | — | rank by rsi; rank rsi; list mover_buy; 🔵; ret5=+1.1; leftover $24.18 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 1 | $14.96 | $0.15 | — | $34.32 | — | rank by rsi; rank rsi; list overnight; ret5=+3.0; leftover $24.18 | — |
| 2026-08-27 09:30 ET | **BUY** | `MNSO` | 2 | $10.89 | $0.22 | — | $12.32 | — | rank by rsi; rank rsi; list overnight; ret5=+2.7; leftover $24.18 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.32 | ▲ close $10,252.88 vs 09:30 $10,202.78 (session +51.21) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.32 | ▲ 09:30 equity $10,326.08 vs yday $10,252.88 (+73.20) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 249 | $6.27 | $3.27 | $+78.18 | $1,570.28 | ▲ +78.18 after sell → book $10,322.81; vs 09:30 mark -3.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 10 | $132.80 | $2.04 | $-99.66 | $2,896.24 | ▼ -99.66 after sell → book $10,320.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RGNX` | 181 | $10.00 | $2.58 | $+331.55 | $4,703.66 | ▲ +331.55 after sell → book $10,318.19; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 8 | $172.76 | $2.04 | $-22.05 | $6,083.71 | ▼ -22.05 after sell → book $10,316.16; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INDP` | 1285 | $1.16 | $16.80 | $-20.53 | $7,557.51 | ▼ -20.53 after sell → book $10,299.36; vs 09:30 mark -16.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 1303 | $1.16 | $16.81 | — | $6,029.22 | — | rank by rsi; rank rsi; list overnight; ret5=-13.8; leftover $1511.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 389 | $3.88 | $5.02 | — | $4,514.88 | — | rank by rsi; rank rsi; list earn_react; ret5=-8.6; leftover $1511.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 113 | $13.37 | $2.33 | — | $3,001.74 | — | rank by rsi; rank rsi; list yday_mover; ret5=-14.9; leftover $1511.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 16 | $91.75 | $2.04 | — | $1,531.70 | — | rank by rsi; rank rsi; list yday_mover; ret5=-13.2; leftover $1511.50 | — |
| 2026-08-28 09:30 ET | **BUY** | `QBTS` | 86 | $17.56 | $2.25 | — | $19.29 | — | rank by rsi; rank rsi; list yday_mover; ret5=-4.8; leftover $1511.50 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.29 | ▼ close $9,958.20 vs 09:30 $10,326.08 (session -312.71) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.29 | ▼ 09:30 equity $9,681.68 vs yday $9,958.20 (-276.52) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 133 | $8.70 | $2.42 | $-322.68 | $1,173.97 | ▼ -322.68 after sell → book $9,679.26; vs 09:30 mark -2.42 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 3 | $298.01 | $2.02 | $-280.65 | $2,065.98 | ▼ -280.65 after sell → book $9,677.24; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MAIR` | 4 | $26.28 | $1.08 | $-7.44 | $2,170.02 | ▼ -7.44 after sell → book $9,676.16; vs 09:30 mark -1.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `OSUR` | 35 | $3.61 | $1.39 | $-8.41 | $2,294.98 | ▼ -8.41 after sell → book $9,674.77; vs 09:30 mark -1.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BILI` | 8 | $16.53 | $1.37 | $-0.21 | $2,425.86 | ▼ -0.21 after sell → book $9,673.41; vs 09:30 mark -1.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `OKTA` | 1 | $164.83 | $1.67 | $+33.88 | $2,589.01 | ▲ +33.88 after sell → book $9,671.73; vs 09:30 mark -1.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,589.01 | ▲ close $9,797.66 vs 09:30 $9,681.68 (session +125.93) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,589.01 | ▼ 09:30 equity $9,582.88 vs yday $9,797.66 (-214.78) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 5 | $4.57 | $0.26 | $-0.51 | $2,611.60 | ▼ -0.51 after sell → book $9,582.61; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBAR` | 1 | $14.82 | $0.17 | $-0.46 | $2,626.25 | ▼ -0.46 after sell → book $9,582.44; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MNSO` | 2 | $9.39 | $0.21 | $-3.44 | $2,644.82 | ▼ -3.44 after sell → book $9,582.23; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,644.82 | ▼ close $9,330.83 vs 09:30 $9,582.88 (session -251.39) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,644.82 | ▲ 09:30 equity $9,332.46 vs yday $9,330.83 (+1.63) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `LX` | 1303 | $0.91 | $15.94 | $-363.71 | $3,809.39 | ▼ -363.71 after sell → book $9,316.52; vs 09:30 mark -15.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 389 | $3.32 | $5.09 | $-227.95 | $5,095.78 | ▼ -227.95 after sell → book $9,311.42; vs 09:30 mark -5.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `JKS` | 113 | $12.45 | $2.36 | $-108.65 | $6,500.27 | ▼ -108.65 after sell → book $9,309.07; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SYRE` | 16 | $88.05 | $2.06 | $-63.30 | $7,907.01 | ▼ -63.30 after sell → book $9,307.01; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `QBTS` | 86 | $16.28 | $2.27 | $-114.69 | $9,304.73 | ▼ -114.69 after sell → book $9,304.73; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,304.73 | ▲ close $9,304.73 vs 09:30 $9,332.46 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,304.73 | ▲ 09:30 equity $9,304.73 vs yday $9,304.73 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1406 | $0.83 | $15.85 | — | $8,126.13 | — | rank by rsi; rank rsi; list yday_mover; ret5=-30.4; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 112 | $10.38 | $2.33 | — | $6,961.80 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-56.2; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1817 | $0.64 | $17.08 | — | $5,781.84 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.0; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 52 | $22.00 | $2.15 | — | $4,635.69 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-17.3; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 159 | $7.31 | $2.47 | — | $3,470.94 | — | rank by rsi; rank rsi; list yday_gainer; 🔵; ret5=+18.5; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 91 | $12.78 | $2.26 | — | $2,305.69 | — | rank by rsi; rank rsi; list overnight; 🔵; ret5=-4.4; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 409 | $2.84 | $5.28 | — | $1,138.86 | — | rank by rsi; rank rsi; list yday_mover; ret5=-26.9; leftover $1163.09 | — |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 57 | $19.86 | $2.16 | — | $4.68 | — | rank by rsi; rank rsi; list overnight; ret5=-5.5; leftover $1163.09 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.68 | ▼ close $9,095.94 vs 09:30 $9,304.73 (session -159.23) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.68 | ▲ 09:30 equity $9,328.74 vs yday $9,095.94 (+232.80) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.68 | ▼ close $9,231.98 vs 09:30 $9,328.74 (session -96.76) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.68 | ▼ 09:30 equity $9,188.05 vs yday $9,231.98 (-43.93) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.68 | ▼ close $9,066.69 vs 09:30 $9,188.05 (session -121.36) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.68 | ▼ 09:30 equity $9,021.10 vs yday $9,066.69 (-45.59) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `LX` | 1406 | $0.83 | $16.20 | $-20.80 | $1,162.49 | ▼ -20.80 after sell → book $9,004.90; vs 09:30 mark -16.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 112 | $10.49 | $2.35 | $+8.20 | $2,335.01 | ▲ +8.20 after sell → book $9,002.55; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 1817 | $0.59 | $16.56 | $-117.22 | $3,397.75 | ▼ -117.22 after sell → book $8,985.99; vs 09:30 mark -16.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OSW` | 52 | $21.86 | $2.17 | $-11.59 | $4,532.31 | ▼ -11.59 after sell → book $8,983.83; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 159 | $7.27 | $2.50 | $-11.33 | $5,685.73 | ▼ -11.33 after sell → book $8,981.32; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SWBI` | 91 | $13.12 | $2.29 | $+26.39 | $6,877.37 | ▲ +26.39 after sell → book $8,979.04; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FJET` | 409 | $2.63 | $5.35 | $-96.52 | $7,947.68 | ▼ -96.52 after sell → book $8,973.68; vs 09:30 mark -5.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PL` | 57 | $18.00 | $2.18 | $-110.36 | $8,971.50 | ▼ -110.36 after sell → book $8,971.50; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.50 | ▲ close $8,971.50 vs 09:30 $9,021.10 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,971.50 | ▲ 09:30 equity $8,971.50 vs yday $8,971.50 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.50 | ▲ close $8,971.50 vs 09:30 $8,971.50 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,971.50 | ▲ 09:30 equity $8,971.50 vs yday $8,971.50 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 20 | $54.66 | $2.05 | — | $7,876.25 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.3; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 318 | $3.52 | $4.10 | — | $6,752.79 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-19.2; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 54 | $20.61 | $2.15 | — | $5,637.70 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.7; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `SION` | 143 | $7.79 | $2.42 | — | $4,521.31 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+4.2; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 8 | $135.71 | $2.01 | — | $3,433.61 | — | rank by rsi; rank rsi; list earn_react; ret5=-9.2; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 76 | $14.71 | $2.22 | — | $2,313.44 | — | rank by rsi; rank rsi; list yday_mover; ret5=-12.8; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `AXGN` | 26 | $42.48 | $2.07 | — | $1,206.89 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-13.4; leftover $1121.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `SLBT` | 536 | $2.09 | $6.91 | — | $79.73 | — | rank by rsi; rank rsi; list yday_mover; ret5=-36.6; leftover $1121.44 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.73 | ▲ close $8,958.86 vs 09:30 $8,971.50 (session +11.30) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.73 | ▼ 09:30 equity $8,901.21 vs yday $8,958.86 (-57.65) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.73 | ▲ close $9,211.07 vs 09:30 $8,901.21 (session +309.86) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.73 | ▼ 09:30 equity $9,120.76 vs yday $9,211.07 (-90.31) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.73 | ▼ close $8,900.21 vs 09:30 $9,120.76 (session -220.55) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.73 | ▲ 09:30 equity $8,962.72 vs yday $8,900.21 (+62.51) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `COO` | 20 | $54.37 | $2.07 | $-9.92 | $1,165.06 | ▼ -9.92 after sell → book $8,960.65; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RWT` | 318 | $3.98 | $4.17 | $+138.01 | $2,426.54 | ▲ +138.01 after sell → book $8,956.49; vs 09:30 mark -4.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NAVN` | 54 | $22.50 | $2.17 | $+97.74 | $3,639.37 | ▲ +97.74 after sell → book $8,954.32; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SION` | 143 | $6.95 | $2.45 | $-124.99 | $4,630.76 | ▼ -124.99 after sell → book $8,951.86; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 8 | $125.55 | $2.03 | $-85.33 | $5,633.13 | ▼ -85.33 after sell → book $8,949.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AEO` | 76 | $14.82 | $2.24 | $+3.90 | $6,757.21 | ▲ +3.90 after sell → book $8,947.59; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AXGN` | 26 | $44.87 | $2.09 | $+57.98 | $7,921.74 | ▲ +57.98 after sell → book $8,945.50; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SLBT` | 536 | $1.91 | $7.01 | $-110.41 | $8,938.49 | ▼ -110.41 after sell → book $8,938.49; vs 09:30 mark -7.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1323 | $0.84 | $15.14 | — | $7,806.74 | — | rank by rsi; rank rsi; list yday_mover; ret5=-33.5; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 108 | $10.30 | $2.31 | — | $6,692.03 | — | rank by rsi; rank rsi; list yday_mover; ret5=-23.0; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 162 | $6.86 | $2.48 | — | $5,578.23 | — | rank by rsi; rank rsi; list yday_mover; ret5=-34.7; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6983 | $0.16 | $32.12 | — | $4,428.83 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-23.8; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 305 | $3.66 | $3.93 | — | $3,308.59 | — | rank by rsi; rank rsi; list yday_mover; ret5=-19.7; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 27 | $40.93 | $2.07 | — | $2,201.41 | — | rank by rsi; rank rsi; list earn_react; ret5=-3.1; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 410 | $2.72 | $5.29 | — | $1,080.92 | — | rank by rsi; rank rsi; list yday_mover; ret5=-26.3; leftover $1117.31 | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 459 | $2.34 | $5.92 | — | $0.94 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.2; leftover $1117.31 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.94 | ▼ close $8,725.98 vs 09:30 $8,962.72 (session -143.24) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.94 | ▲ 09:30 equity $8,751.88 vs yday $8,725.98 (+25.90) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.94 | ▲ close $8,854.92 vs 09:30 $8,751.88 (session +103.04) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.94 | ▲ 09:30 equity $8,884.50 vs yday $8,854.92 (+29.58) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.94 | ▼ close $8,762.07 vs 09:30 $8,884.50 (session -122.42) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.94 | ▲ 09:30 equity $8,875.00 vs yday $8,762.07 (+112.93) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `NMRA` | 1323 | $0.74 | $13.95 | $-170.65 | $962.04 | ▼ -170.65 after sell → book $8,861.05; vs 09:30 mark -13.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ALHC` | 108 | $8.33 | $2.34 | $-217.42 | $1,859.34 | ▼ -217.42 after sell → book $8,858.71; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRBP` | 162 | $7.51 | $2.51 | $+100.31 | $3,073.45 | ▲ +100.31 after sell → book $8,856.20; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `DVLT` | 6983 | $0.16 | $33.29 | $-65.41 | $4,157.44 | ▼ -65.41 after sell → book $8,822.91; vs 09:30 mark -33.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 305 | $3.87 | $4.00 | $+56.12 | $5,333.79 | ▲ +56.12 after sell → book $8,818.91; vs 09:30 mark -4.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 27 | $41.00 | $2.09 | $-2.27 | $6,438.70 | ▼ -2.27 after sell → book $8,816.82; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CTMX` | 410 | $2.80 | $5.37 | $+22.14 | $7,581.34 | ▲ +22.14 after sell → book $8,811.46; vs 09:30 mark -5.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ZSQR` | 459 | $2.68 | $6.01 | $+144.13 | $8,805.45 | ▲ +144.13 after sell → book $8,805.45; vs 09:30 mark -6.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 27 | $40.00 | $2.07 | — | $7,723.38 | — | rank by rsi; rank rsi; list yday_mover; ret5=-32.2; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 183 | $6.00 | $2.54 | — | $6,622.84 | — | rank by rsi; rank rsi; list yday_mover; ret5=-24.1; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 16 | $68.39 | $2.04 | — | $5,526.56 | — | rank by rsi; rank rsi; list overnight; ret5=-7.0; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `KDK` | 321 | $3.42 | $4.14 | — | $4,424.60 | — | rank by rsi; rank rsi; list yday_mover; ret5=-12.3; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 52 | $20.85 | $2.15 | — | $3,338.25 | — | rank by rsi; rank rsi; list overnight; ret5=-2.5; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 10 | $105.72 | $2.02 | — | $2,279.03 | — | rank by rsi; rank rsi; list overnight; ret5=-11.5; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKV` | 48 | $22.68 | $2.13 | — | $1,188.26 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+10.8; leftover $1100.68 | — |
| 2026-09-21 09:30 ET | **BUY** | `PUMP` | 105 | $10.40 | $2.31 | — | $93.95 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+5.9; leftover $1100.68 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.95 | ▼ close $8,672.93 vs 09:30 $8,875.00 (session -113.12) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.95 | ▼ 09:30 equity $8,671.10 vs yday $8,672.93 (-1.83) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.95 | ▲ close $8,676.59 vs 09:30 $8,671.10 (session +5.49) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.95 | ▼ 09:30 equity $8,619.70 vs yday $8,676.59 (-56.89) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `XNDU` | 1 | $5.99 | $0.06 | — | $87.90 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.9; leftover $11.74 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 9 | $1.22 | $0.14 | — | $76.78 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-33.0; leftover $11.74 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 15 | $0.77 | $0.16 | — | $65.10 | — | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $11.74 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.10 | ▼ close $8,327.05 vs 09:30 $8,619.70 (session -292.30) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.10 | ▼ 09:30 equity $8,317.84 vs yday $8,327.05 (-9.21) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `XENE` | 27 | $36.50 | $2.09 | $-98.66 | $1,048.51 | ▼ -98.66 after sell → book $8,315.75; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SION` | 183 | $5.50 | $2.58 | $-96.62 | $2,052.43 | ▼ -96.62 after sell → book $8,313.17; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `THO` | 16 | $72.58 | $2.06 | $+62.94 | $3,211.66 | ▲ +62.94 after sell → book $8,311.12; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `KDK` | 321 | $2.96 | $4.20 | $-156.01 | $4,157.61 | ▼ -156.01 after sell → book $8,306.91; vs 09:30 mark -4.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MLKN` | 52 | $19.96 | $2.17 | $-50.59 | $5,193.37 | ▼ -50.59 after sell → book $8,304.75; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABVX` | 10 | $92.97 | $2.04 | $-131.56 | $6,121.03 | ▼ -131.56 after sell → book $8,302.71; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKV` | 48 | $23.28 | $2.15 | $+24.51 | $7,236.31 | ▲ +24.51 after sell → book $8,300.55; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `PUMP` | 105 | $9.88 | $2.33 | $-59.24 | $8,271.38 | ▼ -59.24 after sell → book $8,298.22; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,271.38 | ▼ close $8,297.24 vs 09:30 $8,317.84 (session -0.97) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,018.46 | ▲ 09:30 equity $8,390.26 vs yday $8,389.16 (+1.10) | 09:30 open · cash $8,018.46 (unchanged overnight, no fees) · equity $8,390.26 vs prior close $8,389.16 (+1.10) · 8 name(s) re-marked at the open (per-name table). CBRL×2 yday $51.84 → 09:30 $52.39 +1.10; CMPX×26 yday $1.13 → 09:30 $1.13 +0.00; EVER×1 yday $18.06 → 09:30 $18.06 +0.00; GIS×2 yday $34.83 → 09:30 $34.83 +0.00; KBH×1 yday $47.65 → 09:30 $47.65 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; NMRA×41 yday $0.70 → 09:30 $0.70 +0.00; XNDU×5 yday $5.10 → 09:30 $5.10 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `CBRL` | 2 | $52.39 | $1.07 | $+11.79 | $8,122.17 | ▲ +11.79 after sell → book $8,389.19; vs 09:30 mark -1.07 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 45 | $22.21 | $2.12 | — | $7,120.59 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-19.2; leftover $1015.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GEN` | 44 | $22.91 | $2.12 | — | $6,110.43 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-23.6; leftover $1015.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 461 | $2.20 | $5.95 | — | $5,090.28 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.1; leftover $1015.27 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NEOV` | 424 | $2.39 | $5.47 | — | $4,071.45 | — | rank by rsi; rank rsi; list yday_mover; ret5=-31.4; leftover $1015.27 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `LRMR` | 305 | $3.32 | $3.93 | — | $3,054.92 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-12.6; leftover $1015.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RZLT` | 254 | $3.99 | $3.28 | — | $2,038.18 | — | rank by rsi; rank rsi; list earn_react; ret5=-0.3; leftover $1015.27 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SGMT` | 111 | $9.11 | $2.32 | — | $1,024.65 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1015.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SMWB` | 135 | $7.50 | $2.40 | — | $9.75 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-9.7; leftover $1015.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.75 | ▼ close $8,058.09 vs 09:30 $8,390.26 (session -303.51) | 16:00 close · cash $9.75 · equity $8,058.09 vs 09:30 $8,390.26 (-332.17; session marks -303.51) · 15 name(s) marked open→close (per-name table). CMPX×26 09:30 $1.14 → close $1.14 -0.00; EVER×1 09:30 $18.06 → close $18.06 -0.00; GIS×2 09:30 $34.83 → close $34.83 +0.00; KBH×1 09:30 $47.65 → close $47.65 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; NMRA×41 09:30 $0.70 → close $0.70 +0.00; XNDU×5 09:30 $5.10 → close $5.10 -0.00; ACAD×45 09:30 $22.21 → close $20.68 -68.85; GEN×44 09:30 $22.91 → close $21.62 -56.76; SFIX×461 09:30 $2.20 → close $2.15 -20.75; NEOV×424 09:30 $2.39 → close $2.19 -84.80; LRMR×305 09:30 $3.32 → close $3.08 -74.72; RZLT×254 09:30 $3.99 → close $3.90 -22.86; SGMT×111 09:30 $9.11 → close $9.24 +14.43; SMWB×135 09:30 $7.50 → close $7.58 +10.80 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.75 | ▼ 09:30 equity $8,034.34 vs yday $8,058.09 (-23.75) | 09:30 open · cash $9.75 (unchanged overnight, no fees) · equity $8,034.34 vs prior close $8,058.09 (-23.75) · 15 name(s) re-marked at the open (per-name table). ACAD×45 yday $20.68 → 09:30 $20.53 -6.75; CMPX×26 yday $1.13 → 09:30 $1.14 +0.13; EVER×1 yday $18.06 → 09:30 $18.53 +0.47; GEN×44 yday $21.62 → 09:30 $21.44 -7.92; GIS×2 yday $34.83 → 09:30 $33.60 -2.46; KBH×1 yday $47.65 → 09:30 $47.58 -0.07; LRMR×305 yday $3.08 → 09:30 $3.23 +47.52; MKC×1 yday $47.82 → 09:30 $47.83 +0.01; NEOV×424 yday $2.19 → 09:30 $2.20 +4.24; NMRA×41 yday $0.70 → 09:30 $0.65 -2.26; RZLT×254 yday $3.90 → 09:30 $3.85 -12.70; SFIX×461 yday $2.15 → 09:30 $2.12 -15.21; SGMT×111 yday $9.24 → 09:30 $9.15 -9.99; SMWB×135 yday $7.58 → 09:30 $7.45 -17.55; XNDU×5 yday $5.10 → 09:30 $4.86 -1.20 | — |
| 2026-09-28 09:30 ET | **SELL** | `CMPX` | 26 | $1.14 | $0.39 | $-2.87 | $39.00 | ▼ -2.87 after sell → book $8,033.95; vs 09:30 mark -0.39 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `EVER` | 1 | $18.53 | $0.21 | $-1.34 | $57.32 | ▼ -1.34 after sell → book $8,033.74; vs 09:30 mark -0.21 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `GIS` | 2 | $33.60 | $0.70 | $-6.14 | $123.82 | ▼ -6.14 after sell → book $8,033.04; vs 09:30 mark -0.70 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `KBH` | 1 | $47.58 | $0.50 | $-2.81 | $170.90 | ▼ -2.81 after sell → book $8,032.54; vs 09:30 mark -0.50 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `MKC` | 1 | $47.83 | $0.50 | $-2.74 | $218.23 | ▼ -2.74 after sell → book $8,032.04; vs 09:30 mark -0.50 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `NMRA` | 41 | $0.65 | $0.41 | $-5.77 | $244.39 | ▼ -5.77 after sell → book $8,031.63; vs 09:30 mark -0.41 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `XNDU` | 5 | $4.86 | $0.28 | $-6.24 | $268.41 | ▼ -6.24 after sell → book $8,031.36; vs 09:30 mark -0.27 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.41 | ▲ close $8,371.61 vs 09:30 $8,034.34 (session +340.26) | 16:00 close · cash $268.41 · equity $8,371.61 vs 09:30 $8,034.34 (+337.27; session marks +340.26) · 8 name(s) marked open→close (per-name table). ACAD×45 09:30 $20.53 → close $20.53 +0.00; GEN×44 09:30 $21.44 → close $20.85 -25.96; LRMR×305 09:30 $3.23 → close $3.17 -18.54; NEOV×424 09:30 $2.20 → close $2.28 +33.92; RZLT×254 09:30 $3.85 → close $4.18 +83.82; SFIX×461 09:30 $2.12 → close $2.47 +160.43; SGMT×111 09:30 $9.15 → close $9.94 +87.69; SMWB×135 09:30 $7.45 → close $7.59 +18.90 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.41 | ▲ 09:30 equity $8,466.39 vs yday $8,371.61 (+94.78) | 09:30 open · cash $268.41 (unchanged overnight, no fees) · equity $8,466.39 vs prior close $8,371.61 (+94.78) · 8 name(s) re-marked at the open (per-name table). ACAD×45 yday $20.53 → 09:30 $20.37 -7.20; GEN×44 yday $20.85 → 09:30 $21.20 +15.40; LRMR×305 yday $3.17 → 09:30 $3.13 -12.20; NEOV×424 yday $2.28 → 09:30 $2.29 +4.24; RZLT×254 yday $4.18 → 09:30 $4.54 +92.71; SFIX×461 yday $2.47 → 09:30 $2.47 +0.00; SGMT×111 yday $9.94 → 09:30 $9.92 -2.22; SMWB×135 yday $7.59 → 09:30 $7.62 +4.05 | — |
| 2026-09-29 09:30 ET | **BUY** | `OCUL` | 4 | $7.76 | $0.32 | — | $237.05 | — | rank by rsi; rank rsi; list yday_mover; ret5=-23.4; leftover $33.55 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SWMR` | 1 | $18.59 | $0.19 | — | $218.27 | — | rank by rsi; rank rsi; list yday_mover; ret5=-22.5; leftover $33.55 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TDTH` | 37 | $0.89 | $0.44 | — | $184.90 | — | rank by rsi; rank rsi; list yday_mover; ret5=-36.6; leftover $33.55 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MRLN` | 19 | $1.75 | $0.39 | — | $151.26 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-5.9; leftover $33.55 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AUR` | 6 | $5.32 | $0.34 | — | $119.00 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-18.7; leftover $33.55 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `UEC` | 3 | $9.91 | $0.31 | — | $88.97 | — | rank by rsi; rank rsi; list earn_react; ret5=-8.6; leftover $33.55 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.97 | ▼ close $8,413.96 vs 09:30 $8,466.39 (session -50.45) | 16:00 close · cash $88.97 · equity $8,413.96 vs 09:30 $8,466.39 (-52.43; session marks -50.45) · 14 name(s) marked open→close (per-name table). ACAD×45 09:30 $20.37 → close $20.22 -6.75; GEN×44 09:30 $21.20 → close $20.85 -15.40; LRMR×305 09:30 $3.13 → close $3.14 +3.05; NEOV×424 09:30 $2.29 → close $2.54 +106.00; RZLT×254 09:30 $4.54 → close $4.09 -115.57; SFIX×461 09:30 $2.47 → close $2.53 +27.66; SGMT×111 09:30 $9.92 → close $9.33 -65.49; SMWB×135 09:30 $7.62 → close $7.69 +9.45; OCUL×4 09:30 $7.76 → close $7.87 +0.44; SWMR×1 09:30 $18.59 → close $16.37 -2.22; TDTH×37 09:30 $0.89 → close $1.14 +9.25; MRLN×19 09:30 $1.75 → close $1.78 +0.57; AUR×6 09:30 $5.32 → close $5.39 +0.42; UEC×3 09:30 $9.91 → close $9.29 -1.86 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.97 | ▲ 09:30 equity $8,413.96 vs yday $8,413.96 (+0.00) | 09:30 open · cash $88.97 (unchanged overnight, no fees) · equity $8,413.96 vs prior close $8,413.96 (+0.00) · 14 name(s) re-marked at the open (per-name table). ACAD×45 yday $20.22 → 09:30 $20.22 +0.00; AUR×6 yday $5.39 → 09:30 $5.39 +0.00; GEN×44 yday $20.85 → 09:30 $20.85 +0.00; LRMR×305 yday $3.14 → 09:30 $3.14 +0.00; MRLN×19 yday $1.78 → 09:30 $1.78 +0.00; NEOV×424 yday $2.54 → 09:30 $2.54 +0.00; OCUL×4 yday $7.87 → 09:30 $7.87 +0.00; RZLT×254 yday $4.09 → 09:30 $4.09 +0.00; SFIX×461 yday $2.53 → 09:30 $2.53 +0.00; SGMT×111 yday $9.33 → 09:30 $9.33 +0.00; SMWB×135 yday $7.69 → 09:30 $7.69 +0.00; SWMR×1 yday $16.37 → 09:30 $16.37 +0.00; TDTH×37 yday $1.14 → 09:30 $1.14 +0.00; UEC×3 yday $9.29 → 09:30 $9.29 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.97 | ▲ close $8,413.96 vs 09:30 $8,413.96 (session +0.00) | 16:00 close · cash $88.97 · equity $8,413.96 vs 09:30 $8,413.96 (+0.00; session marks +0.00) · 14 name(s) marked open→close (per-name table). ACAD×45 09:30 $20.22 → close $20.22 +0.00; AUR×6 09:30 $5.39 → close $5.39 +0.00; GEN×44 09:30 $20.85 → close $20.85 +0.00; LRMR×305 09:30 $3.14 → close $3.14 +0.00; MRLN×19 09:30 $1.78 → close $1.78 +0.00; NEOV×424 09:30 $2.54 → close $2.54 +0.00; OCUL×4 09:30 $7.87 → close $7.87 +0.00; RZLT×254 09:30 $4.09 → close $4.09 +0.00; SFIX×461 09:30 $2.53 → close $2.53 +0.00; SGMT×111 09:30 $9.33 → close $9.33 +0.00; SMWB×135 09:30 $7.69 → close $7.69 +0.00; SWMR×1 09:30 $16.37 → close $16.37 +0.00; TDTH×37 09:30 $1.14 → close $1.14 +0.00; UEC×3 09:30 $9.29 → close $9.29 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.97 | ▲ 09:30 equity $8,597.08 vs yday $8,413.96 (+183.12) | 09:30 open · cash $88.97 (unchanged overnight, no fees) · equity $8,597.08 vs prior close $8,413.96 (+183.12) · 14 name(s) re-marked at the open (per-name table). ACAD×45 yday $20.22 → 09:30 $19.95 -12.15; AUR×6 yday $5.39 → 09:30 $5.58 +1.14; GEN×44 yday $20.85 → 09:30 $22.49 +72.16; LRMR×305 yday $3.14 → 09:30 $3.20 +18.30; MRLN×19 yday $1.78 → 09:30 $1.85 +1.33; NEOV×424 yday $2.54 → 09:30 $2.60 +25.44; OCUL×4 yday $7.87 → 09:30 $7.63 -0.96; RZLT×254 yday $4.09 → 09:30 $4.14 +12.70; SFIX×461 yday $2.53 → 09:30 $2.55 +9.22; SGMT×111 yday $9.33 → 09:30 $9.63 +33.30; SMWB×135 yday $7.69 → 09:30 $7.88 +25.65; SWMR×1 yday $16.37 → 09:30 $17.50 +1.13; TDTH×37 yday $1.14 → 09:30 $1.02 -4.44; UEC×3 yday $9.29 → 09:30 $9.39 +0.30 | — |
| 2026-10-01 09:30 ET | **SELL** | `ACAD` | 45 | $19.95 | $2.15 | $-105.97 | $984.58 | ▼ -105.97 after sell → book $8,594.94; vs 09:30 mark -2.14 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `AUR` | 6 | $5.58 | $0.37 | $+0.85 | $1,017.68 | ▲ +0.85 after sell → book $8,594.56; vs 09:30 mark -0.38 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GEN` | 44 | $22.49 | $2.14 | $-22.74 | $2,005.10 | ▼ -22.74 after sell → book $8,592.42; vs 09:30 mark -2.14 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `LRMR` | 305 | $3.20 | $4.00 | $-44.53 | $2,977.11 | ▼ -44.53 after sell → book $8,588.43; vs 09:30 mark -3.99 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MRLN` | 19 | $1.85 | $0.43 | $+1.08 | $3,011.83 | ▲ +1.08 after sell → book $8,588.00; vs 09:30 mark -0.43 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `NEOV` | 424 | $2.60 | $5.55 | $+78.02 | $4,108.68 | ▲ +78.02 after sell → book $8,582.45; vs 09:30 mark -5.55 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `OCUL` | 4 | $7.63 | $0.34 | $-1.18 | $4,138.86 | ▼ -1.18 after sell → book $8,582.11; vs 09:30 mark -0.34 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `RZLT` | 254 | $4.14 | $3.33 | $+31.49 | $5,187.09 | ▲ +31.49 after sell → book $8,578.78; vs 09:30 mark -3.33 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SFIX` | 461 | $2.55 | $6.03 | $+149.37 | $6,356.61 | ▲ +149.37 after sell → book $8,572.75; vs 09:30 mark -6.03 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SGMT` | 111 | $9.63 | $2.35 | $+53.05 | $7,423.19 | ▲ +53.05 after sell → book $8,570.40; vs 09:30 mark -2.35 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SMWB` | 135 | $7.88 | $2.43 | $+46.48 | $8,484.56 | ▲ +46.48 after sell → book $8,567.97; vs 09:30 mark -2.43 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SWMR` | 1 | $17.50 | $0.20 | $-1.48 | $8,501.86 | ▼ -1.48 after sell → book $8,567.77; vs 09:30 mark -0.20 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TDTH` | 37 | $1.02 | $0.51 | $+3.86 | $8,539.09 | ▲ +3.86 after sell → book $8,567.26; vs 09:30 mark -0.51 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `UEC` | 3 | $9.39 | $0.31 | $-2.18 | $8,566.95 | ▼ -2.18 after sell → book $8,566.95; vs 09:30 mark -0.31 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `GLAS` | 217 | $5.62 | $2.80 | — | $7,344.61 | — | rank by rsi; rank rsi; list yday_mover; ret5=-28.5; leftover $1223.85 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `LQDA` | 51 | $23.96 | $2.14 | — | $6,120.51 | — | rank by rsi; rank rsi; list yday_mover; ret5=-55.2; leftover $1223.85 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `QTTB` | 146 | $8.36 | $2.43 | — | $4,897.52 | — | rank by rsi; rank rsi; list yday_mover; ret5=-13.5; leftover $1223.85 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVXL` | 611 | $2.00 | $7.88 | — | $3,667.64 | — | rank by rsi; rank rsi; list yday_mover; ret5=-14.0; leftover $1223.85 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MKC` | 26 | $46.80 | $2.07 | — | $2,448.77 | — | rank by rsi; rank rsi; list earn_react; ret5=-5.5; leftover $1223.85 | join🟢 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PYXS` | 529 | $2.31 | $6.82 | — | $1,219.96 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-27.6; leftover $1223.85 | join🟢 sector🔴 gen🔴 news🔴 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `NKE` | 34 | $35.45 | $2.09 | — | $12.56 | — | rank by rsi; rank rsi; list overnight,overnight_mega; ret5=-1.8; leftover $1223.85 | join🔴 sector🔴 gen🔴 news🔴 digest🔴 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.56 | ▼ close $8,455.03 vs 09:30 $8,597.08 (session -85.68) | 16:00 close · cash $12.56 · equity $8,455.03 vs 09:30 $8,597.08 (-142.05; session marks -85.68) · 7 name(s) marked open→close (per-name table). GLAS×217 09:30 $5.62 → close $5.32 -65.10; LQDA×51 09:30 $23.96 → close $27.99 +205.53; QTTB×146 09:30 $8.36 → close $8.25 -16.06; AVXL×611 09:30 $2.00 → close $1.89 -67.21; MKC×26 09:30 $46.80 → close $44.14 -69.16; PYXS×529 09:30 $2.31 → close $2.19 -63.48; NKE×34 09:30 $35.45 → close $35.15 -10.20 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.56 | ▼ 09:30 equity $8,299.04 vs yday $8,455.03 (-155.99) | 09:30 open · cash $12.56 (unchanged overnight, no fees) · equity $8,299.04 vs prior close $8,455.03 (-155.99) · 7 name(s) re-marked at the open (per-name table). AVXL×611 yday $1.89 → 09:30 $1.88 -6.11; GLAS×217 yday $5.32 → 09:30 $5.30 -4.34; LQDA×51 yday $27.99 → 09:30 $26.69 -66.30; MKC×26 yday $44.14 → 09:30 $43.52 -16.12; NKE×34 yday $35.15 → 09:30 $32.55 -88.30; PYXS×529 yday $2.19 → 09:30 $2.21 +10.58; QTTB×146 yday $8.25 → 09:30 $8.35 +14.60 | — |
| 2026-10-02 09:30 ET | **BUY** | `CMPX` | 1 | $0.94 | $0.01 | — | $11.61 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-20.6; leftover $1.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `LUCD` | 2 | $0.63 | $0.02 | — | $10.32 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-24.2; leftover $1.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.32 | ▲ close $8,423.06 vs 09:30 $8,299.04 (session +124.05) | 16:00 close · cash $10.32 · equity $8,423.06 vs 09:30 $8,299.04 (+124.02; session marks +124.05) · 9 name(s) marked open→close (per-name table). AVXL×611 09:30 $1.88 → close $1.82 -36.66; GLAS×217 09:30 $5.30 → close $5.49 +41.23; LQDA×51 09:30 $26.69 → close $28.64 +99.45; MKC×26 09:30 $43.52 → close $44.67 +29.90; NKE×34 09:30 $32.55 → close $33.87 +44.78; PYXS×529 09:30 $2.21 → close $2.14 -37.03; QTTB×146 09:30 $8.35 → close $8.23 -17.52; CMPX×1 09:30 $0.94 → close $0.86 -0.08; LUCD×2 09:30 $0.63 → close $0.63 -0.02 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.32 | ▼ 09:30 equity $8,345.32 vs yday $8,423.06 (-77.74) | 09:30 open · cash $10.32 (unchanged overnight, no fees) · equity $8,345.32 vs prior close $8,423.06 (-77.74) · 9 name(s) re-marked at the open (per-name table). AVXL×611 yday $1.82 → 09:30 $1.79 -18.33; CMPX×1 yday $0.86 → 09:30 $0.90 +0.04; GLAS×217 yday $5.49 → 09:30 $5.49 +0.00; LQDA×51 yday $28.64 → 09:30 $28.07 -29.07; LUCD×2 yday $0.63 → 09:30 $0.62 -0.01; MKC×26 yday $44.67 → 09:30 $44.76 +2.34; NKE×34 yday $33.87 → 09:30 $33.82 -1.70; PYXS×529 yday $2.14 → 09:30 $2.12 -10.58; QTTB×146 yday $8.23 → 09:30 $8.09 -20.44 | — |
| 2026-10-05 09:30 ET | **BUY** | `ECX` | 1 | $0.88 | $0.01 | — | $9.43 | — | rank by rsi; rank rsi; list yday_mover; ret5=-21.2; leftover $1.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `LU` | 1 | $1.06 | $0.01 | — | $8.35 | — | rank by rsi; rank rsi; list yday_mover; ret5=-17.1; leftover $1.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.35 | ▼ close $8,333.71 vs 09:30 $8,345.32 (session -11.59) | 16:00 close · cash $8.35 · equity $8,333.71 vs 09:30 $8,345.32 (-11.61; session marks -11.59) · 11 name(s) marked open→close (per-name table). AVXL×611 09:30 $1.79 → close $1.65 -88.60; CMPX×1 09:30 $0.90 → close $0.96 +0.06; GLAS×217 09:30 $5.49 → close $5.35 -30.38; LQDA×51 09:30 $28.07 → close $27.64 -21.93; LUCD×2 09:30 $0.62 → close $0.59 -0.06; MKC×26 09:30 $44.76 → close $45.54 +20.28; NKE×34 09:30 $33.82 → close $33.96 +4.76; PYXS×529 09:30 $2.12 → close $2.24 +63.48; QTTB×146 09:30 $8.09 → close $8.37 +40.88; ECX×1 09:30 $0.88 → close $0.83 -0.05; LU×1 09:30 $1.06 → close $1.03 -0.03 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.35 | ▲ 09:30 equity $8,337.45 vs yday $8,333.71 (+3.74) | 09:30 open · cash $8.35 (unchanged overnight, no fees) · equity $8,337.45 vs prior close $8,333.71 (+3.74) · 11 name(s) re-marked at the open (per-name table). AVXL×611 yday $1.65 → 09:30 $1.66 +9.16; CMPX×1 yday $0.96 → 09:30 $0.99 +0.03; ECX×1 yday $0.83 → 09:30 $0.83 +0.00; GLAS×217 yday $5.35 → 09:30 $5.35 +0.00; LQDA×51 yday $27.64 → 09:30 $27.95 +15.81; LU×1 yday $1.03 → 09:30 $1.02 -0.01; LUCD×2 yday $0.59 → 09:30 $0.60 +0.01; MKC×26 yday $45.54 → 09:30 $45.75 +5.46; NKE×34 yday $33.96 → 09:30 $33.70 -8.84; PYXS×529 yday $2.24 → 09:30 $2.22 -10.58; QTTB×146 yday $8.37 → 09:30 $8.32 -7.30 | — |
| 2026-10-06 09:30 ET | **SELL** | `AVXL` | 611 | $1.66 | $7.99 | $-223.62 | $1,014.62 | ▼ -223.62 after sell → book $8,329.46; vs 09:30 mark -7.99 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `GLAS` | 217 | $5.35 | $2.85 | $-64.23 | $2,172.72 | ▼ -64.23 after sell → book $8,326.61; vs 09:30 mark -2.85 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `LQDA` | 51 | $27.95 | $2.16 | $+199.18 | $3,596.01 | ▲ +199.18 after sell → book $8,324.45; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `MKC` | 26 | $45.75 | $2.09 | $-31.46 | $4,783.42 | ▼ -31.46 after sell → book $8,322.36; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `NKE` | 34 | $33.70 | $2.11 | $-63.70 | $5,927.11 | ▼ -63.70 after sell → book $8,320.25; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `PYXS` | 529 | $2.22 | $6.92 | $-61.36 | $7,094.57 | ▼ -61.36 after sell → book $8,313.33; vs 09:30 mark -6.92 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `QTTB` | 146 | $8.32 | $2.46 | $-10.73 | $8,306.82 | ▼ -10.73 after sell → book $8,310.87; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **BUY** | `NXH` | 508 | $2.04 | $6.55 | — | $7,263.95 | — | rank by rsi; rank rsi; list yday_mover; ret5=-33.3; leftover $1038.35 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `STZ` | 9 | $113.83 | $2.02 | — | $6,237.46 | — | rank by rsi; rank rsi; list overnight; ret5=+0.3; leftover $1038.35 | join🔴 sector🔴 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `RPM` | 10 | $96.25 | $2.02 | — | $5,272.94 | — | rank by rsi; rank rsi; list earn_react; 🔵; ret5=-4.9; leftover $1038.35 | join🔴 sector🟢 gen🟡 news🟡 digest🔴 ab🟢 peer🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ZNTL` | 369 | $2.81 | $4.76 | — | $4,231.29 | — | rank by rsi; rank rsi; list probable; ret5=+6.9; leftover $1038.35 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `VELO` | 106 | $9.77 | $2.31 | — | $3,193.36 | — | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-12.8; leftover $1038.35 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TOST` | 34 | $30.07 | $2.09 | — | $2,168.89 | — | rank by rsi; rank rsi; list flatten; ⚪; ret5=-1.1; leftover $1038.35 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PCOR` | 19 | $54.22 | $2.05 | — | $1,136.67 | — | rank by rsi; rank rsi; list ohlc_hot; 🔵; ret5=+10.4; leftover $1038.35 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 70 | $14.70 | $2.20 | — | $105.47 | — | rank by rsi; rank rsi; list probable,yday_gainer; ret5=+1.4; leftover $1038.35 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.47 | ▼ close $8,034.84 vs 09:30 $8,337.45 (session -252.02) | 16:00 close · cash $105.47 · equity $8,034.84 vs 09:30 $8,337.45 (-302.61; session marks -252.02) · 12 name(s) marked open→close (per-name table). CMPX×1 09:30 $0.99 → close $1.00 +0.01; ECX×1 09:30 $0.83 → close $0.84 +0.01; LU×1 09:30 $1.02 → close $1.01 -0.01; LUCD×2 09:30 $0.60 → close $0.60 +0.00; NXH×508 09:30 $2.04 → close $1.83 -106.68; STZ×9 09:30 $113.83 → close $115.67 +16.56; RPM×10 09:30 $96.25 → close $98.31 +20.60; ZNTL×369 09:30 $2.81 → close $2.67 -51.66; VELO×106 09:30 $9.77 → close $9.57 -21.20; TOST×34 09:30 $30.07 → close $30.25 +6.12; PCOR×19 09:30 $54.22 → close $54.39 +3.23; ARCT×70 09:30 $14.70 → close $13.00 -119.00 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.47 | ▼ 09:30 equity $7,917.07 vs yday $8,034.84 (-117.77) | 09:30 open · cash $105.47 (unchanged overnight, no fees) · equity $7,917.07 vs prior close $8,034.84 (-117.77) · 12 name(s) re-marked at the open (per-name table). ARCT×70 yday $13.00 → 09:30 $12.71 -20.30; CMPX×1 yday $1.00 → 09:30 $0.97 -0.03; ECX×1 yday $0.84 → 09:30 $0.82 -0.02; LU×1 yday $1.01 → 09:30 $1.01 +0.00; LUCD×2 yday $0.60 → 09:30 $0.59 -0.02; NXH×508 yday $1.83 → 09:30 $1.81 -10.16; PCOR×19 yday $54.39 → 09:30 $54.04 -6.65; RPM×10 yday $98.31 → 09:30 $98.10 -2.10; STZ×9 yday $115.67 → 09:30 $114.34 -11.97; TOST×34 yday $30.25 → 09:30 $30.13 -4.08; VELO×106 yday $9.57 → 09:30 $9.12 -47.70; ZNTL×369 yday $2.67 → 09:30 $2.63 -14.76 | — |
| 2026-10-07 09:30 ET | **SELL** | `CMPX` | 1 | $0.97 | $0.03 | $-0.01 | $106.41 | ▼ -0.01 after sell → book $7,917.03; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **SELL** | `LUCD` | 2 | $0.59 | $0.04 | $-0.14 | $107.55 | ▼ -0.14 after sell → book $7,917.00; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.55 | ▲ close $7,938.89 vs 09:30 $7,917.07 (session +21.90) | 16:00 close · cash $107.55 · equity $7,938.89 vs 09:30 $7,917.07 (+21.82; session marks +21.90) · 10 name(s) marked open→close (per-name table). ARCT×70 09:30 $12.71 → close $12.77 +3.85; ECX×1 09:30 $0.82 → close $0.85 +0.03; LU×1 09:30 $1.01 → close $0.98 -0.03; NXH×508 09:30 $1.81 → close $1.77 -17.78; PCOR×19 09:30 $54.04 → close $54.80 +14.44; RPM×10 09:30 $98.10 → close $98.33 +2.30; STZ×9 09:30 $114.34 → close $118.39 +36.45; TOST×34 09:30 $30.13 → close $30.55 +14.28; VELO×106 09:30 $9.12 → close $9.10 -2.12; ZNTL×369 09:30 $2.63 → close $2.55 -29.52 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STUB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STNE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 3.37 < 1 share @ 39.85 |
| 2026-08-17 | `CAPR` | cash | leftover split 3.37 < 1 share @ 6.87 |
| 2026-08-17 | `ZNTL` | cash | leftover split 3.37 < 1 share @ 3.56 |
| 2026-08-17 | `AMPG` | cash | leftover split 3.37 < 1 share @ 4.09 |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `STNE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `KLC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CSAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CAPR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `KLC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CSAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | cash | leftover split 40.38 < 1 share @ 42.41 |
| 2026-08-21 | `WMT` | cash | leftover split 40.38 < 1 share @ 103.69 |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ROST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `RGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BILI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OKTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MAIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `OSUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BILI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `OKTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MNSO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MNSO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `JKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SYRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `QBTS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SSTK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BYND` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `JKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SYRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `QBTS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FJET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AIIO` | cash | leftover split 0.94 < 1 share @ 1.75 |
| 2026-09-04 | `CRDO` | cash | leftover split 0.94 < 1 share @ 162.10 |
| 2026-09-04 | `FCEL` | cash | leftover split 0.94 < 1 share @ 14.52 |
| 2026-09-04 | `MAMA` | cash | leftover split 0.94 < 1 share @ 15.70 |
| 2026-09-04 | `UNFI` | cash | leftover split 0.94 < 1 share @ 43.80 |
| 2026-09-08 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OSW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SWBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FJET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `COO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AXGN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ANAB` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `EQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AXGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `EU` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARQQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CNTB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRLN` | cash | leftover split 0.16 < 1 share @ 2.27 |
| 2026-09-17 | `JBHT` | cash | leftover split 0.16 < 1 share @ 238.60 |
| 2026-09-17 | `RCAT` | cash | leftover split 0.16 < 1 share @ 7.39 |
| 2026-09-17 | `PALI` | cash | leftover split 0.16 < 1 share @ 1.75 |
| 2026-09-17 | `CRDO` | cash | leftover split 0.16 < 1 share @ 168.65 |
| 2026-09-17 | `PUMP` | cash | leftover split 0.16 < 1 share @ 10.31 |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ALHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FLNC` | cash | leftover split 0.13 < 1 share @ 7.54 |
| 2026-09-18 | `RARE` | cash | leftover split 0.13 < 1 share @ 14.79 |
| 2026-09-18 | `DCX` | cash | leftover split 0.13 < 1 share @ 0.35 |
| 2026-09-18 | `ALMU` | cash | leftover split 0.13 < 1 share @ 11.64 |
| 2026-09-18 | `AKBA` | cash | leftover split 0.13 < 1 share @ 0.89 |
| 2026-09-18 | `XE` | cash | leftover split 0.13 < 1 share @ 16.28 |
| 2026-09-18 | `BHVN` | cash | leftover split 0.13 < 1 share @ 14.07 |
| 2026-09-22 | `XENE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `KDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PUMP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 15.66 < 1 share @ 93.97 |
| 2026-09-22 | `KBH` | cash | leftover split 15.66 < 1 share @ 49.39 |
| 2026-09-22 | `GIS` | cash | leftover split 15.66 < 1 share @ 35.96 |
| 2026-09-23 | `XENE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `KDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MLKN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABVX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `PUMP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `EVER` | cash | leftover split 11.74 < 1 share @ 19.46 |
| 2026-09-23 | `CBRL` | cash | leftover split 11.74 < 1 share @ 47.57 |
| 2026-09-23 | `GIS` | cash | leftover split 11.74 < 1 share @ 35.74 |
| 2026-09-23 | `FUL` | cash | leftover split 11.74 < 1 share @ 50.51 |
| 2026-09-23 | `KBH` | cash | leftover split 11.74 < 1 share @ 47.15 |
| 2026-09-24 | `XNDU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `LXEO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ALKT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CCOI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BYND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PBLS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XNDU` | 1 | 2026-09-23 @ $5.99 | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-25.9; leftover $11.74 |
| `CMPX` | 9 | 2026-09-23 @ $1.22 | rank by rsi; rank rsi; list yday_mover; 🔵; ret5=-33.0; leftover $11.74 |
| `NMRA` | 15 | 2026-09-23 @ $0.77 | rank by rsi; rank rsi; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $11.74 |
