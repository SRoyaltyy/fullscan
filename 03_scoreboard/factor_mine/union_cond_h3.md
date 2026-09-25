# Factor mine action — `union_cond_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **-17.23%** ($8,276) · signal-only (no cash/fees) was -15.16%. Starts YES **0/30**. Fills 190 · skips 307 · realized $-613.92.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,694.96.

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
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 1 | $10.83 | $0.11 | — | $86.59 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 1 | $7.29 | $0.08 | — | $79.23 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 2 | $5.84 | $0.12 | — | $67.42 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.42 | ▲ close $10,435.15 vs 09:30 $10,178.12 (session +257.34) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.42 | ▼ 09:30 equity $10,414.41 vs yday $10,435.15 (-20.74) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 7 | $1.15 | $0.10 | — | $59.27 | — | rank by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $8.43 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.27 | ▲ close $10,523.19 vs 09:30 $10,414.41 (session +108.88) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.27 | ▼ 09:30 equity $10,390.25 vs yday $10,523.19 (-132.94) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,257.20 | ▼ -0.12 after sell → book $10,388.18; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $2,424.77 | ▼ -83.63 after sell → book $10,386.05; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $4,163.61 | ▲ +471.89 after sell → book $10,365.87; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $5,337.64 | ▼ -69.50 after sell → book $10,363.78; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,677.27 | ▲ +97.12 after sell → book $10,361.45; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $7,907.18 | ▼ -14.65 after sell → book $10,359.36; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $9,079.49 | ▼ -66.33 after sell → book $10,357.19; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $10,319.89 | ▲ +23.38 after sell → book $10,355.11; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,319.89 | ▲ close $10,356.37 vs 09:30 $10,390.25 (session +1.26) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,319.89 | ▼ 09:30 equity $10,356.14 vs yday $10,356.37 (-0.23) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 1 | $10.85 | $0.13 | $-0.22 | $10,330.61 | ▼ -0.22 after sell → book $10,356.01; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 1 | $6.74 | $0.09 | $-0.72 | $10,337.26 | ▼ -0.72 after sell → book $10,355.92; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SECZ` | 2 | $5.83 | $0.14 | $-0.29 | $10,348.77 | ▼ -0.29 after sell → book $10,355.77; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,348.77 | ▼ close $10,355.54 vs 09:30 $10,356.14 (session -0.24) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,348.77 | ▼ 09:30 equity $10,355.52 vs yday $10,355.54 (-0.02) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `VERI` | 7 | $0.96 | $0.11 | $-1.52 | $10,355.41 | ▼ -1.52 after sell → book $10,355.41; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.13 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.96 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.48 | — | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.11 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.98 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.77 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.99 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.65 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.43 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.65 | ▲ close $10,569.99 vs 09:30 $10,355.52 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.65 | ▲ 09:30 equity $10,845.88 vs yday $10,569.99 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.28 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.79 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.81 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.43 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.04 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.21 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▲ close $10,844.89 vs 09:30 $10,845.88 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,956.63 vs yday $10,844.89 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▼ close $10,921.87 vs 09:30 $10,956.63 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▼ 09:30 equity $10,751.25 vs yday $10,921.87 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,351.69 | ▼ -18.63 after sell → book $10,749.06; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,691.68 | ▲ +63.82 after sell → book $10,747.01; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,958.62 | ▼ -15.53 after sell → book $10,744.81; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,194.40 | ▼ -59.59 after sell → book $10,741.87; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.85 | ▲ +98.31 after sell → book $10,739.67; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,958.47 | ▲ +111.41 after sell → book $10,737.53; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.90 | ▲ +91.65 after sell → book $10,727.86; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.94 | ▲ +91.71 after sell → book $10,725.82; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $9,297.20 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 34 | $38.01 | $2.09 | — | $8,002.77 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $6,689.52 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 111 | $11.90 | $2.32 | — | $5,366.30 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 59 | $22.41 | $2.17 | — | $4,041.94 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+13.9; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $2,732.43 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $1,417.55 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 147 | $8.98 | $2.43 | — | $95.06 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1325.37 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.06 | ▲ close $10,960.08 vs 09:30 $10,751.25 (session +251.57) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.06 | ▼ 09:30 equity $10,892.63 vs yday $10,960.08 (-67.45) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $111.47 | ▼ -0.96 after sell → book $10,892.44; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $141.84 | ▲ +7.88 after sell → book $10,892.11; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $165.65 | ▼ -1.17 after sell → book $10,891.82; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $191.72 | ▲ +0.69 after sell → book $10,891.50; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $221.74 | ▲ +4.63 after sell → book $10,891.12; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 54 | $0.58 | $0.48 | — | $189.78 | — | rank by cond; rank cond; list yday_mover; 🔵; ret5=-27.5; leftover $31.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 6 | $5.21 | $0.33 | — | $158.19 | — | rank by cond; rank cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $31.68 | — |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 5 | $5.75 | $0.30 | — | $129.13 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $31.68 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.13 | ▼ close $10,789.03 vs 09:30 $10,892.63 (session -100.97) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.13 | ▼ 09:30 equity $10,652.54 vs yday $10,789.03 (-136.49) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.57 | $0.19 | — | $110.66 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $18.45 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 7 | $2.60 | $0.20 | — | $92.26 | — | rank by cond; rank cond; list flatten,ohlc_hot; ret5=+13.0; leftover $18.45 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.26 | ▲ close $10,690.15 vs 09:30 $10,652.54 (session +38.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.26 | ▲ 09:30 equity $10,726.88 vs yday $10,690.15 (+36.73) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $119.19 | $2.04 | $+3.30 | $1,401.30 | ▲ +3.30 after sell → book $10,724.84; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 34 | $40.12 | $2.11 | $+67.54 | $2,763.27 | ▲ +67.54 after sell → book $10,722.72; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.57 | $2.06 | $+20.38 | $4,096.90 | ▲ +20.38 after sell → book $10,720.66; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 111 | $11.55 | $2.35 | $-43.52 | $5,376.60 | ▼ -43.52 after sell → book $10,718.31; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 59 | $20.90 | $2.19 | $-93.44 | $6,607.51 | ▼ -93.44 after sell → book $10,716.12; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $23.95 | $2.18 | $+5.57 | $7,922.58 | ▲ +5.57 after sell → book $10,713.95; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 30 | $44.51 | $2.10 | $+18.32 | $9,255.78 | ▲ +18.32 after sell → book $10,711.85; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 147 | $9.05 | $2.47 | $+5.39 | $10,583.67 | ▲ +5.39 after sell → book $10,709.38; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,284.02 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,006.17 | — | rank by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,802.91 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,494.89 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,211.99 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $2,890.72 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,730.96 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1322.96 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $411.58 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1322.96 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $411.58 | ▼ close $10,272.11 vs 09:30 $10,726.88 (session -421.15) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $411.58 | ▲ 09:30 equity $10,325.17 vs yday $10,272.11 (+53.06) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 54 | $0.51 | $0.46 | $-4.88 | $438.66 | ▼ -4.88 after sell → book $10,324.71; vs 09:30 mark -0.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 6 | $5.00 | $0.34 | $-1.93 | $468.32 | ▼ -1.93 after sell → book $10,324.37; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTG` | 5 | $5.63 | $0.32 | $-1.22 | $496.15 | ▼ -1.22 after sell → book $10,324.05; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $496.15 | ▲ close $10,350.95 vs 09:30 $10,325.17 (session +26.90) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $496.15 | ▼ 09:30 equity $10,169.29 vs yday $10,350.95 (-181.66) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.57 | $0.21 | $-0.41 | $514.22 | ▼ -0.41 after sell → book $10,169.08; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 7 | $2.67 | $0.23 | $+0.06 | $532.68 | ▲ +0.06 after sell → book $10,168.85; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $532.68 | ▼ close $10,136.88 vs 09:30 $10,169.29 (session -31.97) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $532.68 | ▼ 09:30 equity $10,123.21 vs yday $10,136.88 (-13.67) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $1,802.82 | ▼ -29.50 after sell → book $10,121.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $2,997.78 | ▼ -82.89 after sell → book $10,119.15; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,067.51 | ▼ -133.53 after sell → book $10,117.13; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $5,290.42 | ▼ -85.12 after sell → book $10,115.12; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,539.83 | ▼ -33.48 after sell → book $10,113.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $60.37 | $2.07 | $-55.58 | $7,805.52 | ▼ -55.58 after sell → book $10,110.99; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $268.12 | $2.02 | $-89.30 | $8,875.98 | ▼ -89.30 after sell → book $10,108.97; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $112.09 | $2.04 | $-88.44 | $10,106.93 | ▼ -88.44 after sell → book $10,106.93; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.93 | ▲ close $10,106.93 vs 09:30 $10,123.21 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.93 | ▲ 09:30 equity $10,106.93 vs yday $10,106.93 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $8,846.96 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 654 | $1.93 | $8.44 | — | $7,576.31 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 579 | $2.18 | $7.47 | — | $6,306.62 | — | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $5,059.57 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 121 | $10.42 | $2.35 | — | $3,796.40 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $53.45 | $2.06 | — | $2,564.99 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 47 | $26.74 | $2.13 | — | $1,306.08 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1263.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $112.01 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1263.37 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.01 | ▼ close $9,878.62 vs 09:30 $10,106.93 (session -199.55) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.01 | ▼ 09:30 equity $9,851.53 vs yday $9,878.62 (-27.09) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 5 | $3.46 | $0.19 | — | $94.52 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $18.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 7 | $2.52 | $0.20 | — | $76.69 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $18.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $63.13 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $18.67 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.13 | ▲ close $9,973.02 vs 09:30 $9,851.53 (session +122.01) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.13 | ▼ 09:30 equity $9,864.00 vs yday $9,973.02 (-109.02) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.13 | ▼ close $9,787.28 vs 09:30 $9,864.00 (session -76.72) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.13 | ▼ 09:30 equity $9,729.19 vs yday $9,787.28 (-58.09) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 75 | $15.46 | $2.24 | $-102.70 | $1,220.39 | ▼ -102.70 after sell → book $9,726.95; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 654 | $1.94 | $8.56 | $-10.45 | $2,480.59 | ▼ -10.45 after sell → book $9,718.39; vs 09:30 mark -8.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 579 | $2.22 | $7.58 | $+8.12 | $3,758.40 | ▲ +8.12 after sell → book $9,710.82; vs 09:30 mark -7.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $4,974.59 | ▼ -30.85 after sell → book $9,708.72; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 121 | $10.02 | $2.38 | $-53.14 | $6,184.63 | ▼ -53.14 after sell → book $9,706.34; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PBH` | 23 | $49.53 | $2.08 | $-94.30 | $7,321.74 | ▼ -94.30 after sell → book $9,704.26; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PCRX` | 47 | $25.62 | $2.15 | $-56.92 | $8,523.73 | ▼ -56.92 after sell → book $9,702.11; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $9,653.62 | ▼ -64.17 after sell → book $9,700.07; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,653.62 | ▼ close $9,696.38 vs 09:30 $9,729.19 (session -3.69) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,653.62 | ▼ 09:30 equity $9,695.63 vs yday $9,696.38 (-0.75) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 5 | $2.85 | $0.18 | $-3.42 | $9,667.69 | ▼ -3.42 after sell → book $9,695.45; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 7 | $2.22 | $0.20 | $-2.49 | $9,683.04 | ▼ -2.49 after sell → book $9,695.26; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $9,695.11 | ▼ -1.49 after sell → book $9,695.11; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.11 | ▲ close $9,695.11 vs 09:30 $9,695.63 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.11 | ▲ 09:30 equity $9,695.11 vs yday $9,695.11 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 23 | $52.55 | $2.06 | — | $8,484.40 | — | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $7,331.38 | — | rank by cond; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 119 | $10.11 | $2.35 | — | $6,125.94 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $4,944.13 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $3,813.76 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 66 | $18.30 | $2.19 | — | $2,603.77 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 102 | $11.88 | $2.30 | — | $1,389.72 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+20.7; leftover $1211.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $179.64 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1211.89 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.64 | ▲ close $9,787.53 vs 09:30 $9,695.11 (session +109.43) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.64 | ▼ 09:30 equity $9,623.70 vs yday $9,787.53 (-163.83) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.64 | ▼ close $9,313.43 vs 09:30 $9,623.70 (session -310.27) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.64 | ▼ 09:30 equity $9,278.03 vs yday $9,313.43 (-35.40) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.64 | ▲ close $9,345.87 vs 09:30 $9,278.03 (session +67.84) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.64 | ▼ 09:30 equity $9,266.14 vs yday $9,345.87 (-79.73) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 23 | $48.60 | $2.08 | $-94.99 | $1,295.36 | ▼ -94.99 after sell → book $9,264.06; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $2,273.54 | ▼ -174.84 after sell → book $9,262.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 119 | $9.39 | $2.38 | $-90.40 | $3,388.57 | ▼ -90.40 after sell → book $9,259.65; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 66 | $17.73 | $2.21 | $-42.02 | $4,556.54 | ▼ -42.02 after sell → book $9,257.44; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `APPS` | 102 | $11.30 | $2.32 | $-63.78 | $5,706.82 | ▼ -63.78 after sell → book $9,255.12; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 22 | $50.69 | $2.08 | $-96.97 | $6,819.93 | ▼ -96.97 after sell → book $9,253.05; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 20 | $55.66 | $2.05 | — | $5,704.68 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $1136.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 43 | $26.27 | $2.12 | — | $4,572.95 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $1136.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 41 | $27.09 | $2.11 | — | $3,460.14 | — | rank by cond; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1136.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 79 | $14.31 | $2.23 | — | $2,327.43 | — | rank by cond; rank cond; list flatten; ret5=+4.8; leftover $1136.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 4 | $236.92 | $2.00 | — | $1,377.74 | — | rank by cond; rank cond; list ohlc_hot; ret5=+15.5; leftover $1136.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `ILMN` | 5 | $224.49 | $2.00 | — | $253.29 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+5.3; leftover $1136.65 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $253.29 | ▲ close $9,248.52 vs 09:30 $9,266.14 (session +7.99) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $253.29 | ▲ 09:30 equity $9,318.97 vs yday $9,248.52 (+70.45) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $+30.78 | $1,465.88 | ▲ +30.78 after sell → book $9,316.92; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $+16.59 | $2,612.84 | ▲ +16.59 after sell → book $9,314.88; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 46 | $7.95 | $2.13 | — | $2,245.01 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $373.26 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 20 | $18.04 | $2.05 | — | $1,882.26 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $373.26 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 155 | $2.40 | $2.46 | — | $1,507.80 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $373.26 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 2 | $170.85 | $2.00 | — | $1,164.11 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $373.26 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 20 | $18.56 | $2.05 | — | $790.86 | — | rank by cond; rank cond; list yday_gainer; 🔵; ret5=+4.8; leftover $373.26 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 14 | $25.95 | $2.03 | — | $425.53 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $373.26 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 5 | $67.91 | $2.00 | — | $83.97 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $373.26 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.97 | ▲ close $9,373.41 vs 09:30 $9,318.97 (session +73.25) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.97 | ▲ 09:30 equity $9,428.91 vs yday $9,373.41 (+55.50) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 1 | $7.98 | $0.08 | — | $75.91 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $14.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 3 | $3.94 | $0.13 | — | $63.96 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $14.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.96 | ▼ close $9,296.43 vs 09:30 $9,428.91 (session -132.27) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.96 | ▲ 09:30 equity $9,355.04 vs yday $9,296.43 (+58.61) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 20 | $58.23 | $2.07 | $+47.28 | $1,226.49 | ▲ +47.28 after sell → book $9,352.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 43 | $25.94 | $2.14 | $-18.45 | $2,339.77 | ▼ -18.45 after sell → book $9,350.83; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 41 | $28.69 | $2.13 | $+61.35 | $3,513.93 | ▲ +61.35 after sell → book $9,348.70; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 79 | $13.65 | $2.25 | $-56.62 | $4,590.03 | ▼ -56.62 after sell → book $9,346.45; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 4 | $231.62 | $2.02 | $-25.22 | $5,514.49 | ▼ -25.22 after sell → book $9,344.43; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ILMN` | 5 | $241.51 | $2.02 | $+81.07 | $6,720.01 | ▲ +81.07 after sell → book $9,342.40; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $5,797.01 | — | rank by cond; rank cond; list ohlc_hot; ret5=+12.5; leftover $960.00 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 2 | $326.48 | $2.00 | — | $5,142.05 | — | rank by cond; rank cond; list ohlc_hot; ret5=+3.9; leftover $960.00 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 7 | $123.00 | $2.01 | — | $4,279.04 | — | rank by cond; rank cond; list ohlc_hot; ret5=+3.0; leftover $960.00 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 38 | $24.93 | $2.10 | — | $3,329.60 | — | rank by cond; rank cond; list ohlc_hot; ret5=+8.7; leftover $960.00 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 89 | $10.71 | $2.26 | — | $2,374.15 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $960.00 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMD` | 1 | $583.88 | $1.99 | — | $1,788.28 | — | rank by cond; rank cond; list ohlc_hot; ret5=+8.5; leftover $960.00 | — |
| 2026-09-21 09:30 ET | **BUY** | `ARM` | 3 | $294.36 | $2.00 | — | $903.20 | — | rank by cond; rank cond; list ohlc_hot; ret5=+4.1; leftover $960.00 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $903.20 | ▲ close $9,403.77 vs 09:30 $9,355.04 (session +75.73) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $903.20 | ▼ 09:30 equity $9,332.33 vs yday $9,403.77 (-71.44) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `BULL` | 46 | $8.28 | $2.15 | $+10.67 | $1,281.70 | ▲ +10.67 after sell → book $9,330.18; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 20 | $18.51 | $2.07 | $+5.38 | $1,649.83 | ▲ +5.38 after sell → book $9,328.11; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `AXTI` | 5 | $76.64 | $2.02 | $+39.62 | $2,031.01 | ▲ +39.62 after sell → book $9,326.09; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 500 | $0.58 | $4.40 | — | $1,736.61 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $290.14 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 10 | $28.02 | $2.02 | — | $1,454.39 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $290.14 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,454.39 | ▲ close $9,353.42 vs 09:30 $9,332.33 (session +33.75) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,454.39 | ▲ 09:30 equity $9,545.55 vs yday $9,353.42 (+192.13) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 155 | $2.24 | $2.49 | $-29.75 | $1,799.10 | ▼ -29.75 after sell → book $9,543.06; vs 09:30 mark -2.49 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 2 | $174.50 | $2.02 | $+3.29 | $2,146.08 | ▲ +3.29 after sell → book $9,541.04; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 1 | $7.95 | $0.10 | $-0.22 | $2,153.93 | ▼ -0.22 after sell → book $9,540.94; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RXT` | 3 | $4.07 | $0.15 | $+0.11 | $2,165.99 | ▲ +0.11 after sell → book $9,540.79; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 2 | $166.54 | $2.00 | — | $1,830.91 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $361.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 36 | $9.90 | $2.10 | — | $1,472.41 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $361.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 4 | $89.50 | $2.00 | — | $1,112.41 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.3; leftover $361.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 3 | $116.85 | $2.00 | — | $759.86 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+3.3; leftover $361.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 17 | $20.65 | $2.04 | — | $406.77 | — | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $361.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 36 | $9.81 | $2.10 | — | $51.51 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $361.00 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.51 | ▼ close $9,507.57 vs 09:30 $9,545.55 (session -20.98) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.51 | ▼ 09:30 equity $9,292.33 vs yday $9,507.57 (-215.24) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `AMRX` | 20 | $19.29 | $2.07 | $+10.48 | $435.24 | ▲ +10.48 after sell → book $9,290.26; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 14 | $26.22 | $2.05 | $-0.30 | $800.27 | ▼ -0.30 after sell → book $9,288.21; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+173.42 | $1,896.69 | ▲ +173.42 after sell → book $9,286.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `COHR` | 2 | $294.90 | $2.02 | $-67.17 | $2,484.47 | ▼ -67.17 after sell → book $9,284.17; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FORM` | 7 | $126.98 | $2.03 | $+23.82 | $3,371.30 | ▲ +23.82 after sell → book $9,282.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `UMC` | 38 | $24.11 | $2.12 | $-35.39 | $4,285.36 | ▼ -35.39 after sell → book $9,280.02; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABTC` | 89 | $9.64 | $2.28 | $-99.77 | $5,141.04 | ▼ -99.77 after sell → book $9,277.74; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMD` | 1 | $600.27 | $2.01 | $+12.38 | $5,739.29 | ▲ +12.38 after sell → book $9,275.72; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARM` | 3 | $319.23 | $2.02 | $+70.59 | $6,694.96 | ▲ +70.59 after sell → book $9,273.70; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,694.96 | ▲ close $9,292.67 vs 09:30 $9,292.33 (session +18.97) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,581.88 | ▲ 09:30 equity $8,243.61 vs yday $8,242.65 (+0.96) | 09:30 open · cash $7,581.88 (unchanged overnight, no fees) · equity $8,243.61 vs prior close $8,242.65 (+0.96) · 11 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; AMC×30 yday $2.91 → 09:30 $2.91 +0.00; APPS×7 yday $10.88 → 09:30 $10.88 +0.00; ARQQ×3 yday $23.12 → 09:30 $23.12 +0.00; BFLY×4 yday $9.41 → 09:30 $9.41 +0.00; FIVN×2 yday $36.66 → 09:30 $36.66 +0.00; FSLY×3 yday $26.68 → 09:30 $26.68 +0.00; IBRX×4 yday $8.64 → 09:30 $8.64 +0.00; NEOG×3 yday $13.66 → 09:30 $13.66 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; UPXI×71 yday $1.17 → 09:30 $1.17 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 120 | $7.85 | $2.35 | — | $6,637.53 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $947.74 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 2 | $324.97 | $2.00 | — | $5,985.59 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $947.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 236 | $4.00 | $3.04 | — | $5,037.37 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $947.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DUOT` | 98 | $9.59 | $2.28 | — | $4,095.27 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $947.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 7 | $123.50 | $2.01 | — | $3,228.75 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $947.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 8 | $115.36 | $2.01 | — | $2,303.86 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.1; leftover $947.74 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 603 | $1.57 | $7.78 | — | $1,349.37 | — | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $947.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PRGO` | 63 | $14.81 | $2.18 | — | $414.16 | — | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.9; leftover $947.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $414.16 | ▲ close $8,276.48 vs 09:30 $8,243.61 (session +56.53) | 16:00 close · cash $414.16 · equity $8,276.48 vs 09:30 $8,243.61 (+32.87; session marks +56.53) · 19 name(s) marked open→close (per-name table). ADMA×4 09:30 $9.52 → close $9.52 +0.00; AMC×30 09:30 $2.91 → close $2.91 +0.00; APPS×7 09:30 $10.88 → close $10.88 +0.00; ARQQ×3 09:30 $23.12 → close $23.12 +0.00; BFLY×4 09:30 $9.41 → close $9.41 -0.00; FIVN×2 09:30 $36.66 → close $36.66 -0.00; FSLY×3 09:30 $26.68 → close $26.68 +0.00; IBRX×4 09:30 $8.64 → close $8.64 +0.00; NEOG×3 09:30 $13.66 → close $13.66 -0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; UPXI×71 09:30 $1.17 → close $1.17 -0.00; RSKD×120 09:30 $7.85 → close $7.78 -8.40; CDNS×2 09:30 $324.97 → close $326.13 +2.32; CYPH×236 09:30 $4.00 → close $4.12 +27.14; DUOT×98 09:30 $9.59 → close $9.14 -44.10; GRAL×7 09:30 $123.50 → close $126.89 +23.73; HALO×8 09:30 $115.36 → close $113.90 -11.68; PACB×603 09:30 $1.57 → close $1.62 +30.15; PRGO×63 09:30 $14.81 → close $15.42 +38.43 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BRUN` | cash | leftover split 12.19 < 1 share @ 26.25 |
| 2026-08-14 | `HLIT` | cash | leftover split 12.19 < 1 share @ 13.18 |
| 2026-08-14 | `MNTN` | cash | leftover split 12.19 < 1 share @ 12.50 |
| 2026-08-14 | `QMCO` | cash | leftover split 12.19 < 1 share @ 24.68 |
| 2026-08-14 | `SNDK` | cash | leftover split 12.19 < 1 share @ 1646.93 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LPTH` | cash | leftover split 8.43 < 1 share @ 14.94 |
| 2026-08-17 | `DVN` | cash | leftover split 8.43 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 8.43 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 8.43 < 1 share @ 202.70 |
| 2026-08-17 | `AAOI` | cash | leftover split 8.43 < 1 share @ 152.64 |
| 2026-08-17 | `ABX` | cash | leftover split 8.43 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 8.43 < 1 share @ 14.66 |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DNN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AGRO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NOK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 31.68 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 31.68 < 1 share @ 118.50 |
| 2026-08-26 | `FIGR` | cash | leftover split 31.68 < 1 share @ 40.50 |
| 2026-08-26 | `FUTU` | cash | leftover split 31.68 < 1 share @ 124.67 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 18.45 < 1 share @ 81.65 |
| 2026-08-27 | `MT` | cash | leftover split 18.45 < 1 share @ 74.54 |
| 2026-08-27 | `MU` | cash | leftover split 18.45 < 1 share @ 967.01 |
| 2026-08-27 | `TX` | cash | leftover split 18.45 < 1 share @ 55.25 |
| 2026-08-27 | `ANET` | cash | leftover split 18.45 < 1 share @ 205.90 |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PBH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PCRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 18.67 < 1 share @ 52.03 |
| 2026-09-04 | `CRM` | cash | leftover split 18.67 < 1 share @ 263.36 |
| 2026-09-04 | `MLYS` | cash | leftover split 18.67 < 1 share @ 28.00 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PBH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PCRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `APPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `APPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRWD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AXTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FIVN` | cash | leftover split 14.00 < 1 share @ 34.44 |
| 2026-09-18 | `ECO` | cash | leftover split 14.00 < 1 share @ 85.00 |
| 2026-09-18 | `RBRK` | cash | leftover split 14.00 < 1 share @ 108.55 |
| 2026-09-18 | `TH` | cash | leftover split 14.00 < 1 share @ 20.91 |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AMRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AXTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `META` | cash | leftover split 290.14 < 1 share @ 731.40 |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FORM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ARM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `FSLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AKAM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 500 | 2026-09-22 @ $0.58 | rank by cond; rank cond; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $290.14 |
| `FSLY` | 10 | 2026-09-22 @ $28.02 | rank by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $290.14 |
| `A` | 2 | 2026-09-23 @ $166.54 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $361.00 |
| `BFLY` | 36 | 2026-09-23 @ $9.90 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $361.00 |
| `DXCM` | 4 | 2026-09-23 @ $89.50 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.3; leftover $361.00 |
| `HALO` | 3 | 2026-09-23 @ $116.85 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+3.3; leftover $361.00 |
| `OMER` | 17 | 2026-09-23 @ $20.65 | rank by cond; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $361.00 |
| `ADMA` | 36 | 2026-09-23 @ $9.81 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $361.00 |
