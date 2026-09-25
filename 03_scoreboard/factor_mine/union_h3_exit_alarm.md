# Factor mine action — `union_h3_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hold 3d, sell next 09:30 if 🚨

Cash book **-20.81%** ($7,919) · signal-only (no cash/fees) was +1.45%. Starts YES **1/30**. Fills 199 · skips 293 · realized $-284.89.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- Early exit: sell at the next 09:30 if 🚨 prints, even inside the minimum hold.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,472.50.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 1543 | $1.07 | $20.17 | $+363.88 | $1,694.78 | ▲ +363.88 after sell → book $10,394.60; vs 09:30 mark -20.18 | exit 🚨 after 2 sess | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $1,508.20 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+6.7; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $1,364.00 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.8; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $1,159.31 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+8.3; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 52 | $4.05 | $2.15 | — | $946.56 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-12.3; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 25 | $8.46 | $2.06 | — | $733.00 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.4; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $550.10 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-7.2; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 65 | $3.24 | $2.19 | — | $337.32 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+0.3; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 41 | $5.07 | $2.11 | — | $127.33 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-4.7; leftover $211.85 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.33 | ▼ close $10,364.63 vs 09:30 $10,414.78 (session -14.37) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▼ 09:30 equity $10,233.31 vs yday $10,364.63 (-131.32) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,325.26 | ▼ -0.12 after sell → book $10,231.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,499.29 | ▼ -69.50 after sell → book $10,229.15; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,739.69 | ▲ +23.38 after sell → book $10,227.07; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,969.61 | ▼ -14.65 after sell → book $10,224.99; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,309.23 | ▲ +97.12 after sell → book $10,222.65; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,476.79 | ▼ -83.63 after sell → book $10,220.51; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $8,649.10 | ▼ -66.33 after sell → book $10,218.34; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,649.10 | ▲ close $10,219.20 vs 09:30 $10,233.31 (session +0.85) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,649.10 | ▲ 09:30 equity $10,244.20 vs yday $10,219.20 (+25.00) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $8,657.90 | ▼ -0.31 after sell → book $10,244.09; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $8,669.17 | ▼ -1.08 after sell → book $10,243.92; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $8,680.37 | ▼ -0.94 after sell → book $10,243.76; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,680.37 | ▼ close $10,240.81 vs 09:30 $10,244.20 (session -2.95) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,680.37 | ▼ 09:30 equity $10,240.75 vs yday $10,240.81 (-0.06) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 4 | $49.02 | $1.99 | $+7.51 | $8,874.46 | ▲ +7.51 after sell → book $10,238.76; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $9,024.37 | ▲ +5.71 after sell → book $10,237.22; vs 09:30 mark -1.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,235.87 | ▲ +6.80 after sell → book $10,235.21; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 52 | $3.92 | $2.17 | $-11.07 | $9,437.54 | ▼ -11.07 after sell → book $10,233.04; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 25 | $8.35 | $2.08 | $-6.90 | $9,644.21 | ▼ -6.90 after sell → book $10,230.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ELF` | 2 | $98.15 | $1.99 | $+11.41 | $9,838.52 | ▲ +11.41 after sell → book $10,228.97; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 65 | $3.20 | $2.21 | $-6.99 | $10,044.31 | ▼ -6.99 after sell → book $10,226.76; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 41 | $4.45 | $1.97 | $-29.50 | $10,224.79 | ▼ -29.50 after sell → book $10,224.79; vs 09:30 mark -1.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,948.52 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,672.35 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,410.52 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 221 | $5.77 | $2.85 | — | $5,132.50 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,854.37 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,578.16 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 730 | $1.75 | $9.42 | — | $1,291.24 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1278.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $132.91 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1278.10 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.91 | ▲ close $10,439.68 vs 09:30 $10,240.75 (session +239.85) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.91 | ▲ 09:30 equity $10,714.27 vs yday $10,439.68 (+274.59) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $121.66 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $16.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 6 | $2.47 | $0.17 | — | $106.68 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $16.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 8 | $1.93 | $0.18 | — | $91.06 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $16.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 12 | $1.32 | $0.19 | — | $75.02 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $16.61 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.02 | ▼ close $10,712.47 vs 09:30 $10,714.27 (session -1.14) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.02 | ▲ 09:30 equity $10,820.83 vs yday $10,712.47 (+108.36) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 8 | $159.50 | $2.03 | $+115.63 | $1,348.99 | ▲ +115.63 after sell → book $10,818.80; vs 09:30 mark -2.03 | exit 🚨 after 2 sess | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 1 | $13.33 | $0.16 | $+1.93 | $1,362.16 | ▲ +1.93 after sell → book $10,818.64; vs 09:30 mark -0.16 | exit 🚨 after 1 sess | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,362.16 | ▼ close $10,778.26 vs 09:30 $10,820.83 (session -40.38) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,362.16 | ▼ 09:30 equity $10,638.37 vs yday $10,778.26 (-139.89) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $2,619.81 | ▼ -18.63 after sell → book $10,636.18; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $3,959.79 | ▲ +63.82 after sell → book $10,634.12; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 61 | $20.47 | $2.19 | $-15.35 | $5,206.27 | ▼ -15.35 after sell → book $10,631.93; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 221 | $5.53 | $2.90 | $-58.79 | $6,425.50 | ▼ -58.79 after sell → book $10,629.03; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $7,801.95 | ▲ +98.31 after sell → book $10,626.83; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $9,189.57 | ▲ +111.41 after sell → book $10,624.69; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 730 | $1.90 | $9.55 | $+90.53 | $10,567.02 | ▲ +90.53 after sell → book $10,615.14; vs 09:30 mark -9.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,257.51 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.0; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 120 | $10.98 | $2.35 | — | $7,937.56 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+1.2; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,650.52 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+7.4; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.35 | $2.46 | — | $5,328.76 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 267 | $4.94 | $3.44 | — | $4,006.33 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+7.1; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,723.42 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+6.0; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 182 | $7.25 | $2.54 | — | $1,401.39 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1320.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3689 | $0.36 | $24.27 | — | $56.45 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-15.6; leftover $1320.88 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.45 | ▲ close $10,820.81 vs 09:30 $10,638.37 (session +246.94) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.45 | ▼ 09:30 equity $10,818.79 vs yday $10,820.81 (-2.02) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 6 | $2.41 | $0.18 | $-0.71 | $70.73 | ▼ -0.71 after sell → book $10,818.61; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 8 | $2.03 | $0.21 | $+0.42 | $86.76 | ▲ +0.42 after sell → book $10,818.40; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 12 | $1.60 | $0.25 | $+2.92 | $105.71 | ▲ +2.92 after sell → book $10,818.15; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 1 | $31.21 | $0.32 | — | $74.19 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $52.86 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 4 | $11.12 | $0.46 | — | $29.25 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $52.86 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.25 | ▲ close $11,103.97 vs 09:30 $10,818.79 (session +286.59) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.25 | ▼ 09:30 equity $11,093.43 vs yday $11,103.97 (-10.54) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 2 | $2.60 | $0.06 | — | $23.99 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ret5=+13.0; leftover $5.85 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.99 | ▼ close $11,081.23 vs 09:30 $11,093.43 (session -12.14) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.99 | ▼ 09:30 equity $11,028.05 vs yday $11,081.23 (-53.18) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 120 | $10.97 | $2.38 | $-5.93 | $1,338.01 | ▼ -5.93 after sell → book $11,025.67; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $60.52 | $2.07 | $-18.20 | $2,606.86 | ▼ -18.20 after sell → book $11,023.60; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.28 | $2.50 | $-16.02 | $3,912.60 | ▼ -16.02 after sell → book $11,021.09; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 267 | $4.95 | $3.50 | $-4.27 | $5,230.75 | ▼ -4.27 after sell → book $11,017.60; vs 09:30 mark -3.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $6,500.01 | ▼ -13.65 after sell → book $11,015.58; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 182 | $9.73 | $2.58 | $+446.24 | $8,268.29 | ▲ +446.24 after sell → book $11,013.00; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3689 | $0.36 | $25.16 | $-23.61 | $9,589.62 | ▼ -23.61 after sell → book $10,987.84; vs 09:30 mark -25.16 | exit 🚨 after 3 sess | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 38 | $41.74 | $2.10 | — | $8,001.40 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+2.4; leftover $1598.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 109 | $14.63 | $2.32 | — | $6,404.41 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+5.8; leftover $1598.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 48 | $32.90 | $2.13 | — | $4,823.08 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1598.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 102 | $15.66 | $2.30 | — | $3,223.46 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1598.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 20 | $79.42 | $2.05 | — | $1,633.01 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1598.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 6 | $252.24 | $2.01 | — | $117.56 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1598.27 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.56 | ▼ close $10,701.60 vs 09:30 $11,028.05 (session -273.33) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.56 | ▲ 09:30 equity $10,738.38 vs yday $10,701.60 (+36.78) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.68 | $2.18 | $-9.28 | $1,417.79 | ▼ -9.28 after sell → book $10,736.21; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 1 | $29.94 | $0.32 | $-1.91 | $1,447.40 | ▼ -1.91 after sell → book $10,735.88; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 4 | $10.82 | $0.46 | $-2.12 | $1,490.22 | ▼ -2.12 after sell → book $10,735.42; vs 09:30 mark -0.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,490.22 | ▲ close $10,813.11 vs 09:30 $10,738.38 (session +77.69) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,490.22 | ▲ 09:30 equity $10,870.86 vs yday $10,813.11 (+57.75) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 2 | $2.67 | $0.08 | $+0.00 | $1,495.48 | ▼ +0.00 after sell → book $10,870.78; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,495.48 | ▼ close $10,859.46 vs 09:30 $10,870.86 (session -11.32) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,495.48 | ▼ 09:30 equity $10,773.64 vs yday $10,859.46 (-85.82) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 38 | $42.10 | $2.13 | $+9.45 | $3,093.15 | ▲ +9.45 after sell → book $10,771.51; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 109 | $15.70 | $2.35 | $+111.96 | $4,802.10 | ▲ +111.96 after sell → book $10,769.16; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 48 | $32.42 | $2.16 | $-27.33 | $6,356.11 | ▼ -27.33 after sell → book $10,767.01; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 102 | $13.92 | $2.32 | $-182.10 | $7,773.62 | ▼ -182.10 after sell → book $10,764.68; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 20 | $78.84 | $2.07 | $-15.72 | $9,348.35 | ▼ -15.72 after sell → book $10,762.61; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 6 | $235.71 | $2.03 | $-103.22 | $10,760.58 | ▼ -103.22 after sell → book $10,760.58; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,760.58 | ▲ close $10,760.58 vs 09:30 $10,773.64 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,760.58 | ▲ 09:30 equity $10,760.58 vs yday $10,760.58 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,436.52 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,103.60 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 370 | $3.63 | $4.77 | — | $6,755.73 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 167 | $8.03 | $2.49 | — | $5,412.23 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,085.71 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.45 | $2.25 | — | $2,739.31 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,423.79 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1345.07 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $79.96 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1345.07 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.96 | ▼ close $10,500.96 vs 09:30 $10,760.58 (session -239.69) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.96 | ▲ 09:30 equity $10,505.06 vs yday $10,500.96 (+4.10) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 5 | $2.52 | $0.14 | — | $67.22 | — | hold 3d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $13.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $60.44 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $13.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 7 | $1.90 | $0.15 | — | $46.98 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $13.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $37.32 | — | hold 3d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $13.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 8 | $1.59 | $0.15 | — | $24.45 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $13.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $13.02 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $13.33 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.02 | ▲ close $10,535.68 vs 09:30 $10,505.06 (session +31.35) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.02 | ▲ 09:30 equity $10,567.72 vs yday $10,535.68 (+32.04) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `VSTM` | 167 | $8.20 | $2.53 | $+23.37 | $1,379.89 | ▲ +23.37 after sell → book $10,565.19; vs 09:30 mark -2.53 | exit 🚨 after 2 sess | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 7 | $2.00 | $0.18 | $+0.37 | $1,393.71 | ▲ +0.37 after sell → book $10,565.01; vs 09:30 mark -0.18 | exit 🚨 after 1 sess | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,393.71 | ▼ close $10,405.28 vs 09:30 $10,567.72 (session -159.73) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,393.71 | ▼ 09:30 equity $10,365.53 vs yday $10,405.28 (-39.75) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 25 | $53.16 | $2.09 | $+2.85 | $2,720.63 | ▲ +2.85 after sell → book $10,363.44; vs 09:30 mark -2.09 | exit 🚨 after 3 sess | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 31 | $42.01 | $2.10 | $-32.71 | $4,020.83 | ▼ -32.71 after sell → book $10,361.34; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 370 | $3.28 | $4.84 | $-139.12 | $5,229.59 | ▼ -139.12 after sell → book $10,356.49; vs 09:30 mark -4.85 | exit 🚨 after 3 sess | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 10 | $125.77 | $2.04 | $-70.86 | $6,485.25 | ▼ -70.86 after sell → book $10,354.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 87 | $15.16 | $2.28 | $-29.76 | $7,801.89 | ▼ -29.76 after sell → book $10,352.18; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 9 | $140.29 | $2.04 | $-54.90 | $9,062.51 | ▼ -54.90 after sell → book $10,350.14; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 80 | $15.46 | $2.25 | $-109.28 | $10,297.06 | ▼ -109.28 after sell → book $10,347.89; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,297.06 | ▼ close $10,345.73 vs 09:30 $10,365.53 (session -2.16) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,297.06 | ▼ 09:30 equity $10,344.92 vs yday $10,345.73 (-0.81) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 5 | $2.22 | $0.15 | $-1.79 | $10,308.01 | ▼ -1.79 after sell → book $10,344.77; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $10,314.04 | ▼ -0.75 after sell → book $10,344.69; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $10,321.78 | ▼ -1.92 after sell → book $10,344.59; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 8 | $1.53 | $0.17 | $-0.80 | $10,333.85 | ▼ -0.80 after sell → book $10,344.42; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $10,344.29 | ▼ -0.98 after sell → book $10,344.29; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,344.29 | ▲ close $10,344.29 vs 09:30 $10,344.92 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,344.29 | ▲ 09:30 equity $10,344.29 vs yday $10,344.29 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 79 | $16.28 | $2.23 | — | $9,055.94 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-1.1; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 473 | $2.73 | $6.10 | — | $7,758.55 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-3.0; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,515.50 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+8.3; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,362.48 | — | hold 3d, sell next 09:30 if 🚨; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,098.23 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+4.7; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,806.10 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+19.6; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 633 | $2.04 | $8.17 | — | $1,506.61 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1293.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 272 | $4.75 | $3.51 | — | $211.11 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1293.04 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.11 | ▼ close $10,298.59 vs 09:30 $10,344.29 (session -17.61) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.11 | ▼ 09:30 equity $9,994.19 vs yday $10,298.59 (-304.40) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 272 | $4.82 | $3.56 | $+11.97 | $1,518.58 | ▲ +11.97 after sell → book $9,990.63; vs 09:30 mark -3.56 | exit 🚨 after 1 sess | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,518.58 | ▼ close $9,857.54 vs 09:30 $9,994.19 (session -133.08) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,518.58 | ▲ 09:30 equity $9,900.92 vs yday $9,857.54 (+43.38) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,518.58 | ▼ close $9,739.53 vs 09:30 $9,900.92 (session -161.39) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,518.58 | ▲ 09:30 equity $9,789.39 vs yday $9,739.53 (+49.86) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 79 | $16.16 | $2.25 | $-13.96 | $2,792.97 | ▼ -13.96 after sell → book $9,787.14; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 473 | $2.72 | $6.19 | $-17.02 | $4,073.34 | ▼ -17.02 after sell → book $9,780.95; vs 09:30 mark -6.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 6 | $194.84 | $2.03 | $-76.04 | $5,240.35 | ▼ -76.04 after sell → book $9,778.92; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $6,218.53 | ▼ -174.84 after sell → book $9,776.89; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 8 | $147.79 | $2.03 | $-83.97 | $7,398.82 | ▼ -83.97 after sell → book $9,774.86; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 23 | $51.29 | $2.08 | $-114.54 | $8,576.41 | ▼ -114.54 after sell → book $9,772.78; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 633 | $1.89 | $8.28 | $-111.40 | $9,764.50 | ▼ -111.40 after sell → book $9,764.50; vs 09:30 mark -8.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,678.94 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+4.0; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,520.10 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ret5=+7.2; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 85 | $14.31 | $2.25 | — | $6,301.51 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+4.8; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $5,096.24 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+2.9; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 65 | $18.61 | $2.19 | — | $3,884.40 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 67 | $18.21 | $2.19 | — | $2,662.14 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-19.1; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $1,490.67 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1220.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 207 | $5.87 | $2.67 | — | $272.91 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1220.56 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $272.91 | ▲ close $9,940.26 vs 09:30 $9,789.39 (session +193.22) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.91 | ▲ 09:30 equity $10,103.66 vs yday $9,940.26 (+163.40) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $241.84 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $34.11 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $211.17 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $34.11 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 200 | $0.17 | $0.94 | — | $176.23 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $34.11 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 2 | $15.87 | $0.32 | — | $144.16 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $34.11 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.16 | ▲ close $10,157.55 vs 09:30 $10,103.66 (session +55.79) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.16 | ▲ 09:30 equity $10,185.80 vs yday $10,157.55 (+28.25) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 18 | $0.97 | $0.23 | — | $126.48 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $18.02 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 4 | $3.95 | $0.17 | — | $110.51 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $18.02 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $96.29 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $18.02 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.29 | ▼ close $10,108.82 vs 09:30 $10,185.80 (session -76.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.29 | ▲ 09:30 equity $10,176.58 vs yday $10,108.82 (+67.76) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,161.31 | ▼ -20.54 after sell → book $10,174.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $2,303.30 | ▼ -16.84 after sell → book $10,172.50; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 85 | $13.65 | $2.27 | $-60.61 | $3,461.29 | ▼ -60.61 after sell → book $10,170.24; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 33 | $36.70 | $2.11 | $+3.72 | $4,670.28 | ▲ +3.72 after sell → book $10,168.13; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 65 | $22.11 | $2.21 | $+223.11 | $6,105.22 | ▲ +223.11 after sell → book $10,165.92; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 67 | $20.55 | $2.21 | $+152.38 | $7,479.86 | ▲ +152.38 after sell → book $10,163.71; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+170.83 | $8,822.15 | ▲ +170.83 after sell → book $10,161.64; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 207 | $5.62 | $2.71 | $-57.13 | $9,982.78 | ▼ -57.13 after sell → book $10,158.93; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `IOVA` | 3 | $10.43 | $0.34 | $-0.12 | $10,013.73 | ▼ -0.12 after sell → book $10,158.59; vs 09:30 mark -0.34 | exit 🚨 after 2 sess | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 4 | $7.84 | $0.35 | $+0.34 | $10,044.74 | ▲ +0.34 after sell → book $10,158.24; vs 09:30 mark -0.35 | exit 🚨 after 2 sess | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,937.64 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+6.5; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,777.04 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-5.8; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,531.39 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+7.6; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 134 | $9.31 | $2.39 | — | $5,281.46 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 93 | $13.47 | $2.27 | — | $4,026.01 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1131 | $1.11 | $14.59 | — | $2,756.01 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 125 | $9.99 | $2.37 | — | $1,504.90 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1255.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 687 | $1.82 | $8.86 | — | $242.26 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1255.59 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.26 | ▼ close $10,004.97 vs 09:30 $10,176.58 (session -116.74) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.26 | ▼ 09:30 equity $9,979.05 vs yday $10,004.97 (-25.92) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 200 | $0.16 | $0.96 | $-3.90 | $273.30 | ▼ -3.90 after sell → book $9,978.08; vs 09:30 mark -0.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 58 | $0.58 | $0.51 | — | $239.15 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $34.16 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.15 | ▲ close $10,195.05 vs 09:30 $9,979.05 (session +217.48) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.15 | ▼ 09:30 equity $10,185.52 vs yday $10,195.05 (-9.53) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 2 | $17.10 | $0.37 | $+1.77 | $272.98 | ▲ +1.77 after sell → book $10,185.16; vs 09:30 mark -0.36 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 18 | $0.89 | $0.23 | $-1.90 | $288.77 | ▼ -1.90 after sell → book $10,184.92; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 4 | $4.10 | $0.20 | $+0.23 | $304.97 | ▲ +0.23 after sell → book $10,184.73; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $319.64 | ▲ +0.45 after sell → book $10,184.55; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $291.57 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+7.0; leftover $53.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 6 | $7.95 | $0.49 | — | $243.37 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+12.4; leftover $53.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 5 | $9.81 | $0.51 | — | $193.82 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+4.0; leftover $53.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 2 | $20.25 | $0.41 | — | $152.91 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+15.0; leftover $53.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 2 | $20.65 | $0.42 | — | $111.19 | — | hold 3d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $53.27 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.19 | ▼ close $9,818.47 vs 09:30 $10,185.52 (session -363.97) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.19 | ▼ 09:30 equity $9,738.37 vs yday $9,818.47 (-80.10) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,256.81 | ▲ +38.52 after sell → book $9,736.33; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $2,378.41 | ▼ -39.00 after sell → book $9,734.32; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $3,603.81 | ▼ -20.25 after sell → book $9,732.26; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 134 | $8.67 | $2.42 | $-90.58 | $4,763.16 | ▼ -90.58 after sell → book $9,729.84; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 93 | $12.26 | $2.29 | $-117.56 | $5,901.05 | ▼ -117.56 after sell → book $9,727.55; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1131 | $1.05 | $14.79 | $-97.24 | $7,073.81 | ▼ -97.24 after sell → book $9,712.76; vs 09:30 mark -14.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 125 | $9.80 | $2.40 | $-28.51 | $8,296.41 | ▼ -28.51 after sell → book $9,710.36; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 687 | $1.73 | $8.99 | $-86.55 | $9,472.50 | ▼ -86.55 after sell → book $9,701.38; vs 09:30 mark -8.98 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,472.50 | ▲ close $9,703.73 vs 09:30 $9,738.37 (session +2.35) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,623.43 | ▲ 09:30 equity $7,899.60 vs yday $7,898.64 (+0.96) | 09:30 open · cash $6,623.43 (unchanged overnight, no fees) · equity $7,899.60 vs prior close $7,898.64 (+0.96) · 13 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; APPS×13 yday $10.88 → 09:30 $10.88 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×286 yday $0.53 → 09:30 $0.53 +0.00; DLO×11 yday $13.88 → 09:30 $13.88 +0.00; EL×1 yday $95.37 → 09:30 $95.37 +0.00; FTRE×2 yday $20.02 → 09:30 $20.02 +0.00; MKC×3 yday $47.82 → 09:30 $47.82 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; PACS×4 yday $41.46 → 09:30 $41.46 +0.00; PGEN×5 yday $7.70 → 09:30 $7.70 +0.00; TDC×5 yday $29.46 → 09:30 $29.46 +0.00; USFD×1 yday $93.82 → 09:30 $93.82 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $5,817.57 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+0.8; leftover $946.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 8 | $115.36 | $2.01 | — | $4,892.67 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+5.1; leftover $946.20 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 24 | $38.51 | $2.06 | — | $3,966.37 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+4.7; leftover $946.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 123 | $7.65 | $2.36 | — | $3,023.06 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+5.2; leftover $946.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 36 | $26.27 | $2.10 | — | $2,075.24 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $946.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 11 | $83.76 | $2.02 | — | $1,151.86 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $946.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 104 | $9.05 | $2.30 | — | $208.36 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-27.1; leftover $946.20 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.36 | ▲ close $7,919.08 vs 09:30 $7,899.60 (session +34.33) | 16:00 close · cash $208.36 · equity $7,919.08 vs 09:30 $7,899.60 (+19.48; session marks +34.33) · 20 name(s) marked open→close (per-name table). ADMA×4 09:30 $9.52 → close $9.52 +0.00; APPS×13 09:30 $10.88 → close $10.88 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×286 09:30 $0.53 → close $0.53 +0.00; DLO×11 09:30 $13.88 → close $13.88 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FTRE×2 09:30 $20.02 → close $20.02 +0.00; MKC×3 09:30 $47.82 → close $47.82 -0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; PACS×4 09:30 $41.46 → close $41.46 -0.00; PGEN×5 09:30 $7.70 → close $7.70 -0.00; TDC×5 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×8 09:30 $115.36 → close $113.90 -11.68; BLFS×24 09:30 $38.51 → close $38.49 -0.48; MRVI×123 09:30 $7.65 → close $7.60 -6.15; WRBY×36 09:30 $26.27 → close $26.71 +15.84; TXG×11 09:30 $83.76 → close $85.71 +21.45; AEHL×104 09:30 $9.05 → close $9.36 +32.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 16.61 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 16.61 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 16.61 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 16.61 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 5.85 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 5.85 < 1 share @ 14.42 |
| 2026-08-27 | `KURA` | cash | leftover split 5.85 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 5.85 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 34.11 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 34.11 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 34.11 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 34.11 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 18.02 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 18.02 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 18.02 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 18.02 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 18.02 < 1 share @ 34.44 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 34.16 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 53.27 < 1 share @ 116.85 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 58 | 2026-09-22 @ $0.58 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $34.16 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+7.0; leftover $53.27 |
| `PGEN` | 6 | 2026-09-23 @ $7.95 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+12.4; leftover $53.27 |
| `ADMA` | 5 | 2026-09-23 @ $9.81 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+4.0; leftover $53.27 |
| `FTRE` | 2 | 2026-09-23 @ $20.25 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+15.0; leftover $53.27 |
| `OMER` | 2 | 2026-09-23 @ $20.65 | hold 3d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $53.27 |
