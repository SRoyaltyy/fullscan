# Factor mine action — `union_h5_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hold 5d, sell next 09:30 if 🚨

Cash book **-11.63%** ($8,837) · signal-only (no cash/fees) was +17.52%. Starts YES **6/30**. Fills 275 · skips 456 · realized $+341.96.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- Early exit: sell at the next 09:30 if 🚨 prints, even inside the minimum hold.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $141.22.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 1543 | $1.07 | $20.17 | $+363.88 | $1,694.78 | ▲ +363.88 after sell → book $10,394.60; vs 09:30 mark -20.18 | exit 🚨 after 2 sess | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $1,508.20 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+6.7; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $1,364.00 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.8; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $1,159.31 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+8.3; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 52 | $4.05 | $2.15 | — | $946.56 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-12.3; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 25 | $8.46 | $2.06 | — | $733.00 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.4; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $550.10 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=-7.2; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 65 | $3.24 | $2.19 | — | $337.32 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+0.3; leftover $211.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 41 | $5.07 | $2.11 | — | $127.33 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=-4.7; leftover $211.85 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.33 | ▼ close $10,364.63 vs 09:30 $10,414.78 (session -14.37) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▼ 09:30 equity $10,233.31 vs yday $10,364.63 (-131.32) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.33 | ▲ close $10,323.08 vs 09:30 $10,233.31 (session +89.76) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▲ 09:30 equity $10,454.84 vs yday $10,323.08 (+131.76) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.33 | ▲ close $10,649.12 vs 09:30 $10,454.84 (session +194.29) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▼ 09:30 equity $10,584.39 vs yday $10,649.12 (-64.73) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,298.06 | ▼ -27.32 after sell → book $10,582.32; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,442.39 | ▼ -99.20 after sell → book $10,580.23; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,713.75 | ▲ +54.34 after sell → book $10,578.14; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $5,002.91 | ▲ +44.60 after sell → book $10,576.06; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,467.62 | ▲ +222.19 after sell → book $10,573.72; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,753.20 | ▲ +34.39 after sell → book $10,571.58; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $8,975.86 | ▼ -15.98 after sell → book $10,569.42; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 54 | $20.55 | $2.15 | — | $7,864.01 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,769.86 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,652.61 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 194 | $5.77 | $2.57 | — | $4,530.66 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,409.59 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $2,311.18 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 641 | $1.75 | $8.27 | — | $1,181.16 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1121.98 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $167.37 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1121.98 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.37 | ▲ close $10,756.85 vs 09:30 $10,584.39 (session +210.88) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.37 | ▲ 09:30 equity $11,026.36 vs yday $10,756.85 (+269.51) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $178.93 | ▲ +2.46 after sell → book $11,026.22; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $190.03 | ▼ -1.24 after sell → book $11,026.05; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $203.13 | ▲ +0.96 after sell → book $11,025.87; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $185.76 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $25.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $163.27 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $25.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $138.29 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $25.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $112.91 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $25.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $87.52 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $25.39 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.52 | ▲ close $11,095.77 vs 09:30 $11,026.36 (session +71.18) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.52 | ▲ 09:30 equity $11,180.78 vs yday $11,095.77 (+85.01) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.89 | $1.99 | $+6.99 | $281.09 | ▲ +6.99 after sell → book $11,178.79; vs 09:30 mark -1.99 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.07 | $1.54 | $+6.33 | $431.62 | ▲ +6.33 after sell → book $11,177.25; vs 09:30 mark -1.54 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $210.00 | $2.01 | $+3.29 | $639.61 | ▲ +3.29 after sell → book $11,175.23; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 52 | $4.62 | $2.17 | $+25.59 | $877.94 | ▲ +25.59 after sell → book $11,173.07; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 25 | $9.26 | $2.08 | $+15.85 | $1,107.36 | ▲ +15.85 after sell → book $11,170.98; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $102.20 | $2.02 | $+19.49 | $1,309.74 | ▲ +19.49 after sell → book $11,168.97; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `NB` | 41 | $4.54 | $2.00 | $-26.05 | $1,493.67 | ▼ -26.05 after sell → book $11,166.96; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 7 | $159.50 | $2.03 | $+100.68 | $2,608.14 | ▲ +100.68 after sell → book $11,164.93; vs 09:30 mark -2.03 | exit 🚨 after 2 sess | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 2 | $13.33 | $0.29 | $+3.88 | $2,634.51 | ▲ +3.88 after sell → book $11,164.64; vs 09:30 mark -0.29 | exit 🚨 after 1 sess | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,634.51 | ▼ close $11,130.22 vs 09:30 $11,180.78 (session -34.42) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,634.51 | ▼ 09:30 equity $11,008.41 vs yday $11,130.22 (-121.81) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `DNN` | 65 | $3.55 | $2.21 | $+15.76 | $2,863.05 | ▲ +15.76 after sell → book $11,006.20; vs 09:30 mark -2.21 | dropped from list after 6 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 15 | $23.77 | $2.04 | — | $2,504.47 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.0; leftover $357.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 32 | $10.98 | $2.09 | — | $2,151.02 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+1.2; leftover $357.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 5 | $61.19 | $2.00 | — | $1,843.07 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+7.4; leftover $357.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 42 | $8.35 | $2.12 | — | $1,490.25 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+8.0; leftover $357.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 72 | $4.94 | $2.21 | — | $1,132.37 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+7.1; leftover $357.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 49 | $7.25 | $2.14 | — | $774.98 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $357.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 999 | $0.36 | $6.57 | — | $410.76 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-15.6; leftover $357.88 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $410.76 | ▲ close $11,375.10 vs 09:30 $11,008.41 (session +388.05) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $410.76 | ▼ 09:30 equity $11,220.88 vs yday $11,375.10 (-154.22) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 4 | $31.21 | $1.26 | — | $284.66 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $136.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 12 | $11.12 | $1.37 | — | $149.85 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $136.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.85 | ▼ close $11,196.52 vs 09:30 $11,220.88 (session -21.73) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.85 | ▲ 09:30 equity $11,221.74 vs yday $11,196.52 (+25.22) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 54 | $20.93 | $2.17 | $+16.20 | $1,277.90 | ▲ +16.20 after sell → book $11,219.57; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 12 | $95.52 | $2.05 | $+50.05 | $2,422.10 | ▲ +50.05 after sell → book $11,217.52; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 54 | $21.31 | $2.17 | $+31.32 | $3,570.66 | ▲ +31.32 after sell → book $11,215.35; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 194 | $5.49 | $2.61 | $-59.51 | $4,633.11 | ▼ -59.51 after sell → book $11,212.74; vs 09:30 mark -2.61 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 57 | $21.47 | $2.18 | $+100.54 | $5,854.72 | ▲ +100.54 after sell → book $11,210.56; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 37 | $32.32 | $2.12 | $+95.31 | $7,048.44 | ▲ +95.31 after sell → book $11,208.44; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 641 | $1.91 | $8.39 | $+85.91 | $8,264.36 | ▲ +85.91 after sell → book $11,200.05; vs 09:30 mark -8.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 39 | $41.44 | $2.11 | — | $6,646.09 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+3.1; leftover $1652.87 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 114 | $14.42 | $2.33 | — | $4,999.88 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+7.1; leftover $1652.87 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 635 | $2.60 | $8.19 | — | $3,340.69 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ret5=+13.0; leftover $1652.87 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 127 | $12.98 | $2.37 | — | $1,689.86 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1652.87 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 170 | $9.68 | $2.50 | — | $41.76 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1652.87 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.76 | ▲ close $11,293.58 vs 09:30 $11,221.74 (session +111.03) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.76 | ▼ 09:30 equity $11,293.31 vs yday $11,293.58 (-0.27) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $58.01 | ▼ -1.12 after sell → book $11,293.12; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.35 | $0.28 | $-1.76 | $81.23 | ▼ -1.76 after sell → book $11,292.83; vs 09:30 mark -0.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.06 | $0.33 | $+1.07 | $107.68 | ▲ +1.07 after sell → book $11,292.51; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 19 | $1.82 | $0.42 | $+8.77 | $141.84 | ▲ +8.77 after sell → book $11,292.08; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 999 | $0.36 | $6.82 | $-6.40 | $499.65 | ▼ -6.40 after sell → book $11,285.26; vs 09:30 mark -6.82 | exit 🚨 after 3 sess | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 3 | $32.90 | $1.00 | — | $399.96 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $124.91 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 7 | $15.66 | $1.12 | — | $289.22 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $124.91 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 1 | $79.42 | $0.80 | — | $209.00 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $124.91 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.00 | ▼ close $11,009.17 vs 09:30 $11,293.31 (session -273.18) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.00 | ▲ 09:30 equity $11,071.57 vs yday $11,009.17 (+62.40) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.00 | ▲ close $11,114.61 vs 09:30 $11,071.57 (session +43.04) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.00 | ▲ 09:30 equity $11,280.49 vs yday $11,114.61 (+165.88) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 15 | $23.94 | $2.06 | $-1.54 | $566.05 | ▼ -1.54 after sell → book $11,278.43; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 32 | $10.42 | $2.11 | $-22.11 | $897.38 | ▼ -22.11 after sell → book $11,276.33; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 5 | $63.00 | $2.02 | $+5.02 | $1,210.36 | ▲ +5.02 after sell → book $11,274.30; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 42 | $8.25 | $2.14 | $-8.45 | $1,554.72 | ▼ -8.45 after sell → book $11,272.17; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 72 | $4.64 | $2.23 | $-26.03 | $1,886.57 | ▼ -26.03 after sell → book $11,269.94; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 49 | $10.77 | $2.16 | $+168.19 | $2,412.15 | ▲ +168.19 after sell → book $11,267.78; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,412.15 | ▼ close $11,196.94 vs 09:30 $11,280.49 (session -70.84) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,412.15 | ▼ 09:30 equity $11,167.61 vs yday $11,196.94 (-29.33) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `AVBP` | 4 | $30.33 | $1.25 | $-6.03 | $2,532.22 | ▼ -6.03 after sell → book $11,166.36; vs 09:30 mark -1.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `FLNC` | 12 | $10.38 | $1.30 | $-11.49 | $2,655.54 | ▼ -11.49 after sell → book $11,165.06; vs 09:30 mark -1.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,655.54 | ▲ close $11,182.77 vs 09:30 $11,167.61 (session +17.71) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,655.54 | ▲ 09:30 equity $11,258.55 vs yday $11,182.77 (+75.78) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 39 | $42.43 | $2.13 | $+34.37 | $4,308.18 | ▲ +34.37 after sell → book $11,256.42; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 635 | $2.49 | $8.31 | $-86.35 | $5,881.02 | ▼ -86.35 after sell → book $11,248.11; vs 09:30 mark -8.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 127 | $13.25 | $2.41 | $+29.51 | $7,561.36 | ▲ +29.51 after sell → book $11,245.71; vs 09:30 mark -2.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 170 | $9.68 | $2.54 | $-5.04 | $9,204.42 | ▼ -5.04 after sell → book $11,243.17; vs 09:30 mark -2.54 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $7,933.24 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1314.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $6,643.26 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1314.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 362 | $3.63 | $4.67 | — | $5,324.53 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1314.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 163 | $8.03 | $2.48 | — | $4,013.16 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1314.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $2,819.09 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1314.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,503.57 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1314.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $193.29 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1314.92 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.29 | ▼ close $10,980.13 vs 09:30 $11,258.55 (session -245.49) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.29 | ▲ 09:30 equity $10,985.34 vs yday $10,980.13 (+5.21) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 114 | $15.00 | $2.36 | $+61.42 | $1,900.92 | ▲ +61.42 after sell → book $10,982.97; vs 09:30 mark -2.37 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 3 | $33.86 | $1.04 | $+0.84 | $2,001.46 | ▲ +0.84 after sell → book $10,981.93; vs 09:30 mark -1.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 7 | $13.56 | $0.99 | $-16.81 | $2,095.39 | ▼ -16.81 after sell → book $10,980.94; vs 09:30 mark -0.99 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 1 | $79.55 | $0.82 | $-1.49 | $2,174.12 | ▼ -1.49 after sell → book $10,980.12; vs 09:30 mark -0.82 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 143 | $2.52 | $2.42 | — | $1,811.34 | — | hold 5d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $362.35 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 54 | $6.71 | $2.15 | — | $1,446.85 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $362.35 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 190 | $1.90 | $2.56 | — | $1,083.29 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $362.35 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 75 | $4.78 | $2.21 | — | $722.57 | — | hold 5d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $362.35 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 227 | $1.59 | $2.93 | — | $358.72 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $362.35 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 31 | $11.31 | $2.08 | — | $6.02 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $362.35 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.02 | ▼ close $10,960.32 vs 09:30 $10,985.34 (session -5.45) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.02 | ▼ 09:30 equity $10,946.15 vs yday $10,960.32 (-14.17) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `VSTM` | 163 | $8.20 | $2.52 | $+22.71 | $1,340.11 | ▲ +22.71 after sell → book $10,943.64; vs 09:30 mark -2.51 | exit 🚨 after 2 sess | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 190 | $2.00 | $2.60 | $+13.84 | $1,717.51 | ▲ +13.84 after sell → book $10,941.04; vs 09:30 mark -2.60 | exit 🚨 after 1 sess | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,717.51 | ▼ close $10,804.64 vs 09:30 $10,946.15 (session -136.40) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,717.51 | ▼ 09:30 equity $10,755.85 vs yday $10,804.64 (-48.79) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $2,991.26 | ▲ +2.58 after sell → book $10,753.77; vs 09:30 mark -2.08 | exit 🚨 after 3 sess | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 362 | $3.28 | $4.74 | $-136.11 | $4,173.88 | ▼ -136.11 after sell → book $10,749.03; vs 09:30 mark -4.74 | exit 🚨 after 3 sess | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,173.88 | ▼ close $10,550.41 vs 09:30 $10,755.85 (session -198.62) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,173.88 | ▼ 09:30 equity $10,461.50 vs yday $10,550.41 (-88.91) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,173.88 | ▼ close $10,411.62 vs 09:30 $10,461.50 (session -49.88) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,173.88 | ▲ 09:30 equity $10,463.57 vs yday $10,411.62 (+51.95) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 30 | $41.30 | $2.10 | $-53.08 | $5,410.78 | ▼ -53.08 after sell → book $10,461.47; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 9 | $122.40 | $2.04 | $-94.50 | $6,510.35 | ▼ -94.50 after sell → book $10,459.43; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 9 | $137.91 | $2.04 | $-76.41 | $7,749.45 | ▼ -76.41 after sell → book $10,457.39; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 78 | $14.06 | $2.25 | $-215.85 | $8,843.89 | ▼ -215.85 after sell → book $10,455.15; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 67 | $16.28 | $2.19 | — | $7,750.94 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-1.1; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 404 | $2.73 | $5.21 | — | $6,642.80 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-3.0; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $5,606.60 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+8.3; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $4,618.01 | — | hold 5d, sell next 09:30 if 🚨; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $3,511.54 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+4.7; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 19 | $56.09 | $2.05 | — | $2,443.78 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+19.6; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 541 | $2.04 | $6.98 | — | $1,333.16 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1105.49 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 232 | $4.75 | $2.99 | — | $228.17 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1105.49 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $228.17 | ▼ close $10,415.46 vs 09:30 $10,463.57 (session -14.24) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $228.17 | ▼ 09:30 equity $10,175.22 vs yday $10,415.46 (-240.24) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 143 | $2.15 | $2.45 | $-57.78 | $533.17 | ▼ -57.78 after sell → book $10,172.77; vs 09:30 mark -2.45 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 54 | $5.93 | $2.17 | $-46.44 | $851.22 | ▼ -46.44 after sell → book $10,170.60; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 75 | $4.13 | $2.24 | $-53.20 | $1,158.73 | ▼ -53.20 after sell → book $10,168.36; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 227 | $1.59 | $2.98 | $-5.90 | $1,516.68 | ▼ -5.90 after sell → book $10,165.38; vs 09:30 mark -2.98 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 31 | $10.73 | $2.10 | $-22.17 | $1,847.21 | ▼ -22.17 after sell → book $10,163.28; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 232 | $4.82 | $3.04 | $+10.21 | $2,962.41 | ▲ +10.21 after sell → book $10,160.24; vs 09:30 mark -3.04 | exit 🚨 after 1 sess | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,962.41 | ▼ close $10,048.64 vs 09:30 $10,175.22 (session -111.60) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,962.41 | ▲ 09:30 equity $10,085.44 vs yday $10,048.64 (+36.80) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,962.41 | ▼ close $9,948.45 vs 09:30 $10,085.44 (session -136.99) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,962.41 | ▲ 09:30 equity $9,989.92 vs yday $9,948.45 (+41.47) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 1 | $270.89 | $1.99 | — | $2,689.53 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+4.0; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 4 | $77.12 | $2.00 | — | $2,379.04 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ret5=+7.2; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 25 | $14.31 | $2.06 | — | $2,019.23 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+4.8; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 10 | $36.46 | $2.02 | — | $1,652.61 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+2.9; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 19 | $18.61 | $2.05 | — | $1,296.97 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 20 | $18.21 | $2.05 | — | $930.72 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-19.1; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 5 | $68.79 | $2.00 | — | $584.77 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $370.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 63 | $5.87 | $2.18 | — | $212.78 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $370.30 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.78 | ▲ close $10,091.15 vs 09:30 $9,989.92 (session +117.59) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.78 | ▲ 09:30 equity $10,307.09 vs yday $10,091.15 (+215.94) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $192.07 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $26.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $169.06 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $26.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 156 | $0.17 | $0.73 | — | $141.81 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $26.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $125.78 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $26.60 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.78 | ▼ close $10,297.68 vs 09:30 $10,307.09 (session -8.07) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.78 | ▲ 09:30 equity $10,341.16 vs yday $10,297.68 (+43.48) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 67 | $16.93 | $2.21 | $+39.15 | $1,257.87 | ▲ +39.15 after sell → book $10,338.94; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 404 | $2.68 | $5.29 | $-30.70 | $2,335.31 | ▼ -30.70 after sell → book $10,333.66; vs 09:30 mark -5.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 5 | $197.76 | $2.02 | $-49.43 | $3,322.08 | ▼ -49.43 after sell → book $10,331.63; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 6 | $150.47 | $2.03 | $-87.80 | $4,222.87 | ▼ -87.80 after sell → book $10,329.60; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 7 | $152.71 | $2.03 | $-39.53 | $5,289.81 | ▼ -39.53 after sell → book $10,327.57; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 19 | $55.80 | $2.07 | $-9.62 | $6,347.94 | ▼ -9.62 after sell → book $10,325.50; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 541 | $1.90 | $7.08 | $-89.80 | $7,368.77 | ▼ -89.80 after sell → book $10,318.43; vs 09:30 mark -7.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 8 | $108.55 | $2.01 | — | $6,498.35 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+21.3; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 4 | $209.52 | $2.00 | — | $5,658.27 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $4,777.79 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 10 | $85.00 | $2.02 | — | $3,925.77 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+18.3; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 26 | $34.44 | $2.07 | — | $3,028.26 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+14.0; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 949 | $0.97 | $12.05 | — | $2,095.68 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 233 | $3.95 | $3.01 | — | $1,172.32 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $921.10 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 65 | $14.07 | $2.19 | — | $255.59 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $921.10 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $255.59 | ▼ close $10,096.18 vs 09:30 $10,341.16 (session -194.90) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $255.59 | ▲ 09:30 equity $10,208.30 vs yday $10,096.18 (+112.12) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IOVA` | 2 | $10.43 | $0.23 | $-0.09 | $276.21 | ▼ -0.09 after sell → book $10,208.06; vs 09:30 mark -0.24 | exit 🚨 after 2 sess | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 3 | $7.84 | $0.26 | $+0.25 | $299.47 | ▲ +0.25 after sell → book $10,207.80; vs 09:30 mark -0.26 | exit 🚨 after 2 sess | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 4 | $9.31 | $0.38 | — | $261.84 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $37.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 2 | $13.47 | $0.28 | — | $234.62 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $37.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 33 | $1.11 | $0.47 | — | $197.52 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $37.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 3 | $9.99 | $0.31 | — | $167.24 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $37.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 20 | $1.82 | $0.42 | — | $130.32 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $37.43 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.32 | ▲ close $10,350.24 vs 09:30 $10,208.30 (session +144.30) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.32 | ▼ 09:30 equity $10,343.66 vs yday $10,350.24 (-6.58) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 28 | $0.58 | $0.25 | — | $113.83 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $16.29 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.83 | ▲ close $10,353.98 vs 09:30 $10,343.66 (session +10.57) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.83 | ▲ 09:30 equity $10,582.96 vs yday $10,353.98 (+228.98) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 1 | $270.66 | $2.01 | $-4.24 | $382.48 | ▼ -4.24 after sell → book $10,580.95; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 4 | $73.61 | $2.02 | $-18.06 | $674.90 | ▼ -18.06 after sell → book $10,578.93; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 25 | $13.12 | $2.08 | $-33.90 | $1,000.81 | ▼ -33.90 after sell → book $10,576.84; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 10 | $38.04 | $2.04 | $+11.74 | $1,379.17 | ▲ +11.74 after sell → book $10,574.80; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 19 | $23.00 | $2.07 | $+79.30 | $1,814.11 | ▲ +79.30 after sell → book $10,572.74; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 20 | $23.30 | $2.07 | $+97.68 | $2,278.04 | ▲ +97.68 after sell → book $10,570.67; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 5 | $76.47 | $2.02 | $+34.37 | $2,658.36 | ▲ +34.37 after sell → book $10,568.64; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 63 | $5.53 | $2.20 | $-25.80 | $3,004.55 | ▼ -25.80 after sell → book $10,566.44; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 4 | $89.50 | $2.00 | — | $2,644.55 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+5.3; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 2 | $166.54 | $2.00 | — | $2,309.47 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.3; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 3 | $116.85 | $2.00 | — | $1,956.92 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+3.3; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 13 | $27.79 | $2.03 | — | $1,593.62 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+7.0; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 47 | $7.95 | $2.13 | — | $1,217.84 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+12.4; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 38 | $9.81 | $2.10 | — | $842.96 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+4.0; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 18 | $20.25 | $2.04 | — | $476.42 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+15.0; leftover $375.57 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 18 | $20.65 | $2.04 | — | $102.67 | — | hold 5d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $375.57 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.67 | ▼ close $10,488.60 vs 09:30 $10,582.96 (session -61.49) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.67 | ▼ 09:30 equity $10,412.05 vs yday $10,488.60 (-76.55) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 156 | $0.15 | $0.74 | $-4.59 | $125.33 | ▼ -4.59 after sell → book $10,411.32; vs 09:30 mark -0.73 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 1 | $16.07 | $0.18 | $-0.15 | $141.22 | ▼ -0.15 after sell → book $10,411.13; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.22 | ▼ close $10,382.43 vs 09:30 $10,412.05 (session -28.71) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.82 | ▲ 09:30 equity $9,158.42 vs yday $9,132.94 (+25.48) | 09:30 open · cash $996.82 (unchanged overnight, no fees) · equity $9,158.42 vs prior close $9,132.94 (+25.48) · 16 name(s) re-marked at the open (per-name table). A×8 yday $172.84 → 09:30 $171.98 -6.88; ADMA×138 yday $9.52 → 09:30 $9.52 +0.00; APPS×2 yday $10.88 → 09:30 $10.88 +0.00; BHVN×1 yday $13.19 → 09:30 $13.19 +0.00; BNC×4 yday $6.26 → 09:30 $6.26 +0.00; BTDR×1 yday $12.15 → 09:30 $12.15 +0.00; CYPH×5 yday $4.08 → 09:30 $4.00 -0.37; DEFT×52 yday $0.53 → 09:30 $0.53 +0.00; DLO×2 yday $13.88 → 09:30 $13.88 +0.00; DXCM×15 yday $87.47 → 09:30 $87.47 +0.00; EYPT×6 yday $3.65 → 09:30 $3.65 +0.00; FTRE×67 yday $20.02 → 09:30 $20.02 +0.00; HALO×11 yday $115.22 → 09:30 $115.36 +1.54; MGTX×1 yday $11.05 → 09:30 $11.05 +0.00; OMER×65 yday $20.13 → 09:30 $20.61 +31.20; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 4 | $38.51 | $1.55 | — | $841.23 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+4.7; leftover $166.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 21 | $7.65 | $1.67 | — | $678.91 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+5.2; leftover $166.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 6 | $26.27 | $1.59 | — | $519.69 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $166.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 1 | $83.76 | $0.84 | — | $435.09 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $166.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 18 | $9.05 | $1.68 | — | $270.51 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-27.1; leftover $166.14 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $270.51 | ▼ close $9,116.66 vs 09:30 $9,158.42 (session -34.41) | 16:00 close · cash $270.51 · equity $9,116.66 vs 09:30 $9,158.42 (-41.76; session marks -34.41) · 21 name(s) marked open→close (per-name table). A×8 09:30 $171.98 → close $172.79 +6.48; ADMA×138 09:30 $9.52 → close $9.52 +0.00; APPS×2 09:30 $10.88 → close $10.88 +0.00; BHVN×1 09:30 $13.19 → close $13.19 -0.00; BNC×4 09:30 $6.26 → close $6.26 +0.00; BTDR×1 09:30 $12.15 → close $12.15 -0.00; CYPH×5 09:30 $4.00 → close $4.12 +0.58; DEFT×52 09:30 $0.53 → close $0.53 +0.00; DLO×2 09:30 $13.88 → close $13.88 +0.00; DXCM×15 09:30 $87.47 → close $87.47 +0.00; EYPT×6 09:30 $3.65 → close $3.65 +0.00; FTRE×67 09:30 $20.02 → close $20.02 +0.00; HALO×11 09:30 $115.36 → close $113.90 -16.06; MGTX×1 09:30 $11.05 → close $11.05 +0.00; OMER×65 09:30 $20.61 → close $20.08 -34.45; TDC×1 09:30 $29.46 → close $29.46 -0.00; BLFS×4 09:30 $38.51 → close $38.49 -0.08; MRVI×21 09:30 $7.65 → close $7.60 -1.05; WRBY×6 09:30 $26.27 → close $26.71 +2.64; TXG×1 09:30 $83.76 → close $85.71 +1.95; AEHL×18 09:30 $9.05 → close $9.36 +5.58 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $270.51 | ▼ 09:30 equity $8,991.74 vs yday $9,116.66 (-124.92) | 09:30 open · cash $270.51 (unchanged overnight, no fees) · equity $8,991.74 vs prior close $9,116.66 (-124.92) · 21 name(s) re-marked at the open (per-name table). A×8 yday $172.79 → 09:30 $170.00 -22.32; ADMA×138 yday $9.52 → 09:30 $9.38 -19.32; AEHL×18 yday $9.36 → 09:30 $9.01 -6.30; APPS×2 yday $10.88 → 09:30 $11.00 +0.24; BHVN×1 yday $13.19 → 09:30 $12.82 -0.37; BLFS×4 yday $38.49 → 09:30 $38.13 -1.44; BNC×4 yday $6.26 → 09:30 $5.90 -1.44; BTDR×1 yday $12.15 → 09:30 $11.37 -0.78; CYPH×5 yday $4.12 → 09:30 $4.03 -0.44; DEFT×52 yday $0.53 → 09:30 $0.51 -1.36; DLO×2 yday $13.88 → 09:30 $13.91 +0.06; DXCM×15 yday $87.47 → 09:30 $86.87 -9.00; EYPT×6 yday $3.65 → 09:30 $3.59 -0.36; FTRE×67 yday $20.02 → 09:30 $19.54 -32.16; HALO×11 yday $113.90 → 09:30 $113.34 -6.16; MGTX×1 yday $11.05 → 09:30 $10.79 -0.26; MRVI×21 yday $7.60 → 09:30 $7.49 -2.31; OMER×65 yday $20.08 → 09:30 $19.83 -16.25; TDC×1 yday $29.46 → 09:30 $28.34 -1.12; TXG×1 yday $85.71 → 09:30 $86.14 +0.43; WRBY×6 yday $26.71 → 09:30 $26.00 -4.26 | — |
| 2026-09-28 09:30 ET | **SELL** | `ADMA` | 138 | $9.38 | $2.44 | $-64.18 | $1,562.51 | ▼ -64.18 after sell → book $8,989.31; vs 09:30 mark -2.43 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `APPS` | 2 | $11.00 | $0.25 | $-2.98 | $1,584.27 | ▼ -2.98 after sell → book $8,989.06; vs 09:30 mark -0.25 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `BHVN` | 1 | $12.82 | $0.15 | $-1.54 | $1,596.94 | ▼ -1.54 after sell → book $8,988.91; vs 09:30 mark -0.15 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `BNC` | 4 | $5.90 | $0.27 | $-0.23 | $1,620.27 | ▼ -0.23 after sell → book $8,988.64; vs 09:30 mark -0.27 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `BTDR` | 1 | $11.37 | $0.14 | $-2.38 | $1,631.50 | ▼ -2.38 after sell → book $8,988.50; vs 09:30 mark -0.14 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 5 | $4.03 | $0.24 | $-0.29 | $1,651.43 | ▼ -0.29 after sell → book $8,988.27; vs 09:30 mark -0.23 | exit 🚨 after 5 sess | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `DEFT` | 52 | $0.51 | $0.44 | $-5.28 | $1,677.29 | ▼ -5.28 after sell → book $8,987.83; vs 09:30 mark -0.44 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DLO` | 2 | $13.91 | $0.30 | $-1.78 | $1,704.81 | ▼ -1.78 after sell → book $8,987.53; vs 09:30 mark -0.30 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DXCM` | 15 | $86.87 | $2.06 | $-43.54 | $3,005.80 | ▼ -43.54 after sell → book $8,985.47; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `EYPT` | 6 | $3.59 | $0.25 | $-2.67 | $3,027.09 | ▼ -2.67 after sell → book $8,985.22; vs 09:30 mark -0.25 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FTRE` | 67 | $19.54 | $2.21 | $-51.97 | $4,334.05 | ▼ -51.97 after sell → book $8,983.00; vs 09:30 mark -2.22 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `MGTX` | 1 | $10.79 | $0.13 | $-2.95 | $4,344.71 | ▼ -2.95 after sell → book $8,982.87; vs 09:30 mark -0.13 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `TDC` | 1 | $28.34 | $0.31 | $-2.05 | $4,372.75 | ▼ -2.05 after sell → book $8,982.57; vs 09:30 mark -0.30 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,372.75 | ▼ close $8,980.60 vs 09:30 $8,991.74 (session -1.97) | 16:00 close · cash $4,372.75 · equity $8,980.60 vs 09:30 $8,991.74 (-11.14; session marks -1.97) · 8 name(s) marked open→close (per-name table). A×8 09:30 $170.00 → close $175.21 +41.68; AEHL×18 09:30 $9.01 → close $8.23 -14.04; BLFS×4 09:30 $38.13 → close $39.15 +4.08; HALO×11 09:30 $113.34 → close $112.89 -4.95; MRVI×21 09:30 $7.49 → close $7.63 +2.94; OMER×65 09:30 $19.83 → close $19.43 -26.00; TXG×1 09:30 $86.14 → close $88.50 +2.36; WRBY×6 09:30 $26.00 → close $24.66 -8.04 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,372.75 | ▼ 09:30 equity $8,963.32 vs yday $8,980.60 (-17.28) | 09:30 open · cash $4,372.75 (unchanged overnight, no fees) · equity $8,963.32 vs prior close $8,980.60 (-17.28) · 8 name(s) re-marked at the open (per-name table). A×8 yday $175.21 → 09:30 $174.49 -5.76; AEHL×18 yday $8.23 → 09:30 $8.26 +0.45; BLFS×4 yday $39.15 → 09:30 $39.04 -0.44; HALO×11 yday $112.89 → 09:30 $112.89 +0.00; MRVI×21 yday $7.63 → 09:30 $7.52 -2.31; OMER×65 yday $19.43 → 09:30 $19.26 -11.05; TXG×1 yday $88.50 → 09:30 $88.83 +0.33; WRBY×6 yday $24.66 → 09:30 $24.91 +1.50 | — |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 2 | $184.05 | $2.00 | — | $4,002.65 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+8.7; leftover $546.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 9 | $57.39 | $2.02 | — | $3,484.13 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+2.6; leftover $546.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 17 | $30.40 | $2.04 | — | $2,965.29 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+1.9; leftover $546.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 30 | $17.76 | $2.08 | — | $2,430.41 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+9.8; leftover $546.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 10 | $50.25 | $2.02 | — | $1,925.89 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+6.5; leftover $546.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 12 | $45.06 | $2.03 | — | $1,383.14 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+4.8; leftover $546.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SFIX` | 221 | $2.47 | $2.85 | — | $834.42 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-13.3; leftover $546.59 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ANNX` | 136 | $3.99 | $2.40 | — | $289.38 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=-5.0; leftover $546.59 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $289.38 | ▲ close $8,991.20 vs 09:30 $8,963.32 (session +45.31) | 16:00 close · cash $289.38 · equity $8,991.20 vs 09:30 $8,963.32 (+27.88; session marks +45.31) · 16 name(s) marked open→close (per-name table). A×8 09:30 $174.49 → close $175.03 +4.32; AEHL×18 09:30 $8.26 → close $8.59 +6.03; BLFS×4 09:30 $39.04 → close $39.22 +0.72; HALO×11 09:30 $112.89 → close $111.56 -14.63; MRVI×21 09:30 $7.52 → close $7.65 +2.73; OMER×65 09:30 $19.26 → close $19.25 -0.65; TXG×1 09:30 $88.83 → close $90.04 +1.21; WRBY×6 09:30 $24.91 → close $27.47 +15.36; SN×2 09:30 $184.05 → close $182.44 -3.22; DT×9 09:30 $57.39 → close $57.53 +1.26; TOST×17 09:30 $30.40 → close $30.46 +1.02; SONO×30 09:30 $17.76 → close $17.85 +2.70; PDFS×10 09:30 $50.25 → close $49.77 -4.80; SHOO×12 09:30 $45.06 → close $45.14 +0.96; SFIX×221 09:30 $2.47 → close $2.53 +13.26; ANNX×136 09:30 $3.99 → close $4.13 +19.04 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $289.38 | ▲ 09:30 equity $8,991.20 vs yday $8,991.20 (+0.00) | 09:30 open · cash $289.38 (unchanged overnight, no fees) · equity $8,991.20 vs prior close $8,991.20 (+0.00) · 16 name(s) re-marked at the open (per-name table). A×8 yday $175.03 → 09:30 $175.03 +0.00; AEHL×18 yday $8.59 → 09:30 $8.59 +0.00; ANNX×136 yday $4.13 → 09:30 $4.13 +0.00; BLFS×4 yday $39.22 → 09:30 $39.22 +0.00; DT×9 yday $57.53 → 09:30 $57.53 +0.00; HALO×11 yday $111.56 → 09:30 $111.56 +0.00; MRVI×21 yday $7.65 → 09:30 $7.65 +0.00; OMER×65 yday $19.25 → 09:30 $19.25 +0.00; PDFS×10 yday $49.77 → 09:30 $49.77 +0.00; SFIX×221 yday $2.53 → 09:30 $2.53 +0.00; SHOO×12 yday $45.14 → 09:30 $45.14 +0.00; SN×2 yday $182.44 → 09:30 $182.44 +0.00; SONO×30 yday $17.85 → 09:30 $17.85 +0.00; TOST×17 yday $30.46 → 09:30 $30.46 +0.00; TXG×1 yday $90.04 → 09:30 $90.04 +0.00; WRBY×6 yday $27.47 → 09:30 $27.47 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $289.38 | ▲ close $8,991.20 vs 09:30 $8,991.20 (session +0.00) | 16:00 close · cash $289.38 · equity $8,991.20 vs 09:30 $8,991.20 (+0.00; session marks +0.00) · 16 name(s) marked open→close (per-name table). A×8 09:30 $175.03 → close $175.03 +0.00; AEHL×18 09:30 $8.59 → close $8.59 +0.00; ANNX×136 09:30 $4.13 → close $4.13 +0.00; BLFS×4 09:30 $39.22 → close $39.22 +0.00; DT×9 09:30 $57.53 → close $57.53 +0.00; HALO×11 09:30 $111.56 → close $111.56 +0.00; MRVI×21 09:30 $7.65 → close $7.65 +0.00; OMER×65 09:30 $19.25 → close $19.25 +0.00; PDFS×10 09:30 $49.77 → close $49.77 +0.00; SFIX×221 09:30 $2.53 → close $2.53 +0.00; SHOO×12 09:30 $45.14 → close $45.14 +0.00; SN×2 09:30 $182.44 → close $182.44 +0.00; SONO×30 09:30 $17.85 → close $17.85 +0.00; TOST×17 09:30 $30.46 → close $30.46 +0.00; TXG×1 09:30 $90.04 → close $90.04 +0.00; WRBY×6 09:30 $27.47 → close $27.47 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $289.38 | ▼ 09:30 equity $8,961.27 vs yday $8,991.20 (-29.93) | 09:30 open · cash $289.38 (unchanged overnight, no fees) · equity $8,961.27 vs prior close $8,991.20 (-29.93) · 16 name(s) re-marked at the open (per-name table). A×8 yday $175.03 → 09:30 $172.00 -24.24; AEHL×18 yday $8.59 → 09:30 $9.74 +20.70; ANNX×136 yday $4.13 → 09:30 $4.18 +6.80; BLFS×4 yday $39.22 → 09:30 $38.90 -1.28; DT×9 yday $57.53 → 09:30 $59.04 +13.59; HALO×11 yday $111.56 → 09:30 $109.93 -17.93; MRVI×21 yday $7.65 → 09:30 $7.67 +0.42; OMER×65 yday $19.25 → 09:30 $18.87 -24.70; PDFS×10 yday $49.77 → 09:30 $51.41 +16.40; SFIX×221 yday $2.53 → 09:30 $2.55 +4.42; SHOO×12 yday $45.14 → 09:30 $44.46 -8.16; SN×2 yday $182.44 → 09:30 $182.44 +0.00; SONO×30 yday $17.85 → 09:30 $18.09 +7.20; TOST×17 yday $30.46 → 09:30 $29.05 -23.97; TXG×1 yday $90.04 → 09:30 $89.30 -0.74; WRBY×6 yday $27.47 → 09:30 $27.73 +1.56 | — |
| 2026-10-01 09:30 ET | **SELL** | `A` | 8 | $172.00 | $2.04 | $+39.63 | $1,663.34 | ▲ +39.63 after sell → book $8,959.23; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `AEHL` | 18 | $9.74 | $1.83 | $+8.91 | $1,836.84 | ▲ +8.91 after sell → book $8,957.41; vs 09:30 mark -1.82 | exit 🚨 after 4 sess | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `ANNX` | 136 | $4.18 | $2.43 | $+21.01 | $2,402.89 | ▲ +21.01 after sell → book $8,954.98; vs 09:30 mark -2.43 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `BLFS` | 4 | $38.90 | $1.59 | $-1.58 | $2,556.90 | ▼ -1.58 after sell → book $8,953.39; vs 09:30 mark -1.59 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DT` | 9 | $59.04 | $2.04 | $+10.80 | $3,086.22 | ▲ +10.80 after sell → book $8,951.35; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `HALO` | 11 | $109.93 | $2.04 | $-80.19 | $4,293.41 | ▼ -80.19 after sell → book $8,949.31; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MRVI` | 21 | $7.67 | $1.69 | $-2.94 | $4,452.79 | ▼ -2.94 after sell → book $8,947.62; vs 09:30 mark -1.69 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `OMER` | 65 | $18.87 | $2.21 | $-120.09 | $5,677.13 | ▼ -120.09 after sell → book $8,945.41; vs 09:30 mark -2.21 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 10 | $51.41 | $2.04 | $+7.54 | $6,189.19 | ▲ +7.54 after sell → book $8,943.37; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SFIX` | 221 | $2.55 | $2.90 | $+11.93 | $6,749.84 | ▲ +11.93 after sell → book $8,940.47; vs 09:30 mark -2.90 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SHOO` | 12 | $44.46 | $2.05 | $-11.27 | $7,281.32 | ▼ -11.27 after sell → book $8,938.43; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 30 | $18.09 | $2.10 | $+5.72 | $7,821.92 | ▲ +5.72 after sell → book $8,936.33; vs 09:30 mark -2.10 | exit 🚨 after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TOST` | 17 | $29.05 | $2.06 | $-27.05 | $8,313.71 | ▼ -27.05 after sell → book $8,934.27; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `TXG` | 1 | $89.30 | $0.92 | $+3.78 | $8,402.09 | ▲ +3.78 after sell → book $8,933.35; vs 09:30 mark -0.92 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `WRBY` | 6 | $27.73 | $1.70 | $+5.46 | $8,566.77 | ▲ +5.46 after sell → book $8,931.65; vs 09:30 mark -1.70 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 11 | $92.93 | $2.02 | — | $7,542.51 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+0.2; leftover $1070.85 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 27 | $38.99 | $2.07 | — | $6,487.71 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-2.4; leftover $1070.85 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 75 | $14.27 | $2.21 | — | $5,415.25 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $1070.85 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 50 | $21.28 | $2.14 | — | $4,349.11 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.7; leftover $1070.85 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 964 | $1.11 | $12.44 | — | $3,266.63 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; leftover $1070.85 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IVA` | 317 | $3.38 | $4.09 | — | $2,192.67 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=+7.0; leftover $1070.85 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `CDNL` | 35 | $30.30 | $2.10 | — | $1,130.07 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=+6.7; leftover $1070.85 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `INO` | 863 | $1.24 | $11.13 | — | $48.82 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1070.85 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.82 | ▲ close $8,917.91 vs 09:30 $8,961.27 (session +24.46) | 16:00 close · cash $48.82 · equity $8,917.91 vs 09:30 $8,961.27 (-43.36; session marks +24.46) · 9 name(s) marked open→close (per-name table). SN×2 09:30 $182.44 → close $182.44 +0.00; KSPI×11 09:30 $92.93 → close $92.02 -10.01; IOT×27 09:30 $38.99 → close $40.04 +28.35; AVPT×75 09:30 $14.27 → close $14.08 -14.25; RELY×50 09:30 $21.28 → close $21.48 +10.00; TLSA×964 09:30 $1.11 → close $1.14 +28.92; IVA×317 09:30 $3.38 → close $3.46 +26.94; CDNL×35 09:30 $30.30 → close $29.74 -19.60; INO×863 09:30 $1.24 → close $1.21 -25.89 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.82 | ▲ 09:30 equity $9,047.65 vs yday $8,917.91 (+129.74) | 09:30 open · cash $48.82 (unchanged overnight, no fees) · equity $9,047.65 vs prior close $8,917.91 (+129.74) · 9 name(s) re-marked at the open (per-name table). AVPT×75 yday $14.08 → 09:30 $14.22 +10.50; CDNL×35 yday $29.74 → 09:30 $30.29 +19.25; INO×863 yday $1.21 → 09:30 $1.23 +17.26; IOT×27 yday $40.04 → 09:30 $40.41 +9.99; IVA×317 yday $3.46 → 09:30 $3.57 +34.87; KSPI×11 yday $92.02 → 09:30 $92.05 +0.33; RELY×50 yday $21.48 → 09:30 $21.80 +16.00; SN×2 yday $182.44 → 09:30 $183.57 +2.26; TLSA×964 yday $1.14 → 09:30 $1.16 +19.28 | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 2 | $183.57 | $2.02 | $-4.97 | $413.94 | ▼ -4.97 after sell → book $9,045.63; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 1 | $27.63 | $0.28 | — | $386.03 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+4.6; leftover $51.74 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `BLFS` | 1 | $37.02 | $0.37 | — | $348.64 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-4.7; leftover $51.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `QNC` | 30 | $1.69 | $0.60 | — | $297.34 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-7.9; leftover $51.74 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $297.34 | ▼ close $8,982.74 vs 09:30 $9,047.65 (session -61.64) | 16:00 close · cash $297.34 · equity $8,982.74 vs 09:30 $9,047.65 (-64.91; session marks -61.64) · 11 name(s) marked open→close (per-name table). AVPT×75 09:30 $14.22 → close $14.07 -11.25; CDNL×35 09:30 $30.29 → close $30.73 +15.40; INO×863 09:30 $1.23 → close $1.16 -60.41; IOT×27 09:30 $40.41 → close $41.12 +19.17; IVA×317 09:30 $3.57 → close $3.56 -3.17; KSPI×11 09:30 $92.05 → close $94.05 +22.00; RELY×50 09:30 $21.80 → close $21.77 -1.50; TLSA×964 09:30 $1.16 → close $1.12 -38.56; WRBY×1 09:30 $27.63 → close $27.03 -0.60; BLFS×1 09:30 $37.02 → close $37.30 +0.28; QNC×30 09:30 $1.69 → close $1.59 -3.00 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $297.34 | ▲ 09:30 equity $9,019.79 vs yday $8,982.74 (+37.05) | 09:30 open · cash $297.34 (unchanged overnight, no fees) · equity $9,019.79 vs prior close $8,982.74 (+37.05) · 11 name(s) re-marked at the open (per-name table). AVPT×75 yday $14.07 → 09:30 $13.91 -12.00; BLFS×1 yday $37.30 → 09:30 $37.16 -0.14; CDNL×35 yday $30.73 → 09:30 $30.68 -1.75; INO×863 yday $1.16 → 09:30 $1.20 +34.52; IOT×27 yday $41.12 → 09:30 $42.00 +23.76; IVA×317 yday $3.56 → 09:30 $3.52 -12.68; KSPI×11 yday $94.05 → 09:30 $94.50 +4.95; QNC×30 yday $1.59 → 09:30 $1.62 +0.90; RELY×50 yday $21.77 → 09:30 $21.76 -0.50; TLSA×964 yday $1.12 → 09:30 $1.12 +0.00; WRBY×1 yday $27.03 → 09:30 $27.02 -0.01 | — |
| 2026-10-05 09:30 ET | **BUY** | `GPRK` | 3 | $10.87 | $0.34 | — | $264.39 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=-3.1; leftover $37.17 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 3 | $10.26 | $0.32 | — | $233.30 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=-2.7; leftover $37.17 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VET` | 3 | $11.62 | $0.36 | — | $198.08 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+0.7; leftover $37.17 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `WNC` | 2 | $14.02 | $0.29 | — | $169.75 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+7.8; leftover $37.17 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `INDP` | 15 | $2.35 | $0.40 | — | $134.11 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-38.2; leftover $37.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.11 | ▼ close $8,969.98 vs 09:30 $9,019.79 (session -48.12) | 16:00 close · cash $134.11 · equity $8,969.98 vs 09:30 $9,019.79 (-49.81; session marks -48.12) · 16 name(s) marked open→close (per-name table). AVPT×75 09:30 $13.91 → close $14.66 +56.25; BLFS×1 09:30 $37.16 → close $38.61 +1.45; CDNL×35 09:30 $30.68 → close $28.71 -68.95; INO×863 09:30 $1.20 → close $1.15 -43.15; IOT×27 09:30 $42.00 → close $42.39 +10.53; IVA×317 09:30 $3.52 → close $3.48 -12.68; KSPI×11 09:30 $94.50 → close $93.98 -5.72; QNC×30 09:30 $1.62 → close $1.64 +0.60; RELY×50 09:30 $21.76 → close $22.91 +57.50; TLSA×964 09:30 $1.12 → close $1.07 -48.20; WRBY×1 09:30 $27.02 → close $26.65 -0.37; GPRK×3 09:30 $10.87 → close $11.28 +1.23; OBE×3 09:30 $10.26 → close $10.50 +0.72; VET×3 09:30 $11.62 → close $11.73 +0.33; WNC×2 09:30 $14.02 → close $13.99 -0.06; INDP×15 09:30 $2.35 → close $2.51 +2.40 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.11 | ▲ 09:30 equity $9,026.88 vs yday $8,969.98 (+56.90) | 09:30 open · cash $134.11 (unchanged overnight, no fees) · equity $9,026.88 vs prior close $8,969.98 (+56.90) · 16 name(s) re-marked at the open (per-name table). AVPT×75 yday $14.66 → 09:30 $14.77 +8.25; BLFS×1 yday $38.61 → 09:30 $38.61 +0.00; CDNL×35 yday $28.71 → 09:30 $28.89 +6.30; GPRK×3 yday $11.28 → 09:30 $11.32 +0.12; INDP×15 yday $2.51 → 09:30 $2.51 +0.00; INO×863 yday $1.15 → 09:30 $1.14 -7.77; IOT×27 yday $42.39 → 09:30 $42.70 +8.37; IVA×317 yday $3.48 → 09:30 $3.53 +15.85; KSPI×11 yday $93.98 → 09:30 $94.50 +5.72; OBE×3 yday $10.50 → 09:30 $10.44 -0.18; QNC×30 yday $1.64 → 09:30 $1.63 -0.30; RELY×50 yday $22.91 → 09:30 $23.12 +10.50; TLSA×964 yday $1.07 → 09:30 $1.08 +9.64; VET×3 yday $11.73 → 09:30 $11.67 -0.18; WNC×2 yday $13.99 → 09:30 $14.16 +0.34; WRBY×1 yday $26.65 → 09:30 $26.89 +0.24 | — |
| 2026-10-06 09:30 ET | **BUY** | `EBS` | 2 | $7.16 | $0.15 | — | $119.64 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.5; leftover $19.16 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 1 | $14.70 | $0.15 | — | $104.79 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=+1.4; leftover $19.16 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ALVO` | 3 | $6.07 | $0.19 | — | $86.39 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-0.7; leftover $19.16 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XERS` | 1 | $10.23 | $0.11 | — | $76.05 | — | hold 5d, sell next 09:30 if 🚨; list probable; ret5=+3.1; leftover $19.16 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.05 | ▼ close $8,965.99 vs 09:30 $9,026.88 (session -60.29) | 16:00 close · cash $76.05 · equity $8,965.99 vs 09:30 $9,026.88 (-60.89; session marks -60.29) · 20 name(s) marked open→close (per-name table). AVPT×75 09:30 $14.77 → close $14.59 -13.50; BLFS×1 09:30 $38.61 → close $38.61 +0.00; CDNL×35 09:30 $28.89 → close $28.72 -5.95; GPRK×3 09:30 $11.32 → close $11.44 +0.36; INDP×15 09:30 $2.51 → close $2.59 +1.20; INO×863 09:30 $1.14 → close $1.13 -9.49; IOT×27 09:30 $42.70 → close $41.74 -25.92; IVA×317 09:30 $3.53 → close $3.46 -22.19; KSPI×11 09:30 $94.50 → close $93.16 -14.74; OBE×3 09:30 $10.44 → close $10.62 +0.54; QNC×30 09:30 $1.63 → close $1.63 +0.00; RELY×50 09:30 $23.12 → close $23.20 +4.00; TLSA×964 09:30 $1.08 → close $1.11 +28.92; VET×3 09:30 $11.67 → close $11.60 -0.21; WNC×2 09:30 $14.16 → close $13.87 -0.58; WRBY×1 09:30 $26.89 → close $26.23 -0.66; EBS×2 09:30 $7.16 → close $6.95 -0.42; ARCT×1 09:30 $14.70 → close $13.00 -1.70; ALVO×3 09:30 $6.07 → close $6.14 +0.21; XERS×1 09:30 $10.23 → close $10.07 -0.16 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.05 | ▼ 09:30 equity $8,911.02 vs yday $8,965.99 (-54.97) | 09:30 open · cash $76.05 (unchanged overnight, no fees) · equity $8,911.02 vs prior close $8,965.99 (-54.97) · 20 name(s) re-marked at the open (per-name table). ALVO×3 yday $6.14 → 09:30 $6.10 -0.12; ARCT×1 yday $13.00 → 09:30 $12.71 -0.29; AVPT×75 yday $14.59 → 09:30 $14.51 -6.00; BLFS×1 yday $38.61 → 09:30 $38.61 +0.00; CDNL×35 yday $28.72 → 09:30 $28.20 -18.20; EBS×2 yday $6.95 → 09:30 $7.05 +0.20; GPRK×3 yday $11.44 → 09:30 $11.50 +0.18; INDP×15 yday $2.59 → 09:30 $2.57 -0.30; INO×863 yday $1.13 → 09:30 $1.12 -8.63; IOT×27 yday $41.74 → 09:30 $41.60 -3.78; IVA×317 yday $3.46 → 09:30 $3.46 +0.00; KSPI×11 yday $93.16 → 09:30 $92.70 -5.06; OBE×3 yday $10.62 → 09:30 $10.72 +0.30; QNC×30 yday $1.63 → 09:30 $1.60 -0.90; RELY×50 yday $23.20 → 09:30 $22.96 -11.75; TLSA×964 yday $1.11 → 09:30 $1.11 +0.00; VET×3 yday $11.60 → 09:30 $11.70 +0.30; WNC×2 yday $13.87 → 09:30 $13.57 -0.60; WRBY×1 yday $26.23 → 09:30 $25.98 -0.25; XERS×1 yday $10.07 → 09:30 $10.00 -0.07 | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.05 | ▼ close $8,760.62 vs 09:30 $8,911.02 (session -150.40) | 16:00 close · cash $76.05 · equity $8,760.62 vs 09:30 $8,911.02 (-150.40; session marks -150.40) · 20 name(s) marked open→close (per-name table). ALVO×3 09:30 $6.10 → close $5.85 -0.75; ARCT×1 09:30 $12.71 → close $12.77 +0.05; AVPT×75 09:30 $14.51 → close $14.28 -17.25; BLFS×1 09:30 $38.61 → close $38.61 +0.00; CDNL×35 09:30 $28.20 → close $26.07 -74.55; EBS×2 09:30 $7.05 → close $7.07 +0.04; GPRK×3 09:30 $11.50 → close $11.11 -1.17; INDP×15 09:30 $2.57 → close $2.26 -4.65; INO×863 09:30 $1.12 → close $1.16 +30.20; IOT×27 09:30 $41.60 → close $40.51 -29.43; IVA×317 09:30 $3.46 → close $3.37 -30.11; KSPI×11 09:30 $92.70 → close $91.87 -9.13; OBE×3 09:30 $10.72 → close $10.63 -0.27; QNC×30 09:30 $1.60 → close $1.56 -1.20; RELY×50 09:30 $22.96 → close $22.93 -1.75; TLSA×964 09:30 $1.11 → close $1.10 -9.64; VET×3 09:30 $11.70 → close $11.53 -0.51; WNC×2 09:30 $13.57 → close $13.71 +0.28; WRBY×1 09:30 $25.98 → close $25.31 -0.67; XERS×1 09:30 $10.00 → close $10.11 +0.11 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.05 | ▲ 09:30 equity $8,760.62 vs yday $8,760.62 (+0.00) | 09:30 open · cash $76.05 (unchanged overnight, no fees) · equity $8,760.62 vs prior close $8,760.62 (+0.00) · 20 name(s) re-marked at the open (per-name table). ALVO×3 yday $5.85 → 09:30 $5.85 +0.00; ARCT×1 yday $12.77 → 09:30 $12.77 +0.00; AVPT×75 yday $14.28 → 09:30 $14.28 +0.00; BLFS×1 yday $38.61 → 09:30 $38.61 +0.00; CDNL×35 yday $26.07 → 09:30 $26.07 +0.00; EBS×2 yday $7.07 → 09:30 $7.07 +0.00; GPRK×3 yday $11.11 → 09:30 $11.11 +0.00; INDP×15 yday $2.26 → 09:30 $2.26 +0.00; INO×863 yday $1.16 → 09:30 $1.16 +0.00; IOT×27 yday $40.51 → 09:30 $40.51 +0.00; IVA×317 yday $3.37 → 09:30 $3.37 +0.00; KSPI×11 yday $91.87 → 09:30 $91.87 +0.00; OBE×3 yday $10.63 → 09:30 $10.63 +0.00; QNC×30 yday $1.56 → 09:30 $1.56 +0.00; RELY×50 yday $22.93 → 09:30 $22.93 +0.00; TLSA×964 yday $1.10 → 09:30 $1.10 +0.00; VET×3 yday $11.53 → 09:30 $11.53 +0.00; WNC×2 yday $13.71 → 09:30 $13.71 +0.00; WRBY×1 yday $25.31 → 09:30 $25.31 +0.00; XERS×1 yday $10.11 → 09:30 $10.11 +0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.05 | ▲ close $8,760.62 vs 09:30 $8,760.62 (session +0.00) | 16:00 close · cash $76.05 · equity $8,760.62 vs 09:30 $8,760.62 (+0.00; session marks +0.00) · 20 name(s) marked open→close (per-name table). ALVO×3 09:30 $5.85 → close $5.85 +0.00; ARCT×1 09:30 $12.77 → close $12.77 +0.00; AVPT×75 09:30 $14.28 → close $14.28 +0.00; BLFS×1 09:30 $38.61 → close $38.61 +0.00; CDNL×35 09:30 $26.07 → close $26.07 +0.00; EBS×2 09:30 $7.07 → close $7.07 +0.00; GPRK×3 09:30 $11.11 → close $11.11 +0.00; INDP×15 09:30 $2.26 → close $2.26 +0.00; INO×863 09:30 $1.16 → close $1.16 +0.00; IOT×27 09:30 $40.51 → close $40.51 +0.00; IVA×317 09:30 $3.37 → close $3.37 +0.00; KSPI×11 09:30 $91.87 → close $91.87 +0.00; OBE×3 09:30 $10.63 → close $10.63 +0.00; QNC×30 09:30 $1.56 → close $1.56 +0.00; RELY×50 09:30 $22.93 → close $22.93 +0.00; TLSA×964 09:30 $1.10 → close $1.10 +0.00; VET×3 09:30 $11.53 → close $11.53 +0.00; WNC×2 09:30 $13.71 → close $13.71 +0.00; WRBY×1 09:30 $25.31 → close $25.31 +0.00; XERS×1 09:30 $10.11 → close $10.11 +0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.05 | ▲ 09:30 equity $8,805.14 vs yday $8,760.62 (+44.52) | 09:30 open · cash $76.05 (unchanged overnight, no fees) · equity $8,805.14 vs prior close $8,760.62 (+44.52) · 20 name(s) re-marked at the open (per-name table). ALVO×3 yday $5.85 → 09:30 $5.67 -0.54; ARCT×1 yday $12.77 → 09:30 $13.18 +0.41; AVPT×75 yday $14.28 → 09:30 $14.44 +12.00; BLFS×1 yday $38.61 → 09:30 $38.61 +0.00; CDNL×35 yday $26.07 → 09:30 $25.00 -37.45; EBS×2 yday $7.07 → 09:30 $6.73 -0.68; GPRK×3 yday $11.11 → 09:30 $11.40 +0.87; INDP×15 yday $2.26 → 09:30 $2.35 +1.35; INO×863 yday $1.16 → 09:30 $1.10 -47.46; IOT×27 yday $40.51 → 09:30 $41.71 +32.40; IVA×317 yday $3.37 → 09:30 $3.35 -4.76; KSPI×11 yday $91.87 → 09:30 $93.64 +19.47; OBE×3 yday $10.63 → 09:30 $11.05 +1.26; QNC×30 yday $1.56 → 09:30 $1.58 +0.60; RELY×50 yday $22.93 → 09:30 $23.50 +28.50; TLSA×964 yday $1.10 → 09:30 $1.14 +38.56; VET×3 yday $11.53 → 09:30 $11.78 +0.75; WNC×2 yday $13.71 → 09:30 $13.50 -0.42; WRBY×1 yday $25.31 → 09:30 $24.92 -0.39; XERS×1 yday $10.11 → 09:30 $10.15 +0.04 | — |
| 2026-10-09 09:30 ET | **SELL** | `ALVO` | 3 | $5.67 | $0.20 | $-1.59 | $92.86 | ▼ -1.59 after sell → book $8,804.94; vs 09:30 mark -0.20 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `ARCT` | 1 | $13.18 | $0.15 | $-1.82 | $105.89 | ▼ -1.82 after sell → book $8,804.79; vs 09:30 mark -0.15 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `AVPT` | 75 | $14.44 | $2.24 | $+8.30 | $1,186.65 | ▲ +8.30 after sell → book $8,802.55; vs 09:30 mark -2.24 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `CDNL` | 35 | $25.00 | $2.12 | $-189.71 | $2,059.53 | ▼ -189.71 after sell → book $8,800.43; vs 09:30 mark -2.12 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `EBS` | 2 | $6.73 | $0.16 | $-1.17 | $2,072.83 | ▼ -1.17 after sell → book $8,800.27; vs 09:30 mark -0.16 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `GPRK` | 3 | $11.40 | $0.37 | $+0.88 | $2,106.66 | ▲ +0.88 after sell → book $8,799.90; vs 09:30 mark -0.37 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `INDP` | 15 | $2.35 | $0.42 | $-0.81 | $2,141.49 | ▼ -0.81 after sell → book $8,799.48; vs 09:30 mark -0.42 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `INO` | 863 | $1.10 | $11.29 | $-143.24 | $3,079.51 | ▼ -143.24 after sell → book $8,788.20; vs 09:30 mark -11.28 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IOT` | 27 | $41.71 | $2.09 | $+69.28 | $4,203.59 | ▲ +69.28 after sell → book $8,786.11; vs 09:30 mark -2.09 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IVA` | 317 | $3.35 | $4.15 | $-16.17 | $5,261.39 | ▼ -16.17 after sell → book $8,781.96; vs 09:30 mark -4.15 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `KSPI` | 11 | $93.64 | $2.04 | $+3.74 | $6,289.38 | ▲ +3.74 after sell → book $8,779.91; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `OBE` | 3 | $11.05 | $0.36 | $+1.69 | $6,322.17 | ▲ +1.69 after sell → book $8,779.55; vs 09:30 mark -0.36 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `QNC` | 30 | $1.58 | $0.58 | $-4.48 | $6,368.99 | ▼ -4.48 after sell → book $8,778.97; vs 09:30 mark -0.58 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `RELY` | 50 | $23.50 | $2.16 | $+106.70 | $7,541.83 | ▲ +106.70 after sell → book $8,776.81; vs 09:30 mark -2.16 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `TLSA` | 964 | $1.14 | $12.61 | $+3.88 | $8,628.18 | ▲ +3.88 after sell → book $8,764.20; vs 09:30 mark -12.61 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `VET` | 3 | $11.78 | $0.38 | $-0.26 | $8,663.14 | ▼ -0.26 after sell → book $8,763.82; vs 09:30 mark -0.38 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `WNC` | 2 | $13.50 | $0.30 | $-1.62 | $8,689.84 | ▼ -1.62 after sell → book $8,763.52; vs 09:30 mark -0.30 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `WRBY` | 1 | $24.92 | $0.27 | $-3.26 | $8,714.49 | ▼ -3.26 after sell → book $8,763.25; vs 09:30 mark -0.27 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `XERS` | 1 | $10.15 | $0.12 | $-0.31 | $8,724.52 | ▼ -0.31 after sell → book $8,763.13; vs 09:30 mark -0.12 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **BUY** | `CDNA` | 17 | $62.21 | $2.04 | — | $7,664.91 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-6.1; leftover $1090.56 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `RRC` | 26 | $40.62 | $2.07 | — | $6,606.72 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+8.4; leftover $1090.56 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `PACS` | 24 | $43.75 | $2.06 | — | $5,554.66 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+6.6; leftover $1090.56 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-09 09:30 ET | **BUY** | `ETON` | 19 | $54.86 | $2.05 | — | $4,510.27 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.3; leftover $1090.56 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `IPSC` | 746 | $1.46 | $9.62 | — | $3,411.49 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.3; leftover $1090.56 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ACRS` | 290 | $3.76 | $3.74 | — | $2,317.35 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-14.1; leftover $1090.56 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `SECZ` | 82 | $13.20 | $2.24 | — | $1,232.71 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=-7.9; leftover $1090.56 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `CLVT` | 677 | $1.61 | $8.73 | — | $134.01 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; ret5=+1.9; leftover $1090.56 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.01 | ▲ close $8,836.87 vs 09:30 $8,805.14 (session +106.29) | 16:00 close · cash $134.01 · equity $8,836.87 vs 09:30 $8,805.14 (+31.73; session marks +106.29) · 9 name(s) marked open→close (per-name table). BLFS×1 09:30 $38.61 → close $38.61 +0.00; CDNA×17 09:30 $62.21 → close $63.67 +24.82; RRC×26 09:30 $40.62 → close $40.89 +7.02; PACS×24 09:30 $43.75 → close $43.30 -10.80; ETON×19 09:30 $54.86 → close $55.21 +6.65; IPSC×746 09:30 $1.46 → close $1.47 +7.46; ACRS×290 09:30 $3.76 → close $3.83 +20.30; SECZ×82 09:30 $13.20 → close $13.82 +50.84; CLVT×677 09:30 $1.61 → close $1.61 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ELF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `NB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ELF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `NB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 25.39 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 25.39 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 25.39 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HCA` | cash | leftover split 357.88 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 136.92 < 1 share @ 427.50 |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `SIMO` | cash | leftover split 124.91 < 1 share @ 252.24 |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `URBN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `URBN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `MRNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `MRNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `OVID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ORCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COHU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 26.60 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 26.60 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 26.60 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 26.60 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `IQV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 37.43 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 37.43 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 37.43 < 1 share @ 88.83 |
| 2026-09-22 | `IQV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 16.29 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `EYPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `EYPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `RBRK` | 8 | 2026-09-18 @ $108.55 | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+21.3; leftover $921.10 |
| `GNRC` | 4 | 2026-09-18 @ $209.52 | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $921.10 |
| `VICR` | 4 | 2026-09-18 @ $219.62 | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $921.10 |
| `ECO` | 10 | 2026-09-18 @ $85.00 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+18.3; leftover $921.10 |
| `FIVN` | 26 | 2026-09-18 @ $34.44 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+14.0; leftover $921.10 |
| `TLSA` | 949 | 2026-09-18 @ $0.97 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $921.10 |
| `EYPT` | 233 | 2026-09-18 @ $3.95 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $921.10 |
| `BHVN` | 65 | 2026-09-18 @ $14.07 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $921.10 |
| `BKKT` | 4 | 2026-09-21 @ $9.31 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $37.43 |
| `BTDR` | 2 | 2026-09-21 @ $13.47 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $37.43 |
| `ORBS` | 33 | 2026-09-21 @ $1.11 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $37.43 |
| `SBET` | 3 | 2026-09-21 @ $9.99 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $37.43 |
| `BTBT` | 20 | 2026-09-21 @ $1.82 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $37.43 |
| `DEFT` | 28 | 2026-09-22 @ $0.58 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $16.29 |
| `DXCM` | 4 | 2026-09-23 @ $89.50 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+5.3; leftover $375.57 |
| `A` | 2 | 2026-09-23 @ $166.54 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.3; leftover $375.57 |
| `HALO` | 3 | 2026-09-23 @ $116.85 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+3.3; leftover $375.57 |
| `ARQT` | 13 | 2026-09-23 @ $27.79 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+7.0; leftover $375.57 |
| `PGEN` | 47 | 2026-09-23 @ $7.95 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+12.4; leftover $375.57 |
| `ADMA` | 38 | 2026-09-23 @ $9.81 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+4.0; leftover $375.57 |
| `FTRE` | 18 | 2026-09-23 @ $20.25 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+15.0; leftover $375.57 |
| `OMER` | 18 | 2026-09-23 @ $20.65 | hold 5d, sell next 09:30 if 🚨; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $375.57 |
