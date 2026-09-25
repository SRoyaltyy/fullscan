# Factor mine action — `union_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **-8.85%** ($9,115) · signal-only (no cash/fees) was -0.80%. Starts YES **9/30**. Fills 248 · skips 107 · realized $+589.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- Keep the first 8 names in list order.
- Give about 40% of leftover cash to the first name; split the rest.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,589.36.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $4000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+12.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+6.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+19.7; leftover $857.14 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.57 | ▲ close $10,123.05 vs 09:30 $10,000.00 (session +149.49) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 66 | $59.65 | $2.23 | $-14.32 | $4,095.24 | ▼ -14.32 after sell → book $10,107.55; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 18 | $44.09 | $2.06 | $-38.13 | $4,886.80 | ▼ -38.13 after sell → book $10,105.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 16 | $55.29 | $2.06 | $+70.57 | $5,769.38 | ▲ +70.57 after sell → book $10,103.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 17 | $47.27 | $2.06 | $-45.41 | $6,570.91 | ▼ -45.41 after sell → book $10,101.37; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 73 | $12.40 | $2.23 | $+46.66 | $7,473.88 | ▲ +46.66 after sell → book $10,099.14; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $8,287.98 | ▼ -20.69 after sell → book $10,097.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1058 | $0.93 | $13.20 | $+102.02 | $9,258.73 | ▲ +102.02 after sell → book $10,083.85; vs 09:30 mark -13.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 36 | $22.92 | $2.12 | $-18.98 | $10,081.73 | ▼ -18.98 after sell → book $10,081.73; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 11 | $359.83 | $2.02 | — | $6,121.57 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.9; leftover $4032.69 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $5,385.07 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+3.6; leftover $864.15 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $4,543.06 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+0.6; leftover $864.15 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $3,879.24 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-8.6; leftover $864.15 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $3,070.67 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.7; leftover $864.15 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 95 | $9.01 | $2.27 | — | $2,212.45 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $864.15 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 922 | $0.94 | $11.41 | — | $1,337.13 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $864.15 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 576 | $1.50 | $7.43 | — | $465.70 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $864.15 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $465.70 | ▲ close $10,139.92 vs 09:30 $10,109.78 (session +89.37) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $465.70 | ▲ 09:30 equity $10,187.76 vs yday $10,139.92 (+47.84) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 11 | $367.88 | $2.07 | $+84.46 | $4,510.31 | ▲ +84.46 after sell → book $10,185.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $5,255.14 | ▲ +8.32 after sell → book $10,183.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $6,144.90 | ▲ +47.76 after sell → book $10,181.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $6,816.77 | ▲ +8.05 after sell → book $10,179.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $7,589.90 | ▼ -35.44 after sell → book $10,177.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 95 | $9.22 | $2.30 | $+15.37 | $8,463.50 | ▲ +15.37 after sell → book $10,175.27; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 922 | $0.91 | $11.29 | $-50.36 | $9,288.46 | ▼ -50.36 after sell → book $10,163.98; vs 09:30 mark -11.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 576 | $1.52 | $7.54 | $-3.45 | $10,156.44 | ▼ -3.45 after sell → book $10,156.44; vs 09:30 mark -7.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 87 | $46.18 | $2.25 | — | $6,136.53 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $4062.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 6 | $142.77 | $2.01 | — | $5,277.90 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.8; leftover $870.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 4 | $202.70 | $2.00 | — | $4,465.10 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+8.3; leftover $870.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 214 | $4.05 | $2.76 | — | $3,595.64 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $870.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 102 | $8.46 | $2.30 | — | $2,730.42 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $870.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,913.55 | — | 40% to #1, rest split; list flatten; ret5=-7.2; leftover $870.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 268 | $3.24 | $3.46 | — | $1,041.77 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $870.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 180 | $4.81 | $2.53 | — | $173.44 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $870.55 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.44 | ▲ close $10,259.27 vs 09:30 $10,187.76 (session +122.15) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.44 | ▼ 09:30 equity $10,256.62 vs yday $10,259.27 (-2.65) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 87 | $48.00 | $2.30 | $+153.79 | $4,347.14 | ▲ +153.79 after sell → book $10,254.32; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 6 | $148.04 | $2.03 | $+27.58 | $5,233.35 | ▲ +27.58 after sell → book $10,252.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 4 | $208.93 | $2.02 | $+20.90 | $6,067.05 | ▲ +20.90 after sell → book $10,250.27; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 214 | $3.72 | $2.81 | $-76.19 | $6,860.33 | ▼ -76.19 after sell → book $10,247.47; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 102 | $8.55 | $2.32 | $+4.56 | $7,730.10 | ▲ +4.56 after sell → book $10,245.14; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $8,569.03 | ▲ +22.05 after sell → book $10,243.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 268 | $3.11 | $3.51 | $-41.81 | $9,398.99 | ▼ -41.81 after sell → book $10,239.59; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 180 | $4.67 | $2.57 | $-30.30 | $10,237.02 | ▼ -30.30 after sell → book $10,237.02; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,237.02 | ▲ close $10,237.02 vs 09:30 $10,256.62 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.02 | ▲ 09:30 equity $10,237.02 vs yday $10,237.02 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,237.02 | ▲ close $10,237.02 vs 09:30 $10,237.02 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.02 | ▲ 09:30 equity $10,237.02 vs yday $10,237.02 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,144.99 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4094.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,323.88 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $877.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,454.46 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $877.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,574.98 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $877.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,709.14 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $877.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,847.79 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $877.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $964.58 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $877.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $95.33 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $877.46 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.33 | ▲ close $10,479.79 vs 09:30 $10,237.02 (session +264.60) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.33 | ▲ 09:30 equity $10,779.65 vs yday $10,479.79 (+299.86) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 199 | $21.90 | $2.65 | $+263.41 | $4,450.77 | ▲ +263.41 after sell → book $10,776.99; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,310.22 | ▲ +38.34 after sell → book $10,774.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 42 | $21.75 | $2.14 | $+41.95 | $6,221.58 | ▲ +41.95 after sell → book $10,772.82; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 152 | $5.67 | $2.48 | $-20.13 | $7,080.94 | ▼ -20.13 after sell → book $10,770.34; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 44 | $21.17 | $2.14 | $+63.50 | $8,010.28 | ▲ +63.50 after sell → book $10,768.20; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 29 | $32.17 | $2.10 | $+69.49 | $8,941.11 | ▲ +69.49 after sell → book $10,766.10; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 501 | $1.79 | $6.56 | $+7.02 | $9,831.34 | ▲ +7.02 after sell → book $10,759.54; vs 09:30 mark -6.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 6 | $154.70 | $2.03 | $+56.92 | $10,757.52 | ▲ +56.92 after sell → book $10,757.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 36 | $119.43 | $2.10 | — | $6,455.94 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $4303.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 53 | $17.20 | $2.15 | — | $5,542.19 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $922.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,674.99 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $922.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 82 | $11.13 | $2.24 | — | $3,760.09 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $922.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 373 | $2.47 | $4.81 | — | $2,833.97 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $922.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 477 | $1.93 | $6.15 | — | $1,907.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $922.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $1,009.37 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $922.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 698 | $1.32 | $9.00 | — | $79.01 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $922.07 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.01 | ▲ close $10,962.33 vs 09:30 $10,779.65 (session +235.30) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.01 | ▲ 09:30 equity $11,207.31 vs yday $10,962.33 (+244.98) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 36 | $120.51 | $2.14 | $+34.64 | $4,415.22 | ▲ +34.64 after sell → book $11,205.16; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 53 | $16.57 | $2.17 | $-37.71 | $5,291.26 | ▼ -37.71 after sell → book $11,202.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $6,157.36 | ▼ -1.10 after sell → book $11,200.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 82 | $13.33 | $2.26 | $+175.90 | $7,248.16 | ▲ +175.90 after sell → book $11,198.71; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 373 | $2.40 | $4.88 | $-35.81 | $8,138.48 | ▼ -35.81 after sell → book $11,193.83; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 477 | $1.88 | $6.24 | $-36.25 | $9,029.00 | ▼ -36.25 after sell → book $11,187.59; vs 09:30 mark -6.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.75 | $2.06 | $-18.64 | $9,908.19 | ▼ -18.64 after sell → book $11,185.53; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 698 | $1.83 | $9.13 | $+337.85 | $11,176.40 | ▲ +337.85 after sell → book $11,176.40; vs 09:30 mark -9.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,176.40 | ▲ close $11,176.40 vs 09:30 $11,207.31 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,176.40 | ▲ 09:30 equity $11,176.40 vs yday $11,176.40 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 188 | $23.77 | $2.55 | — | $6,705.09 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $4470.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 87 | $10.98 | $2.25 | — | $5,747.58 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+1.2; leftover $957.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 15 | $61.19 | $2.04 | — | $4,827.69 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+7.4; leftover $957.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 114 | $8.35 | $2.33 | — | $3,873.46 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.0; leftover $957.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 193 | $4.94 | $2.57 | — | $2,917.47 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $957.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $2,061.53 | — | 40% to #1, rest split; list flatten; ret5=+6.0; leftover $957.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 132 | $7.25 | $2.39 | — | $1,102.15 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $957.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 2675 | $0.36 | $17.60 | — | $126.90 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-15.6; leftover $957.98 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.90 | ▲ close $11,393.79 vs 09:30 $11,176.40 (session +251.11) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.90 | ▲ 09:30 equity $11,477.48 vs yday $11,393.79 (+83.69) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 132 | $8.29 | $2.42 | $+132.48 | $1,218.76 | ▲ +132.48 after sell → book $11,475.06; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 2675 | $0.35 | $17.92 | $-48.90 | $2,145.11 | ▼ -48.90 after sell → book $11,457.14; vs 09:30 mark -17.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 27 | $31.21 | $2.07 | — | $1,300.37 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $858.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 115 | $11.12 | $2.33 | — | $19.24 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1287.07 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.24 | ▼ close $11,323.79 vs 09:30 $11,477.48 (session -128.95) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.24 | ▲ 09:30 equity $11,339.27 vs yday $11,323.79 (+15.48) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 87 | $10.63 | $2.28 | $-34.98 | $941.77 | ▼ -34.98 after sell → book $11,336.99; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 15 | $62.10 | $2.06 | $+9.56 | $1,871.22 | ▲ +9.56 after sell → book $11,334.94; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 114 | $8.49 | $2.36 | $+11.27 | $2,836.72 | ▲ +11.27 after sell → book $11,332.58; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 193 | $5.07 | $2.61 | $+19.91 | $3,812.61 | ▲ +19.91 after sell → book $11,329.96; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-8.73 | $4,659.82 | ▼ -8.73 after sell → book $11,327.95; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $2,834.34 | — | 40% to #1, rest split; list flatten; ret5=+3.1; leftover $1863.93 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 48 | $14.42 | $2.13 | — | $2,140.04 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $698.97 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 268 | $2.60 | $3.46 | — | $1,439.79 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+13.0; leftover $698.97 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 53 | $12.98 | $2.15 | — | $749.70 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $698.97 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 72 | $9.68 | $2.21 | — | $50.53 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $698.97 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.53 | ▲ close $11,316.60 vs 09:30 $11,339.27 (session +0.72) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.53 | ▲ 09:30 equity $11,333.54 vs yday $11,316.60 (+16.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 27 | $30.53 | $2.09 | $-22.52 | $872.75 | ▼ -22.52 after sell → book $11,331.45; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 115 | $11.27 | $2.36 | $+12.55 | $2,166.43 | ▲ +12.55 after sell → book $11,329.08; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 53 | $13.05 | $2.17 | $-0.61 | $2,855.92 | ▼ -0.61 after sell → book $11,326.92; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 72 | $9.88 | $2.23 | $+9.97 | $3,565.05 | ▲ +9.97 after sell → book $11,324.69; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $2,148.23 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1426.02 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 45 | $15.66 | $2.12 | — | $1,441.40 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $713.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 8 | $79.42 | $2.01 | — | $804.03 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $713.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 214 | $3.32 | $2.76 | — | $90.79 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=+6.4; leftover $713.01 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.79 | ▼ close $11,060.17 vs 09:30 $11,333.54 (session -255.50) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.79 | ▲ 09:30 equity $11,097.56 vs yday $11,060.17 (+37.39) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 188 | $23.68 | $2.62 | $-22.09 | $4,540.01 | ▼ -22.09 after sell → book $11,094.94; vs 09:30 mark -2.62 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 44 | $42.00 | $2.15 | $+20.37 | $6,385.86 | ▲ +20.37 after sell → book $11,092.79; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 48 | $14.54 | $2.15 | $+1.47 | $7,081.63 | ▲ +1.47 after sell → book $11,090.64; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 268 | $2.58 | $3.51 | $-12.33 | $7,769.56 | ▼ -12.33 after sell → book $11,087.13; vs 09:30 mark -3.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 43 | $31.15 | $2.14 | $-79.51 | $9,106.87 | ▼ -79.51 after sell → book $11,084.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 45 | $14.44 | $2.15 | $-59.17 | $9,754.52 | ▼ -59.17 after sell → book $11,082.84; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 8 | $80.44 | $2.03 | $+4.11 | $10,396.01 | ▲ +4.11 after sell → book $11,080.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 214 | $3.20 | $2.81 | $-31.25 | $11,078.00 | ▼ -31.25 after sell → book $11,078.00; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,078.00 | ▲ close $11,078.00 vs 09:30 $11,097.56 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,078.00 | ▲ 09:30 equity $11,078.00 vs yday $11,078.00 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,078.00 | ▲ close $11,078.00 vs 09:30 $11,078.00 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,078.00 | ▲ 09:30 equity $11,078.00 vs yday $11,078.00 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,078.00 | ▲ close $11,078.00 vs 09:30 $11,078.00 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,078.00 | ▲ 09:30 equity $11,078.00 vs yday $11,078.00 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 83 | $52.88 | $2.24 | — | $6,686.72 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4431.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 22 | $42.93 | $2.06 | — | $5,740.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $949.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 261 | $3.63 | $3.37 | — | $4,789.41 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $949.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 118 | $8.03 | $2.34 | — | $3,839.53 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $949.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $132.45 | $2.01 | — | $2,910.36 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $949.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 61 | $15.45 | $2.17 | — | $1,965.74 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $949.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 6 | $145.94 | $2.01 | — | $1,088.06 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $949.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 56 | $16.77 | $2.16 | — | $146.79 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $949.54 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.79 | ▼ close $10,862.75 vs 09:30 $11,078.00 (session -196.90) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.79 | ▼ 09:30 equity $10,835.81 vs yday $10,862.75 (-26.94) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 22 | $41.50 | $2.08 | $-35.59 | $1,057.71 | ▼ -35.59 after sell → book $10,833.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 118 | $7.91 | $2.37 | $-18.88 | $1,988.72 | ▼ -18.88 after sell → book $10,831.36; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 7 | $130.03 | $2.03 | $-20.98 | $2,896.89 | ▼ -20.98 after sell → book $10,829.32; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 61 | $15.00 | $2.19 | $-31.82 | $3,809.70 | ▼ -31.82 after sell → book $10,827.13; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 6 | $153.62 | $2.03 | $+42.01 | $4,729.39 | ▲ +42.01 after sell → book $10,825.10; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 56 | $15.61 | $2.18 | $-69.30 | $5,601.38 | ▼ -69.30 after sell → book $10,822.93; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 889 | $2.52 | $11.47 | — | $3,349.63 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $2240.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 100 | $6.71 | $2.29 | — | $2,676.34 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $672.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 353 | $1.90 | $4.55 | — | $2,001.08 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $672.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 140 | $4.78 | $2.41 | — | $1,329.47 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $672.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 422 | $1.59 | $5.44 | — | $653.05 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $672.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 57 | $11.31 | $2.16 | — | $6.22 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $672.17 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.22 | ▼ close $10,694.80 vs 09:30 $10,835.81 (session -99.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.22 | ▲ 09:30 equity $10,817.40 vs yday $10,694.80 (+122.60) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 83 | $54.31 | $2.29 | $+114.16 | $4,511.66 | ▲ +114.16 after sell → book $10,815.11; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 261 | $3.43 | $3.42 | $-58.99 | $5,403.47 | ▼ -58.99 after sell → book $10,811.69; vs 09:30 mark -3.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 889 | $2.38 | $11.63 | $-147.56 | $7,507.66 | ▼ -147.56 after sell → book $10,800.06; vs 09:30 mark -11.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 100 | $6.57 | $2.32 | $-18.61 | $8,162.34 | ▼ -18.61 after sell → book $10,797.74; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 353 | $2.00 | $4.62 | $+26.12 | $8,863.72 | ▲ +26.12 after sell → book $10,793.12; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 140 | $4.30 | $2.44 | $-72.05 | $9,463.28 | ▼ -72.05 after sell → book $10,790.68; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 422 | $1.63 | $5.52 | $+5.91 | $10,145.61 | ▲ +5.91 after sell → book $10,785.15; vs 09:30 mark -5.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 57 | $11.22 | $2.18 | $-9.47 | $10,782.97 | ▼ -9.47 after sell → book $10,782.97; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,782.97 | ▲ close $10,782.97 vs 09:30 $10,817.40 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,782.97 | ▲ 09:30 equity $10,782.97 vs yday $10,782.97 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,782.97 | ▲ close $10,782.97 vs 09:30 $10,782.97 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,782.97 | ▲ 09:30 equity $10,782.97 vs yday $10,782.97 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,782.97 | ▲ close $10,782.97 vs 09:30 $10,782.97 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,782.97 | ▲ 09:30 equity $10,782.97 vs yday $10,782.97 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 264 | $16.28 | $3.41 | — | $6,481.64 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-1.1; leftover $4313.19 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 338 | $2.73 | $4.36 | — | $5,554.54 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-3.0; leftover $924.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 4 | $206.84 | $2.00 | — | $4,725.18 | — | 40% to #1, rest split; list flatten; ret5=+8.3; leftover $924.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 5 | $164.43 | $2.00 | — | $3,901.03 | — | 40% to #1, rest split; list flatten,earn_react; ⚪; ret5=+4.9; leftover $924.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 5 | $157.78 | $2.00 | — | $3,110.12 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+4.7; leftover $924.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 16 | $56.09 | $2.04 | — | $2,210.64 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+19.6; leftover $924.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 453 | $2.04 | $5.84 | — | $1,280.68 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $924.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 194 | $4.75 | $2.57 | — | $356.61 | — | 40% to #1, rest split; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $924.25 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $356.61 | ▼ close $10,702.42 vs 09:30 $10,782.97 (session -56.32) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.61 | ▼ 09:30 equity $10,484.11 vs yday $10,702.42 (-218.31) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 264 | $16.03 | $3.48 | $-72.89 | $4,585.05 | ▼ -72.89 after sell → book $10,480.63; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 338 | $2.75 | $4.43 | $-0.34 | $5,511.81 | ▼ -0.34 after sell → book $10,476.20; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 4 | $206.50 | $2.02 | $-5.38 | $6,335.79 | ▼ -5.38 after sell → book $10,474.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 5 | $141.42 | $2.02 | $-119.08 | $7,040.86 | ▼ -119.08 after sell → book $10,472.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 16 | $52.23 | $2.06 | $-65.86 | $7,874.48 | ▼ -65.86 after sell → book $10,470.09; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 453 | $2.01 | $5.93 | $-25.36 | $8,779.09 | ▼ -25.36 after sell → book $10,464.17; vs 09:30 mark -5.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 194 | $4.82 | $2.61 | $+8.39 | $9,711.55 | ▲ +8.39 after sell → book $10,461.55; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,711.55 | ▼ close $10,444.75 vs 09:30 $10,484.11 (session -16.80) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,711.55 | ▲ 09:30 equity $10,467.15 vs yday $10,444.75 (+22.40) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 5 | $151.12 | $2.02 | $-37.33 | $10,465.13 | ▼ -37.33 after sell → book $10,465.13; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,465.13 | ▲ close $10,465.13 vs 09:30 $10,467.15 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,465.13 | ▲ 09:30 equity $10,465.13 vs yday $10,465.13 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 15 | $270.89 | $2.04 | — | $6,399.74 | — | 40% to #1, rest split; list flatten; ret5=+4.0; leftover $4186.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 11 | $77.12 | $2.02 | — | $5,549.40 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+7.2; leftover $897.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 62 | $14.31 | $2.18 | — | $4,660.00 | — | 40% to #1, rest split; list flatten; ret5=+4.8; leftover $897.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 24 | $36.46 | $2.06 | — | $3,782.90 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+2.9; leftover $897.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 48 | $18.61 | $2.13 | — | $2,887.49 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $897.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 49 | $18.21 | $2.14 | — | $1,993.06 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-19.1; leftover $897.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 13 | $68.79 | $2.03 | — | $1,096.76 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $897.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 152 | $5.87 | $2.45 | — | $202.07 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $897.01 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.07 | ▲ close $10,566.53 vs 09:30 $10,465.13 (session +118.45) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.07 | ▲ 09:30 equity $10,739.95 vs yday $10,566.53 (+173.42) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 15 | $273.15 | $2.08 | $+29.79 | $4,297.25 | ▲ +29.79 after sell → book $10,737.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 11 | $76.44 | $2.04 | $-11.55 | $5,136.04 | ▼ -11.55 after sell → book $10,735.83; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 62 | $14.33 | $2.20 | $-3.13 | $6,022.31 | ▼ -3.13 after sell → book $10,733.64; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 24 | $36.67 | $2.08 | $+0.90 | $6,900.30 | ▲ +0.90 after sell → book $10,731.55; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 48 | $22.46 | $2.15 | $+180.51 | $7,976.23 | ▲ +180.51 after sell → book $10,729.40; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 49 | $19.59 | $2.16 | $+63.33 | $8,933.98 | ▲ +63.33 after sell → book $10,727.24; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 13 | $72.70 | $2.05 | $+46.75 | $9,877.03 | ▲ +46.75 after sell → book $10,725.19; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 152 | $5.58 | $2.48 | $-49.01 | $10,722.71 | ▼ -49.01 after sell → book $10,722.71; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 18 | $233.85 | $2.04 | — | $6,511.37 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+11.7; leftover $4289.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 6 | $151.43 | $2.01 | — | $5,600.78 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $919.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 6 | $147.61 | $2.01 | — | $4,713.11 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+17.7; leftover $919.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 89 | $10.25 | $2.26 | — | $3,798.61 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $919.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 121 | $7.59 | $2.35 | — | $2,877.86 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $919.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 26 | $34.93 | $2.07 | — | $1,967.62 | — | 40% to #1, rest split; list flatten; ret5=+1.6; leftover $919.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 5406 | $0.17 | $25.41 | — | $1,023.19 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $919.09 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 57 | $15.87 | $2.16 | — | $116.44 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $919.09 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.44 | ▲ close $10,901.49 vs 09:30 $10,739.95 (session +219.08) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.44 | ▲ 09:30 equity $11,104.90 vs yday $10,901.49 (+203.41) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 18 | $249.13 | $2.09 | $+270.91 | $4,598.69 | ▲ +270.91 after sell → book $11,102.81; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 6 | $158.04 | $2.03 | $+35.62 | $5,544.90 | ▲ +35.62 after sell → book $11,100.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 6 | $146.50 | $2.03 | $-10.70 | $6,421.87 | ▼ -10.70 after sell → book $11,098.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 89 | $10.12 | $2.28 | $-16.11 | $7,320.27 | ▼ -16.11 after sell → book $11,096.47; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 121 | $7.98 | $2.38 | $+42.45 | $8,283.47 | ▲ +42.45 after sell → book $11,094.09; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 26 | $34.52 | $2.09 | $-14.82 | $9,178.90 | ▼ -14.82 after sell → book $11,092.00; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 5406 | $0.17 | $26.32 | $-51.72 | $10,071.60 | ▼ -51.72 after sell → book $11,065.68; vs 09:30 mark -26.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 57 | $17.44 | $2.18 | $+85.15 | $11,063.50 | ▲ +85.15 after sell → book $11,063.50; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 40 | $108.55 | $2.11 | — | $6,719.39 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+21.3; leftover $4425.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $6,124.25 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+16.1; leftover $948.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 4 | $209.52 | $2.00 | — | $5,284.17 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $948.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $4,403.68 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $948.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 11 | $85.00 | $2.02 | — | $3,466.66 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+18.3; leftover $948.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 27 | $34.44 | $2.07 | — | $2,534.71 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+14.0; leftover $948.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 977 | $0.97 | $12.41 | — | $1,574.61 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $948.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 455 | $2.08 | $5.87 | — | $622.34 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $948.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $622.34 | ▼ close $10,857.90 vs 09:30 $11,104.90 (session -175.12) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $622.34 | ▲ 09:30 equity $10,971.67 vs yday $10,857.90 (+113.77) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 40 | $107.57 | $2.15 | $-43.46 | $4,922.99 | ▼ -43.46 after sell → book $10,969.52; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 1 | $586.77 | $2.01 | $-10.39 | $5,507.75 | ▼ -10.39 after sell → book $10,967.51; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 4 | $210.00 | $2.02 | $-2.10 | $6,345.72 | ▼ -2.10 after sell → book $10,965.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 4 | $230.25 | $2.02 | $+38.50 | $7,264.70 | ▲ +38.50 after sell → book $10,963.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 11 | $82.83 | $2.04 | $-27.94 | $8,173.79 | ▼ -27.94 after sell → book $10,961.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 27 | $33.00 | $2.09 | $-43.04 | $9,062.70 | ▼ -43.04 after sell → book $10,959.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 977 | $0.94 | $12.29 | $-54.00 | $9,968.79 | ▼ -54.00 after sell → book $10,947.04; vs 09:30 mark -12.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 455 | $2.15 | $5.96 | $+20.03 | $10,941.09 | ▲ +20.03 after sell → book $10,941.09; vs 09:30 mark -5.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 27 | $157.87 | $2.07 | — | $6,676.52 | — | 40% to #1, rest split; list flatten; ret5=+6.5; leftover $4376.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 2 | $386.20 | $2.00 | — | $5,902.13 | — | 40% to #1, rest split; list flatten; ret5=-5.8; leftover $937.81 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 10 | $88.83 | $2.02 | — | $5,011.81 | — | 40% to #1, rest split; list flatten; ret5=+7.6; leftover $937.81 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 119 | $7.84 | $2.35 | — | $4,076.50 | — | 40% to #1, rest split; list flatten; ret5=+13.6; leftover $937.81 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 89 | $10.43 | $2.26 | — | $3,145.97 | — | 40% to #1, rest split; list flatten; ret5=+19.2; leftover $937.81 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 69 | $13.47 | $2.20 | — | $2,214.35 | — | 40% to #1, rest split; list flatten; ret5=+3.6; leftover $937.81 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 234 | $4.00 | $3.02 | — | $1,275.33 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $937.81 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 100 | $9.31 | $2.29 | — | $342.04 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $937.81 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.04 | ▼ close $10,798.61 vs 09:30 $10,971.67 (session -124.28) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $342.04 | ▲ 09:30 equity $10,823.46 vs yday $10,798.61 (+24.85) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 89 | $10.18 | $2.28 | $-26.79 | $1,245.78 | ▼ -26.79 after sell → book $10,821.18; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 234 | $3.51 | $3.07 | $-120.75 | $2,064.05 | ▼ -120.75 after sell → book $10,818.11; vs 09:30 mark -3.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 1 | $93.97 | $0.94 | — | $1,969.14 | — | 40% to #1, rest split; list flatten; ret5=-0.6; leftover $176.92 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 305 | $0.58 | $2.68 | — | $1,789.55 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $176.92 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,789.55 | ▼ close $10,799.19 vs 09:30 $10,823.46 (session -15.29) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,789.55 | ▲ 09:30 equity $10,932.47 vs yday $10,799.19 (+133.28) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 2 | $370.00 | $2.02 | $-36.41 | $2,527.54 | ▼ -36.41 after sell → book $10,930.45; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 69 | $12.26 | $2.22 | $-87.91 | $3,371.26 | ▼ -87.91 after sell → book $10,928.23; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 100 | $9.50 | $2.32 | $+14.39 | $4,318.94 | ▲ +14.39 after sell → book $10,925.92; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 1 | $93.97 | $0.96 | $-1.91 | $4,411.95 | ▼ -1.91 after sell → book $10,924.95; vs 09:30 mark -0.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 305 | $0.57 | $2.73 | $-6.94 | $4,584.59 | ▼ -6.94 after sell → book $10,922.22; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 15 | $116.85 | $2.04 | — | $2,829.81 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1833.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 24 | $27.79 | $2.06 | — | $2,160.79 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+7.0; leftover $687.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 70 | $9.81 | $2.20 | — | $1,471.89 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.0; leftover $687.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 33 | $20.25 | $2.09 | — | $801.55 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+15.0; leftover $687.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 33 | $20.65 | $2.09 | — | $118.01 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $687.69 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.01 | ▼ close $10,671.61 vs 09:30 $10,932.47 (session -240.14) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.01 | ▼ 09:30 equity $10,606.47 vs yday $10,671.61 (-65.14) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 27 | $163.95 | $2.12 | $+159.97 | $4,542.54 | ▲ +159.97 after sell → book $10,604.35; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 10 | $87.67 | $2.04 | $-15.61 | $5,417.25 | ▼ -15.61 after sell → book $10,602.31; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 119 | $7.38 | $2.38 | $-59.46 | $6,293.10 | ▼ -59.46 after sell → book $10,599.94; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 15 | $112.22 | $2.06 | $-73.54 | $7,974.34 | ▼ -73.54 after sell → book $10,597.88; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 24 | $26.22 | $2.08 | $-41.82 | $8,601.54 | ▼ -41.82 after sell → book $10,595.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 70 | $9.67 | $2.22 | $-14.22 | $9,276.21 | ▼ -14.22 after sell → book $10,593.57; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 33 | $19.40 | $2.11 | $-32.25 | $9,914.31 | ▼ -32.25 after sell → book $10,591.47; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 33 | $20.52 | $2.11 | $-8.49 | $10,589.36 | ▼ -8.49 after sell → book $10,589.36; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.36 | ▲ close $10,589.36 vs 09:30 $10,606.47 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,172.21 | ▲ 09:30 equity $9,172.21 vs yday $9,172.21 (+0.00) | 09:30 open · cash $9,172.21 · no holdings · equity $9,172.21 vs prior close $9,172.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 4 | $803.87 | $2.00 | — | $5,954.73 | — | 40% to #1, rest split; list flatten; ret5=+0.8; leftover $3668.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 6 | $115.36 | $2.01 | — | $5,260.56 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.1; leftover $786.19 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 38 | $20.61 | $2.10 | — | $4,475.28 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+9.1; leftover $786.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 20 | $38.51 | $2.05 | — | $3,703.03 | — | 40% to #1, rest split; list flatten; ret5=+4.7; leftover $786.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 102 | $7.65 | $2.30 | — | $2,920.43 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.2; leftover $786.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 29 | $26.27 | $2.08 | — | $2,156.52 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $786.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 9 | $83.76 | $2.02 | — | $1,400.67 | — | 40% to #1, rest split; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $786.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 86 | $9.05 | $2.25 | — | $620.12 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-27.1; leftover $786.19 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $620.12 | ▼ close $9,114.66 vs 09:30 $9,172.21 (session -40.75) | 16:00 close · cash $620.12 · equity $9,114.66 vs 09:30 $9,172.21 (-57.55; session marks -40.75) · 8 name(s) marked open→close (per-name table). REGN×4 09:30 $803.87 → close $788.04 -63.32; HALO×6 09:30 $115.36 → close $113.90 -8.76; OMER×38 09:30 $20.61 → close $20.08 -20.14; BLFS×20 09:30 $38.51 → close $38.49 -0.40; MRVI×102 09:30 $7.65 → close $7.60 -5.10; WRBY×29 09:30 $26.27 → close $26.71 +12.76; TXG×9 09:30 $83.76 → close $85.71 +17.55; AEHL×86 09:30 $9.05 → close $9.36 +26.66 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
