# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-18.94%** ($8,106) · signal-only (no cash/fees) was -4.35%. Starts YES **0/30**. Fills 158 · skips 0 · realized $-550.20.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
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
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,449.77.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.73 | ▲ close $10,203.79 vs 09:30 $10,000.00 (session +216.83) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,743.19 | ▼ -8.21 after sell → book $10,215.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $3,510.36 | ▲ +145.14 after sell → book $10,213.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 33 | $47.27 | $2.11 | $-84.39 | $5,068.16 | ▼ -84.39 after sell → book $10,211.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,826.50 | ▲ +94.53 after sell → book $10,208.65; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 56 | $29.15 | $2.18 | $-37.38 | $8,456.72 | ▼ -37.38 after sell → book $10,206.47; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 75 | $23.33 | $2.24 | $+94.54 | $10,204.23 | ▲ +94.54 after sell → book $10,204.23; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,209.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $7,940.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 141 | $9.01 | $2.41 | — | $6,667.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1361 | $0.94 | $16.84 | — | $5,375.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 850 | $1.50 | $10.96 | — | $4,089.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,814.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $1,538.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 305 | $4.18 | $3.93 | — | $260.01 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1275.53 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.01 | ▼ close $10,058.44 vs 09:30 $10,217.42 (session -101.53) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.01 | ▲ 09:30 equity $10,098.53 vs yday $10,058.44 (+40.09) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,268.81 | ▲ +14.07 after sell → book $10,096.52; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,484.87 | ▼ -53.41 after sell → book $10,094.44; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 141 | $9.22 | $2.45 | $+24.75 | $3,782.45 | ▲ +24.75 after sell → book $10,091.99; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1361 | $0.91 | $16.66 | $-74.33 | $5,000.21 | ▼ -74.33 after sell → book $10,075.33; vs 09:30 mark -16.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 850 | $1.52 | $11.12 | $-5.08 | $6,281.09 | ▼ -5.08 after sell → book $10,064.21; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,454.44 | ▼ -101.70 after sell → book $10,061.94; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 295 | $4.60 | $3.87 | $+77.88 | $8,807.58 | ▲ +77.88 after sell → book $10,058.08; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 305 | $4.10 | $4.00 | $-32.33 | $10,054.08 | ▼ -32.33 after sell → book $10,054.08; vs 09:30 mark -4.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 310 | $4.05 | $4.00 | — | $8,794.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 148 | $8.46 | $2.43 | — | $7,540.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 387 | $3.24 | $4.99 | — | $6,281.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,043.76 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $3,791.92 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $2,549.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,299.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1256.76 | — |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 335 | $3.75 | $4.32 | — | $39.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $1256.76 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.21 | ▼ close $9,814.08 vs 09:30 $10,098.53 (session -215.35) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.21 | ▼ 09:30 equity $9,676.32 vs yday $9,814.08 (-137.76) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 310 | $3.72 | $4.06 | $-110.36 | $1,188.35 | ▼ -110.36 after sell → book $9,672.26; vs 09:30 mark -4.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 148 | $8.55 | $2.47 | $+8.42 | $2,451.29 | ▲ +8.42 after sell → book $9,669.80; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 387 | $3.11 | $5.07 | $-60.37 | $3,649.79 | ▼ -60.37 after sell → book $9,664.73; vs 09:30 mark -5.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,936.36 | ▲ +49.13 after sell → book $9,662.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $6,171.03 | ▼ -17.16 after sell → book $9,660.19; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $7,270.42 | ▼ -143.13 after sell → book $9,657.98; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,483.23 | ▼ -36.80 after sell → book $9,655.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 335 | $3.50 | $4.39 | $-92.46 | $9,651.35 | ▼ -92.46 after sell → book $9,651.35; vs 09:30 mark -4.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,651.35 | ▲ close $9,651.35 vs 09:30 $9,676.32 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,651.35 | ▲ 09:30 equity $9,651.35 vs yday $9,651.35 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,651.35 | ▲ close $9,651.35 vs 09:30 $9,651.35 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,651.35 | ▲ 09:30 equity $9,651.35 vs yday $9,651.35 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,457.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,272.12 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 209 | $5.77 | $2.70 | — | $6,063.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,863.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,676.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 689 | $1.75 | $8.89 | — | $2,461.95 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 162 | $7.44 | $2.48 | — | $1,254.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1206.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 127 | $9.46 | $2.37 | — | $50.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1206.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.40 | ▲ close $9,956.69 vs 09:30 $9,651.35 (session +330.25) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.40 | ▲ 09:30 equity $10,205.85 vs yday $9,956.69 (+249.16) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,318.41 | ▲ +73.95 after sell → book $10,203.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,560.73 | ▲ +57.15 after sell → book $10,201.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 209 | $5.67 | $2.74 | $-26.34 | $3,743.01 | ▼ -26.34 after sell → book $10,198.87; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,032.19 | ▲ +89.57 after sell → book $10,196.68; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,316.86 | ▲ +97.36 after sell → book $10,194.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 689 | $1.79 | $9.01 | $+9.66 | $7,541.16 | ▲ +9.66 after sell → book $10,185.54; vs 09:30 mark -9.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 162 | $8.28 | $2.51 | $+131.09 | $8,880.00 | ▲ +131.09 after sell → book $10,183.02; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 127 | $10.26 | $2.40 | $+96.83 | $10,180.62 | ▲ +96.83 after sell → book $10,180.62; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 28 | $59.72 | $2.07 | — | $8,506.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1696.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $6,847.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1696.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 312 | $5.43 | $4.02 | — | $5,149.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1696.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 26 | $64.39 | $2.07 | — | $3,473.38 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1696.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 94 | $17.93 | $2.27 | — | $1,785.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1696.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 698 | $2.43 | $9.00 | — | $80.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1696.77 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.07 | ▼ close $10,134.35 vs 09:30 $10,205.85 (session -24.80) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.07 | ▼ 09:30 equity $10,084.95 vs yday $10,134.35 (-49.40) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 28 | $58.75 | $2.10 | $-31.33 | $1,722.97 | ▼ -31.33 after sell → book $10,082.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $3,410.79 | ▲ +29.20 after sell → book $10,080.80; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 312 | $5.20 | $4.09 | $-81.43 | $5,027.54 | ▼ -81.43 after sell → book $10,076.71; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 26 | $63.15 | $2.09 | $-36.40 | $6,667.35 | ▼ -36.40 after sell → book $10,074.62; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 94 | $18.05 | $2.30 | $+6.71 | $8,362.22 | ▲ +6.71 after sell → book $10,072.32; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 698 | $2.45 | $9.13 | $-4.18 | $10,063.18 | ▼ -4.18 after sell → book $10,063.18; vs 09:30 mark -9.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.18 | ▲ close $10,063.18 vs 09:30 $10,084.95 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.18 | ▲ 09:30 equity $10,063.18 vs yday $10,063.18 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.35 | $2.44 | — | $8,808.24 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 70 | $17.89 | $2.20 | — | $7,553.74 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-7.5; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 662 | $1.90 | $8.54 | — | $6,287.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 82 | $15.28 | $2.24 | — | $5,032.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 90 | $13.96 | $2.26 | — | $3,773.55 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.0; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 11 | $112.17 | $2.02 | — | $2,537.66 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,464.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1257.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZM` | 12 | $103.50 | $2.03 | — | $220.18 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=-0.7; leftover $1257.90 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.18 | ▲ close $10,083.37 vs 09:30 $10,063.18 (session +43.91) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.18 | ▲ 09:30 equity $10,354.61 vs yday $10,083.37 (+271.24) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 150 | $8.60 | $2.48 | $+32.58 | $1,507.71 | ▲ +32.58 after sell → book $10,352.14; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 70 | $17.82 | $2.22 | $-9.32 | $2,752.88 | ▼ -9.32 after sell → book $10,349.91; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 662 | $1.91 | $8.66 | $-10.58 | $4,008.64 | ▼ -10.58 after sell → book $10,341.25; vs 09:30 mark -8.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 82 | $16.77 | $2.26 | $+117.68 | $5,381.52 | ▲ +117.68 after sell → book $10,338.99; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VIPS` | 90 | $14.00 | $2.29 | $-0.95 | $6,639.24 | ▼ -0.95 after sell → book $10,336.71; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 11 | $131.37 | $2.04 | $+207.13 | $8,082.26 | ▲ +207.13 after sell → book $10,334.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HEI` | 3 | $370.00 | $2.02 | $+34.53 | $9,190.24 | ▲ +34.53 after sell → book $10,332.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZM` | 12 | $95.20 | $2.05 | $-103.67 | $10,330.60 | ▼ -103.67 after sell → book $10,330.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,330.60 | ▲ close $10,330.60 vs 09:30 $10,354.61 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,330.60 | ▲ 09:30 equity $10,330.60 vs yday $10,330.60 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,330.60 | ▲ close $10,330.60 vs 09:30 $10,330.60 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,330.60 | ▲ 09:30 equity $10,330.60 vs yday $10,330.60 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $9,100.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,125.25 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $6,842.36 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $5,583.91 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,424.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,224.53 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,121.72 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1291.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `OLED` | 15 | $85.02 | $2.04 | — | $844.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1291.32 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $844.39 | ▼ close $10,007.40 vs 09:30 $10,330.60 (session -307.04) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $844.39 | ▲ 09:30 equity $10,054.46 vs yday $10,007.40 (+47.06) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $2,030.65 | ▼ -43.86 after sell → book $10,052.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,996.10 | ▼ -9.78 after sell → book $10,050.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,245.51 | ▼ -33.48 after sell → book $10,048.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $5,452.64 | ▼ -51.32 after sell → book $10,046.28; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,571.62 | ▼ -40.78 after sell → book $10,044.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $7,725.18 | ▼ -46.06 after sell → book $10,042.22; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $8,790.99 | ▼ -36.98 after sell → book $10,040.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OLED` | 15 | $83.28 | $2.06 | $-30.19 | $10,038.14 | ▼ -30.19 after sell → book $10,038.14; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,038.14 | ▲ close $10,038.14 vs 09:30 $10,054.46 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,038.14 | ▲ 09:30 equity $10,038.14 vs yday $10,038.14 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,038.14 | ▲ close $10,038.14 vs 09:30 $10,038.14 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,038.14 | ▲ 09:30 equity $10,038.14 vs yday $10,038.14 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,038.14 | ▲ close $10,038.14 vs 09:30 $10,038.14 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,038.14 | ▲ 09:30 equity $10,038.14 vs yday $10,038.14 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,819.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,572.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 345 | $3.63 | $4.45 | — | $6,315.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,121.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $3,878.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 575 | $2.18 | $7.42 | — | $2,617.82 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 59 | $21.03 | $2.17 | — | $1,374.88 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1254.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 108 | $11.54 | $2.31 | — | $126.24 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1254.77 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.24 | ▼ close $9,774.96 vs 09:30 $10,038.14 (session -238.46) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.24 | ▼ 09:30 equity $9,723.24 vs yday $9,774.96 (-51.72) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,294.48 | ▼ -25.83 after sell → book $9,721.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $2,447.38 | ▼ -90.29 after sell → book $9,718.97; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 575 | $2.16 | $7.52 | $-26.44 | $3,681.86 | ▼ -26.44 after sell → book $9,711.45; vs 09:30 mark -7.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 59 | $20.58 | $2.19 | $-30.90 | $4,893.89 | ▼ -30.90 after sell → book $9,709.26; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 485 | $2.52 | $6.26 | — | $3,665.44 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1223.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 182 | $6.71 | $2.54 | — | $2,441.68 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1223.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 255 | $4.78 | $3.29 | — | $1,219.49 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1223.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $164.05 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1223.47 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.05 | ▼ close $9,529.08 vs 09:30 $9,723.24 (session -166.10) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.05 | ▼ 09:30 equity $9,493.51 vs yday $9,529.08 (-35.57) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,411.10 | ▲ +28.75 after sell → book $9,491.43; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,632.80 | ▼ -25.34 after sell → book $9,489.33; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 345 | $3.43 | $4.52 | $-77.97 | $3,811.64 | ▼ -77.97 after sell → book $9,484.82; vs 09:30 mark -4.51 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 108 | $11.22 | $2.34 | $-39.22 | $5,021.05 | ▼ -39.22 after sell → book $9,482.47; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 485 | $2.38 | $6.35 | $-80.50 | $6,169.01 | ▼ -80.50 after sell → book $9,476.13; vs 09:30 mark -6.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 182 | $6.57 | $2.58 | $-30.59 | $7,362.17 | ▼ -30.59 after sell → book $9,473.55; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 255 | $4.30 | $3.34 | $-129.03 | $8,455.33 | ▼ -129.03 after sell → book $9,470.21; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,468.19 | ▼ -42.58 after sell → book $9,468.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,468.19 | ▲ close $9,468.19 vs 09:30 $9,493.51 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,468.19 | ▲ 09:30 equity $9,468.19 vs yday $9,468.19 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,468.19 | ▲ close $9,468.19 vs 09:30 $9,468.19 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,468.19 | ▲ 09:30 equity $9,468.19 vs yday $9,468.19 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,468.19 | ▲ close $9,468.19 vs 09:30 $9,468.19 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,468.19 | ▲ 09:30 equity $9,468.19 vs yday $9,468.19 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 60 | $52.55 | $2.17 | — | $6,313.02 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3156.06 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 312 | $10.11 | $4.02 | — | $3,154.67 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $3156.06 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 966 | $3.25 | $12.46 | — | $2.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $3156.06 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.71 | ▲ close $9,537.97 vs 09:30 $9,468.19 (session +88.44) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.71 | ▼ 09:30 equity $9,492.67 vs yday $9,537.97 (-45.30) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 60 | $56.90 | $2.21 | $+256.62 | $3,414.50 | ▲ +256.62 after sell → book $9,490.46; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 312 | $10.00 | $4.10 | $-42.45 | $6,530.40 | ▼ -42.45 after sell → book $9,486.36; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 966 | $3.06 | $12.65 | $-208.65 | $9,473.72 | ▼ -208.65 after sell → book $9,473.72; vs 09:30 mark -12.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,473.72 | ▲ close $9,473.72 vs 09:30 $9,492.67 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,473.72 | ▲ 09:30 equity $9,473.72 vs yday $9,473.72 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,473.72 | ▲ close $9,473.72 vs 09:30 $9,473.72 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,473.72 | ▲ 09:30 equity $9,473.72 vs yday $9,473.72 (-0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,473.72 | ▲ close $9,473.72 vs 09:30 $9,473.72 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,473.72 | ▲ 09:30 equity $9,473.72 vs yday $9,473.72 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,473.72 | ▲ close $9,473.72 vs 09:30 $9,473.72 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,473.72 | ▲ 09:30 equity $9,473.72 vs yday $9,473.72 (-0.00) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 452 | $20.91 | $5.83 | — | $16.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $9473.72 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.57 | ▲ close $9,594.45 vs 09:30 $9,473.72 (session +126.56) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.57 | ▲ 09:30 equity $9,802.37 vs yday $9,594.45 (+207.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 452 | $21.65 | $5.98 | $+322.67 | $9,796.38 | ▲ +322.67 after sell → book $9,796.38; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.38 | ▲ close $9,796.38 vs 09:30 $9,802.37 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.38 | ▲ 09:30 equity $9,796.38 vs yday $9,796.38 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,796.38 | ▲ close $9,796.38 vs 09:30 $9,796.38 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.38 | ▲ 09:30 equity $9,796.38 vs yday $9,796.38 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $8,630.85 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 44 | $27.79 | $2.12 | — | $7,405.97 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 124 | $9.81 | $2.36 | — | $6,187.17 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 59 | $20.65 | $2.17 | — | $4,966.65 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 10 | $116.00 | $2.02 | — | $3,804.63 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 6 | $196.78 | $2.01 | — | $2,621.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 11 | $109.67 | $2.02 | — | $1,413.55 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $1224.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `SNX` | 4 | $283.46 | $2.00 | — | $277.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1224.55 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.71 | ▼ close $9,602.35 vs 09:30 $9,796.38 (session -177.30) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.71 | ▼ 09:30 equity $9,466.68 vs yday $9,602.35 (-135.67) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-27.80 | $1,415.43 | ▼ -27.80 after sell → book $9,464.63; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 44 | $26.22 | $2.14 | $-73.34 | $2,566.97 | ▼ -73.34 after sell → book $9,462.48; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 124 | $9.67 | $2.39 | $-22.11 | $3,763.66 | ▼ -22.11 after sell → book $9,460.09; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 59 | $20.52 | $2.19 | $-12.02 | $4,972.15 | ▼ -12.02 after sell → book $9,457.90; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 10 | $112.33 | $2.04 | $-40.76 | $6,093.41 | ▼ -40.76 after sell → book $9,455.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 6 | $192.26 | $2.03 | $-31.16 | $7,244.94 | ▼ -31.16 after sell → book $9,453.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 11 | $105.49 | $2.04 | $-50.02 | $8,403.31 | ▼ -50.02 after sell → book $9,451.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SNX` | 4 | $262.12 | $2.02 | $-89.38 | $9,449.77 | ▼ -89.38 after sell → book $9,449.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.77 | ▲ close $9,449.77 vs 09:30 $9,466.68 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,200.46 | ▲ 09:30 equity $8,200.46 vs yday $8,200.46 (+0.00) | 09:30 open · cash $8,200.46 · no holdings · equity $8,200.46 vs prior close $8,200.46 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 178 | $7.65 | $2.52 | — | $6,836.24 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1366.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 16 | $83.76 | $2.04 | — | $5,494.04 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1366.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 16 | $83.69 | $2.04 | — | $4,152.88 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1366.74 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SAIL` | 61 | $22.05 | $2.17 | — | $2,805.66 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $1366.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 132 | $10.28 | $2.39 | — | $1,446.31 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ⚪; ret5=+9.7; leftover $1366.74 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RGEN` | 7 | $189.92 | $2.01 | — | $114.86 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1366.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.86 | ▼ close $8,105.98 vs 09:30 $8,200.46 (session -81.31) | 16:00 close · cash $114.86 · equity $8,105.98 vs 09:30 $8,200.46 (-94.48; session marks -81.31) · 6 name(s) marked open→close (per-name table). MRVI×178 09:30 $7.65 → close $7.60 -8.90; TXG×16 09:30 $83.76 → close $85.71 +31.20; TEM×16 09:30 $83.69 → close $85.01 +21.04; SAIL×61 09:30 $22.05 → close $20.64 -86.01; SENS×132 09:30 $10.28 → close $10.00 -36.96; RGEN×7 09:30 $189.92 → close $189.68 -1.68 | — |
