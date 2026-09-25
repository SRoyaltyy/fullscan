# Factor mine action — `union_h5_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **-12.82%** ($8,718) · signal-only (no cash/fees) was +16.74%. Starts YES **14/30**. Fills 176 · skips 469 · realized $+84.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $152.08.

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
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $13.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.52 | ▲ close $10,395.68 vs 09:30 $10,109.78 (session +286.33) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.52 | ▼ 09:30 equity $10,380.01 vs yday $10,395.68 (-15.67) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $49.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $10.67 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.59 | ▲ close $10,388.13 vs 09:30 $10,380.01 (session +8.96) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▼ 09:30 equity $10,277.67 vs yday $10,388.13 (-110.46) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.59 | ▲ close $10,375.43 vs 09:30 $10,277.67 (session +97.76) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▲ 09:30 equity $10,504.56 vs yday $10,375.43 (+129.13) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.59 | ▲ close $10,678.44 vs 09:30 $10,504.56 (session +173.88) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▼ 09:30 equity $10,599.55 vs yday $10,678.44 (-78.89) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 66 | $58.64 | $2.23 | $-80.98 | $3,909.60 | ▼ -80.98 after sell → book $10,597.32; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 18 | $42.46 | $2.06 | $-67.47 | $4,671.82 | ▼ -67.47 after sell → book $10,595.25; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 16 | $53.06 | $2.06 | $+34.89 | $5,518.72 | ▲ +34.89 after sell → book $10,593.20; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 17 | $51.65 | $2.06 | $+29.05 | $6,394.71 | ▲ +29.05 after sell → book $10,591.14; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 73 | $13.84 | $2.23 | $+151.78 | $7,402.79 | ▲ +151.78 after sell → book $10,588.90; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $8,259.18 | ▲ +21.59 after sell → book $10,586.81; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1058 | $1.30 | $13.83 | $+492.84 | $9,620.75 | ▲ +492.84 after sell → book $10,572.98; vs 09:30 mark -13.83 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 36 | $23.11 | $2.12 | $-12.14 | $10,450.59 | ▼ -12.14 after sell → book $10,570.86; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 203 | $20.55 | $2.62 | — | $6,276.32 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4180.24 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,455.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 43 | $20.65 | $2.12 | — | $4,565.14 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 155 | $5.77 | $2.46 | — | $3,668.34 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 45 | $19.63 | $2.12 | — | $2,782.86 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 30 | $29.63 | $2.08 | — | $1,891.88 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 511 | $1.75 | $6.59 | — | $991.04 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $121.79 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $895.76 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.79 | ▲ close $10,821.22 vs 09:30 $10,599.55 (session +272.38) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.79 | ▲ 09:30 equity $11,128.77 vs yday $10,821.22 (+307.55) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $133.35 | ▲ +2.46 after sell → book $11,128.63; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $145.31 | ▼ -1.34 after sell → book $11,128.45; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $160.05 | ▲ +1.08 after sell → book $11,128.25; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $148.81 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $136.32 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $13.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 7 | $1.93 | $0.16 | — | $122.65 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $13.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 10 | $1.32 | $0.16 | — | $109.29 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $13.72 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.29 | ▼ close $11,000.40 vs 09:30 $11,128.77 (session -127.28) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.29 | ▲ 09:30 equity $11,110.36 vs yday $11,000.40 (+109.96) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 1 | $48.89 | $0.51 | $+1.73 | $157.67 | ▲ +1.73 after sell → book $11,109.85; vs 09:30 mark -0.51 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $166.80 | ▲ +0.94 after sell → book $11,109.73; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $175.94 | ▲ +0.60 after sell → book $11,109.61; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $186.31 | ▲ +0.54 after sell → book $11,109.48; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $196.28 | ▲ +0.25 after sell → book $11,109.35; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $196.28 | ▼ close $11,010.68 vs 09:30 $11,110.36 (session -98.67) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.28 | ▼ 09:30 equity $10,810.20 vs yday $11,010.68 (-200.48) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 3 | $23.77 | $0.72 | — | $124.25 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $78.51 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $113.16 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+1.2; leftover $16.82 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $96.29 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.0; leftover $16.82 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $4.94 | $0.16 | — | $81.31 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $16.82 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 2 | $7.25 | $0.15 | — | $66.66 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $16.82 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 46 | $0.36 | $0.30 | — | $49.89 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-15.6; leftover $16.82 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.89 | ▲ close $11,259.59 vs 09:30 $10,810.20 (session +451.01) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.89 | ▼ 09:30 equity $11,024.09 vs yday $11,259.59 (-235.50) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.89 | ▼ close $10,976.00 vs 09:30 $11,024.09 (session -48.08) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.89 | ▲ 09:30 equity $10,983.44 vs yday $10,976.00 (+7.44) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 203 | $20.93 | $2.69 | $+71.83 | $4,295.99 | ▲ +71.83 after sell → book $10,980.75; vs 09:30 mark -2.69 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 9 | $95.52 | $2.04 | $+36.54 | $5,153.63 | ▲ +36.54 after sell → book $10,978.72; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 43 | $21.31 | $2.14 | $+24.12 | $6,067.82 | ▲ +24.12 after sell → book $10,976.58; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 155 | $5.49 | $2.49 | $-48.35 | $6,916.28 | ▼ -48.35 after sell → book $10,974.09; vs 09:30 mark -2.49 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 45 | $21.47 | $2.15 | $+78.53 | $7,880.29 | ▲ +78.53 after sell → book $10,971.94; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 30 | $32.32 | $2.10 | $+76.52 | $8,847.79 | ▲ +76.52 after sell → book $10,969.84; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 511 | $1.91 | $6.69 | $+68.48 | $9,817.11 | ▲ +68.48 after sell → book $10,963.15; vs 09:30 mark -6.69 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 6 | $155.89 | $2.03 | $+64.06 | $10,750.42 | ▲ +64.06 after sell → book $10,961.13; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 103 | $41.44 | $2.30 | — | $6,479.81 | — | 40% to #1, rest split; list flatten; ret5=+3.1; leftover $4300.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 74 | $14.42 | $2.21 | — | $5,410.51 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $1075.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 413 | $2.60 | $5.33 | — | $4,331.39 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+13.0; leftover $1075.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 82 | $12.98 | $2.24 | — | $3,264.79 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1075.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 34 | $30.79 | $2.09 | — | $2,215.84 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1075.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `FLNC` | 93 | $11.52 | $2.27 | — | $1,142.21 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-8.2; leftover $1075.04 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 111 | $9.68 | $2.32 | — | $65.41 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1075.04 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.41 | ▲ close $11,027.45 vs 09:30 $10,983.44 (session +85.08) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.41 | ▼ 09:30 equity $11,017.28 vs yday $11,027.45 (-10.17) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.43 | $0.18 | $+4.01 | $80.66 | ▲ +4.01 after sell → book $11,017.10; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 5 | $2.35 | $0.15 | $-0.89 | $92.26 | ▼ -0.89 after sell → book $11,016.95; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 7 | $2.06 | $0.19 | $+0.57 | $106.49 | ▲ +0.57 after sell → book $11,016.76; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 10 | $1.82 | $0.23 | $+4.61 | $124.46 | ▲ +4.61 after sell → book $11,016.53; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 1 | $32.90 | $0.33 | — | $91.23 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $49.78 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 1 | $15.66 | $0.16 | — | $75.41 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $24.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 7 | $3.32 | $0.25 | — | $51.91 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=+6.4; leftover $24.89 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.91 | ▼ close $10,805.53 vs 09:30 $11,017.28 (session -210.26) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.91 | ▲ 09:30 equity $10,882.45 vs yday $10,805.53 (+76.92) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.91 | ▼ close $10,827.89 vs 09:30 $10,882.45 (session -54.56) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.91 | ▲ 09:30 equity $10,947.95 vs yday $10,827.89 (+120.06) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 3 | $23.94 | $0.75 | $-0.96 | $122.99 | ▼ -0.96 after sell → book $10,947.20; vs 09:30 mark -0.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $133.28 | ▼ -0.80 after sell → book $10,947.08; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $149.59 | ▼ -0.56 after sell → book $10,946.88; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.64 | $0.17 | $-1.23 | $163.34 | ▼ -1.23 after sell → book $10,946.72; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 2 | $10.77 | $0.24 | $+6.65 | $184.64 | ▲ +6.65 after sell → book $10,946.47; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 46 | $0.36 | $0.33 | $-0.31 | $201.10 | ▼ -0.31 after sell → book $10,946.15; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.10 | ▼ close $10,926.26 vs 09:30 $10,947.95 (session -19.89) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.10 | ▼ 09:30 equity $10,887.70 vs yday $10,926.26 (-38.56) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.10 | ▲ close $10,952.49 vs 09:30 $10,887.70 (session +64.80) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.10 | ▼ 09:30 equity $10,948.31 vs yday $10,952.49 (-4.18) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 103 | $42.43 | $2.35 | $+97.32 | $4,569.04 | ▲ +97.32 after sell → book $10,945.96; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 413 | $2.49 | $5.41 | $-56.16 | $5,592.01 | ▼ -56.16 after sell → book $10,940.56; vs 09:30 mark -5.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 82 | $13.25 | $2.26 | $+17.64 | $6,676.25 | ▲ +17.64 after sell → book $10,938.30; vs 09:30 mark -2.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AVBP` | 34 | $30.58 | $2.11 | $-11.34 | $7,713.85 | ▼ -11.34 after sell → book $10,936.18; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `FLNC` | 93 | $10.01 | $2.29 | $-144.81 | $8,642.68 | ▼ -144.81 after sell → book $10,933.89; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 111 | $9.68 | $2.35 | $-4.67 | $9,714.80 | ▼ -4.67 after sell → book $10,931.54; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 73 | $52.88 | $2.21 | — | $5,852.35 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3885.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 22 | $42.93 | $2.06 | — | $4,905.84 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $971.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 267 | $3.63 | $3.44 | — | $3,933.18 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $971.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 120 | $8.03 | $2.35 | — | $2,967.23 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $971.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $132.45 | $2.01 | — | $2,038.07 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $971.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 6 | $145.94 | $2.01 | — | $1,160.40 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $971.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 57 | $16.77 | $2.16 | — | $202.34 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $971.48 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.34 | ▼ close $10,712.93 vs 09:30 $10,948.31 (session -202.38) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.34 | ▼ 09:30 equity $10,690.38 vs yday $10,712.93 (-22.55) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 74 | $15.00 | $2.23 | $+38.47 | $1,310.11 | ▲ +38.47 after sell → book $10,688.15; vs 09:30 mark -2.23 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 1 | $33.86 | $0.36 | $+0.27 | $1,343.61 | ▲ +0.27 after sell → book $10,687.79; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 1 | $13.56 | $0.16 | $-2.42 | $1,357.01 | ▼ -2.42 after sell → book $10,687.63; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 7 | $3.53 | $0.29 | $+0.93 | $1,381.43 | ▲ +0.93 after sell → book $10,687.34; vs 09:30 mark -0.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 219 | $2.52 | $2.83 | — | $826.73 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $552.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 24 | $6.71 | $1.68 | — | $664.00 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $165.77 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 87 | $1.90 | $1.91 | — | $496.79 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $165.77 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 34 | $4.78 | $1.73 | — | $332.54 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $165.77 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 104 | $1.59 | $1.97 | — | $165.22 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $165.77 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 14 | $11.31 | $1.63 | — | $5.25 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $165.77 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.25 | ▼ close $10,642.74 vs 09:30 $10,690.38 (session -32.86) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.25 | ▲ 09:30 equity $10,780.96 vs yday $10,642.74 (+138.22) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.25 | ▼ close $10,647.42 vs 09:30 $10,780.96 (session -133.54) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.25 | ▼ 09:30 equity $10,575.34 vs yday $10,647.42 (-72.08) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.25 | ▼ close $10,296.14 vs 09:30 $10,575.34 (session -279.20) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.25 | ▼ 09:30 equity $10,157.31 vs yday $10,296.14 (-138.83) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.25 | ▼ close $10,084.29 vs 09:30 $10,157.31 (session -73.02) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.25 | ▲ 09:30 equity $10,186.14 vs yday $10,084.29 (+101.85) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 73 | $53.53 | $2.25 | $+42.99 | $3,910.69 | ▲ +42.99 after sell → book $10,183.89; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 22 | $41.30 | $2.08 | $-39.99 | $4,817.21 | ▼ -39.99 after sell → book $10,181.81; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 267 | $2.77 | $3.50 | $-236.56 | $5,553.31 | ▼ -236.56 after sell → book $10,178.32; vs 09:30 mark -3.49 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 120 | $7.70 | $2.38 | $-44.33 | $6,474.93 | ▼ -44.33 after sell → book $10,175.94; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 7 | $122.40 | $2.03 | $-74.39 | $7,329.69 | ▼ -74.39 after sell → book $10,173.90; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 6 | $137.91 | $2.03 | $-52.28 | $8,155.10 | ▼ -52.28 after sell → book $10,171.88; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 57 | $14.06 | $2.18 | $-158.81 | $8,954.34 | ▼ -158.81 after sell → book $10,169.70; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 220 | $16.28 | $2.84 | — | $5,369.90 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-1.1; leftover $3581.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 281 | $2.73 | $3.62 | — | $4,599.14 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-3.0; leftover $767.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 3 | $206.84 | $2.00 | — | $3,976.62 | — | 40% to #1, rest split; list flatten; ret5=+8.3; leftover $767.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $3,316.90 | — | 40% to #1, rest split; list flatten,earn_react; ⚪; ret5=+4.9; leftover $767.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 4 | $157.78 | $2.00 | — | $2,683.78 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+4.7; leftover $767.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 13 | $56.09 | $2.03 | — | $1,952.58 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+19.6; leftover $767.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 376 | $2.04 | $4.85 | — | $1,180.69 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $767.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 161 | $4.75 | $2.47 | — | $413.47 | — | 40% to #1, rest split; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $767.51 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $413.47 | ▼ close $10,094.84 vs 09:30 $10,186.14 (session -53.04) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $413.47 | ▼ 09:30 equity $9,928.55 vs yday $10,094.84 (-166.29) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 219 | $2.15 | $2.87 | $-86.73 | $881.45 | ▼ -86.73 after sell → book $9,925.68; vs 09:30 mark -2.87 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 24 | $5.93 | $1.52 | $-21.92 | $1,022.25 | ▼ -21.92 after sell → book $9,924.16; vs 09:30 mark -1.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 87 | $1.72 | $1.78 | $-19.79 | $1,169.68 | ▼ -19.79 after sell → book $9,922.38; vs 09:30 mark -1.78 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 34 | $4.13 | $1.53 | $-25.35 | $1,308.57 | ▼ -25.35 after sell → book $9,920.86; vs 09:30 mark -1.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 104 | $1.59 | $1.99 | $-3.96 | $1,471.94 | ▼ -3.96 after sell → book $9,918.86; vs 09:30 mark -2.00 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 14 | $10.73 | $1.56 | $-11.31 | $1,620.59 | ▼ -11.31 after sell → book $9,917.30; vs 09:30 mark -1.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,620.59 | ▲ close $9,958.48 vs 09:30 $9,928.55 (session +41.19) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,620.59 | ▲ 09:30 equity $9,962.25 vs yday $9,958.48 (+3.77) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,620.59 | ▼ close $9,772.16 vs 09:30 $9,962.25 (session -190.09) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,620.59 | ▲ 09:30 equity $9,814.85 vs yday $9,772.16 (+42.69) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $1,076.82 | — | 40% to #1, rest split; list flatten; ret5=+4.0; leftover $648.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 1 | $77.12 | $0.77 | — | $998.92 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+7.2; leftover $138.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 9 | $14.31 | $1.31 | — | $868.82 | — | 40% to #1, rest split; list flatten; ret5=+4.8; leftover $138.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 3 | $36.46 | $1.10 | — | $758.34 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+2.9; leftover $138.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 7 | $18.61 | $1.32 | — | $626.74 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $138.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 7 | $18.21 | $1.30 | — | $497.98 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-19.1; leftover $138.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 2 | $68.79 | $1.38 | — | $359.02 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $138.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 23 | $5.87 | $1.42 | — | $222.59 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $138.91 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.59 | ▲ close $9,975.48 vs 09:30 $9,814.85 (session +171.23) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.59 | ▲ 09:30 equity $10,126.54 vs yday $9,975.48 (+151.06) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 1 | $10.25 | $0.11 | — | $212.23 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $19.08 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $196.89 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $19.08 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 112 | $0.17 | $0.53 | — | $177.33 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $19.08 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $161.29 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $19.08 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.29 | ▼ close $10,090.72 vs 09:30 $10,126.54 (session -34.86) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.29 | ▲ 09:30 equity $10,105.23 vs yday $10,090.72 (+14.51) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 220 | $16.93 | $2.90 | $+137.26 | $3,882.99 | ▲ +137.26 after sell → book $10,102.33; vs 09:30 mark -2.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 281 | $2.68 | $3.68 | $-21.36 | $4,632.39 | ▼ -21.36 after sell → book $10,098.65; vs 09:30 mark -3.68 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 3 | $197.76 | $2.02 | $-31.26 | $5,223.65 | ▼ -31.26 after sell → book $10,096.63; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 4 | $150.47 | $2.02 | $-59.86 | $5,823.51 | ▼ -59.86 after sell → book $10,094.61; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 4 | $152.71 | $2.02 | $-24.30 | $6,432.33 | ▼ -24.30 after sell → book $10,092.59; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 13 | $55.80 | $2.05 | $-7.85 | $7,155.68 | ▼ -7.85 after sell → book $10,090.54; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 376 | $1.90 | $4.92 | $-62.41 | $7,865.15 | ▼ -62.41 after sell → book $10,085.61; vs 09:30 mark -4.93 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 161 | $4.50 | $2.51 | $-45.23 | $8,587.14 | ▼ -45.23 after sell → book $10,083.10; vs 09:30 mark -2.51 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 31 | $108.55 | $2.08 | — | $5,220.01 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+21.3; leftover $3434.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $4,624.87 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+16.1; leftover $736.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 3 | $209.52 | $2.00 | — | $3,994.31 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $736.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $3,333.45 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $736.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 8 | $85.00 | $2.01 | — | $2,651.44 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+18.3; leftover $736.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 21 | $34.44 | $2.05 | — | $1,926.14 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+14.0; leftover $736.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 758 | $0.97 | $9.63 | — | $1,181.26 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $736.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 353 | $2.08 | $4.55 | — | $442.46 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $736.04 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $442.46 | ▼ close $9,900.57 vs 09:30 $10,105.23 (session -156.21) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $442.46 | ▲ 09:30 equity $10,002.17 vs yday $9,900.57 (+101.60) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $283.01 | — | 40% to #1, rest split; list flatten; ret5=+6.5; leftover $176.99 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 3 | $13.47 | $0.41 | — | $242.19 | — | 40% to #1, rest split; list flatten; ret5=+3.6; leftover $53.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 13 | $4.00 | $0.56 | — | $189.63 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $53.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 5 | $9.31 | $0.48 | — | $142.60 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $53.10 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.60 | ▲ close $10,126.26 vs 09:30 $10,002.17 (session +127.12) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.60 | ▼ 09:30 equity $10,120.02 vs yday $10,126.26 (-6.24) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 21 | $0.58 | $0.18 | — | $130.23 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $12.22 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.23 | ▼ close $10,103.73 vs 09:30 $10,120.02 (session -16.10) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.23 | ▲ 09:30 equity $10,288.19 vs yday $10,103.73 (+184.46) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $669.54 | ▼ -4.47 after sell → book $10,286.18; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 1 | $73.61 | $0.76 | $-5.04 | $742.39 | ▼ -5.04 after sell → book $10,285.42; vs 09:30 mark -0.76 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 9 | $13.12 | $1.23 | $-13.25 | $859.24 | ▼ -13.25 after sell → book $10,284.19; vs 09:30 mark -1.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 3 | $38.04 | $1.17 | $+2.47 | $972.19 | ▲ +2.47 after sell → book $10,283.02; vs 09:30 mark -1.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 7 | $23.00 | $1.65 | $+27.76 | $1,131.54 | ▲ +27.76 after sell → book $10,281.37; vs 09:30 mark -1.65 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 7 | $23.30 | $1.67 | $+32.66 | $1,292.97 | ▲ +32.66 after sell → book $10,279.70; vs 09:30 mark -1.67 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 2 | $76.47 | $1.56 | $+12.42 | $1,444.35 | ▲ +12.42 after sell → book $10,278.14; vs 09:30 mark -1.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 23 | $5.53 | $1.36 | $-10.60 | $1,570.18 | ▼ -10.60 after sell → book $10,276.78; vs 09:30 mark -1.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 7 | $89.50 | $2.01 | — | $941.67 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.3; leftover $628.07 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $823.65 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+3.3; leftover $188.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 6 | $27.79 | $1.69 | — | $655.22 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+7.0; leftover $188.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 19 | $9.81 | $1.92 | — | $466.91 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.0; leftover $188.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 9 | $20.25 | $1.85 | — | $282.81 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+15.0; leftover $188.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 9 | $20.65 | $1.89 | — | $95.08 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $188.42 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.08 | ▲ close $10,533.61 vs 09:30 $10,288.19 (session +267.35) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.08 | ▼ 09:30 equity $10,454.32 vs yday $10,533.61 (-79.29) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 1 | $10.39 | $0.13 | $-0.09 | $105.34 | ▼ -0.09 after sell → book $10,454.19; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $119.93 | ▼ -0.75 after sell → book $10,454.02; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 112 | $0.15 | $0.53 | $-3.30 | $136.19 | ▼ -3.30 after sell → book $10,453.49; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 1 | $16.07 | $0.18 | $-0.15 | $152.08 | ▼ -0.15 after sell → book $10,453.30; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.08 | ▲ close $10,557.06 vs 09:30 $10,454.32 (session +103.76) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.77 | ▲ 09:30 equity $8,744.81 vs yday $8,728.42 (+16.39) | 09:30 open · cash $251.77 (unchanged overnight, no fees) · equity $8,744.81 vs prior close $8,728.42 (+16.39) · 15 name(s) re-marked at the open (per-name table). A×5 yday $172.84 → 09:30 $171.98 -4.30; ADMA×88 yday $9.52 → 09:30 $9.52 +0.00; ARQT×31 yday $26.27 → 09:30 $26.27 +0.00; BHVN×1 yday $13.19 → 09:30 $13.19 +0.00; BTDR×1 yday $12.15 → 09:30 $12.15 +0.00; CYPH×6 yday $4.08 → 09:30 $4.00 -0.45; DEFT×23 yday $0.53 → 09:30 $0.53 +0.00; DXCM×38 yday $87.47 → 09:30 $87.47 +0.00; EYPT×5 yday $3.65 → 09:30 $3.65 +0.00; FJET×6 yday $1.80 → 09:30 $1.80 +0.00; FTRE×42 yday $20.02 → 09:30 $20.02 +0.00; HALO×7 yday $115.22 → 09:30 $115.36 +0.98; MGTX×1 yday $11.05 → 09:30 $11.05 +0.00; OMER×42 yday $20.13 → 09:30 $20.61 +20.16; PACS×1 yday $41.46 → 09:30 $41.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 3 | $7.65 | $0.24 | — | $228.58 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.2; leftover $30.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 1 | $26.27 | $0.27 | — | $202.05 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $30.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 3 | $9.05 | $0.28 | — | $174.62 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-27.1; leftover $30.21 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.62 | ▼ close $8,717.50 vs 09:30 $8,744.81 (session -26.52) | 16:00 close · cash $174.62 · equity $8,717.50 vs 09:30 $8,744.81 (-27.31; session marks -26.52) · 18 name(s) marked open→close (per-name table). A×5 09:30 $171.98 → close $172.79 +4.05; ADMA×88 09:30 $9.52 → close $9.52 +0.00; ARQT×31 09:30 $26.27 → close $26.27 +0.00; BHVN×1 09:30 $13.19 → close $13.19 -0.00; BTDR×1 09:30 $12.15 → close $12.15 -0.00; CYPH×6 09:30 $4.00 → close $4.12 +0.69; DEFT×23 09:30 $0.53 → close $0.53 +0.00; DXCM×38 09:30 $87.47 → close $87.47 +0.00; EYPT×5 09:30 $3.65 → close $3.65 +0.00; FJET×6 09:30 $1.80 → close $1.80 -0.00; FTRE×42 09:30 $20.02 → close $20.02 +0.00; HALO×7 09:30 $115.36 → close $113.90 -10.22; MGTX×1 09:30 $11.05 → close $11.05 +0.00; OMER×42 09:30 $20.61 → close $20.08 -22.26; PACS×1 09:30 $41.46 → close $41.46 -0.00; MRVI×3 09:30 $7.65 → close $7.60 -0.15; WRBY×1 09:30 $26.27 → close $26.71 +0.44; AEHL×3 09:30 $9.05 → close $9.36 +0.93 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 64.23 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 10.67 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.67 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.67 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 64.02 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 13.72 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 13.72 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 13.72 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `INSP` | cash | leftover split 16.82 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 16.82 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 19.95 < 1 share @ 427.50 |
| 2026-08-26 | `INSP` | cash | leftover split 9.98 < 1 share @ 60.07 |
| 2026-08-26 | `AVBP` | cash | leftover split 9.98 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 9.98 < 1 share @ 11.12 |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `URBN` | cash | leftover split 24.89 < 1 share @ 79.42 |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-02 | `AVBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PYXS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `PYXS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `MRNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `MRNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 89.03 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 19.08 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 19.08 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 19.08 < 1 share @ 34.93 |
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
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `HUM` | cash | leftover split 53.10 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 53.10 < 1 share @ 88.83 |
| 2026-09-22 | `IQV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 12.22 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SWRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `RBRK` | 31 | 2026-09-18 @ $108.55 | 40% to #1, rest split; list flatten; ⚪; ret5=+21.3; leftover $3434.86 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+16.1; leftover $736.04 |
| `GNRC` | 3 | 2026-09-18 @ $209.52 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $736.04 |
| `VICR` | 3 | 2026-09-18 @ $219.62 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $736.04 |
| `ECO` | 8 | 2026-09-18 @ $85.00 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+18.3; leftover $736.04 |
| `FIVN` | 21 | 2026-09-18 @ $34.44 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+14.0; leftover $736.04 |
| `TLSA` | 758 | 2026-09-18 @ $0.97 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $736.04 |
| `SWRD` | 353 | 2026-09-18 @ $2.08 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $736.04 |
| `A` | 1 | 2026-09-21 @ $157.87 | 40% to #1, rest split; list flatten; ret5=+6.5; leftover $176.99 |
| `MGTX` | 3 | 2026-09-21 @ $13.47 | 40% to #1, rest split; list flatten; ret5=+3.6; leftover $53.10 |
| `CYPH` | 13 | 2026-09-21 @ $4.00 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $53.10 |
| `BKKT` | 5 | 2026-09-21 @ $9.31 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $53.10 |
| `DEFT` | 21 | 2026-09-22 @ $0.58 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $12.22 |
| `DXCM` | 7 | 2026-09-23 @ $89.50 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.3; leftover $628.07 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+3.3; leftover $188.42 |
| `ARQT` | 6 | 2026-09-23 @ $27.79 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+7.0; leftover $188.42 |
| `ADMA` | 19 | 2026-09-23 @ $9.81 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.0; leftover $188.42 |
| `FTRE` | 9 | 2026-09-23 @ $20.25 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+15.0; leftover $188.42 |
| `OMER` | 9 | 2026-09-23 @ $20.65 | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $188.42 |
