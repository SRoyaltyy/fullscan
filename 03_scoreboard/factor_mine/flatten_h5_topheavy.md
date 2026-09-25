# Factor mine action — `flatten_h5_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **-17.46%** ($8,254) · signal-only (no cash/fees) was +4.47%. Starts YES **12/30**. Fills 128 · skips 345 · realized $+372.21.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $126.46.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $4000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $857.14 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.57 | ▲ close $10,123.05 vs 09:30 $10,000.00 (session +149.49) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.52 | ▲ close $10,395.68 vs 09:30 $10,109.78 (session +286.33) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.52 | ▼ 09:30 equity $10,380.01 vs yday $10,395.68 (-15.67) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $49.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.67 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 203 | $20.55 | $2.62 | — | $6,276.32 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4180.24 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,455.21 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 43 | $20.65 | $2.12 | — | $4,565.14 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 155 | $5.77 | $2.46 | — | $3,668.34 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 45 | $19.63 | $2.12 | — | $2,782.86 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 30 | $29.63 | $2.08 | — | $1,891.88 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 511 | $1.75 | $6.59 | — | $991.04 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $895.76 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $121.79 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $895.76 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.79 | ▲ close $10,821.22 vs 09:30 $10,599.55 (session +272.38) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.79 | ▲ 09:30 equity $11,128.77 vs yday $10,821.22 (+307.55) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $133.35 | ▲ +2.46 after sell → book $11,128.63; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $145.31 | ▼ -1.34 after sell → book $11,128.45; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $160.05 | ▲ +1.08 after sell → book $11,128.25; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $148.81 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $13.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $136.32 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $13.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 7 | $1.93 | $0.16 | — | $122.65 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $13.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 10 | $1.32 | $0.16 | — | $109.29 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $13.72 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.29 | ▼ close $11,000.40 vs 09:30 $11,128.77 (session -127.28) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.29 | ▲ 09:30 equity $11,110.36 vs yday $11,000.40 (+109.96) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 1 | $48.89 | $0.51 | $+1.73 | $157.67 | ▲ +1.73 after sell → book $11,109.85; vs 09:30 mark -0.51 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $166.80 | ▲ +0.94 after sell → book $11,109.73; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $175.94 | ▲ +0.60 after sell → book $11,109.61; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $186.31 | ▲ +0.54 after sell → book $11,109.48; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $196.28 | ▲ +0.25 after sell → book $11,109.35; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $196.28 | ▼ close $11,010.68 vs 09:30 $11,110.36 (session -98.67) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.28 | ▼ 09:30 equity $10,810.20 vs yday $11,010.68 (-200.48) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 3 | $23.77 | $0.72 | — | $124.25 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $78.51 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 2 | $10.98 | $0.23 | — | $102.07 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $23.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $85.19 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $23.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 4 | $4.94 | $0.21 | — | $65.22 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $23.55 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.22 | ▲ close $11,257.95 vs 09:30 $10,810.20 (session +449.08) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.22 | ▼ 09:30 equity $11,022.40 vs yday $11,257.95 (-235.55) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.22 | ▼ close $10,970.67 vs 09:30 $11,022.40 (session -51.73) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.22 | ▲ 09:30 equity $10,978.06 vs yday $10,970.67 (+7.39) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 203 | $20.93 | $2.69 | $+71.83 | $4,311.33 | ▲ +71.83 after sell → book $10,975.38; vs 09:30 mark -2.68 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 9 | $95.52 | $2.04 | $+36.54 | $5,168.97 | ▲ +36.54 after sell → book $10,973.34; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 43 | $21.31 | $2.14 | $+24.12 | $6,083.16 | ▲ +24.12 after sell → book $10,971.20; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 155 | $5.49 | $2.49 | $-48.35 | $6,931.62 | ▼ -48.35 after sell → book $10,968.71; vs 09:30 mark -2.49 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 45 | $21.47 | $2.15 | $+78.53 | $7,895.63 | ▲ +78.53 after sell → book $10,966.57; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 30 | $32.32 | $2.10 | $+76.52 | $8,863.13 | ▲ +76.52 after sell → book $10,964.47; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 511 | $1.91 | $6.69 | $+68.48 | $9,832.45 | ▲ +68.48 after sell → book $10,957.78; vs 09:30 mark -6.69 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 6 | $155.89 | $2.03 | $+64.06 | $10,765.76 | ▲ +64.06 after sell → book $10,955.75; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 103 | $41.44 | $2.30 | — | $6,495.14 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $4306.30 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 223 | $14.42 | $2.88 | — | $3,276.61 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $3229.73 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1242 | $2.60 | $16.02 | — | $31.38 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $3229.73 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.38 | ▲ close $11,050.19 vs 09:30 $10,978.06 (session +115.64) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.38 | ▲ 09:30 equity $11,111.60 vs yday $11,050.19 (+61.41) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.43 | $0.18 | $+4.01 | $46.64 | ▲ +4.01 after sell → book $11,111.43; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 5 | $2.35 | $0.15 | $-0.89 | $58.23 | ▼ -0.89 after sell → book $11,111.27; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 7 | $2.06 | $0.19 | $+0.57 | $72.47 | ▲ +0.57 after sell → book $11,111.09; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 10 | $1.82 | $0.23 | $+4.61 | $90.44 | ▲ +4.61 after sell → book $11,110.86; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.44 | ▼ close $10,841.19 vs 09:30 $11,111.60 (session -269.67) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.44 | ▲ 09:30 equity $10,989.98 vs yday $10,841.19 (+148.79) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.44 | ▲ close $11,009.05 vs 09:30 $10,989.98 (session +19.07) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.44 | ▲ 09:30 equity $11,381.98 vs yday $11,009.05 (+372.93) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 3 | $23.94 | $0.75 | $-0.96 | $161.51 | ▼ -0.96 after sell → book $11,381.23; vs 09:30 mark -0.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 2 | $10.42 | $0.23 | $-1.58 | $182.11 | ▼ -1.58 after sell → book $11,380.99; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $198.42 | ▼ -0.56 after sell → book $11,380.80; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 4 | $4.64 | $0.22 | $-1.63 | $216.77 | ▼ -1.63 after sell → book $11,380.59; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.77 | ▼ close $11,249.01 vs 09:30 $11,381.98 (session -131.58) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.77 | ▼ 09:30 equity $11,146.75 vs yday $11,249.01 (-102.26) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.77 | ▼ close $11,050.85 vs 09:30 $11,146.75 (session -95.90) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.77 | ▲ 09:30 equity $11,124.99 vs yday $11,050.85 (+74.14) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 103 | $42.43 | $2.35 | $+97.32 | $4,584.70 | ▲ +97.32 after sell → book $11,122.63; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 223 | $15.45 | $2.94 | $+223.87 | $8,027.11 | ▲ +223.87 after sell → book $11,119.69; vs 09:30 mark -2.94 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1242 | $2.49 | $16.25 | $-168.89 | $11,103.44 | ▼ -168.89 after sell → book $11,103.44; vs 09:30 mark -16.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 83 | $52.88 | $2.24 | — | $6,712.16 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $4441.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 38 | $42.93 | $2.10 | — | $5,078.72 | — | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $1665.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 458 | $3.63 | $5.91 | — | $3,410.27 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $1665.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 207 | $8.03 | $2.67 | — | $1,745.39 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $1665.52 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 12 | $132.45 | $2.03 | — | $153.96 | — | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $1665.52 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.96 | ▼ close $10,912.08 vs 09:30 $11,124.99 (session -176.41) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.96 | ▼ 09:30 equity $10,831.86 vs yday $10,912.08 (-80.22) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 24 | $2.52 | $0.68 | — | $92.81 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $61.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $79.25 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $18.48 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 9 | $1.90 | $0.20 | — | $61.95 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $18.48 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $4.78 | $0.15 | — | $47.46 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $18.48 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 11 | $1.59 | $0.21 | — | $29.76 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $18.48 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $18.33 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $18.48 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.33 | ▲ close $10,882.14 vs 09:30 $10,831.86 (session +51.76) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.33 | ▲ 09:30 equity $11,070.31 vs yday $10,882.14 (+188.17) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.33 | ▼ close $10,902.66 vs 09:30 $11,070.31 (session -167.65) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.33 | ▼ 09:30 equity $10,827.09 vs yday $10,902.66 (-75.57) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.33 | ▼ close $10,587.14 vs 09:30 $10,827.09 (session -239.95) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.33 | ▼ 09:30 equity $10,464.99 vs yday $10,587.14 (-122.15) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.33 | ▼ close $10,376.97 vs 09:30 $10,464.99 (session -88.02) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.33 | ▲ 09:30 equity $10,481.24 vs yday $10,376.97 (+104.27) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 83 | $53.53 | $2.29 | $+49.42 | $4,459.03 | ▲ +49.42 after sell → book $10,478.95; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 38 | $41.30 | $2.13 | $-66.17 | $6,026.31 | ▼ -66.17 after sell → book $10,476.83; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 458 | $2.77 | $5.99 | $-405.78 | $7,288.97 | ▼ -405.78 after sell → book $10,470.83; vs 09:30 mark -6.00 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 207 | $7.70 | $2.72 | $-73.70 | $8,880.16 | ▼ -73.70 after sell → book $10,468.12; vs 09:30 mark -2.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 12 | $122.40 | $2.05 | $-124.67 | $10,346.91 | ▼ -124.67 after sell → book $10,466.07; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 254 | $16.28 | $3.28 | — | $6,208.51 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $4138.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 454 | $2.73 | $5.86 | — | $4,963.23 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1241.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $3,720.19 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1241.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $2,567.17 | — | 40% to #1, rest split; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1241.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $1,460.69 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1241.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $224.66 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1241.63 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.66 | ▼ close $10,394.54 vs 09:30 $10,481.24 (session -54.31) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.66 | ▼ 09:30 equity $10,094.55 vs yday $10,394.54 (-299.99) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 24 | $2.15 | $0.61 | $-10.16 | $275.65 | ▼ -10.16 after sell → book $10,093.95; vs 09:30 mark -0.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 2 | $5.93 | $0.14 | $-1.84 | $287.37 | ▼ -1.84 after sell → book $10,093.80; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 9 | $1.72 | $0.20 | $-2.06 | $302.60 | ▼ -2.06 after sell → book $10,093.60; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 3 | $4.13 | $0.15 | $-2.26 | $314.84 | ▼ -2.26 after sell → book $10,093.45; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 11 | $1.59 | $0.23 | $-0.44 | $332.10 | ▼ -0.44 after sell → book $10,093.22; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 1 | $10.73 | $0.13 | $-0.83 | $342.70 | ▼ -0.83 after sell → book $10,093.09; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.70 | ▼ close $10,070.39 vs 09:30 $10,094.55 (session -22.70) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $342.70 | ▲ 09:30 equity $10,110.54 vs yday $10,070.39 (+40.15) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.70 | ▼ close $9,929.11 vs 09:30 $10,110.54 (session -181.43) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $342.70 | ▲ 09:30 equity $9,994.38 vs yday $9,929.11 (+65.27) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 4 | $14.31 | $0.58 | — | $284.87 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $68.54 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 1 | $36.46 | $0.37 | — | $248.05 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $68.54 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.05 | ▲ close $10,199.25 vs 09:30 $9,994.38 (session +205.82) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.05 | ▲ 09:30 equity $10,382.99 vs yday $10,199.25 (+183.74) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $227.34 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $29.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $204.33 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $29.77 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.33 | ▼ close $10,374.15 vs 09:30 $10,382.99 (session -8.39) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.33 | ▲ 09:30 equity $10,394.20 vs yday $10,374.15 (+20.05) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 254 | $16.93 | $3.35 | $+158.47 | $4,501.20 | ▲ +158.47 after sell → book $10,390.85; vs 09:30 mark -3.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 454 | $2.68 | $5.94 | $-34.50 | $5,711.97 | ▼ -34.50 after sell → book $10,384.90; vs 09:30 mark -5.95 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 6 | $197.76 | $2.03 | $-58.52 | $6,896.51 | ▼ -58.52 after sell → book $10,382.88; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 7 | $150.47 | $2.03 | $-101.76 | $7,947.76 | ▼ -101.76 after sell → book $10,380.84; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 7 | $152.71 | $2.03 | $-39.53 | $9,014.70 | ▼ -39.53 after sell → book $10,378.81; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 22 | $55.80 | $2.08 | $-10.51 | $10,240.23 | ▼ -10.51 after sell → book $10,376.74; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 37 | $108.55 | $2.10 | — | $6,221.78 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $4096.09 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $5,033.48 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1228.83 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $3,983.88 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1228.83 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $2,883.77 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1228.83 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $1,691.74 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1228.83 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 35 | $34.44 | $2.10 | — | $484.24 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1228.83 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $484.24 | ▼ close $10,180.39 vs 09:30 $10,394.20 (session -184.11) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $484.24 | ▲ 09:30 equity $10,289.42 vs yday $10,180.39 (+109.03) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $324.79 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $193.70 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 5 | $13.47 | $0.69 | — | $256.75 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $72.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 18 | $4.00 | $0.77 | — | $183.98 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $72.64 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.98 | ▲ close $10,533.55 vs 09:30 $10,289.42 (session +247.17) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.98 | ▼ 09:30 equity $10,524.49 vs yday $10,533.55 (-9.06) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.98 | ▼ close $10,487.61 vs 09:30 $10,524.49 (session -36.88) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.98 | ▲ 09:30 equity $10,697.17 vs yday $10,487.61 (+209.56) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 4 | $13.12 | $0.56 | $-5.90 | $235.90 | ▼ -5.90 after sell → book $10,696.62; vs 09:30 mark -0.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 1 | $38.04 | $0.40 | $+0.81 | $273.54 | ▲ +0.81 after sell → book $10,696.21; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 1 | $89.50 | $0.90 | — | $183.14 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.3; leftover $109.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $155.07 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $32.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 3 | $9.81 | $0.30 | — | $125.34 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $32.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 1 | $20.25 | $0.21 | — | $104.88 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $32.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $84.02 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $32.82 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.02 | ▲ close $10,799.11 vs 09:30 $10,697.17 (session +104.79) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.02 | ▼ 09:30 equity $10,638.76 vs yday $10,799.11 (-160.35) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 2 | $10.39 | $0.23 | $-0.16 | $104.57 | ▼ -0.16 after sell → book $10,638.52; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $126.46 | ▼ -1.12 after sell → book $10,638.27; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.46 | ▲ close $10,641.81 vs 09:30 $10,638.76 (session +3.54) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $264.02 | ▲ 09:30 equity $8,284.03 vs yday $8,267.74 (+16.29) | 09:30 open · cash $264.02 (unchanged overnight, no fees) · equity $8,284.03 vs prior close $8,267.74 (+16.29) · 11 name(s) re-marked at the open (per-name table). A×4 yday $172.84 → 09:30 $171.98 -3.44; ADMA×84 yday $9.52 → 09:30 $9.52 +0.00; ARQT×29 yday $26.27 → 09:30 $26.27 +0.00; CYPH×6 yday $4.08 → 09:30 $4.00 -0.45; DLO×1 yday $13.88 → 09:30 $13.88 +0.00; DXCM×37 yday $87.47 → 09:30 $87.47 +0.00; FTRE×40 yday $20.02 → 09:30 $20.02 +0.00; HALO×7 yday $115.22 → 09:30 $115.36 +0.98; MGTX×2 yday $11.05 → 09:30 $11.05 +0.00; OMER×40 yday $20.13 → 09:30 $20.61 +19.20; PACS×1 yday $41.46 → 09:30 $41.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 2 | $38.51 | $0.78 | — | $186.22 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $79.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 10 | $7.65 | $0.80 | — | $108.93 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $79.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.93 | ▼ close $8,254.43 vs 09:30 $8,284.03 (session -28.03) | 16:00 close · cash $108.93 · equity $8,254.43 vs 09:30 $8,284.03 (-29.60; session marks -28.03) · 13 name(s) marked open→close (per-name table). A×4 09:30 $171.98 → close $172.79 +3.24; ADMA×84 09:30 $9.52 → close $9.52 +0.00; ARQT×29 09:30 $26.27 → close $26.27 +0.00; CYPH×6 09:30 $4.00 → close $4.12 +0.69; DLO×1 09:30 $13.88 → close $13.88 +0.00; DXCM×37 09:30 $87.47 → close $87.47 +0.00; FTRE×40 09:30 $20.02 → close $20.02 +0.00; HALO×7 09:30 $115.36 → close $113.90 -10.22; MGTX×2 09:30 $11.05 → close $11.05 +0.00; OMER×40 09:30 $20.61 → close $20.08 -21.20; PACS×1 09:30 $41.46 → close $41.46 -0.00; BLFS×2 09:30 $38.51 → close $38.49 -0.04; MRVI×10 09:30 $7.65 → close $7.60 -0.50 | — |

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
| 2026-08-25 | `INSP` | cash | leftover split 23.55 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 23.55 < 1 share @ 426.97 |
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
| 2026-08-26 | `HCA` | cash | leftover split 26.09 < 1 share @ 427.50 |
| 2026-08-26 | `INSP` | cash | leftover split 39.13 < 1 share @ 60.07 |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `OVID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ORCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COHU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `IQV` | cash | leftover split 137.08 < 1 share @ 270.89 |
| 2026-09-16 | `RDNT` | cash | leftover split 68.54 < 1 share @ 77.12 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 99.22 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 29.77 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 29.77 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 29.77 < 1 share @ 34.93 |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `HUM` | cash | leftover split 72.64 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 72.64 < 1 share @ 88.83 |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 22.08 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 32.82 < 1 share @ 116.85 |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 37 | 2026-09-18 @ $108.55 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $4096.09 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1228.83 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1228.83 |
| `VICR` | 5 | 2026-09-18 @ $219.62 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1228.83 |
| `ECO` | 14 | 2026-09-18 @ $85.00 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1228.83 |
| `FIVN` | 35 | 2026-09-18 @ $34.44 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1228.83 |
| `A` | 1 | 2026-09-21 @ $157.87 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $193.70 |
| `MGTX` | 5 | 2026-09-21 @ $13.47 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $72.64 |
| `CYPH` | 18 | 2026-09-21 @ $4.00 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $72.64 |
| `DXCM` | 1 | 2026-09-23 @ $89.50 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.3; leftover $109.42 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $32.82 |
| `ADMA` | 3 | 2026-09-23 @ $9.81 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $32.82 |
| `FTRE` | 1 | 2026-09-23 @ $20.25 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $32.82 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $32.82 |
