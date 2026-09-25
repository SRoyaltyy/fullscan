# Factor mine action — `union_h3_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **-19.83%** ($8,017) · signal-only (no cash/fees) was -4.92%. Starts YES **0/30**. Fills 184 · skips 290 · realized $-875.40.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,780.68.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $2222.22 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=+12.3; leftover $1944.44 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1388.89 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.2; leftover $555.56 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; ⚪; ret5=+19.7; leftover $277.78 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.05 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +139.38) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $118.95 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $10.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $112.30 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.11 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $109.27 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3.56 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.27 | ▲ close $10,260.71 vs 09:30 $10,103.42 (session +157.50) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.27 | ▲ 09:30 equity $10,281.18 vs yday $10,260.71 (+20.47) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $96.99 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $15.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $88.44 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $12.14 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $85.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $6.07 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.16 | ▲ close $10,290.17 vs 09:30 $10,281.18 (session +9.25) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▼ 09:30 equity $10,157.73 vs yday $10,290.17 (-132.44) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 37 | $60.00 | $2.13 | $+3.17 | $2,303.03 | ▲ +3.17 after sell → book $10,155.60; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 42 | $43.56 | $2.14 | $-105.90 | $4,130.41 | ▼ -105.90 after sell → book $10,153.46; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $5,784.94 | ▲ +32.50 after sell → book $10,151.35; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 27 | $49.28 | $2.09 | $-15.50 | $7,113.41 | ▼ -15.50 after sell → book $10,149.26; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 94 | $12.66 | $2.30 | $+85.67 | $8,301.16 | ▲ +85.67 after sell → book $10,146.97; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 28 | $27.85 | $2.09 | $-57.09 | $9,078.86 | ▼ -57.09 after sell → book $10,144.87; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 685 | $1.14 | $8.96 | $+209.49 | $9,850.80 | ▲ +209.49 after sell → book $10,135.91; vs 09:30 mark -8.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 11 | $22.16 | $2.04 | $-16.94 | $10,092.52 | ▼ -16.94 after sell → book $10,133.87; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.52 | ▼ close $10,133.65 vs 09:30 $10,157.73 (session -0.21) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.52 | ▲ 09:30 equity $10,134.11 vs yday $10,133.65 (+0.46) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,101.32 | ▼ -0.31 after sell → book $10,134.00; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 7 | $0.88 | $0.10 | $-0.59 | $10,107.37 | ▼ -0.59 after sell → book $10,133.89; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 2 | $1.42 | $0.05 | $-0.25 | $10,110.16 | ▼ -0.25 after sell → book $10,133.84; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,110.16 | ▼ close $10,133.76 vs 09:30 $10,134.11 (session -0.08) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,110.16 | ▼ 09:30 equity $10,133.47 vs yday $10,133.76 (-0.29) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 3 | $3.92 | $0.15 | $-0.67 | $10,121.77 | ▼ -0.67 after sell → book $10,133.32; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,130.02 | ▼ -0.30 after sell → book $10,133.22; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,133.16 | ▼ -0.13 after sell → book $10,133.16; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 109 | $20.55 | $2.32 | — | $7,890.89 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2251.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $5,977.63 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1970.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 81 | $20.65 | $2.23 | — | $4,302.75 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1688.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 243 | $5.77 | $3.13 | — | $2,897.50 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1407.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $1,776.43 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1125.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $944.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $844.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 321 | $1.75 | $4.14 | — | $378.83 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $562.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $232.84 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $281.48 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.84 | ▲ close $10,332.74 vs 09:30 $10,133.47 (session +219.14) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.84 | ▲ 09:30 equity $10,606.36 vs yday $10,332.74 (+273.62) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $198.09 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $45.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.60 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $32.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.62 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $25.87 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 10 | $1.93 | $0.22 | — | $131.10 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $19.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 4 | $1.32 | $0.06 | — | $125.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $6.47 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.76 | ▼ close $10,508.12 vs 09:30 $10,606.36 (session -97.10) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▲ 09:30 equity $10,605.68 vs yday $10,508.12 (+97.56) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.76 | ▼ close $10,515.27 vs 09:30 $10,605.68 (session -90.41) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▼ 09:30 equity $10,346.34 vs yday $10,515.27 (-168.93) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 109 | $20.32 | $2.35 | $-29.74 | $2,338.28 | ▼ -29.74 after sell → book $10,343.98; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 21 | $95.86 | $2.08 | $+97.72 | $4,349.26 | ▲ +97.72 after sell → book $10,341.90; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 81 | $20.47 | $2.26 | $-19.07 | $6,005.07 | ▼ -19.07 after sell → book $10,339.64; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 243 | $5.53 | $3.19 | $-64.64 | $7,345.68 | ▼ -64.64 after sell → book $10,336.46; vs 09:30 mark -3.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.21 | $2.18 | $+85.72 | $8,552.47 | ▲ +85.72 after sell → book $10,334.28; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 28 | $32.32 | $2.09 | $+71.15 | $9,455.33 | ▲ +71.15 after sell → book $10,332.18; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 321 | $1.90 | $4.20 | $+39.80 | $10,061.03 | ▲ +39.80 after sell → book $10,327.98; vs 09:30 mark -4.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 1 | $156.51 | $1.59 | $+8.93 | $10,215.95 | ▲ +8.93 after sell → book $10,326.39; vs 09:30 mark -1.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 95 | $23.77 | $2.27 | — | $7,955.53 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.0; leftover $2270.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 180 | $10.98 | $2.53 | — | $5,976.60 | — | rank-weighted leftover; list flatten; 🔵; ret5=+1.2; leftover $1986.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 27 | $61.19 | $2.07 | — | $4,322.39 | — | rank-weighted leftover; list flatten; 🔵; ret5=+7.4; leftover $1702.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 169 | $8.35 | $2.50 | — | $2,908.75 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1418.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 229 | $4.94 | $2.95 | — | $1,774.53 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $1135.11 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $1,345.57 | — | rank-weighted leftover; list flatten; ret5=+6.0; leftover $851.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 78 | $7.25 | $2.22 | — | $777.85 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $567.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 792 | $0.36 | $5.21 | — | $489.10 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-15.6; leftover $283.78 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $489.10 | ▲ close $10,467.06 vs 09:30 $10,346.34 (session +162.42) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $489.10 | ▲ 09:30 equity $10,482.07 vs yday $10,467.06 (+15.01) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $521.94 | ▼ -1.91 after sell → book $10,481.72; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $552.31 | ▲ +7.88 after sell → book $10,481.38; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $576.12 | ▼ -1.17 after sell → book $10,481.09; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 10 | $2.03 | $0.25 | $+0.52 | $596.16 | ▲ +0.52 after sell → book $10,480.84; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 4 | $1.60 | $0.10 | $+0.96 | $602.47 | ▲ +0.96 after sell → book $10,480.74; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 12 | $31.21 | $2.03 | — | $225.92 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $401.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 18 | $11.12 | $2.04 | — | $23.72 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $200.82 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.72 | ▲ close $10,534.26 vs 09:30 $10,482.07 (session +57.59) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.72 | ▼ 09:30 equity $10,518.39 vs yday $10,534.26 (-15.87) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1 | $2.60 | $0.03 | — | $21.09 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+13.0; leftover $4.74 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.09 | ▼ close $10,489.49 vs 09:30 $10,518.39 (session -28.88) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.09 | ▼ 09:30 equity $10,481.53 vs yday $10,489.49 (-7.96) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 180 | $10.97 | $2.58 | $-6.91 | $1,993.11 | ▼ -6.91 after sell → book $10,478.95; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 27 | $60.52 | $2.09 | $-22.26 | $3,625.06 | ▼ -22.26 after sell → book $10,476.86; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 169 | $8.28 | $2.54 | $-16.86 | $5,021.84 | ▼ -16.86 after sell → book $10,474.32; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 229 | $4.95 | $3.00 | $-3.67 | $6,152.39 | ▼ -3.67 after sell → book $10,471.32; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $423.76 | $2.01 | $-7.22 | $6,574.14 | ▼ -7.22 after sell → book $10,469.31; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 78 | $9.73 | $2.25 | $+188.97 | $7,330.83 | ▲ +188.97 after sell → book $10,467.06; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 792 | $0.36 | $5.41 | $-5.08 | $7,614.50 | ▼ -5.08 after sell → book $10,461.65; vs 09:30 mark -5.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 52 | $41.74 | $2.15 | — | $5,441.88 | — | rank-weighted leftover; list flatten; ret5=+2.4; leftover $2175.57 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 123 | $14.63 | $2.36 | — | $3,640.03 | — | rank-weighted leftover; list flatten; ret5=+5.8; leftover $1812.98 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 44 | $32.90 | $2.12 | — | $2,190.31 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1450.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 69 | $15.66 | $2.20 | — | $1,107.57 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1087.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 9 | $79.42 | $2.02 | — | $390.77 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $725.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 109 | $3.32 | $2.32 | — | $26.57 | — | rank-weighted leftover; list probable,yday_gainer; ret5=+6.4; leftover $362.60 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.57 | ▼ close $10,200.80 vs 09:30 $10,481.53 (session -247.69) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.57 | ▲ 09:30 equity $10,244.93 vs yday $10,200.80 (+44.13) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 95 | $23.68 | $2.31 | $-13.13 | $2,273.87 | ▼ -13.13 after sell → book $10,242.63; vs 09:30 mark -2.30 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 12 | $29.94 | $2.05 | $-19.31 | $2,631.10 | ▼ -19.31 after sell → book $10,240.58; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 18 | $10.82 | $2.02 | $-9.47 | $2,823.84 | ▼ -9.47 after sell → book $10,238.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,823.84 | ▲ close $10,283.97 vs 09:30 $10,244.93 (session +45.41) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,823.84 | ▲ 09:30 equity $10,438.52 vs yday $10,283.97 (+154.55) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 1 | $2.67 | $0.05 | $-0.01 | $2,826.46 | ▼ -0.01 after sell → book $10,438.47; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,826.46 | ▲ close $10,492.06 vs 09:30 $10,438.52 (session +53.59) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,826.46 | ▼ 09:30 equity $10,419.33 vs yday $10,492.06 (-72.73) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 52 | $42.10 | $2.17 | $+14.40 | $5,013.49 | ▲ +14.40 after sell → book $10,417.16; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 123 | $15.70 | $2.39 | $+126.86 | $6,942.19 | ▲ +126.86 after sell → book $10,414.76; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 44 | $32.42 | $2.14 | $-25.39 | $8,366.53 | ▼ -25.39 after sell → book $10,412.62; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 69 | $13.92 | $2.22 | $-124.48 | $9,324.79 | ▼ -124.48 after sell → book $10,410.40; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 9 | $78.84 | $2.04 | $-9.27 | $10,032.31 | ▼ -9.27 after sell → book $10,408.36; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 109 | $3.45 | $2.35 | $+9.51 | $10,406.02 | ▲ +9.51 after sell → book $10,406.02; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,406.02 | ▲ close $10,406.02 vs 09:30 $10,419.33 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,406.02 | ▲ 09:30 equity $10,406.02 vs yday $10,406.02 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 43 | $52.88 | $2.12 | — | $8,130.06 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2312.45 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 47 | $42.93 | $2.13 | — | $6,110.22 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2023.39 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 477 | $3.63 | $6.15 | — | $4,372.55 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1734.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 179 | $8.03 | $2.53 | — | $2,932.66 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1445.28 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $1,871.04 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1156.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 56 | $15.45 | $2.16 | — | $1,003.68 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $867.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 3 | $145.94 | $2.00 | — | $563.85 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $578.11 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 17 | $16.77 | $2.04 | — | $276.72 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $289.06 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.72 | ▼ close $10,181.67 vs 09:30 $10,406.02 (session -203.20) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.72 | ▼ 09:30 equity $10,137.29 vs yday $10,181.67 (-44.38) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 31 | $2.52 | $0.87 | — | $197.72 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $79.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 9 | $6.71 | $0.63 | — | $136.70 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $65.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 27 | $1.90 | $0.59 | — | $84.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $52.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 8 | $4.78 | $0.41 | — | $46.16 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $39.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 16 | $1.59 | $0.30 | — | $20.42 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $26.35 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $8.99 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $13.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.99 | ▲ close $10,197.38 vs 09:30 $10,137.29 (session +63.01) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.99 | ▲ 09:30 equity $10,287.17 vs yday $10,197.38 (+89.79) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.99 | ▼ close $10,112.17 vs 09:30 $10,287.17 (session -175.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.99 | ▼ 09:30 equity $10,062.89 vs yday $10,112.17 (-49.28) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 43 | $53.16 | $2.15 | $+7.77 | $2,292.73 | ▲ +7.77 after sell → book $10,060.74; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 47 | $42.01 | $2.16 | $-47.53 | $4,265.04 | ▼ -47.53 after sell → book $10,058.59; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 179 | $8.01 | $2.57 | $-8.68 | $5,696.26 | ▼ -8.68 after sell → book $10,056.02; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 8 | $125.77 | $2.03 | $-57.49 | $6,700.39 | ▼ -57.49 after sell → book $10,053.98; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 56 | $15.16 | $2.18 | $-20.58 | $7,547.17 | ▼ -20.58 after sell → book $10,051.81; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 3 | $140.29 | $2.02 | $-20.97 | $7,966.04 | ▼ -20.97 after sell → book $10,049.79; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 17 | $15.46 | $2.06 | $-26.37 | $8,226.80 | ▼ -26.37 after sell → book $10,047.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,226.80 | ▼ close $9,858.06 vs 09:30 $10,062.89 (session -189.66) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,226.80 | ▼ 09:30 equity $9,825.89 vs yday $9,858.06 (-32.17) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 477 | $2.85 | $6.24 | $-384.46 | $9,580.00 | ▼ -384.46 after sell → book $9,819.65; vs 09:30 mark -6.24 | dropped from list after 4 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 31 | $2.22 | $0.80 | $-10.98 | $9,648.02 | ▼ -10.98 after sell → book $9,818.85; vs 09:30 mark -0.80 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 9 | $6.11 | $0.60 | $-6.63 | $9,702.41 | ▼ -6.63 after sell → book $9,818.25; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 27 | $1.83 | $0.60 | $-3.08 | $9,751.23 | ▼ -3.08 after sell → book $9,817.65; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 8 | $3.92 | $0.36 | $-7.63 | $9,782.25 | ▼ -7.63 after sell → book $9,817.30; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 16 | $1.53 | $0.31 | $-1.58 | $9,806.41 | ▼ -1.58 after sell → book $9,816.98; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $9,816.86 | ▼ -0.98 after sell → book $9,816.86; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,816.86 | ▲ close $9,816.86 vs 09:30 $9,825.89 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,816.86 | ▲ 09:30 equity $9,816.86 vs yday $9,816.86 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 134 | $16.28 | $2.39 | — | $7,632.94 | — | rank-weighted leftover; list flatten; 🔵; ret5=-1.1; leftover $2181.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 699 | $2.73 | $9.02 | — | $5,715.66 | — | rank-weighted leftover; list flatten; 🔵; ret5=-3.0; leftover $1908.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 7 | $206.84 | $2.01 | — | $4,265.77 | — | rank-weighted leftover; list flatten; ret5=+8.3; leftover $1636.14 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $2,948.31 | — | rank-weighted leftover; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1363.45 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $1,999.62 | — | rank-weighted leftover; list flatten; 🔵; ret5=+4.7; leftover $1090.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 14 | $56.09 | $2.03 | — | $1,212.33 | — | rank-weighted leftover; list flatten; 🔵; ret5=+19.6; leftover $818.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 267 | $2.04 | $3.44 | — | $664.21 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $545.38 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 57 | $4.75 | $2.16 | — | $391.30 | — | rank-weighted leftover; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $272.69 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.30 | ▼ close $9,728.06 vs 09:30 $9,816.86 (session -63.72) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.30 | ▼ 09:30 equity $9,484.55 vs yday $9,728.06 (-243.51) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.30 | ▼ close $9,426.87 vs 09:30 $9,484.55 (session -57.68) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.30 | ▲ 09:30 equity $9,451.82 vs yday $9,426.87 (+24.95) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.30 | ▼ close $9,267.05 vs 09:30 $9,451.82 (session -184.77) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.30 | ▲ 09:30 equity $9,321.18 vs yday $9,267.05 (+54.13) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 134 | $16.16 | $2.43 | $-20.90 | $2,554.30 | ▼ -20.90 after sell → book $9,318.74; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 699 | $2.72 | $9.15 | $-25.16 | $4,446.44 | ▼ -25.16 after sell → book $9,309.60; vs 09:30 mark -9.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 7 | $194.84 | $2.03 | $-88.04 | $5,808.28 | ▼ -88.04 after sell → book $9,307.56; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 8 | $140.03 | $2.03 | $-199.25 | $6,926.49 | ▼ -199.25 after sell → book $9,305.53; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 6 | $147.79 | $2.03 | $-63.98 | $7,811.20 | ▼ -63.98 after sell → book $9,303.50; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 14 | $51.29 | $2.05 | $-71.28 | $8,527.21 | ▼ -71.28 after sell → book $9,301.45; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 267 | $1.89 | $3.50 | $-46.99 | $9,028.34 | ▼ -46.99 after sell → book $9,297.95; vs 09:30 mark -3.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 57 | $4.73 | $2.18 | $-5.48 | $9,295.77 | ▼ -5.48 after sell → book $9,295.77; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 7 | $270.89 | $2.01 | — | $7,397.53 | — | rank-weighted leftover; list flatten; ret5=+4.0; leftover $2065.73 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 23 | $77.12 | $2.06 | — | $5,621.71 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+7.2; leftover $1807.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 108 | $14.31 | $2.31 | — | $4,073.92 | — | rank-weighted leftover; list flatten; ret5=+4.8; leftover $1549.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 35 | $36.46 | $2.10 | — | $2,795.72 | — | rank-weighted leftover; list flatten; 🔵; ret5=+2.9; leftover $1291.08 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 55 | $18.61 | $2.15 | — | $1,770.02 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1032.86 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 42 | $18.21 | $2.12 | — | $1,003.08 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-19.1; leftover $774.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 7 | $68.79 | $2.01 | — | $519.54 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $516.43 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 43 | $5.87 | $2.12 | — | $265.01 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $258.22 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $265.01 | ▲ close $9,439.95 vs 09:30 $9,321.18 (session +161.06) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $265.01 | ▲ 09:30 equity $9,573.19 vs yday $9,439.95 (+133.24) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $233.94 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $36.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $210.94 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $29.45 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 86 | $0.17 | $0.40 | — | $195.91 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $14.72 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.91 | ▼ close $9,541.26 vs 09:30 $9,573.19 (session -30.97) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.91 | ▼ 09:30 equity $9,540.66 vs yday $9,541.26 (-0.60) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 11 | $0.97 | $0.14 | — | $185.10 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $10.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 2 | $2.08 | $0.05 | — | $180.90 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $5.44 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.90 | ▼ close $9,472.46 vs 09:30 $9,540.66 (session -68.02) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.90 | ▲ 09:30 equity $9,518.71 vs yday $9,472.46 (+46.25) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 7 | $266.76 | $2.04 | $-32.96 | $2,046.18 | ▼ -32.96 after sell → book $9,516.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 23 | $76.27 | $2.08 | $-23.69 | $3,798.31 | ▼ -23.69 after sell → book $9,514.59; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 108 | $13.65 | $2.34 | $-75.94 | $5,270.16 | ▼ -75.94 after sell → book $9,512.24; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 35 | $36.70 | $2.12 | $+4.19 | $6,552.55 | ▲ +4.19 after sell → book $9,510.13; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 55 | $22.11 | $2.17 | $+188.17 | $7,766.42 | ▲ +188.17 after sell → book $9,507.95; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 42 | $20.55 | $2.14 | $+94.03 | $8,627.39 | ▲ +94.03 after sell → book $9,505.82; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 7 | $79.08 | $2.03 | $+67.99 | $9,178.92 | ▲ +67.99 after sell → book $9,503.79; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 43 | $5.62 | $2.14 | $-15.01 | $9,418.44 | ▼ -15.01 after sell → book $9,501.65; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 17 | $157.87 | $2.04 | — | $6,732.61 | — | rank-weighted leftover; list flatten; ret5=+6.5; leftover $2690.98 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 5 | $386.20 | $2.00 | — | $4,799.60 | — | rank-weighted leftover; list flatten; ret5=-5.8; leftover $2242.49 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 20 | $88.83 | $2.05 | — | $3,020.95 | — | rank-weighted leftover; list flatten; ret5=+7.6; leftover $1793.99 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 99 | $13.47 | $2.29 | — | $1,685.13 | — | rank-weighted leftover; list flatten; ret5=+3.6; leftover $1345.49 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 224 | $4.00 | $2.89 | — | $786.24 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $896.99 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 48 | $9.31 | $2.13 | — | $337.23 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $448.50 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $337.23 | ▼ close $9,343.06 vs 09:30 $9,518.71 (session -145.18) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $337.23 | ▲ 09:30 equity $9,367.67 vs yday $9,343.06 (+24.61) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 3 | $10.18 | $0.33 | $-0.86 | $367.44 | ▼ -0.86 after sell → book $9,367.34; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 86 | $0.16 | $0.42 | $-1.68 | $380.78 | ▼ -1.68 after sell → book $9,366.92; vs 09:30 mark -0.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 18 | $0.58 | $0.16 | — | $370.18 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $10.58 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.18 | ▲ close $9,415.14 vs 09:30 $9,367.67 (session +48.38) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.18 | ▼ 09:30 equity $9,415.09 vs yday $9,415.14 (-0.05) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 11 | $0.89 | $0.15 | $-1.17 | $379.82 | ▼ -1.17 after sell → book $9,414.94; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 2 | $2.16 | $0.07 | $+0.04 | $384.07 | ▲ +0.04 after sell → book $9,414.87; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $266.05 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $128.02 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 3 | $27.79 | $0.84 | — | $181.83 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $102.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 7 | $9.81 | $0.71 | — | $112.46 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $76.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 2 | $20.25 | $0.41 | — | $71.55 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $51.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $50.69 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $25.60 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.69 | ▼ close $9,235.59 vs 09:30 $9,415.09 (session -175.94) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.69 | ▼ 09:30 equity $9,121.77 vs yday $9,235.59 (-113.82) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $72.58 | ▼ -1.12 after sell → book $9,121.52; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 17 | $163.95 | $2.07 | $+99.25 | $2,857.65 | ▲ +99.25 after sell → book $9,119.45; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 5 | $374.54 | $2.03 | $-62.34 | $4,728.32 | ▼ -62.34 after sell → book $9,117.42; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 20 | $87.67 | $2.07 | $-27.22 | $6,479.75 | ▼ -27.22 after sell → book $9,115.35; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 99 | $11.42 | $2.31 | $-207.55 | $7,608.01 | ▼ -207.55 after sell → book $9,113.03; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 224 | $3.40 | $2.94 | $-140.23 | $8,366.68 | ▼ -140.23 after sell → book $9,110.09; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 48 | $8.67 | $2.15 | $-35.01 | $8,780.68 | ▼ -35.01 after sell → book $9,107.94; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,780.68 | ▲ close $9,111.10 vs 09:30 $9,121.77 (session +3.16) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,948.76 | ▲ 09:30 equity $8,096.86 vs yday $8,096.86 (+0.00) | 09:30 open · cash $7,948.76 (unchanged overnight, no fees) · equity $8,096.86 vs prior close $8,096.86 (+0.00) · 7 name(s) re-marked at the open (per-name table). ADMA×2 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×11 yday $0.53 → 09:30 $0.53 +0.00; DLO×1 yday $13.88 → 09:30 $13.88 +0.00; FJET×6 yday $1.80 → 09:30 $1.80 +0.00; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×4 yday $7.70 → 09:30 $7.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 2 | $803.87 | $2.00 | — | $6,339.02 | — | rank-weighted leftover; list flatten; ret5=+0.8; leftover $1766.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 13 | $115.36 | $2.03 | — | $4,837.32 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1545.59 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 64 | $20.61 | $2.18 | — | $3,516.09 | — | rank-weighted leftover; list flatten; 🔵; ret5=+9.1; leftover $1324.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $2,435.74 | — | rank-weighted leftover; list flatten; ret5=+4.7; leftover $1103.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 115 | $7.65 | $2.33 | — | $1,553.65 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.2; leftover $883.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 25 | $26.27 | $2.06 | — | $894.84 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $662.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 5 | $83.76 | $2.00 | — | $474.03 | — | rank-weighted leftover; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $441.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 24 | $9.05 | $2.06 | — | $254.77 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-27.1; leftover $220.80 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.77 | ▼ close $8,017.43 vs 09:30 $8,096.86 (session -62.68) | 16:00 close · cash $254.77 · equity $8,017.43 vs 09:30 $8,096.86 (-79.43; session marks -62.68) · 15 name(s) marked open→close (per-name table). ADMA×2 09:30 $9.52 → close $9.52 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×11 09:30 $0.53 → close $0.53 +0.00; DLO×1 09:30 $13.88 → close $13.88 +0.00; FJET×6 09:30 $1.80 → close $1.80 -0.00; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×4 09:30 $7.70 → close $7.70 -0.00; REGN×2 09:30 $803.87 → close $788.04 -31.66; HALO×13 09:30 $115.36 → close $113.90 -18.98; OMER×64 09:30 $20.61 → close $20.08 -33.92; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×115 09:30 $7.65 → close $7.60 -5.75; WRBY×25 09:30 $26.27 → close $26.71 +11.00; TXG×5 09:30 $83.76 → close $85.71 +9.75; AEHL×24 09:30 $9.05 → close $9.36 +7.44 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 51.74 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 38.81 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 12.94 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-27 | `RRC` | cash | leftover split 7.91 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 6.32 < 1 share @ 14.42 |
| 2026-08-27 | `KURA` | cash | leftover split 3.16 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 1.58 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `ILMN` | cash | leftover split 58.89 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 51.53 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 44.17 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 22.08 < 1 share @ 34.93 |
| 2026-09-17 | `BRUN` | cash | leftover split 7.36 < 1 share @ 15.87 |
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
| 2026-09-18 | `RBRK` | cash | leftover split 43.54 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 38.09 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 32.65 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 27.21 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 21.77 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 16.33 < 1 share @ 34.44 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 52.89 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `DEFT` | 18 | 2026-09-22 @ $0.58 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $10.58 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $128.02 |
| `ARQT` | 3 | 2026-09-23 @ $27.79 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $102.42 |
| `ADMA` | 7 | 2026-09-23 @ $9.81 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $76.81 |
| `FTRE` | 2 | 2026-09-23 @ $20.25 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $51.21 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $25.60 |
