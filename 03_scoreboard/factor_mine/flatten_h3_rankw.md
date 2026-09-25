# Factor mine action — `flatten_h3_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **-19.64%** ($8,036) · signal-only (no cash/fees) was -10.41%. Starts YES **1/30**. Fills 135 · skips 213 · realized $-994.96.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,827.70.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $2222.22 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1944.44 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1388.89 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $555.56 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $277.78 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.05 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +139.38) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $118.95 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $10.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $112.30 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $7.11 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $109.27 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $3.56 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.27 | ▲ close $10,260.71 vs 09:30 $10,103.42 (session +157.50) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.27 | ▲ 09:30 equity $10,281.18 vs yday $10,260.71 (+20.47) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $96.99 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $15.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $88.44 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $12.14 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $85.16 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $6.07 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 109 | $20.55 | $2.32 | — | $7,890.89 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $2251.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $5,977.63 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1970.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 81 | $20.65 | $2.23 | — | $4,302.75 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1688.86 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 243 | $5.77 | $3.13 | — | $2,897.50 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1407.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $1,776.43 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1125.91 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $944.72 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $844.43 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 321 | $1.75 | $4.14 | — | $378.83 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $562.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $232.84 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $281.48 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.84 | ▲ close $10,332.74 vs 09:30 $10,133.47 (session +219.14) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.84 | ▲ 09:30 equity $10,606.36 vs yday $10,332.74 (+273.62) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $198.09 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $45.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.60 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $32.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.62 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $25.87 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 10 | $1.93 | $0.22 | — | $131.10 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $19.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 4 | $1.32 | $0.06 | — | $125.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $6.47 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 122 | $23.77 | $2.36 | — | $7,313.65 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $2918.84 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 221 | $10.98 | $2.85 | — | $4,884.22 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $2432.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 31 | $61.19 | $2.08 | — | $2,985.25 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $1945.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 174 | $8.35 | $2.51 | — | $1,529.84 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $1459.42 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 196 | $4.94 | $2.58 | — | $559.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $972.95 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $130.06 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $486.47 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.06 | ▲ close $10,404.15 vs 09:30 $10,346.34 (session +92.13) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.06 | ▲ 09:30 equity $10,427.86 vs yday $10,404.15 (+23.71) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $162.90 | ▼ -1.91 after sell → book $10,427.50; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $193.27 | ▲ +7.88 after sell → book $10,427.17; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $217.08 | ▼ -1.17 after sell → book $10,426.88; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 10 | $2.03 | $0.25 | $+0.52 | $237.12 | ▲ +0.52 after sell → book $10,426.62; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 4 | $1.60 | $0.10 | $+0.96 | $243.43 | ▲ +0.96 after sell → book $10,426.53; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $243.43 | ▼ close $10,361.78 vs 09:30 $10,427.86 (session -64.75) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $243.43 | ▼ 09:30 equity $10,341.35 vs yday $10,361.78 (-20.43) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 2 | $41.44 | $0.83 | — | $159.71 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $121.71 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 5 | $14.42 | $0.74 | — | $86.88 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $81.14 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 15 | $2.60 | $0.43 | — | $47.44 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $40.57 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.44 | ▼ close $10,260.14 vs 09:30 $10,341.35 (session -79.20) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.44 | ▲ 09:30 equity $10,301.34 vs yday $10,260.14 (+41.20) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 221 | $10.97 | $2.91 | $-7.97 | $2,468.90 | ▼ -7.97 after sell → book $10,298.43; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 31 | $60.52 | $2.11 | $-24.96 | $4,342.92 | ▼ -24.96 after sell → book $10,296.33; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 174 | $8.28 | $2.55 | $-17.24 | $5,781.08 | ▼ -17.24 after sell → book $10,293.77; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 196 | $4.95 | $2.62 | $-3.24 | $6,748.66 | ▼ -3.24 after sell → book $10,291.15; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $423.76 | $2.01 | $-7.22 | $7,170.41 | ▼ -7.22 after sell → book $10,289.14; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,170.41 | ▼ close $10,242.23 vs 09:30 $10,301.34 (session -46.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,170.41 | ▲ 09:30 equity $10,254.77 vs yday $10,242.23 (+12.54) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 122 | $23.68 | $2.40 | $-15.74 | $10,056.97 | ▼ -15.74 after sell → book $10,252.37; vs 09:30 mark -2.40 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,056.97 | ▼ close $10,251.81 vs 09:30 $10,254.77 (session -0.56) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,056.97 | ▲ 09:30 equity $10,260.00 vs yday $10,251.81 (+8.19) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 2 | $41.94 | $0.86 | $-0.70 | $10,139.99 | ▼ -0.70 after sell → book $10,259.14; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 5 | $15.82 | $0.83 | $+5.44 | $10,218.26 | ▲ +5.44 after sell → book $10,258.31; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 15 | $2.67 | $0.47 | $+0.15 | $10,257.84 | ▲ +0.15 after sell → book $10,257.84; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,257.84 | ▲ close $10,257.84 vs 09:30 $10,260.00 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,257.84 | ▲ 09:30 equity $10,257.84 vs yday $10,257.84 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,257.84 | ▲ close $10,257.84 vs 09:30 $10,257.84 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,257.84 | ▲ 09:30 equity $10,257.84 vs yday $10,257.84 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 64 | $52.88 | $2.18 | — | $6,871.34 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $3419.28 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 63 | $42.93 | $2.18 | — | $4,164.57 | — | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2735.43 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 565 | $3.63 | $7.29 | — | $2,106.33 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2051.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 170 | $8.03 | $2.50 | — | $738.73 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $1367.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $132.45 | $2.00 | — | $74.48 | — | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $683.86 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.48 | ▼ close $10,045.05 vs 09:30 $10,257.84 (session -196.64) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.48 | ▼ 09:30 equity $9,968.65 vs yday $10,045.05 (-76.40) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 8 | $2.52 | $0.23 | — | $54.09 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $21.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $40.53 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $17.73 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 7 | $1.90 | $0.15 | — | $27.08 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $14.19 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $17.42 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $10.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 4 | $1.59 | $0.08 | — | $10.98 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $7.09 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.98 | ▲ close $10,037.89 vs 09:30 $9,968.65 (session +69.94) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.98 | ▲ 09:30 equity $10,181.17 vs yday $10,037.89 (+143.28) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.98 | ▼ close $10,017.63 vs 09:30 $10,181.17 (session -163.54) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.98 | ▼ 09:30 equity $9,964.44 vs yday $10,017.63 (-53.19) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 64 | $53.16 | $2.22 | $+13.52 | $3,411.00 | ▲ +13.52 after sell → book $9,962.22; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 63 | $42.01 | $2.21 | $-62.35 | $6,055.42 | ▼ -62.35 after sell → book $9,960.01; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 565 | $3.28 | $7.40 | $-212.44 | $7,901.23 | ▼ -212.44 after sell → book $9,952.62; vs 09:30 mark -7.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 170 | $8.01 | $2.54 | $-8.44 | $9,260.39 | ▼ -8.44 after sell → book $9,950.08; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 5 | $125.77 | $2.02 | $-37.43 | $9,887.21 | ▼ -37.43 after sell → book $9,948.05; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,887.21 | ▼ close $9,944.80 vs 09:30 $9,964.44 (session -3.25) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,887.21 | ▼ 09:30 equity $9,943.97 vs yday $9,944.80 (-0.83) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 8 | $2.22 | $0.22 | $-2.85 | $9,904.75 | ▼ -2.85 after sell → book $9,943.74; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $9,916.82 | ▼ -1.49 after sell → book $9,943.60; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 7 | $1.83 | $0.17 | $-0.81 | $9,929.46 | ▼ -0.81 after sell → book $9,943.43; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $9,937.20 | ▼ -1.92 after sell → book $9,943.32; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 4 | $1.53 | $0.09 | $-0.41 | $9,943.23 | ▼ -0.41 after sell → book $9,943.23; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,943.23 | ▲ close $9,943.23 vs 09:30 $9,943.97 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,943.23 | ▲ 09:30 equity $9,943.23 vs yday $9,943.23 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 174 | $16.28 | $2.51 | — | $7,108.00 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $2840.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 867 | $2.73 | $11.18 | — | $4,729.90 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $2367.44 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 9 | $206.84 | $2.02 | — | $2,866.33 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1893.95 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $1,548.87 | — | rank-weighted leftover; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1420.46 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $600.18 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $946.97 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 8 | $56.09 | $2.01 | — | $149.45 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $473.49 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.45 | ▼ close $9,860.24 vs 09:30 $9,943.23 (session -61.24) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.45 | ▼ 09:30 equity $9,634.95 vs yday $9,860.24 (-225.29) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.45 | ▼ close $9,582.65 vs 09:30 $9,634.95 (session -52.30) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.45 | ▲ 09:30 equity $9,609.10 vs yday $9,582.65 (+26.45) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.45 | ▼ close $9,433.49 vs 09:30 $9,609.10 (session -175.61) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.45 | ▲ 09:30 equity $9,490.39 vs yday $9,433.49 (+56.90) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 174 | $16.16 | $2.56 | $-25.96 | $2,958.73 | ▼ -25.96 after sell → book $9,487.83; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 867 | $2.72 | $11.35 | $-31.20 | $5,305.62 | ▼ -31.20 after sell → book $9,476.48; vs 09:30 mark -11.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 9 | $194.84 | $2.04 | $-112.06 | $7,057.14 | ▼ -112.06 after sell → book $9,474.44; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 8 | $140.03 | $2.03 | $-199.25 | $8,175.34 | ▼ -199.25 after sell → book $9,472.40; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 6 | $147.79 | $2.03 | $-63.98 | $9,060.06 | ▼ -63.98 after sell → book $9,470.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 8 | $51.29 | $2.03 | $-42.45 | $9,468.34 | ▼ -42.45 after sell → book $9,468.34; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 13 | $270.89 | $2.03 | — | $5,944.74 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $3787.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 36 | $77.12 | $2.10 | — | $3,166.32 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $2840.50 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 132 | $14.31 | $2.39 | — | $1,275.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $1893.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 25 | $36.46 | $2.06 | — | $361.45 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $946.83 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $361.45 | ▼ close $9,369.26 vs 09:30 $9,490.39 (session -90.50) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $361.45 | ▲ 09:30 equity $9,472.55 vs yday $9,369.26 (+103.29) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 5 | $10.25 | $0.53 | — | $309.68 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $51.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $279.00 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $34.42 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.00 | ▼ close $9,428.67 vs 09:30 $9,472.55 (session -43.04) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.00 | ▼ 09:30 equity $9,413.18 vs yday $9,428.67 (-15.49) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.00 | ▼ close $9,282.52 vs 09:30 $9,413.18 (session -130.66) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.00 | ▲ 09:30 equity $9,295.41 vs yday $9,282.52 (+12.89) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 13 | $266.76 | $2.07 | $-57.79 | $3,744.81 | ▼ -57.79 after sell → book $9,293.34; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 36 | $76.27 | $2.13 | $-34.83 | $6,488.40 | ▼ -34.83 after sell → book $9,291.21; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 132 | $13.65 | $2.42 | $-91.93 | $8,287.78 | ▼ -91.93 after sell → book $9,288.79; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 25 | $36.70 | $2.08 | $+1.85 | $9,203.20 | ▲ +1.85 after sell → book $9,286.71; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 19 | $157.87 | $2.05 | — | $6,201.62 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $3067.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 6 | $386.20 | $2.01 | — | $3,882.41 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; leftover $2454.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 20 | $88.83 | $2.05 | — | $2,103.76 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $1840.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 91 | $13.47 | $2.26 | — | $875.73 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $1227.09 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 153 | $4.00 | $2.45 | — | $261.28 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $613.55 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $261.28 | ▼ close $9,185.25 vs 09:30 $9,295.41 (session -90.64) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $261.28 | ▲ 09:30 equity $9,202.03 vs yday $9,185.25 (+16.78) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 5 | $10.18 | $0.54 | $-1.42 | $311.64 | ▼ -1.42 after sell → book $9,201.49; vs 09:30 mark -0.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $311.64 | ▲ close $9,235.15 vs 09:30 $9,202.03 (session +33.66) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $311.64 | ▼ 09:30 equity $9,217.82 vs yday $9,235.15 (-17.33) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 2 | $27.79 | $0.56 | — | $255.49 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $83.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 6 | $9.81 | $0.61 | — | $196.03 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $62.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 2 | $20.25 | $0.41 | — | $155.12 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $41.55 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $134.26 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $20.78 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.26 | ▼ close $9,100.70 vs 09:30 $9,217.82 (session -115.33) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.26 | ▼ 09:30 equity $9,008.77 vs yday $9,100.70 (-91.93) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 4 | $7.38 | $0.33 | $-1.48 | $163.45 | ▼ -1.48 after sell → book $9,008.44; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 19 | $163.95 | $2.08 | $+111.39 | $3,276.42 | ▲ +111.39 after sell → book $9,006.36; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 6 | $374.54 | $2.04 | $-74.00 | $5,521.62 | ▼ -74.00 after sell → book $9,004.32; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 20 | $87.67 | $2.07 | $-27.22 | $7,273.05 | ▼ -27.22 after sell → book $9,002.25; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 91 | $11.42 | $2.29 | $-191.10 | $8,309.98 | ▼ -191.10 after sell → book $8,999.96; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 153 | $3.40 | $2.48 | $-96.73 | $8,827.70 | ▼ -96.73 after sell → book $8,997.48; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,827.70 | ▲ close $8,997.53 vs 09:30 $9,008.77 (session +0.05) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,729.71 | ▲ 09:30 equity $8,162.85 vs yday $8,162.85 (+0.00) | 09:30 open · cash $7,729.71 (unchanged overnight, no fees) · equity $8,162.85 vs prior close $8,162.85 (+0.00) · 7 name(s) re-marked at the open (per-name table). ADMA×2 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DLO×3 yday $13.88 → 09:30 $13.88 +0.00; EL×1 yday $95.37 → 09:30 $95.37 +0.00; MKC×2 yday $47.82 → 09:30 $47.82 +0.00; PACS×3 yday $41.46 → 09:30 $41.46 +0.00; PGEN×4 yday $7.70 → 09:30 $7.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 3 | $803.87 | $2.00 | — | $5,316.10 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.8; leftover $2576.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 17 | $115.36 | $2.04 | — | $3,352.94 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.1; leftover $2061.26 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 75 | $20.61 | $2.21 | — | $1,804.98 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.1; leftover $1545.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 26 | $38.51 | $2.07 | — | $801.65 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $1030.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 67 | $7.65 | $2.19 | — | $286.91 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $515.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.91 | ▼ close $8,036.41 vs 09:30 $8,162.85 (session -115.93) | 16:00 close · cash $286.91 · equity $8,036.41 vs 09:30 $8,162.85 (-126.44; session marks -115.93) · 12 name(s) marked open→close (per-name table). ADMA×2 09:30 $9.52 → close $9.52 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DLO×3 09:30 $13.88 → close $13.88 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; MKC×2 09:30 $47.82 → close $47.82 -0.00; PACS×3 09:30 $41.46 → close $41.46 -0.00; PGEN×4 09:30 $7.70 → close $7.70 -0.00; REGN×3 09:30 $803.87 → close $788.04 -47.49; HALO×17 09:30 $115.36 → close $113.90 -24.82; OMER×75 09:30 $20.61 → close $20.08 -39.75; BLFS×26 09:30 $38.51 → close $38.49 -0.52; MRVI×67 09:30 $7.65 → close $7.60 -3.35 | — |

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
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIR` | cash | leftover split 3.55 < 1 share @ 11.31 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 103.27 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 86.06 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 68.85 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 17.21 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 79.71 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 66.43 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 53.14 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 39.86 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 26.57 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 13.29 < 1 share @ 34.44 |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 44.52 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 103.88 < 1 share @ 116.85 |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARQT` | 2 | 2026-09-23 @ $27.79 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $83.10 |
| `ADMA` | 6 | 2026-09-23 @ $9.81 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $62.33 |
| `FTRE` | 2 | 2026-09-23 @ $20.25 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $41.55 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $20.78 |
