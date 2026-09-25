# Factor mine action — `union_h5_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **-8.99%** ($9,101) · signal-only (no cash/fees) was +16.74%. Starts YES **11/30**. Fills 196 · skips 518 · realized $+179.08.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On a strong morning (S ≥ +5), spend 1.35× leftover and add 4 extra names — still cash-capped.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $459.66.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.82 | ▲ close $10,195.74 vs 09:30 $10,000.00 (session +227.81) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $10.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 11 | $0.94 | $0.14 | — | $104.27 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $10.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 6 | $1.50 | $0.11 | — | $95.16 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $10.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $86.45 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $10.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $78.00 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $10.32 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.00 | ▲ close $10,434.08 vs 09:30 $10,219.63 (session +214.97) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.00 | ▼ 09:30 equity $10,410.48 vs yday $10,434.08 (-23.60) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.81 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $9.75 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.27 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $9.75 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.44 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $9.75 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.72 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $9.75 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.72 | ▲ close $10,513.02 vs 09:30 $10,410.48 (session +102.92) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.72 | ▼ 09:30 equity $10,384.75 vs yday $10,513.02 (-128.27) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.72 | ▲ close $10,567.86 vs 09:30 $10,384.75 (session +183.12) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.72 | ▲ 09:30 equity $10,728.75 vs yday $10,567.86 (+160.89) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.72 | ▲ close $10,988.26 vs 09:30 $10,728.75 (session +259.52) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.72 | ▼ 09:30 equity $10,903.82 vs yday $10,988.26 (-84.44) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 18 | $58.64 | $2.06 | $-24.99 | $1,095.17 | ▼ -24.99 after sell → book $10,901.75; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 24 | $42.46 | $2.08 | $-88.62 | $2,112.13 | ▼ -88.62 after sell → book $10,899.67; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 21 | $53.06 | $2.07 | $+47.05 | $3,224.32 | ▲ +47.05 after sell → book $10,897.60; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 22 | $51.65 | $2.08 | $+38.77 | $4,358.54 | ▲ +38.77 after sell → book $10,895.52; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $5,657.20 | ▲ +196.59 after sell → book $10,893.22; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 37 | $30.66 | $2.12 | $+29.82 | $6,789.50 | ▲ +29.82 after sell → book $10,891.10; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1371 | $1.30 | $17.93 | $+638.64 | $8,553.88 | ▲ +638.64 after sell → book $10,873.18; vs 09:30 mark -17.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 47 | $23.11 | $2.15 | $-14.62 | $9,637.89 | ▼ -14.62 after sell → book $10,871.02; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 50 | $23.05 | $2.16 | $+47.70 | $10,788.23 | ▲ +47.70 after sell → book $10,868.86; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 65 | $20.55 | $2.19 | — | $9,450.30 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,174.13 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,829.69 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 233 | $5.77 | $3.01 | — | $5,482.28 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 68 | $19.63 | $2.19 | — | $4,145.24 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,809.77 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 770 | $1.75 | $9.93 | — | $1,452.33 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1348.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $149.46 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1348.53 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.46 | ▲ close $11,097.43 vs 09:30 $10,903.82 (session +254.24) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.46 | ▲ 09:30 equity $11,389.43 vs yday $11,097.43 (+292.00) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $161.02 | ▲ +2.46 after sell → book $11,389.29; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 11 | $0.87 | $0.15 | $-1.05 | $170.41 | ▼ -1.05 after sell → book $11,389.15; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 6 | $1.66 | $0.14 | $+0.71 | $180.23 | ▲ +0.71 after sell → book $11,389.01; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $188.97 | ▲ +0.03 after sell → book $11,388.89; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $195.72 | ▼ -1.70 after sell → book $11,388.80; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $178.34 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $155.86 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $133.38 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $109.95 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $85.90 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $24.46 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.90 | ▲ close $11,391.04 vs 09:30 $11,389.43 (session +3.45) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.90 | ▲ 09:30 equity $11,507.74 vs yday $11,391.04 (+116.70) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $95.03 | ▲ +0.94 after sell → book $11,507.62; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $104.17 | ▲ +0.60 after sell → book $11,507.50; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $114.54 | ▲ +0.54 after sell → book $11,507.37; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $124.51 | ▲ +0.25 after sell → book $11,507.24; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.51 | ▼ close $11,471.72 vs 09:30 $11,507.74 (session -35.52) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.51 | ▼ 09:30 equity $11,291.71 vs yday $11,471.72 (-180.01) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $113.42 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+1.2; leftover $15.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $104.98 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.0; leftover $15.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $4.94 | $0.16 | — | $90.01 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $15.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 2 | $7.25 | $0.15 | — | $75.35 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $15.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 43 | $0.36 | $0.28 | — | $59.68 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-15.6; leftover $15.56 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.68 | ▲ close $11,747.62 vs 09:30 $11,291.71 (session +456.70) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.68 | ▼ 09:30 equity $11,536.04 vs yday $11,747.62 (-211.58) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 1 | $11.12 | $0.11 | — | $48.44 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $11.94 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▼ close $11,378.75 vs 09:30 $11,536.04 (session -157.17) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▲ 09:30 equity $11,405.06 vs yday $11,378.75 (+26.31) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 65 | $20.93 | $2.21 | $+20.31 | $1,406.69 | ▲ +20.31 after sell → book $11,402.85; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,741.91 | ▲ +59.06 after sell → book $11,400.80; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,124.86 | ▲ +38.51 after sell → book $11,398.59; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 233 | $5.49 | $3.05 | $-71.30 | $5,400.97 | ▼ -71.30 after sell → book $11,395.54; vs 09:30 mark -3.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 68 | $21.47 | $2.22 | $+120.71 | $6,858.72 | ▲ +120.71 after sell → book $11,393.32; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,310.97 | ▲ +116.78 after sell → book $11,391.18; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 770 | $1.91 | $10.07 | $+103.19 | $9,771.60 | ▲ +103.19 after sell → book $11,381.10; vs 09:30 mark -10.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,172.57 | ▲ +98.09 after sell → book $11,379.06; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 38 | $41.44 | $2.10 | — | $9,595.74 | — | S≥+5: sizeup + more names; list flatten; ret5=+3.1; leftover $1596.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 110 | $14.42 | $2.32 | — | $8,007.22 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $1596.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 66 | $24.00 | $2.19 | — | $6,421.04 | — | S≥+5: sizeup + more names; list flatten; ret5=+8.7; leftover $1596.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 613 | $2.60 | $7.91 | — | $4,819.33 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+13.0; leftover $1596.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 122 | $12.98 | $2.36 | — | $3,233.41 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1596.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 51 | $30.79 | $2.14 | — | $1,660.98 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1596.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 164 | $9.68 | $2.48 | — | $70.98 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1596.08 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.98 | ▲ close $11,459.34 vs 09:30 $11,405.06 (session +101.78) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.98 | ▲ 09:30 equity $11,465.84 vs yday $11,459.34 (+6.50) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $87.23 | ▼ -1.12 after sell → book $11,465.66; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $117.76 | ▲ +8.04 after sell → book $11,465.32; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.35 | $0.26 | $-1.59 | $138.65 | ▼ -1.59 after sell → book $11,465.06; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.06 | $0.30 | $+0.99 | $163.06 | ▲ +0.99 after sell → book $11,464.76; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.82 | $0.40 | $+8.31 | $195.42 | ▲ +8.31 after sell → book $11,464.36; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 1 | $32.90 | $0.33 | — | $162.19 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $48.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 3 | $15.66 | $0.48 | — | $114.73 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $48.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 14 | $3.32 | $0.51 | — | $67.74 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+6.4; leftover $48.86 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.74 | ▼ close $11,214.30 vs 09:30 $11,465.84 (session -248.74) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.74 | ▲ 09:30 equity $11,279.65 vs yday $11,214.30 (+65.35) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.74 | ▲ close $11,305.52 vs 09:30 $11,279.65 (session +25.87) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.74 | ▲ 09:30 equity $11,431.80 vs yday $11,305.52 (+126.28) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $78.04 | ▼ -0.80 after sell → book $11,431.67; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $86.18 | ▼ -0.29 after sell → book $11,431.57; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.64 | $0.17 | $-1.23 | $99.93 | ▼ -1.23 after sell → book $11,431.40; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 2 | $10.77 | $0.24 | $+6.65 | $121.23 | ▲ +6.65 after sell → book $11,431.16; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 43 | $0.36 | $0.31 | $-0.29 | $136.62 | ▼ -0.29 after sell → book $11,430.85; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.62 | ▼ close $11,425.88 vs 09:30 $11,431.80 (session -4.97) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.62 | ▼ 09:30 equity $11,402.39 vs yday $11,425.88 (-23.49) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `FLNC` | 1 | $10.38 | $0.13 | $-0.98 | $146.88 | ▼ -0.98 after sell → book $11,402.26; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.88 | ▲ close $11,501.88 vs 09:30 $11,402.39 (session +99.62) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.88 | ▲ 09:30 equity $11,599.57 vs yday $11,501.88 (+97.69) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 38 | $42.43 | $2.13 | $+33.39 | $1,757.09 | ▲ +33.39 after sell → book $11,597.45; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 66 | $26.12 | $2.21 | $+135.52 | $3,478.80 | ▲ +135.52 after sell → book $11,595.23; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 613 | $2.49 | $8.02 | $-83.36 | $4,997.15 | ▼ -83.36 after sell → book $11,587.21; vs 09:30 mark -8.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 122 | $13.25 | $2.39 | $+28.19 | $6,611.26 | ▲ +28.19 after sell → book $11,584.82; vs 09:30 mark -2.39 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AVBP` | 51 | $30.58 | $2.17 | $-15.02 | $8,168.67 | ▼ -15.02 after sell → book $11,582.66; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 164 | $9.68 | $2.52 | $-5.00 | $9,753.67 | ▼ -5.00 after sell → book $11,580.14; vs 09:30 mark -2.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $8,376.72 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1393.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $7,000.88 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1393.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 383 | $3.63 | $4.94 | — | $5,605.65 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1393.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 173 | $8.03 | $2.51 | — | $4,213.95 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1393.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $2,887.43 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1393.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,571.91 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1393.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.77 | $2.24 | — | $177.76 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1393.38 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.76 | ▼ close $11,301.60 vs 09:30 $11,599.57 (session -260.66) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.76 | ▲ 09:30 equity $11,304.62 vs yday $11,301.60 (+3.02) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 110 | $15.00 | $2.35 | $+59.13 | $1,825.41 | ▲ +59.13 after sell → book $11,302.27; vs 09:30 mark -2.35 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 1 | $33.86 | $0.36 | $+0.27 | $1,858.90 | ▲ +0.27 after sell → book $11,301.90; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 3 | $13.56 | $0.44 | $-7.21 | $1,899.15 | ▼ -7.21 after sell → book $11,301.47; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 14 | $3.53 | $0.56 | $+1.88 | $1,948.01 | ▲ +1.88 after sell → book $11,300.91; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 128 | $2.52 | $2.37 | — | $1,623.08 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $324.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 48 | $6.71 | $2.13 | — | $1,298.86 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $324.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 170 | $1.90 | $2.50 | — | $973.36 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $324.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 67 | $4.78 | $2.19 | — | $650.91 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $324.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 204 | $1.59 | $2.63 | — | $323.92 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $324.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 28 | $11.31 | $2.07 | — | $5.17 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $324.67 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.17 | ▲ close $11,287.82 vs 09:30 $11,304.62 (session +0.81) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.17 | ▼ 09:30 equity $11,277.53 vs yday $11,287.82 (-10.29) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.17 | ▼ close $11,104.08 vs 09:30 $11,277.53 (session -173.45) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.17 | ▼ 09:30 equity $11,042.86 vs yday $11,104.08 (-61.22) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.17 | ▼ close $10,672.08 vs 09:30 $11,042.86 (session -370.78) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.17 | ▼ 09:30 equity $10,532.43 vs yday $10,672.08 (-139.65) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.17 | ▼ close $10,383.46 vs 09:30 $10,532.43 (session -148.97) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.17 | ▲ 09:30 equity $10,484.77 vs yday $10,383.46 (+101.31) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 26 | $53.53 | $2.09 | $+12.74 | $1,394.86 | ▲ +12.74 after sell → book $10,482.68; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 32 | $41.30 | $2.11 | $-56.35 | $2,714.35 | ▼ -56.35 after sell → book $10,480.58; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 383 | $2.77 | $5.01 | $-339.34 | $3,770.25 | ▼ -339.34 after sell → book $10,475.56; vs 09:30 mark -5.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 173 | $7.70 | $2.55 | $-62.15 | $5,099.80 | ▼ -62.15 after sell → book $10,473.01; vs 09:30 mark -2.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 10 | $122.40 | $2.04 | $-104.56 | $6,321.76 | ▼ -104.56 after sell → book $10,470.97; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 9 | $137.91 | $2.04 | $-76.41 | $7,560.87 | ▼ -76.41 after sell → book $10,468.94; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 83 | $14.06 | $2.26 | $-229.43 | $8,725.58 | ▼ -229.43 after sell → book $10,466.67; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 66 | $16.28 | $2.19 | — | $7,648.92 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-1.1; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 399 | $2.73 | $5.15 | — | $6,554.50 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-3.0; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $5,518.29 | — | S≥+5: sizeup + more names; list flatten; ret5=+8.3; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $4,529.71 | — | S≥+5: sizeup + more names; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $3,581.02 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+4.7; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 19 | $56.09 | $2.05 | — | $2,513.26 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+19.6; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 534 | $2.04 | $6.89 | — | $1,417.01 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1090.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 229 | $4.75 | $2.95 | — | $326.31 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1090.70 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $326.31 | ▼ close $10,421.15 vs 09:30 $10,484.77 (session -20.28) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $326.31 | ▼ 09:30 equity $10,186.94 vs yday $10,421.15 (-234.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 128 | $2.15 | $2.41 | $-52.14 | $599.10 | ▼ -52.14 after sell → book $10,184.54; vs 09:30 mark -2.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 48 | $5.93 | $2.15 | $-41.73 | $881.59 | ▼ -41.73 after sell → book $10,182.38; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 170 | $1.72 | $2.54 | $-36.49 | $1,170.60 | ▼ -36.49 after sell → book $10,179.85; vs 09:30 mark -2.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 67 | $4.13 | $2.21 | $-47.95 | $1,445.10 | ▼ -47.95 after sell → book $10,177.63; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 204 | $1.59 | $2.68 | $-5.31 | $1,766.78 | ▼ -5.31 after sell → book $10,174.96; vs 09:30 mark -2.67 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 28 | $10.73 | $2.09 | $-20.41 | $2,065.13 | ▼ -20.41 after sell → book $10,172.86; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,065.13 | ▼ close $10,128.86 vs 09:30 $10,186.94 (session -44.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,065.13 | ▲ 09:30 equity $10,152.27 vs yday $10,128.86 (+23.41) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,065.13 | ▼ close $9,928.46 vs 09:30 $10,152.27 (session -223.81) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,065.13 | ▲ 09:30 equity $9,985.03 vs yday $9,928.46 (+56.57) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 2 | $77.12 | $1.55 | — | $1,909.34 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+7.2; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 12 | $14.31 | $1.75 | — | $1,735.87 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.8; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 4 | $36.46 | $1.47 | — | $1,588.56 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+2.9; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 9 | $18.61 | $1.70 | — | $1,419.37 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 9 | $18.21 | $1.67 | — | $1,253.81 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-19.1; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 2 | $68.79 | $1.38 | — | $1,114.85 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 29 | $5.87 | $1.79 | — | $942.83 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 63 | $2.72 | $1.90 | — | $769.57 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-0.4; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 1 | $87.40 | $0.88 | — | $681.29 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 4 | $38.01 | $1.53 | — | $527.72 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $172.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 6 | $27.09 | $1.64 | — | $363.53 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $172.09 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $363.53 | ▲ close $10,022.86 vs 09:30 $9,985.03 (session +55.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $363.53 | ▲ 09:30 equity $10,214.44 vs yday $10,022.86 (+191.58) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $342.82 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $30.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $319.82 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $30.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 178 | $0.17 | $0.84 | — | $288.72 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $30.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $272.69 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $30.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 1 | $25.95 | $0.26 | — | $246.47 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $30.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 12 | $2.40 | $0.32 | — | $217.35 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $30.29 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.35 | ▼ close $10,194.44 vs 09:30 $10,214.44 (session -17.97) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.35 | ▲ 09:30 equity $10,227.08 vs yday $10,194.44 (+32.64) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 66 | $16.93 | $2.21 | $+38.50 | $1,332.52 | ▲ +38.50 after sell → book $10,224.87; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 399 | $2.68 | $5.22 | $-30.32 | $2,396.62 | ▼ -30.32 after sell → book $10,219.65; vs 09:30 mark -5.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 5 | $197.76 | $2.02 | $-49.43 | $3,383.39 | ▼ -49.43 after sell → book $10,217.62; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 6 | $150.47 | $2.03 | $-87.80 | $4,284.19 | ▼ -87.80 after sell → book $10,215.60; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 6 | $152.71 | $2.03 | $-34.46 | $5,198.42 | ▼ -34.46 after sell → book $10,213.57; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 19 | $55.80 | $2.07 | $-9.62 | $6,256.55 | ▼ -9.62 after sell → book $10,211.50; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 534 | $1.90 | $6.99 | $-88.64 | $7,264.16 | ▼ -88.64 after sell → book $10,204.51; vs 09:30 mark -6.99 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 229 | $4.50 | $3.00 | $-63.21 | $8,291.66 | ▼ -63.21 after sell → book $10,201.51; vs 09:30 mark -3.00 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 9 | $108.55 | $2.02 | — | $7,312.69 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+21.3; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $6,717.55 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+16.1; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 4 | $209.52 | $2.00 | — | $5,877.47 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $4,996.99 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 12 | $85.00 | $2.03 | — | $3,974.96 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 30 | $34.44 | $2.08 | — | $2,939.68 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1068 | $0.97 | $13.56 | — | $1,890.16 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1036.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 498 | $2.08 | $6.42 | — | $847.89 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1036.46 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $847.89 | ▼ close $10,025.42 vs 09:30 $10,227.08 (session -143.98) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $847.89 | ▲ 09:30 equity $10,131.73 vs yday $10,025.42 (+106.31) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 6 | $13.47 | $0.83 | — | $766.25 | — | S≥+5: sizeup + more names; list flatten; ret5=+3.6; leftover $84.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 21 | $4.00 | $0.90 | — | $681.34 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $84.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 9 | $9.31 | $0.86 | — | $596.69 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $84.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 6 | $13.47 | $0.83 | — | $515.01 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $84.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 76 | $1.11 | $1.07 | — | $429.58 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $84.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 8 | $9.99 | $0.82 | — | $348.84 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $84.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 46 | $1.82 | $0.98 | — | $263.91 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $84.79 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.91 | ▼ close $10,083.66 vs 09:30 $10,131.73 (session -41.78) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.91 | ▼ 09:30 equity $10,076.04 vs yday $10,083.66 (-7.62) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 56 | $0.58 | $0.49 | — | $230.94 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $32.99 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.94 | ▲ close $10,076.03 vs 09:30 $10,076.04 (session +0.48) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.94 | ▲ 09:30 equity $10,326.71 vs yday $10,076.03 (+250.68) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 2 | $73.61 | $1.50 | $-10.07 | $376.66 | ▼ -10.07 after sell → book $10,325.21; vs 09:30 mark -1.50 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 12 | $13.12 | $1.63 | $-17.66 | $532.47 | ▼ -17.66 after sell → book $10,323.58; vs 09:30 mark -1.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 4 | $38.04 | $1.55 | $+3.30 | $683.08 | ▲ +3.30 after sell → book $10,322.03; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 9 | $23.00 | $2.04 | $+35.77 | $888.04 | ▲ +35.77 after sell → book $10,319.99; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 9 | $23.30 | $2.04 | $+42.11 | $1,095.70 | ▲ +42.11 after sell → book $10,317.95; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 2 | $76.47 | $1.56 | $+12.42 | $1,247.09 | ▲ +12.42 after sell → book $10,316.40; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 29 | $5.53 | $1.71 | $-13.36 | $1,405.74 | ▼ -13.36 after sell → book $10,314.69; vs 09:30 mark -1.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 63 | $3.17 | $2.20 | $+24.25 | $1,603.26 | ▲ +24.25 after sell → book $10,312.49; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `VAL` | 1 | $83.39 | $0.86 | $-5.74 | $1,685.79 | ▼ -5.74 after sell → book $10,311.63; vs 09:30 mark -0.86 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `KRMN` | 4 | $33.34 | $1.37 | $-21.58 | $1,817.78 | ▼ -21.58 after sell → book $10,310.26; vs 09:30 mark -1.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 6 | $27.74 | $1.70 | $+0.55 | $1,982.52 | ▲ +0.55 after sell → book $10,308.56; vs 09:30 mark -1.70 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 3 | $89.50 | $2.00 | — | $1,712.02 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.3; leftover $330.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 1 | $166.54 | $1.67 | — | $1,543.81 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.3; leftover $330.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 2 | $116.85 | $2.00 | — | $1,308.12 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+3.3; leftover $330.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 33 | $9.81 | $2.09 | — | $982.30 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+4.0; leftover $330.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 16 | $20.25 | $2.04 | — | $656.26 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+15.0; leftover $330.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 16 | $20.65 | $2.04 | — | $323.82 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $330.42 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $323.82 | ▲ close $10,563.47 vs 09:30 $10,326.71 (session +266.73) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $323.82 | ▼ 09:30 equity $10,491.94 vs yday $10,563.47 (-71.53) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 2 | $10.39 | $0.23 | $-0.16 | $344.37 | ▼ -0.16 after sell → book $10,491.71; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $366.26 | ▼ -1.12 after sell → book $10,491.46; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 178 | $0.15 | $0.84 | $-5.24 | $392.12 | ▼ -5.24 after sell → book $10,490.62; vs 09:30 mark -0.84 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 1 | $16.07 | $0.18 | $-0.15 | $408.01 | ▼ -0.15 after sell → book $10,490.43; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 1 | $26.22 | $0.29 | $-0.28 | $433.94 | ▼ -0.28 after sell → book $10,490.15; vs 09:30 mark -0.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `SABR` | 12 | $2.17 | $0.32 | $-3.40 | $459.66 | ▼ -3.40 after sell → book $10,489.83; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $459.66 | ▲ close $10,663.73 vs 09:30 $10,491.94 (session +173.90) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $469.04 | ▲ 09:30 equity $9,149.36 vs yday $9,120.78 (+28.58) | 09:30 open · cash $469.04 (unchanged overnight, no fees) · equity $9,149.36 vs prior close $9,120.78 (+28.58) · 17 name(s) re-marked at the open (per-name table). A×8 yday $172.84 → 09:30 $171.98 -6.88; ADMA×151 yday $9.52 → 09:30 $9.52 +0.00; BHVN×2 yday $13.19 → 09:30 $13.19 +0.00; BTBT×9 yday $1.79 → 09:30 $1.79 +0.00; BTDR×1 yday $12.15 → 09:30 $12.15 +0.00; CYPH×4 yday $4.08 → 09:30 $4.00 -0.30; DEFT×16 yday $0.53 → 09:30 $0.53 +0.00; DXCM×16 yday $87.47 → 09:30 $87.47 +0.00; EYPT×7 yday $3.65 → 09:30 $3.65 +0.00; FJET×4 yday $1.80 → 09:30 $1.80 +0.00; FTRE×73 yday $20.02 → 09:30 $20.02 +0.00; HALO×12 yday $115.22 → 09:30 $115.36 +1.68; MGTX×1 yday $11.05 → 09:30 $11.05 +0.00; OMER×71 yday $20.13 → 09:30 $20.61 +34.08; ORBS×15 yday $1.03 → 09:30 $1.03 +0.00; SBET×1 yday $9.95 → 09:30 $9.95 +0.00; SGML×1 yday $9.98 → 09:30 $9.98 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 2 | $38.51 | $0.78 | — | $391.24 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.7; leftover $78.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 10 | $7.65 | $0.80 | — | $313.95 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.2; leftover $78.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 2 | $26.27 | $0.53 | — | $260.88 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $78.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 8 | $9.05 | $0.75 | — | $187.73 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-27.1; leftover $78.17 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.73 | ▼ close $9,101.12 vs 09:30 $9,149.36 (session -45.39) | 16:00 close · cash $187.73 · equity $9,101.12 vs 09:30 $9,149.36 (-48.24; session marks -45.39) · 21 name(s) marked open→close (per-name table). A×8 09:30 $171.98 → close $172.79 +6.48; ADMA×151 09:30 $9.52 → close $9.52 +0.00; BHVN×2 09:30 $13.19 → close $13.19 -0.00; BTBT×9 09:30 $1.79 → close $1.79 -0.00; BTDR×1 09:30 $12.15 → close $12.15 -0.00; CYPH×4 09:30 $4.00 → close $4.12 +0.46; DEFT×16 09:30 $0.53 → close $0.53 +0.00; DXCM×16 09:30 $87.47 → close $87.47 +0.00; EYPT×7 09:30 $3.65 → close $3.65 +0.00; FJET×4 09:30 $1.80 → close $1.80 -0.00; FTRE×73 09:30 $20.02 → close $20.02 +0.00; HALO×12 09:30 $115.36 → close $113.90 -17.52; MGTX×1 09:30 $11.05 → close $11.05 +0.00; OMER×71 09:30 $20.61 → close $20.08 -37.63; ORBS×15 09:30 $1.03 → close $1.03 -0.00; SBET×1 09:30 $9.95 → close $9.95 -0.00; SGML×1 09:30 $9.98 → close $9.98 -0.00; BLFS×2 09:30 $38.51 → close $38.49 -0.04; MRVI×10 09:30 $7.65 → close $7.60 -0.50; WRBY×2 09:30 $26.27 → close $26.71 +0.88; AEHL×8 09:30 $9.05 → close $9.36 +2.48 | — |

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
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 10.32 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 10.32 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 10.32 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 10.32 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 10.32 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 10.32 < 1 share @ 14.80 |
| 2026-08-14 | `WWW` | cash | leftover split 10.32 < 1 share @ 20.60 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 9.75 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 9.75 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 9.75 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.75 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 24.46 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 24.46 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 24.46 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 15.56 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 15.56 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 15.56 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 11.94 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 11.94 < 1 share @ 24.84 |
| 2026-08-26 | `INSP` | cash | leftover split 11.94 < 1 share @ 60.07 |
| 2026-08-26 | `AVBP` | cash | leftover split 11.94 < 1 share @ 31.21 |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `URBN` | cash | leftover split 48.86 < 1 share @ 79.42 |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `AVBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-16 | `IQV` | cash | leftover split 172.09 < 1 share @ 270.89 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 30.29 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 30.29 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 30.29 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 30.29 < 1 share @ 34.93 |
| 2026-09-17 | `AXTI` | cash | leftover split 30.29 < 1 share @ 67.91 |
| 2026-09-17 | `SMTC` | cash | leftover split 30.29 < 1 share @ 170.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `VAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `KRMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 84.79 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 84.79 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 84.79 < 1 share @ 88.83 |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `VAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `KRMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ADPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `ARQT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `SABR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 32.99 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `SABR` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `RBRK` | 9 | 2026-09-18 @ $108.55 | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+21.3; leftover $1036.46 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+16.1; leftover $1036.46 |
| `GNRC` | 4 | 2026-09-18 @ $209.52 | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1036.46 |
| `VICR` | 4 | 2026-09-18 @ $219.62 | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1036.46 |
| `ECO` | 12 | 2026-09-18 @ $85.00 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1036.46 |
| `FIVN` | 30 | 2026-09-18 @ $34.44 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1036.46 |
| `TLSA` | 1068 | 2026-09-18 @ $0.97 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1036.46 |
| `SWRD` | 498 | 2026-09-18 @ $2.08 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1036.46 |
| `MGTX` | 6 | 2026-09-21 @ $13.47 | S≥+5: sizeup + more names; list flatten; ret5=+3.6; leftover $84.79 |
| `CYPH` | 21 | 2026-09-21 @ $4.00 | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $84.79 |
| `BKKT` | 9 | 2026-09-21 @ $9.31 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $84.79 |
| `BTDR` | 6 | 2026-09-21 @ $13.47 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $84.79 |
| `ORBS` | 76 | 2026-09-21 @ $1.11 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $84.79 |
| `SBET` | 8 | 2026-09-21 @ $9.99 | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $84.79 |
| `BTBT` | 46 | 2026-09-21 @ $1.82 | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $84.79 |
| `DEFT` | 56 | 2026-09-22 @ $0.58 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $32.99 |
| `DXCM` | 3 | 2026-09-23 @ $89.50 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.3; leftover $330.42 |
| `A` | 1 | 2026-09-23 @ $166.54 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.3; leftover $330.42 |
| `HALO` | 2 | 2026-09-23 @ $116.85 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+3.3; leftover $330.42 |
| `ADMA` | 33 | 2026-09-23 @ $9.81 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+4.0; leftover $330.42 |
| `FTRE` | 16 | 2026-09-23 @ $20.25 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+15.0; leftover $330.42 |
| `OMER` | 16 | 2026-09-23 @ $20.65 | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $330.42 |
