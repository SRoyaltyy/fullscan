# Factor mine action — `union_h3_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **-19.17%** ($8,083) · signal-only (no cash/fees) was -4.92%. Starts YES **0/30**. Fills 211 · skips 326 · realized $-641.46.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On a strong morning (S ≥ +5), spend 1.35× leftover and add 4 extra names — still cash-capped.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,994.62.

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
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 18 | $60.00 | $2.06 | $-0.51 | $1,119.65 | ▼ -0.51 after sell → book $10,382.68; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 24 | $43.56 | $2.08 | $-62.22 | $2,163.01 | ▼ -62.22 after sell → book $10,380.60; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 21 | $51.77 | $2.07 | $+19.96 | $3,248.11 | ▲ +19.96 after sell → book $10,378.53; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 22 | $49.28 | $2.08 | $-13.37 | $4,330.19 | ▼ -13.37 after sell → book $10,376.45; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 94 | $12.66 | $2.30 | $+85.67 | $5,517.93 | ▲ +85.67 after sell → book $10,374.15; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 37 | $27.85 | $2.12 | $-74.15 | $6,546.26 | ▼ -74.15 after sell → book $10,372.03; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1371 | $1.14 | $17.93 | $+419.29 | $8,091.28 | ▲ +419.29 after sell → book $10,354.11; vs 09:30 mark -17.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 47 | $22.16 | $2.15 | $-59.27 | $9,130.65 | ▼ -59.27 after sell → book $10,351.96; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 50 | $22.82 | $2.16 | $+36.20 | $10,269.49 | ▲ +36.20 after sell → book $10,349.80; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,269.49 | ▼ close $10,349.15 vs 09:30 $10,384.75 (session -0.64) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,269.49 | ▲ 09:30 equity $10,349.65 vs yday $10,349.15 (+0.50) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,278.28 | ▼ -0.31 after sell → book $10,349.53; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 11 | $0.88 | $0.15 | $-0.91 | $10,287.81 | ▼ -0.91 after sell → book $10,349.38; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 6 | $1.42 | $0.12 | $-0.71 | $10,296.21 | ▼ -0.71 after sell → book $10,349.26; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $10,305.67 | ▲ +0.75 after sell → book $10,349.14; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 2 | $3.87 | $0.10 | $-0.81 | $10,313.31 | ▼ -0.81 after sell → book $10,349.04; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.31 | ▲ close $10,349.42 vs 09:30 $10,349.65 (session +0.38) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.31 | ▼ 09:30 equity $10,349.06 vs yday $10,349.42 (-0.36) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $10,321.04 | ▼ -0.45 after sell → book $10,348.95; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,329.28 | ▼ -0.30 after sell → book $10,348.84; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 3 | $3.20 | $0.12 | $-0.35 | $10,338.76 | ▼ -0.35 after sell → book $10,348.72; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 2 | $4.98 | $0.13 | $+0.11 | $10,348.59 | ▲ +0.11 after sell → book $10,348.59; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,072.32 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,796.15 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,513.67 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,218.30 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,940.17 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,663.96 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,361.17 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1293.57 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $202.84 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1293.57 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.84 | ▲ close $10,563.18 vs 09:30 $10,349.06 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.84 | ▲ 09:30 equity $10,839.07 vs yday $10,563.18 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $185.46 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $25.35 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $162.98 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $25.35 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $138.00 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $25.35 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $112.62 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $25.35 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $87.23 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $25.35 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.23 | ▲ close $10,838.08 vs 09:30 $10,839.07 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.23 | ▲ 09:30 equity $10,949.82 vs yday $10,838.08 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.23 | ▼ close $10,915.06 vs 09:30 $10,949.82 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.23 | ▼ 09:30 equity $10,744.44 vs yday $10,915.06 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,344.87 | ▼ -18.63 after sell → book $10,742.24; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,684.86 | ▲ +63.82 after sell → book $10,740.19; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,951.81 | ▼ -15.53 after sell → book $10,738.00; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,187.59 | ▼ -59.59 after sell → book $10,735.06; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,564.03 | ▲ +98.31 after sell → book $10,732.85; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,951.65 | ▲ +111.41 after sell → book $10,730.71; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,346.09 | ▲ +91.65 after sell → book $10,721.05; vs 09:30 mark -9.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,596.13 | ▲ +91.71 after sell → book $10,719.01; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,286.63 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.0; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 120 | $10.98 | $2.35 | — | $7,966.68 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+1.2; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,679.63 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+7.4; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.35 | $2.46 | — | $5,357.87 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $4.94 | $3.46 | — | $4,030.49 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,747.58 | — | S≥+5: sizeup + more names; list flatten; ret5=+6.0; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 182 | $7.25 | $2.54 | — | $1,425.55 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1324.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3699 | $0.36 | $24.34 | — | $76.97 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-15.6; leftover $1324.52 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.97 | ▲ close $10,928.74 vs 09:30 $10,744.44 (session +251.08) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.97 | ▼ 09:30 equity $10,926.13 vs yday $10,928.74 (-2.61) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $93.38 | ▼ -0.96 after sell → book $10,925.94; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $123.74 | ▲ +7.88 after sell → book $10,925.61; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $147.55 | ▼ -1.17 after sell → book $10,925.32; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $173.62 | ▲ +0.69 after sell → book $10,925.00; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $203.64 | ▲ +4.63 after sell → book $10,924.62; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 3 | $31.21 | $0.95 | — | $109.06 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $101.82 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 9 | $11.12 | $1.03 | — | $7.96 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $101.82 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.96 | ▲ close $11,209.25 vs 09:30 $10,926.13 (session +286.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.96 | ▼ 09:30 equity $11,200.30 vs yday $11,209.25 (-8.95) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.96 | ▼ close $11,187.74 vs 09:30 $11,200.30 (session -12.57) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.96 | ▼ 09:30 equity $11,132.66 vs yday $11,187.74 (-55.08) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 120 | $10.97 | $2.38 | $-5.93 | $1,321.97 | ▼ -5.93 after sell → book $11,130.28; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $60.52 | $2.07 | $-18.20 | $2,590.82 | ▼ -18.20 after sell → book $11,128.21; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.28 | $2.50 | $-16.02 | $3,896.56 | ▼ -16.02 after sell → book $11,125.71; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 268 | $4.95 | $3.51 | $-4.29 | $5,219.65 | ▼ -4.29 after sell → book $11,122.19; vs 09:30 mark -3.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $6,488.91 | ▼ -13.65 after sell → book $11,120.17; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 182 | $9.73 | $2.58 | $+446.24 | $8,257.19 | ▲ +446.24 after sell → book $11,117.59; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3699 | $0.36 | $25.22 | $-23.67 | $9,582.10 | ▼ -23.67 after sell → book $11,092.37; vs 09:30 mark -25.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 32 | $41.74 | $2.09 | — | $8,244.33 | — | S≥+5: sizeup + more names; list flatten; ret5=+2.4; leftover $1368.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 93 | $14.63 | $2.27 | — | $6,881.48 | — | S≥+5: sizeup + more names; list flatten; ret5=+5.8; leftover $1368.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 510 | $2.68 | $6.58 | — | $5,508.10 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+16.3; leftover $1368.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $4,157.08 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1368.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 87 | $15.66 | $2.25 | — | $2,792.41 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1368.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,440.23 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1368.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 412 | $3.32 | $5.31 | — | $67.08 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+6.4; leftover $1368.87 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.08 | ▼ close $10,760.47 vs 09:30 $11,132.66 (session -309.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.08 | ▲ 09:30 equity $10,788.01 vs yday $10,760.47 (+27.54) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.68 | $2.18 | $-9.28 | $1,367.30 | ▼ -9.28 after sell → book $10,785.83; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 3 | $29.94 | $0.93 | $-5.68 | $1,456.19 | ▼ -5.68 after sell → book $10,784.90; vs 09:30 mark -0.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 9 | $10.82 | $1.02 | $-4.75 | $1,552.55 | ▼ -4.75 after sell → book $10,783.88; vs 09:30 mark -1.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,552.55 | ▲ close $10,913.77 vs 09:30 $10,788.01 (session +129.89) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,552.55 | ▲ 09:30 equity $10,997.43 vs yday $10,913.77 (+83.66) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,552.55 | ▼ close $10,992.24 vs 09:30 $10,997.43 (session -5.19) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,552.55 | ▼ 09:30 equity $10,931.69 vs yday $10,992.24 (-60.55) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 32 | $42.10 | $2.11 | $+7.33 | $2,897.65 | ▲ +7.33 after sell → book $10,929.59; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 93 | $15.70 | $2.30 | $+94.94 | $4,355.45 | ▲ +94.94 after sell → book $10,927.29; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SLI` | 510 | $2.49 | $6.67 | $-110.15 | $5,618.68 | ▼ -110.15 after sell → book $10,920.62; vs 09:30 mark -6.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 41 | $32.42 | $2.13 | $-23.93 | $6,945.76 | ▼ -23.93 after sell → book $10,918.48; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 87 | $13.92 | $2.28 | $-155.91 | $8,154.53 | ▼ -155.91 after sell → book $10,916.21; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 17 | $78.84 | $2.06 | $-13.96 | $9,492.75 | ▼ -13.96 after sell → book $10,914.15; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 412 | $3.45 | $5.39 | $+42.85 | $10,908.75 | ▲ +42.85 after sell → book $10,908.75; vs 09:30 mark -5.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,908.75 | ▲ close $10,908.75 vs 09:30 $10,931.69 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,908.75 | ▲ 09:30 equity $10,908.75 vs yday $10,908.75 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,584.69 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,251.77 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 375 | $3.63 | $4.84 | — | $6,885.69 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 169 | $8.03 | $2.50 | — | $5,526.12 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,199.60 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,837.75 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,522.22 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1363.59 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $161.62 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1363.59 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.62 | ▼ close $10,646.49 vs 09:30 $10,908.75 (session -242.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.62 | ▲ 09:30 equity $10,650.45 vs yday $10,646.49 (+3.96) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 10 | $2.52 | $0.28 | — | $136.14 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $26.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 4 | $6.71 | $0.28 | — | $109.02 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $26.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 14 | $1.90 | $0.31 | — | $82.11 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $26.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $4.78 | $0.25 | — | $57.96 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $26.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 16 | $1.59 | $0.30 | — | $32.21 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $26.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 2 | $11.31 | $0.23 | — | $9.36 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $26.94 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.36 | ▲ close $10,680.53 vs 09:30 $10,650.45 (session +31.74) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.36 | ▲ 09:30 equity $10,711.35 vs yday $10,680.53 (+30.82) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.36 | ▼ close $10,528.87 vs 09:30 $10,711.35 (session -182.48) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.36 | ▼ 09:30 equity $10,476.85 vs yday $10,528.87 (-52.02) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 25 | $53.16 | $2.09 | $+2.85 | $1,336.28 | ▲ +2.85 after sell → book $10,474.76; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 31 | $42.01 | $2.10 | $-32.71 | $2,636.48 | ▼ -32.71 after sell → book $10,472.66; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 169 | $8.01 | $2.54 | $-8.41 | $3,987.64 | ▼ -8.41 after sell → book $10,470.12; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 10 | $125.77 | $2.04 | $-70.86 | $5,243.30 | ▼ -70.86 after sell → book $10,468.08; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 88 | $15.16 | $2.28 | $-30.05 | $6,575.10 | ▼ -30.05 after sell → book $10,465.80; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 9 | $140.29 | $2.04 | $-54.90 | $7,835.71 | ▼ -54.90 after sell → book $10,463.76; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 81 | $15.46 | $2.26 | $-110.60 | $9,085.72 | ▼ -110.60 after sell → book $10,461.51; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,085.72 | ▼ close $10,316.47 vs 09:30 $10,476.85 (session -145.04) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,085.72 | ▼ 09:30 equity $10,291.96 vs yday $10,316.47 (-24.51) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 375 | $2.85 | $4.91 | $-302.25 | $10,149.56 | ▼ -302.25 after sell → book $10,287.05; vs 09:30 mark -4.91 | dropped from list after 4 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 10 | $2.22 | $0.27 | $-3.55 | $10,171.49 | ▼ -3.55 after sell → book $10,286.78; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 4 | $6.11 | $0.28 | $-2.96 | $10,195.65 | ▼ -2.96 after sell → book $10,286.50; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 14 | $1.83 | $0.32 | $-1.61 | $10,220.95 | ▼ -1.61 after sell → book $10,286.18; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 5 | $3.92 | $0.23 | $-4.78 | $10,240.33 | ▼ -4.78 after sell → book $10,285.95; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 16 | $1.53 | $0.31 | $-1.58 | $10,264.50 | ▼ -1.58 after sell → book $10,285.64; vs 09:30 mark -0.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 2 | $10.57 | $0.24 | $-1.95 | $10,285.40 | ▼ -1.95 after sell → book $10,285.40; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,285.40 | ▲ close $10,285.40 vs 09:30 $10,291.96 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,285.40 | ▲ 09:30 equity $10,285.40 vs yday $10,285.40 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 78 | $16.28 | $2.22 | — | $9,013.34 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-1.1; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 470 | $2.73 | $6.06 | — | $7,724.17 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-3.0; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,481.13 | — | S≥+5: sizeup + more names; list flatten; ret5=+8.3; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,328.10 | — | S≥+5: sizeup + more names; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,063.85 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+4.7; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $2,827.81 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+19.6; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 630 | $2.04 | $8.13 | — | $1,534.49 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1285.68 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 270 | $4.75 | $3.48 | — | $248.50 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1285.68 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.50 | ▼ close $10,239.06 vs 09:30 $10,285.40 (session -18.35) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.50 | ▼ 09:30 equity $9,939.39 vs yday $10,239.06 (-299.67) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.50 | ▼ close $9,884.01 vs 09:30 $9,939.39 (session -55.38) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.50 | ▲ 09:30 equity $9,915.64 vs yday $9,884.01 (+31.63) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.50 | ▼ close $9,647.69 vs 09:30 $9,915.64 (session -267.95) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.50 | ▲ 09:30 equity $9,715.13 vs yday $9,647.69 (+67.44) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 78 | $16.16 | $2.25 | $-13.83 | $1,506.74 | ▼ -13.83 after sell → book $9,712.89; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 470 | $2.72 | $6.15 | $-16.91 | $2,778.99 | ▼ -16.91 after sell → book $9,706.74; vs 09:30 mark -6.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 6 | $194.84 | $2.03 | $-76.04 | $3,946.00 | ▼ -76.04 after sell → book $9,704.71; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $4,924.18 | ▼ -174.84 after sell → book $9,702.68; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 8 | $147.79 | $2.03 | $-83.97 | $6,104.46 | ▼ -83.97 after sell → book $9,700.64; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 22 | $51.29 | $2.08 | $-109.73 | $7,230.77 | ▼ -109.73 after sell → book $9,698.57; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 630 | $1.89 | $8.24 | $-110.87 | $8,413.23 | ▼ -110.87 after sell → book $9,690.33; vs 09:30 mark -8.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 270 | $4.73 | $3.54 | $-12.42 | $9,686.79 | ▼ -12.42 after sell → book $9,686.79; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $9,143.01 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.0; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 10 | $77.12 | $2.02 | — | $8,369.79 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+7.2; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 56 | $14.31 | $2.16 | — | $7,566.27 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.8; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 22 | $36.46 | $2.06 | — | $6,762.10 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+2.9; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 43 | $18.61 | $2.12 | — | $5,959.75 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 44 | $18.21 | $2.12 | — | $5,156.39 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-19.1; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 11 | $68.79 | $2.02 | — | $4,397.67 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 137 | $5.87 | $2.40 | — | $3,591.08 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 296 | $2.72 | $3.82 | — | $2,782.14 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-0.4; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 9 | $87.40 | $2.02 | — | $1,993.53 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 21 | $38.01 | $2.05 | — | $1,193.26 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $807.23 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 29 | $27.09 | $2.08 | — | $405.58 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $807.23 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $405.58 | ▲ close $9,795.04 vs 09:30 $9,715.13 (session +135.11) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $405.58 | ▲ 09:30 equity $9,950.80 vs yday $9,795.04 (+155.76) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $374.51 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $33.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $343.84 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $33.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 198 | $0.17 | $0.93 | — | $309.24 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $33.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 2 | $15.87 | $0.32 | — | $277.18 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $33.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 1 | $25.95 | $0.26 | — | $250.97 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $33.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 14 | $2.40 | $0.38 | — | $216.99 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $33.80 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.99 | ▲ close $10,000.13 vs 09:30 $9,950.80 (session +51.86) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.99 | ▲ 09:30 equity $10,041.38 vs yday $10,000.13 (+41.25) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 27 | $0.97 | $0.34 | — | $190.46 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $27.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 13 | $2.08 | $0.31 | — | $163.11 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $27.12 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.11 | ▼ close $9,963.21 vs 09:30 $10,041.38 (session -77.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.11 | ▲ 09:30 equity $10,037.88 vs yday $9,963.21 (+74.67) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 2 | $266.76 | $2.02 | $-12.27 | $694.61 | ▼ -12.27 after sell → book $10,035.86; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 10 | $76.27 | $2.04 | $-12.56 | $1,455.27 | ▼ -12.56 after sell → book $10,033.82; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 56 | $13.65 | $2.18 | $-41.30 | $2,217.49 | ▼ -41.30 after sell → book $10,031.64; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 22 | $36.70 | $2.08 | $+1.15 | $3,022.82 | ▲ +1.15 after sell → book $10,029.57; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 43 | $22.11 | $2.14 | $+146.24 | $3,971.41 | ▲ +146.24 after sell → book $10,027.43; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 44 | $20.55 | $2.14 | $+98.70 | $4,873.47 | ▲ +98.70 after sell → book $10,025.29; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 11 | $79.08 | $2.04 | $+109.12 | $5,741.30 | ▲ +109.12 after sell → book $10,023.24; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 137 | $5.62 | $2.43 | $-39.08 | $6,508.81 | ▼ -39.08 after sell → book $10,020.81; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 296 | $3.13 | $3.88 | $+113.66 | $7,431.41 | ▲ +113.66 after sell → book $10,016.93; vs 09:30 mark -3.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 9 | $83.46 | $2.04 | $-39.51 | $8,180.52 | ▼ -39.51 after sell → book $10,014.90; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `KRMN` | 21 | $36.30 | $2.07 | $-40.04 | $8,940.74 | ▼ -40.04 after sell → book $10,012.82; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 29 | $28.69 | $2.10 | $+42.23 | $9,770.66 | ▲ +42.23 after sell → book $10,010.73; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $8,821.43 | — | S≥+5: sizeup + more names; list flatten; ret5=+6.5; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 2 | $386.20 | $2.00 | — | $8,047.03 | — | S≥+5: sizeup + more names; list flatten; ret5=-5.8; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 10 | $88.83 | $2.02 | — | $7,156.71 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.6; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 72 | $13.47 | $2.21 | — | $6,184.67 | — | S≥+5: sizeup + more names; list flatten; ret5=+3.6; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 244 | $4.00 | $3.15 | — | $5,205.52 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 104 | $9.31 | $2.30 | — | $4,234.98 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 72 | $13.47 | $2.21 | — | $3,262.57 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 880 | $1.11 | $11.35 | — | $2,274.42 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 97 | $9.99 | $2.28 | — | $1,303.11 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $977.07 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 535 | $1.82 | $6.90 | — | $319.83 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $977.07 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $319.83 | ▼ close $9,710.23 vs 09:30 $10,037.88 (session -264.07) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $319.83 | ▲ 09:30 equity $9,716.88 vs yday $9,710.23 (+6.65) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 3 | $10.18 | $0.33 | $-0.86 | $350.04 | ▼ -0.86 after sell → book $9,716.54; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 198 | $0.16 | $0.95 | $-3.86 | $380.76 | ▼ -3.86 after sell → book $9,715.59; vs 09:30 mark -0.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 82 | $0.58 | $0.72 | — | $332.48 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $47.60 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $332.48 | ▲ close $9,935.86 vs 09:30 $9,716.88 (session +221.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $332.48 | ▼ 09:30 equity $9,901.07 vs yday $9,935.86 (-34.79) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 2 | $17.10 | $0.37 | $+1.77 | $366.31 | ▲ +1.77 after sell → book $9,900.70; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 14 | $2.24 | $0.38 | $-2.99 | $397.30 | ▼ -2.99 after sell → book $9,900.32; vs 09:30 mark -0.38 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 27 | $0.89 | $0.34 | $-2.84 | $420.99 | ▼ -2.84 after sell → book $9,899.98; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 13 | $2.16 | $0.34 | $+0.39 | $448.73 | ▲ +0.39 after sell → book $9,899.64; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 11 | $9.81 | $1.11 | — | $339.70 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+4.0; leftover $112.18 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 5 | $20.25 | $1.03 | — | $237.43 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+15.0; leftover $112.18 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 5 | $20.65 | $1.05 | — | $133.13 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $112.18 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.13 | ▼ close $9,520.56 vs 09:30 $9,901.07 (session -375.89) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.13 | ▼ 09:30 equity $9,380.76 vs yday $9,520.56 (-139.80) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 4 | $7.38 | $0.33 | $-1.48 | $162.32 | ▼ -1.48 after sell → book $9,380.43; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 1 | $26.22 | $0.29 | $-0.28 | $188.26 | ▼ -0.28 after sell → book $9,380.15; vs 09:30 mark -0.28 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $1,169.93 | ▲ +32.44 after sell → book $9,378.12; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 2 | $374.54 | $2.02 | $-27.33 | $1,916.99 | ▼ -27.33 after sell → book $9,376.10; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 10 | $87.67 | $2.04 | $-15.61 | $2,791.70 | ▼ -15.61 after sell → book $9,374.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 72 | $11.42 | $2.23 | $-152.03 | $3,611.72 | ▼ -152.03 after sell → book $9,371.83; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 244 | $3.40 | $3.20 | $-152.75 | $4,438.12 | ▼ -152.75 after sell → book $9,368.64; vs 09:30 mark -3.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 104 | $8.67 | $2.33 | $-71.19 | $5,337.47 | ▼ -71.19 after sell → book $9,366.31; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 72 | $12.26 | $2.23 | $-91.91 | $6,217.96 | ▼ -91.91 after sell → book $9,364.08; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 880 | $1.05 | $11.51 | $-75.66 | $7,130.45 | ▼ -75.66 after sell → book $9,352.57; vs 09:30 mark -11.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 97 | $9.80 | $2.31 | $-23.02 | $8,078.74 | ▼ -23.02 after sell → book $9,350.26; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 535 | $1.73 | $7.00 | $-67.40 | $8,994.62 | ▼ -67.40 after sell → book $9,343.26; vs 09:30 mark -7.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,994.62 | ▲ close $9,343.71 vs 09:30 $9,380.76 (session +0.45) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,806.96 | ▲ 09:30 equity $8,053.49 vs yday $8,053.01 (+0.48) | 09:30 open · cash $7,806.96 (unchanged overnight, no fees) · equity $8,053.49 vs prior close $8,053.01 (+0.48) · 9 name(s) re-marked at the open (per-name table). ADMA×3 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×61 yday $0.53 → 09:30 $0.53 +0.00; DLO×2 yday $13.88 → 09:30 $13.88 +0.00; FJET×17 yday $1.80 → 09:30 $1.80 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PGEN×4 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $7,001.10 | — | S≥+5: sizeup + more names; list flatten; ret5=+0.8; leftover $1115.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $5,960.84 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1115.28 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $4,880.49 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.7; leftover $1115.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 145 | $7.65 | $2.42 | — | $3,768.81 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1115.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 42 | $26.27 | $2.12 | — | $2,663.35 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1115.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $1,572.45 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1115.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 123 | $9.05 | $2.36 | — | $456.94 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-27.1; leftover $1115.28 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.94 | ▲ close $8,083.13 vs 09:30 $8,053.49 (session +44.65) | 16:00 close · cash $456.94 · equity $8,083.13 vs 09:30 $8,053.49 (+29.64; session marks +44.65) · 16 name(s) marked open→close (per-name table). ADMA×3 09:30 $9.52 → close $9.52 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×61 09:30 $0.53 → close $0.53 +0.00; DLO×2 09:30 $13.88 → close $13.88 +0.00; FJET×17 09:30 $1.80 → close $1.80 -0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PGEN×4 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×145 09:30 $7.65 → close $7.60 -7.25; WRBY×42 09:30 $26.27 → close $26.71 +18.48; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×123 09:30 $9.05 → close $9.36 +38.13 | — |

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
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 10.32 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 10.32 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 10.32 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 10.32 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 10.32 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 10.32 < 1 share @ 14.80 |
| 2026-08-14 | `WWW` | cash | leftover split 10.32 < 1 share @ 20.60 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 9.75 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 9.75 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 9.75 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.75 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 25.35 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 25.35 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 25.35 < 1 share @ 59.72 |
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
| 2026-08-27 | `RRC` | cash | leftover split 1.59 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 1.59 < 1 share @ 14.42 |
| 2026-08-27 | `SLI` | cash | leftover split 1.59 < 1 share @ 2.60 |
| 2026-08-27 | `KURA` | cash | leftover split 1.59 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 1.59 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `KRMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 33.80 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 33.80 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 33.80 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 33.80 < 1 share @ 34.93 |
| 2026-09-17 | `AXTI` | cash | leftover split 33.80 < 1 share @ 67.91 |
| 2026-09-17 | `SMTC` | cash | leftover split 33.80 < 1 share @ 170.85 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `KRMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 27.12 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 27.12 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 27.12 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 27.12 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 27.12 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 27.12 < 1 share @ 34.44 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 47.60 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 112.18 < 1 share @ 116.85 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| `DEFT` | 82 | 2026-09-22 @ $0.58 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $47.60 |
| `ADMA` | 11 | 2026-09-23 @ $9.81 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+4.0; leftover $112.18 |
| `FTRE` | 5 | 2026-09-23 @ $20.25 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+15.0; leftover $112.18 |
| `OMER` | 5 | 2026-09-23 @ $20.65 | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $112.18 |
