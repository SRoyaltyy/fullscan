# Factor mine action — `flatten_h3_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **-22.46%** ($7,754) · signal-only (no cash/fees) was -10.41%. Starts YES **1/30**. Fills 139 · skips 219 · realized $-1154.28.

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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,402.18.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1111.11 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.82 | ▲ close $10,195.74 vs 09:30 $10,000.00 (session +227.81) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $101.42 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $87.76 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.76 | ▲ close $10,434.38 vs 09:30 $10,219.63 (session +215.18) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.76 | ▼ 09:30 equity $10,410.12 vs yday $10,434.38 (-24.26) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $79.57 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.97 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $71.02 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.97 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $61.20 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.97 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $51.48 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.97 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.48 | ▲ close $10,512.60 vs 09:30 $10,410.12 (session +102.86) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▼ 09:30 equity $10,384.26 vs yday $10,512.60 (-128.34) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 18 | $60.00 | $2.06 | $-0.51 | $1,129.41 | ▼ -0.51 after sell → book $10,382.19; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 24 | $43.56 | $2.08 | $-62.22 | $2,172.77 | ▼ -62.22 after sell → book $10,380.11; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 21 | $51.77 | $2.07 | $+19.96 | $3,257.87 | ▲ +19.96 after sell → book $10,378.04; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 22 | $49.28 | $2.08 | $-13.37 | $4,339.95 | ▼ -13.37 after sell → book $10,375.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 94 | $12.66 | $2.30 | $+85.67 | $5,527.69 | ▲ +85.67 after sell → book $10,373.66; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 37 | $27.85 | $2.12 | $-74.15 | $6,556.02 | ▼ -74.15 after sell → book $10,371.54; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1371 | $1.14 | $17.93 | $+419.29 | $8,101.04 | ▲ +419.29 after sell → book $10,353.62; vs 09:30 mark -17.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 47 | $22.16 | $2.15 | $-59.27 | $9,140.41 | ▼ -59.27 after sell → book $10,351.47; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 50 | $22.82 | $2.16 | $+36.20 | $10,279.25 | ▲ +36.20 after sell → book $10,349.31; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,279.25 | ▼ close $10,348.42 vs 09:30 $10,384.26 (session -0.89) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,279.25 | ▲ 09:30 equity $10,348.99 vs yday $10,348.42 (+0.57) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,288.04 | ▼ -0.31 after sell → book $10,348.87; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 14 | $0.88 | $0.19 | $-1.16 | $10,300.18 | ▼ -1.16 after sell → book $10,348.69; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 9 | $1.42 | $0.17 | $-1.06 | $10,312.78 | ▼ -1.06 after sell → book $10,348.51; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,312.78 | ▲ close $10,348.89 vs 09:30 $10,348.99 (session +0.38) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,312.78 | ▼ 09:30 equity $10,348.53 vs yday $10,348.89 (-0.36) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $10,320.52 | ▼ -0.45 after sell → book $10,348.43; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,328.76 | ▼ -0.30 after sell → book $10,348.32; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 3 | $3.20 | $0.12 | $-0.35 | $10,338.24 | ▼ -0.35 after sell → book $10,348.20; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 2 | $4.98 | $0.13 | $+0.11 | $10,348.07 | ▲ +0.11 after sell → book $10,348.07; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,071.80 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,795.62 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,513.15 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,217.78 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,939.64 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,663.43 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,360.65 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1293.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $202.32 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1293.51 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.32 | ▲ close $10,562.66 vs 09:30 $10,348.53 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.32 | ▲ 09:30 equity $10,838.55 vs yday $10,562.66 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $184.94 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $25.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $162.45 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $25.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $137.48 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $25.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $112.10 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $25.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $86.71 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $25.29 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.71 | ▲ close $10,837.56 vs 09:30 $10,838.55 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.71 | ▲ 09:30 equity $10,949.30 vs yday $10,837.56 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.71 | ▼ close $10,914.54 vs 09:30 $10,949.30 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.71 | ▼ 09:30 equity $10,743.92 vs yday $10,914.54 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,344.35 | ▼ -18.63 after sell → book $10,741.72; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,684.34 | ▲ +63.82 after sell → book $10,739.67; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,951.28 | ▼ -15.53 after sell → book $10,737.47; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,187.07 | ▼ -59.59 after sell → book $10,734.54; vs 09:30 mark -2.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,563.51 | ▲ +98.31 after sell → book $10,732.33; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,951.13 | ▲ +111.41 after sell → book $10,730.19; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,345.56 | ▲ +91.65 after sell → book $10,720.52; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,595.61 | ▲ +91.71 after sell → book $10,718.49; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 74 | $23.77 | $2.21 | — | $8,834.42 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1765.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 160 | $10.98 | $2.47 | — | $7,075.15 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $1765.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 28 | $61.19 | $2.07 | — | $5,359.75 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $1765.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 211 | $8.35 | $2.72 | — | $3,595.18 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $1765.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 357 | $4.94 | $4.61 | — | $1,827.00 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $1765.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $426.97 | $2.00 | — | $117.11 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $1765.93 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.11 | ▲ close $10,802.81 vs 09:30 $10,743.92 (session +100.41) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.11 | ▲ 09:30 equity $10,804.99 vs yday $10,802.81 (+2.18) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $133.52 | ▼ -0.96 after sell → book $10,804.80; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $163.89 | ▲ +7.88 after sell → book $10,804.47; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $187.70 | ▼ -1.17 after sell → book $10,804.18; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $213.77 | ▲ +0.69 after sell → book $10,803.86; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $243.79 | ▲ +4.63 after sell → book $10,803.48; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $243.79 | ▼ close $10,763.44 vs 09:30 $10,804.99 (session -40.04) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $243.79 | ▼ 09:30 equity $10,759.21 vs yday $10,763.44 (-4.23) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $41.44 | $0.42 | — | $201.93 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $81.26 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 5 | $14.42 | $0.74 | — | $129.09 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $81.26 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 31 | $2.60 | $0.90 | — | $47.59 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $81.26 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.59 | ▼ close $10,645.20 vs 09:30 $10,759.21 (session -111.95) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.59 | ▲ 09:30 equity $10,676.89 vs yday $10,645.20 (+31.69) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 160 | $10.97 | $2.51 | $-6.58 | $1,800.28 | ▼ -6.58 after sell → book $10,674.38; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 28 | $60.52 | $2.10 | $-22.93 | $3,492.75 | ▼ -22.93 after sell → book $10,672.29; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 211 | $8.28 | $2.77 | $-20.26 | $5,237.06 | ▼ -20.26 after sell → book $10,669.52; vs 09:30 mark -2.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 357 | $4.95 | $4.68 | $-5.71 | $6,999.53 | ▼ -5.71 after sell → book $10,664.84; vs 09:30 mark -4.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 4 | $423.76 | $2.03 | $-16.87 | $8,692.54 | ▼ -16.87 after sell → book $10,662.81; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,692.54 | ▼ close $10,630.90 vs 09:30 $10,676.89 (session -31.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,692.54 | ▲ 09:30 equity $10,639.54 vs yday $10,630.90 (+8.64) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 74 | $23.68 | $2.24 | $-11.11 | $10,442.62 | ▼ -11.11 after sell → book $10,637.30; vs 09:30 mark -2.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.62 | ▲ close $10,638.86 vs 09:30 $10,639.54 (session +1.56) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.62 | ▲ 09:30 equity $10,646.43 vs yday $10,638.86 (+7.57) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.94 | $0.44 | $-0.36 | $10,484.12 | ▼ -0.36 after sell → book $10,645.99; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 5 | $15.82 | $0.83 | $+5.44 | $10,562.39 | ▲ +5.44 after sell → book $10,645.16; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 31 | $2.67 | $0.94 | $+0.33 | $10,644.22 | ▲ +0.33 after sell → book $10,644.22; vs 09:30 mark -0.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,644.22 | ▲ close $10,644.22 vs 09:30 $10,646.43 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,644.22 | ▲ 09:30 equity $10,644.22 vs yday $10,644.22 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,644.22 | ▲ close $10,644.22 vs 09:30 $10,644.22 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,644.22 | ▲ 09:30 equity $10,644.22 vs yday $10,644.22 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 40 | $52.88 | $2.11 | — | $8,526.91 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2128.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 49 | $42.93 | $2.14 | — | $6,421.21 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2128.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 586 | $3.63 | $7.56 | — | $4,286.47 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2128.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 265 | $8.03 | $3.42 | — | $2,155.10 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2128.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 16 | $132.45 | $2.04 | — | $33.86 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2128.84 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.86 | ▼ close $10,427.46 vs 09:30 $10,644.22 (session -199.50) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.86 | ▼ 09:30 equity $10,352.75 vs yday $10,427.46 (-74.71) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 2 | $2.52 | $0.06 | — | $28.76 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $5.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 2 | $1.90 | $0.04 | — | $24.92 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $5.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 1 | $4.78 | $0.05 | — | $20.09 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $5.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 3 | $1.59 | $0.06 | — | $15.26 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $5.64 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.26 | ▲ close $10,454.48 vs 09:30 $10,352.75 (session +101.94) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.26 | ▲ 09:30 equity $10,512.39 vs yday $10,454.48 (+57.91) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.26 | ▼ close $10,334.40 vs 09:30 $10,512.39 (session -177.99) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.26 | ▼ 09:30 equity $10,274.97 vs yday $10,334.40 (-59.43) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 40 | $53.16 | $2.14 | $+6.95 | $2,139.53 | ▲ +6.95 after sell → book $10,272.84; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 49 | $42.01 | $2.16 | $-49.38 | $4,195.85 | ▼ -49.38 after sell → book $10,270.67; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 586 | $3.28 | $7.67 | $-220.33 | $6,110.26 | ▼ -220.33 after sell → book $10,263.00; vs 09:30 mark -7.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 265 | $8.01 | $3.48 | $-12.20 | $8,229.43 | ▼ -12.20 after sell → book $10,259.52; vs 09:30 mark -3.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 16 | $125.77 | $2.06 | $-110.98 | $10,239.69 | ▼ -110.98 after sell → book $10,257.46; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,239.69 | ▼ close $10,256.55 vs 09:30 $10,274.97 (session -0.91) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,239.69 | ▼ 09:30 equity $10,256.30 vs yday $10,256.55 (-0.25) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 2 | $2.22 | $0.07 | $-0.73 | $10,244.06 | ▼ -0.73 after sell → book $10,256.23; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 2 | $1.83 | $0.06 | $-0.25 | $10,247.65 | ▼ -0.25 after sell → book $10,256.17; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 1 | $3.92 | $0.06 | $-0.97 | $10,251.51 | ▼ -0.97 after sell → book $10,256.10; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 3 | $1.53 | $0.07 | $-0.31 | $10,256.03 | ▼ -0.31 after sell → book $10,256.03; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,256.03 | ▲ close $10,256.03 vs 09:30 $10,256.30 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,256.03 | ▲ 09:30 equity $10,256.03 vs yday $10,256.03 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 104 | $16.28 | $2.30 | — | $8,560.61 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1709.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 626 | $2.73 | $8.08 | — | $6,843.55 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1709.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,186.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1709.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $3,540.50 | — | S≥+5: sizeup + more names; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1709.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 10 | $157.78 | $2.02 | — | $1,960.68 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1709.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 30 | $56.09 | $2.08 | — | $275.90 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1709.34 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $275.90 | ▼ close $10,201.24 vs 09:30 $10,256.03 (session -36.28) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $275.90 | ▼ 09:30 equity $9,800.75 vs yday $10,201.24 (-400.49) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $275.90 | ▼ close $9,679.50 vs 09:30 $9,800.75 (session -121.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $275.90 | ▲ 09:30 equity $9,749.74 vs yday $9,679.50 (+70.24) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $275.90 | ▼ close $9,553.36 vs 09:30 $9,749.74 (session -196.38) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $275.90 | ▲ 09:30 equity $9,634.88 vs yday $9,553.36 (+81.52) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 104 | $16.16 | $2.33 | $-17.11 | $1,954.20 | ▼ -17.11 after sell → book $9,632.54; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 626 | $2.72 | $8.19 | $-22.53 | $3,648.73 | ▼ -22.53 after sell → book $9,624.35; vs 09:30 mark -8.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 8 | $194.84 | $2.04 | $-100.05 | $5,205.42 | ▼ -100.05 after sell → book $9,622.32; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 10 | $140.03 | $2.04 | $-248.06 | $6,603.67 | ▼ -248.06 after sell → book $9,620.27; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 10 | $147.79 | $2.04 | $-103.96 | $8,079.53 | ▼ -103.96 after sell → book $9,618.23; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 30 | $51.29 | $2.10 | $-148.18 | $9,616.13 | ▼ -148.18 after sell → book $9,616.13; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 8 | $270.89 | $2.01 | — | $7,447.00 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $2404.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 31 | $77.12 | $2.08 | — | $5,054.19 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $2404.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 167 | $14.31 | $2.49 | — | $2,661.93 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $2404.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 65 | $36.46 | $2.19 | — | $289.85 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $2404.03 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $289.85 | ▼ close $9,518.16 vs 09:30 $9,634.88 (session -89.20) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $289.85 | ▲ 09:30 equity $9,621.35 vs yday $9,518.16 (+103.19) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 4 | $10.25 | $0.42 | — | $248.42 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $48.31 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 6 | $7.59 | $0.47 | — | $202.41 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $48.31 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $167.13 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; leftover $48.31 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.13 | ▼ close $9,583.92 vs 09:30 $9,621.35 (session -36.18) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.13 | ▼ 09:30 equity $9,561.82 vs yday $9,583.92 (-22.10) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.13 | ▼ close $9,444.17 vs 09:30 $9,561.82 (session -117.65) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.13 | ▲ 09:30 equity $9,453.86 vs yday $9,444.17 (+9.69) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 8 | $266.76 | $2.04 | $-37.10 | $2,299.17 | ▼ -37.10 after sell → book $9,451.82; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 31 | $76.27 | $2.11 | $-30.54 | $4,661.43 | ▼ -30.54 after sell → book $9,449.71; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 167 | $13.65 | $2.54 | $-115.25 | $6,938.44 | ▼ -115.25 after sell → book $9,447.17; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 65 | $36.70 | $2.21 | $+11.20 | $9,321.72 | ▲ +11.20 after sell → book $9,444.95; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 11 | $157.87 | $2.02 | — | $7,583.13 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $1864.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 4 | $386.20 | $2.00 | — | $6,036.33 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; leftover $1864.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 20 | $88.83 | $2.05 | — | $4,257.68 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $1864.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 138 | $13.47 | $2.40 | — | $2,396.42 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $1864.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 466 | $4.00 | $6.01 | — | $526.40 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $1864.34 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $526.40 | ▼ close $9,116.10 vs 09:30 $9,453.86 (session -314.36) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $526.40 | ▲ 09:30 equity $9,167.32 vs yday $9,116.10 (+51.22) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 4 | $10.18 | $0.44 | $-1.14 | $566.68 | ▼ -1.14 after sell → book $9,166.88; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 1 | $93.97 | $0.94 | — | $471.77 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $94.45 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $471.77 | ▲ close $9,268.42 vs 09:30 $9,167.32 (session +102.48) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $471.77 | ▼ 09:30 equity $9,222.16 vs yday $9,268.42 (-46.26) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 1 | $34.78 | $0.37 | $-0.87 | $506.18 | ▼ -0.87 after sell → book $9,221.79; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 3 | $27.79 | $0.84 | — | $421.97 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $101.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 10 | $9.81 | $1.01 | — | $322.86 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $101.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 4 | $20.25 | $0.82 | — | $241.04 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $101.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 4 | $20.65 | $0.84 | — | $157.60 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $101.24 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.60 | ▼ close $9,009.35 vs 09:30 $9,222.16 (session -208.93) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.60 | ▼ 09:30 equity $8,846.60 vs yday $9,009.35 (-162.75) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 6 | $7.38 | $0.48 | $-2.21 | $201.40 | ▼ -2.21 after sell → book $8,846.12; vs 09:30 mark -0.48 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 11 | $163.95 | $2.05 | $+62.81 | $2,002.80 | ▲ +62.81 after sell → book $8,844.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 4 | $374.54 | $2.02 | $-50.67 | $3,498.94 | ▼ -50.67 after sell → book $8,842.05; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 20 | $87.67 | $2.07 | $-27.22 | $5,250.36 | ▼ -27.22 after sell → book $8,839.97; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 138 | $11.42 | $2.44 | $-287.74 | $6,823.88 | ▼ -287.74 after sell → book $8,837.53; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 466 | $3.40 | $6.10 | $-291.71 | $8,402.18 | ▼ -291.71 after sell → book $8,831.43; vs 09:30 mark -6.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,402.18 | ▼ close $8,830.61 vs 09:30 $8,846.60 (session -0.82) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,553.35 | ▲ 09:30 equity $7,832.29 vs yday $7,831.81 (+0.48) | 09:30 open · cash $7,553.35 (unchanged overnight, no fees) · equity $7,832.29 vs prior close $7,831.81 (+0.48) · 9 name(s) re-marked at the open (per-name table). ADMA×3 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DLO×3 yday $13.88 → 09:30 $13.88 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×3 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 2 | $803.87 | $2.00 | — | $5,943.61 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.8; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 16 | $115.36 | $2.04 | — | $4,095.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.1; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 49 | $38.51 | $2.14 | — | $2,206.69 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 246 | $7.65 | $3.17 | — | $321.62 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.62 | ▼ close $7,754.12 vs 09:30 $7,832.29 (session -68.83) | 16:00 close · cash $321.62 · equity $7,754.12 vs 09:30 $7,832.29 (-78.17; session marks -68.83) · 13 name(s) marked open→close (per-name table). ADMA×3 09:30 $9.52 → close $9.52 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DLO×3 09:30 $13.88 → close $13.88 +0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×3 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; REGN×2 09:30 $803.87 → close $788.04 -31.66; HALO×16 09:30 $115.36 → close $113.90 -23.36; BLFS×49 09:30 $38.51 → close $38.49 -0.98; MRVI×246 09:30 $7.65 → close $7.60 -12.30 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 13.76 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 13.76 < 1 share @ 14.80 |
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
| 2026-08-17 | `DVN` | cash | leftover split 10.97 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 10.97 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.97 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.97 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 25.29 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 25.29 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 25.29 < 1 share @ 59.72 |
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
| 2026-09-04 | `BHC` | cash | leftover split 5.64 < 1 share @ 6.71 |
| 2026-09-04 | `VIR` | cash | leftover split 5.64 < 1 share @ 11.31 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `ILMN` | cash | leftover split 48.31 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 48.31 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 48.31 < 1 share @ 147.61 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 27.85 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 27.85 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 27.85 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 27.85 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 27.85 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 27.85 < 1 share @ 34.44 |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 101.24 < 1 share @ 116.85 |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| `USFD` | 1 | 2026-09-22 @ $93.97 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $94.45 |
| `ARQT` | 3 | 2026-09-23 @ $27.79 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $101.24 |
| `ADMA` | 10 | 2026-09-23 @ $9.81 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $101.24 |
| `FTRE` | 4 | 2026-09-23 @ $20.25 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $101.24 |
| `OMER` | 4 | 2026-09-23 @ $20.65 | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $101.24 |
