# Factor mine action — `union_macd_xup_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_xup, no 🚨

Cash book **-1.94%** ($9,806) · signal-only (no cash/fees) was -19.02%. Starts YES **0/30**. Fills 174 · skips 71 · realized $-1788.82.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: MACD histogram just crossed from ≤0 to >0 on the last finished bar.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `macd_cross_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,211.16.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 1638 | $6.09 | $21.13 | — | $3.45 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $10000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.45 | ▼ close $9,552.99 vs 09:30 $10,000.00 (session -425.88) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.45 | ▲ 09:30 equity $9,815.07 vs yday $9,552.99 (+262.08) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 1638 | $5.99 | $21.48 | $-206.41 | $9,793.59 | ▼ -206.41 after sell → book $9,793.59; vs 09:30 mark -21.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 55 | $177.51 | $2.15 | — | $28.38 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $9793.59 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.38 | ▼ close $9,075.88 vs 09:30 $9,815.07 (session -715.55) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.38 | ▲ 09:30 equity $9,163.88 vs yday $9,075.88 (+88.00) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 55 | $166.10 | $2.24 | $-631.94 | $9,161.65 | ▼ -631.94 after sell → book $9,161.65; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,161.65 | ▲ close $9,161.65 vs 09:30 $9,163.88 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,161.65 | ▲ 09:30 equity $9,161.65 vs yday $9,161.65 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,161.65 | ▲ close $9,161.65 vs 09:30 $9,161.65 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,161.65 | ▲ 09:30 equity $9,161.65 vs yday $9,161.65 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 16 | $109.06 | $2.04 | — | $7,414.65 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1832.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 2591 | $0.71 | $26.09 | — | $5,556.72 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1832.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 18 | $97.43 | $2.04 | — | $3,800.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1832.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 242 | $7.55 | $3.12 | — | $1,970.71 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1832.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `BMNR` | 85 | $21.46 | $2.25 | — | $144.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+13.1; leftover $1832.33 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.37 | ▼ close $9,084.01 vs 09:30 $9,161.65 (session -42.10) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.37 | ▲ 09:30 equity $9,202.71 vs yday $9,084.01 (+118.70) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 16 | $110.92 | $2.06 | $+25.66 | $1,917.03 | ▲ +25.66 after sell → book $9,200.65; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 2591 | $0.67 | $25.68 | $-137.27 | $3,637.68 | ▼ -137.27 after sell → book $9,174.97; vs 09:30 mark -25.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 18 | $96.75 | $2.07 | $-16.35 | $5,377.11 | ▼ -16.35 after sell → book $9,172.90; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 242 | $7.87 | $3.18 | $+71.14 | $7,278.48 | ▲ +71.14 after sell → book $9,169.73; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BMNR` | 85 | $22.25 | $2.27 | $+62.63 | $9,167.45 | ▲ +62.63 after sell → book $9,167.45; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 14 | $127.43 | $2.03 | — | $7,381.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1833.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 1319 | $1.39 | $17.02 | — | $5,530.97 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1833.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 221 | $8.28 | $2.85 | — | $3,698.24 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1833.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 156 | $11.70 | $2.46 | — | $1,870.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1833.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 8 | $212.40 | $2.01 | — | $169.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1833.49 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.37 | ▼ close $9,106.48 vs 09:30 $9,202.71 (session -34.60) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.37 | ▼ 09:30 equity $8,993.54 vs yday $9,106.48 (-112.94) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 14 | $129.99 | $2.06 | $+31.75 | $1,987.17 | ▲ +31.75 after sell → book $8,991.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 1319 | $1.24 | $17.25 | $-232.11 | $3,605.49 | ▼ -232.11 after sell → book $8,974.24; vs 09:30 mark -17.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 221 | $8.59 | $2.90 | $+62.76 | $5,500.97 | ▲ +62.76 after sell → book $8,971.33; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 156 | $11.17 | $2.50 | $-87.64 | $7,241.00 | ▼ -87.64 after sell → book $8,968.84; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 8 | $215.98 | $2.04 | $+24.59 | $8,966.80 | ▲ +24.59 after sell → book $8,966.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,966.80 | ▲ close $8,966.80 vs 09:30 $8,993.54 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,966.80 | ▲ 09:30 equity $8,966.80 vs yday $8,966.80 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 703 | $6.37 | $9.07 | — | $4,479.62 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $4483.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 102 | $43.76 | $2.30 | — | $13.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $4483.40 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.80 | ▲ close $9,036.56 vs 09:30 $8,966.80 (session +81.13) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.80 | ▼ 09:30 equity $8,844.85 vs yday $9,036.56 (-191.71) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 703 | $6.13 | $9.22 | $-187.01 | $4,313.97 | ▼ -187.01 after sell → book $8,835.63; vs 09:30 mark -9.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 102 | $44.33 | $2.35 | $+53.49 | $8,833.29 | ▲ +53.49 after sell → book $8,833.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 94 | $31.21 | $2.27 | — | $5,897.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $2944.43 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 264 | $11.12 | $3.41 | — | $2,958.19 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2944.43 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 13 | $213.94 | $2.03 | — | $174.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $2944.43 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.94 | ▲ close $8,863.95 vs 09:30 $8,844.85 (session +38.37) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.94 | ▲ 09:30 equity $9,062.78 vs yday $8,863.95 (+198.83) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 94 | $30.79 | $2.31 | $-44.06 | $3,066.89 | ▼ -44.06 after sell → book $9,060.47; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 264 | $11.52 | $3.47 | $+98.72 | $6,104.69 | ▲ +98.72 after sell → book $9,056.99; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 13 | $227.10 | $2.06 | $+166.99 | $9,054.93 | ▲ +166.99 after sell → book $9,054.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 69 | $18.50 | $2.20 | — | $7,776.23 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $1293.56 | — |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 144 | $8.95 | $2.42 | — | $6,485.01 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $1293.56 | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 62 | $20.75 | $2.18 | — | $5,196.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot,overnight; ret5=+5.2; leftover $1293.56 | — |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 74 | $17.27 | $2.21 | — | $3,916.14 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+5.5; leftover $1293.56 | — |
| 2026-08-27 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $2,718.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; 🔵; ret5=+12.3; leftover $1293.56 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $29.83 | $2.12 | — | $1,434.12 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+7.6; leftover $1293.56 | — |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 56 | $22.93 | $2.16 | — | $147.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+9.5; leftover $1293.56 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.88 | ▼ close $8,989.34 vs 09:30 $9,062.78 (session -50.29) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.88 | ▲ 09:30 equity $9,253.51 vs yday $8,989.34 (+264.17) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 69 | $18.15 | $2.22 | $-28.57 | $1,398.01 | ▼ -28.57 after sell → book $9,251.29; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `VYX` | 144 | $9.13 | $2.46 | $+21.04 | $2,710.28 | ▲ +21.04 after sell → book $9,248.84; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 62 | $24.69 | $2.20 | $+239.91 | $4,238.86 | ▲ +239.91 after sell → book $9,246.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 74 | $17.06 | $2.23 | $-19.99 | $5,499.07 | ▼ -19.99 after sell → book $9,244.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SMTC` | 8 | $141.76 | $2.03 | $-65.17 | $6,631.11 | ▼ -65.17 after sell → book $9,242.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $30.50 | $2.14 | $+24.55 | $7,940.47 | ▲ +24.55 after sell → book $9,240.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 56 | $23.21 | $2.18 | $+11.34 | $9,238.05 | ▲ +11.34 after sell → book $9,238.05; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 830 | $1.39 | $10.71 | — | $8,073.65 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $122.81 | $2.02 | — | $6,966.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 59 | $19.25 | $2.17 | — | $5,828.42 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+14.1; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 62 | $18.36 | $2.18 | — | $4,687.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+12.8; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `FTNT` | 6 | $172.58 | $2.01 | — | $3,650.44 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.6; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,603.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+7.8; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 11 | $98.95 | $2.02 | — | $1,513.32 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+9.7; leftover $1154.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $427.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+4.8; leftover $1154.76 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $427.33 | ▼ close $8,895.74 vs 09:30 $9,253.51 (session -317.22) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $427.33 | ▼ 09:30 equity $8,825.64 vs yday $8,895.74 (-70.10) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 830 | $1.30 | $10.85 | $-96.26 | $1,495.47 | ▼ -96.26 after sell → book $8,814.78; vs 09:30 mark -10.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 9 | $118.83 | $2.04 | $-39.87 | $2,562.91 | ▼ -39.87 after sell → book $8,812.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 59 | $17.87 | $2.19 | $-85.77 | $3,615.05 | ▼ -85.77 after sell → book $8,810.56; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 62 | $17.77 | $2.20 | $-40.95 | $4,714.59 | ▼ -40.95 after sell → book $8,808.36; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FTNT` | 6 | $166.60 | $2.03 | $-39.92 | $5,712.16 | ▼ -39.92 after sell → book $8,806.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $6,740.98 | ▼ -17.82 after sell → book $8,804.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 11 | $92.83 | $2.04 | $-71.39 | $7,760.07 | ▼ -71.39 after sell → book $8,802.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $8,800.25 | ▼ -45.81 after sell → book $8,800.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,800.25 | ▲ close $8,800.25 vs 09:30 $8,825.64 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,800.25 | ▲ 09:30 equity $8,800.25 vs yday $8,800.25 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,800.25 | ▲ close $8,800.25 vs 09:30 $8,800.25 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,800.25 | ▲ 09:30 equity $8,800.25 vs yday $8,800.25 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,800.25 | ▲ close $8,800.25 vs 09:30 $8,800.25 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,800.25 | ▲ 09:30 equity $8,800.25 vs yday $8,800.25 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $7,370.42 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1466.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 182 | $8.03 | $2.54 | — | $5,906.43 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1466.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 395 | $3.71 | $5.10 | — | $4,435.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+12.3; leftover $1466.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `MLYS` | 50 | $29.15 | $2.14 | — | $2,976.24 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $1466.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 30 | $47.74 | $2.08 | — | $1,541.96 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+15.1; leftover $1466.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 219 | $6.68 | $2.83 | — | $76.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.4; leftover $1466.71 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.22 | ▼ close $8,634.94 vs 09:30 $8,800.25 (session -148.57) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.22 | ▼ 09:30 equity $8,550.66 vs yday $8,634.94 (-84.28) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 27 | $52.03 | $2.09 | $-27.11 | $1,478.93 | ▼ -27.11 after sell → book $8,548.56; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 182 | $7.91 | $2.58 | $-26.95 | $2,915.98 | ▼ -26.95 after sell → book $8,545.99; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 395 | $3.53 | $5.17 | $-81.37 | $4,305.15 | ▼ -81.37 after sell → book $8,540.81; vs 09:30 mark -5.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MLYS` | 50 | $28.00 | $2.16 | $-61.80 | $5,702.99 | ▼ -61.80 after sell → book $8,538.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 30 | $44.59 | $2.10 | $-98.68 | $7,038.59 | ▼ -98.68 after sell → book $8,536.55; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 219 | $6.84 | $2.87 | $+29.34 | $8,533.68 | ▲ +29.34 after sell → book $8,533.68; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 484 | $3.52 | $6.24 | — | $6,823.75 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1706.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $513.78 | $2.00 | — | $5,280.42 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1706.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 365 | $4.67 | $4.71 | — | $3,571.16 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+11.9; leftover $1706.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `RNG` | 22 | $75.35 | $2.06 | — | $1,911.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+11.4; leftover $1706.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 17 | $98.15 | $2.04 | — | $240.81 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+5.9; leftover $1706.74 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.81 | ▲ close $8,720.65 vs 09:30 $8,550.66 (session +204.02) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.81 | ▲ 09:30 equity $8,764.57 vs yday $8,720.65 (+43.92) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 484 | $3.99 | $6.34 | $+214.90 | $2,165.63 | ▲ +214.90 after sell → book $8,758.23; vs 09:30 mark -6.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 3 | $521.15 | $2.02 | $+18.09 | $3,727.06 | ▲ +18.09 after sell → book $8,756.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 365 | $4.75 | $4.78 | $+19.71 | $5,456.03 | ▲ +19.71 after sell → book $8,751.43; vs 09:30 mark -4.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RNG` | 22 | $72.07 | $2.08 | $-76.29 | $7,039.49 | ▼ -76.29 after sell → book $8,749.35; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 17 | $100.58 | $2.06 | $+37.20 | $8,747.28 | ▲ +37.20 after sell → book $8,747.28; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,747.28 | ▲ close $8,747.28 vs 09:30 $8,764.57 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.28 | ▲ 09:30 equity $8,747.28 vs yday $8,747.28 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,747.28 | ▲ close $8,747.28 vs 09:30 $8,747.28 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.28 | ▲ 09:30 equity $8,747.28 vs yday $8,747.28 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,747.28 | ▲ close $8,747.28 vs 09:30 $8,747.28 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.28 | ▲ 09:30 equity $8,747.28 vs yday $8,747.28 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 493 | $5.91 | $6.36 | — | $5,827.29 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $2915.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 245 | $11.88 | $3.16 | — | $2,913.53 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+20.7; leftover $2915.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 41 | $69.88 | $2.11 | — | $46.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.0; leftover $2915.76 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.34 | ▲ close $8,831.63 vs 09:30 $8,747.28 (session +95.98) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.34 | ▼ 09:30 equity $8,771.81 vs yday $8,831.63 (-59.82) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 493 | $5.86 | $6.46 | $-37.47 | $2,928.86 | ▼ -37.47 after sell → book $8,765.35; vs 09:30 mark -6.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `APPS` | 245 | $11.75 | $3.22 | $-38.23 | $5,804.38 | ▼ -38.23 after sell → book $8,762.12; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 41 | $72.14 | $2.15 | $+88.40 | $8,759.97 | ▲ +88.40 after sell → book $8,759.97; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,759.97 | ▲ close $8,759.97 vs 09:30 $8,771.81 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,759.97 | ▲ 09:30 equity $8,759.97 vs yday $8,759.97 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,759.97 | ▲ close $8,759.97 vs 09:30 $8,759.97 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,759.97 | ▲ 09:30 equity $8,759.97 vs yday $8,759.97 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 608 | $1.80 | $7.84 | — | $7,657.73 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 55 | $19.75 | $2.15 | — | $6,569.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 47 | $23.29 | $2.13 | — | $5,472.57 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 7 | $140.88 | $2.01 | — | $4,484.39 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `TENB` | 29 | $36.86 | $2.08 | — | $3,413.38 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+13.0; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 12 | $87.52 | $2.03 | — | $2,361.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+4.3; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `RBRK` | 10 | $101.97 | $2.02 | — | $1,339.39 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.0; leftover $1095.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `DOCU` | 15 | $70.60 | $2.04 | — | $278.36 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+10.4; leftover $1095.00 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $278.36 | ▲ close $8,987.62 vs 09:30 $8,759.97 (session +249.94) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $278.36 | ▲ 09:30 equity $9,012.76 vs yday $8,987.62 (+25.14) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `HLP` | 608 | $2.10 | $7.95 | $+166.60 | $1,547.20 | ▲ +166.60 after sell → book $9,004.80; vs 09:30 mark -7.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 55 | $20.31 | $2.17 | $+26.47 | $2,662.08 | ▲ +26.47 after sell → book $9,002.63; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 47 | $24.09 | $2.15 | $+33.32 | $3,792.16 | ▲ +33.32 after sell → book $9,000.48; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 7 | $147.61 | $2.03 | $+43.07 | $4,823.40 | ▲ +43.07 after sell → book $8,998.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TENB` | 29 | $35.89 | $2.10 | $-32.30 | $5,862.11 | ▼ -32.30 after sell → book $8,996.35; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 12 | $89.27 | $2.05 | $+16.93 | $6,931.30 | ▲ +16.93 after sell → book $8,994.30; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RBRK` | 10 | $102.56 | $2.04 | $+1.84 | $7,954.86 | ▲ +1.84 after sell → book $8,992.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DOCU` | 15 | $69.16 | $2.06 | $-25.69 | $8,990.21 | ▼ -25.69 after sell → book $8,990.21; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 175 | $10.25 | $2.52 | — | $7,193.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1798.04 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 51 | $34.93 | $2.14 | — | $5,410.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; ret5=+1.6; leftover $1798.04 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 749 | $2.40 | $9.66 | — | $3,603.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1798.04 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 29 | $61.90 | $2.08 | — | $1,805.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1798.04 | — |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 63 | $28.23 | $2.18 | — | $25.26 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.3; leftover $1798.04 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.26 | ▼ close $8,905.59 vs 09:30 $9,012.76 (session -66.04) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.26 | ▲ 09:30 equity $8,908.37 vs yday $8,905.59 (+2.78) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 175 | $10.12 | $2.56 | $-27.82 | $1,793.70 | ▼ -27.82 after sell → book $8,905.81; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 51 | $34.52 | $2.17 | $-25.22 | $3,552.06 | ▼ -25.22 after sell → book $8,903.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 749 | $2.29 | $9.80 | $-101.85 | $5,257.47 | ▼ -101.85 after sell → book $8,893.85; vs 09:30 mark -9.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 29 | $63.37 | $2.10 | $+38.45 | $7,093.09 | ▲ +38.45 after sell → book $8,891.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 63 | $28.55 | $2.20 | $+15.78 | $8,889.54 | ▲ +15.78 after sell → book $8,889.54; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $7,839.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 745 | $1.49 | $9.61 | — | $6,720.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 13 | $81.40 | $2.03 | — | $5,660.05 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `FCEL` | 62 | $17.80 | $2.18 | — | $4,554.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+13.6; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 139 | $7.98 | $2.41 | — | $3,442.64 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 202 | $5.49 | $2.61 | — | $2,332.07 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+17.2; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `LTRX` | 188 | $5.89 | $2.55 | — | $1,222.19 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+15.0; leftover $1111.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `VOYG` | 29 | $37.16 | $2.08 | — | $142.48 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.0; leftover $1111.19 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.48 | ▼ close $8,782.44 vs 09:30 $8,908.37 (session -81.64) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.48 | ▲ 09:30 equity $8,892.92 vs yday $8,782.44 (+110.48) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $1,190.45 | ▼ -1.63 after sell → book $8,890.89; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `LVWR` | 745 | $1.65 | $9.74 | $+99.85 | $2,409.96 | ▲ +99.85 after sell → book $8,881.15; vs 09:30 mark -9.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 13 | $79.08 | $2.05 | $-34.24 | $3,435.95 | ▼ -34.24 after sell → book $8,879.10; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FCEL` | 62 | $18.33 | $2.20 | $+28.49 | $4,570.21 | ▲ +28.49 after sell → book $8,876.90; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 139 | $7.84 | $2.44 | $-24.31 | $5,657.53 | ▼ -24.31 after sell → book $8,874.46; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SATL` | 202 | $5.18 | $2.65 | $-66.87 | $6,701.24 | ▼ -66.87 after sell → book $8,871.81; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `LTRX` | 188 | $5.96 | $2.60 | $+8.01 | $7,819.13 | ▲ +8.01 after sell → book $8,869.22; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VOYG` | 29 | $36.21 | $2.10 | $-31.72 | $8,867.12 | ▼ -31.72 after sell → book $8,867.12; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $7,760.02 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; ret5=+6.5; leftover $1108.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 607 | $1.82 | $7.83 | — | $6,644.41 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1108.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 192 | $5.75 | $2.57 | — | $5,536.89 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1108.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 6 | $164.58 | $2.01 | — | $4,547.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1108.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 515 | $2.15 | $6.64 | — | $3,433.51 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1108.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 103 | $10.71 | $2.30 | — | $2,328.08 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1108.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 35 | $31.64 | $2.10 | — | $1,218.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.9; leftover $1108.39 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,218.58 | ▼ close $8,816.78 vs 09:30 $8,892.92 (session -24.88) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,218.58 | ▼ 09:30 equity $8,772.34 vs yday $8,816.78 (-44.44) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 607 | $1.79 | $7.94 | $-33.98 | $2,300.21 | ▼ -33.98 after sell → book $8,764.40; vs 09:30 mark -7.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 192 | $6.05 | $2.61 | $+52.43 | $3,460.16 | ▲ +52.43 after sell → book $8,761.79; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `MSTR` | 6 | $167.55 | $2.03 | $+13.78 | $4,463.43 | ▲ +13.78 after sell → book $8,759.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ASST` | 35 | $29.30 | $2.12 | $-86.11 | $5,486.81 | ▼ -86.11 after sell → book $8,757.64; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 75 | $9.11 | $2.21 | — | $4,801.35 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $685.85 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 679 | $1.01 | $8.76 | — | $4,106.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $685.85 | — |
| 2026-09-22 09:30 ET | **BUY** | `MX` | 215 | $3.18 | $2.77 | — | $3,420.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $685.85 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,420.33 | ▼ close $8,674.21 vs 09:30 $8,772.34 (session -69.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,420.33 | ▼ 09:30 equity $8,663.94 vs yday $8,674.21 (-10.27) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 7 | $166.54 | $2.03 | $+56.65 | $4,584.08 | ▲ +56.65 after sell → book $8,661.91; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 515 | $2.09 | $6.74 | $-44.28 | $5,653.69 | ▼ -44.28 after sell → book $8,655.17; vs 09:30 mark -6.74 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 103 | $10.11 | $2.33 | $-66.43 | $6,692.69 | ▼ -66.43 after sell → book $8,652.84; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 75 | $8.39 | $2.24 | $-58.45 | $7,319.70 | ▼ -58.45 after sell → book $8,650.60; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 679 | $0.95 | $8.61 | $-58.11 | $7,956.14 | ▼ -58.11 after sell → book $8,641.99; vs 09:30 mark -8.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MX` | 215 | $3.19 | $2.82 | $-3.44 | $8,639.17 | ▼ -3.44 after sell → book $8,639.17; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 25 | $41.76 | $2.06 | — | $7,593.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1079.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `THM` | 377 | $2.86 | $4.86 | — | $6,510.03 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+25.5; leftover $1079.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `FWDI` | 131 | $8.20 | $2.38 | — | $5,433.44 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+17.6; leftover $1079.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `FIVN` | 27 | $38.91 | $2.07 | — | $4,380.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.6; leftover $1079.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `XXI` | 155 | $6.93 | $2.46 | — | $3,304.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.2; leftover $1079.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `CNTN` | 442 | $2.44 | $5.70 | — | $2,220.15 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $1079.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 26 | $40.00 | $2.07 | — | $1,178.08 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+6.7; leftover $1079.90 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,178.08 | ▼ close $8,438.86 vs 09:30 $8,663.94 (session -178.71) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,178.08 | ▼ 09:30 equity $8,233.05 vs yday $8,438.86 (-205.81) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 25 | $36.02 | $2.08 | $-147.53 | $2,076.62 | ▼ -147.53 after sell → book $8,230.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `THM` | 377 | $2.79 | $4.94 | $-36.19 | $3,123.52 | ▼ -36.19 after sell → book $8,226.03; vs 09:30 mark -4.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FWDI` | 131 | $7.94 | $2.41 | $-38.86 | $4,161.24 | ▼ -38.86 after sell → book $8,223.61; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FIVN` | 27 | $37.51 | $2.09 | $-41.83 | $5,171.92 | ▼ -41.83 after sell → book $8,221.52; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `XXI` | 155 | $6.70 | $2.49 | $-40.60 | $6,207.93 | ▼ -40.60 after sell → book $8,219.03; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CNTN` | 442 | $2.24 | $5.79 | $-99.89 | $7,192.22 | ▼ -99.89 after sell → book $8,213.24; vs 09:30 mark -5.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 26 | $39.27 | $2.09 | $-23.14 | $8,211.16 | ▼ -23.14 after sell → book $8,211.16; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,211.16 | ▲ close $8,211.16 vs 09:30 $8,233.05 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,805.94 | ▲ 09:30 equity $9,805.94 vs yday $9,805.94 (+0.00) | 09:30 open · cash $9,805.94 · no holdings · equity $9,805.94 vs prior close $9,805.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 79 | $20.61 | $2.23 | — | $8,175.52 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ret5=+9.1; leftover $1634.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 213 | $7.65 | $2.75 | — | $6,543.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1634.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 62 | $26.27 | $2.18 | — | $4,912.41 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1634.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 54 | $29.80 | $2.15 | — | $3,301.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $1634.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CBRL` | 31 | $52.39 | $2.08 | — | $1,674.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1634.32 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 158 | $10.28 | $2.46 | — | $48.18 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ⚪; ret5=+9.7; leftover $1634.32 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.18 | ▲ close $9,806.15 vs 09:30 $9,805.94 (session +14.06) | 16:00 close · cash $48.18 · equity $9,806.15 vs 09:30 $9,805.94 (+0.21; session marks +14.06) · 6 name(s) marked open→close (per-name table). OMER×79 09:30 $20.61 → close $20.08 -41.87; MRVI×213 09:30 $7.65 → close $7.60 -10.65; WRBY×62 09:30 $26.27 → close $26.71 +27.28; QMCO×54 09:30 $29.80 → close $31.68 +101.52; CBRL×31 09:30 $52.39 → close $51.81 -17.98; SENS×158 09:30 $10.28 → close $10.00 -44.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `WFRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `EYPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VSAT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ARQQ` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `WCC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1108.39 < 1 share @ 1826.00 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `AIBZ` | no_price | no 09:30 open |
| 2026-09-23 | `MPWR` | cash | leftover split 1079.90 < 1 share @ 1367.08 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `MDB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SNPS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
