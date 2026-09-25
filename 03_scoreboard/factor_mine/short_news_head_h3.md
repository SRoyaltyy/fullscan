# Factor mine action — `short_news_head_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short prior-export headline🔴

Cash book **-9.19%** ($9,081) · signal-only (no cash/fees) was +2.90%. Starts YES **2/30**. Fills 79 · skips 94 · realized $+602.64.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior-export headline is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `headline=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $26,382.61.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 862 | $1.15 | $11.30 | — | $15,934.53 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ⚪; ret5=-12.2; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 278 | $3.56 | $3.67 | — | $16,920.54 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-15.6; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $17,901.11 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+17.6; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 329 | $3.01 | $4.34 | — | $18,887.07 | — | short prior-export headline🔴; gate headline=bad; list earn_react; ⚪; ret5=-5.3; leftover $991.68 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 145 | $6.80 | $2.49 | — | $19,870.58 | — | short prior-export headline🔴; gate headline=bad; list overnight; ⚪; ret5=+10.4; leftover $991.68 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,870.58 | ▲ close $10,021.64 vs 09:30 $9,916.79 (session +128.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,870.58 | ▲ 09:30 equity $10,172.48 vs yday $10,021.64 (+150.84) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,870.58 | ▲ close $10,410.43 vs 09:30 $10,172.48 (session +237.95) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,870.58 | ▼ 09:30 equity $10,378.48 vs yday $10,410.43 (-31.95) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,341.53 | ▲ +118.60 after sell → book $10,360.27; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,799.89 | ▲ +118.95 after sell → book $10,357.88; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,799.89 | ▲ close $10,405.81 vs 09:30 $10,378.48 (session +47.93) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,799.89 | ▼ 09:30 equity $10,348.47 vs yday $10,405.81 (-57.34) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,238.47 | ▲ +84.87 after sell → book $10,346.22; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 862 | $0.96 | $10.89 | $+139.01 | $14,397.47 | ▲ +139.01 after sell → book $10,335.33; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 278 | $4.01 | $3.59 | $-133.75 | $13,277.72 | ▼ -133.75 after sell → book $10,331.75; vs 09:30 mark -3.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,287.66 | ▼ -9.48 after sell → book $10,329.66; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 329 | $2.95 | $4.24 | $+11.16 | $11,312.87 | ▲ +11.16 after sell → book $10,325.42; vs 09:30 mark -4.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 145 | $6.81 | $2.42 | $-6.36 | $10,322.99 | ▼ -6.36 after sell → book $10,322.99; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 48 | $21.40 | $2.18 | — | $11,348.01 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-25.2; leftover $1032.30 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 233 | $4.43 | $3.08 | — | $12,377.12 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-23.1; leftover $1032.30 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 87 | $11.81 | $2.30 | — | $13,402.72 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1032.30 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 22 | $46.85 | $2.10 | — | $14,431.32 | — | short prior-export headline🔴; gate headline=bad; list earn_react; 🔵; ret5=+5.0; leftover $1032.30 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 223 | $4.61 | $2.95 | — | $15,456.39 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1032.30 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,456.39 | ▲ close $10,447.16 vs 09:30 $10,348.47 (session +136.80) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,456.39 | ▼ 09:30 equity $10,380.00 vs yday $10,447.16 (-67.16) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 417 | $3.11 | $5.49 | — | $16,747.77 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1297.50 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 9 | $133.11 | $2.07 | — | $17,943.69 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $1297.50 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 62 | $20.90 | $2.23 | — | $19,237.26 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1297.50 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 48 | $27.00 | $2.19 | — | $20,531.07 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+10.1; leftover $1297.50 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,531.07 | ▼ close $10,294.03 vs 09:30 $10,380.00 (session -73.99) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,531.07 | ▲ 09:30 equity $10,426.90 vs yday $10,294.03 (+132.87) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,531.07 | ▲ close $10,563.61 vs 09:30 $10,426.90 (session +136.71) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,531.07 | ▼ 09:30 equity $10,539.56 vs yday $10,563.61 (-24.05) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 48 | $20.90 | $2.13 | $+19.68 | $19,525.73 | ▲ +19.68 after sell → book $10,537.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 233 | $4.42 | $3.01 | $-3.76 | $18,492.87 | ▼ -3.76 after sell → book $10,534.42; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 87 | $11.00 | $2.25 | $+66.35 | $17,533.62 | ▲ +66.35 after sell → book $10,532.17; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 22 | $43.63 | $2.06 | $+66.68 | $16,571.70 | ▲ +66.68 after sell → book $10,530.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 223 | $4.77 | $2.88 | $-41.51 | $15,505.11 | ▼ -41.51 after sell → book $10,527.23; vs 09:30 mark -2.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 386 | $13.62 | $5.23 | — | $20,759.14 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $5263.62 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,759.14 | ▼ close $10,339.18 vs 09:30 $10,539.56 (session -182.83) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,759.14 | ▲ 09:30 equity $10,403.33 vs yday $10,339.18 (+64.15) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 417 | $2.83 | $5.38 | $+105.89 | $19,573.65 | ▲ +105.89 after sell → book $10,397.95; vs 09:30 mark -5.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 9 | $154.20 | $2.02 | $-193.90 | $18,183.83 | ▼ -193.90 after sell → book $10,395.93; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 62 | $20.50 | $2.18 | $+20.39 | $16,910.65 | ▲ +20.39 after sell → book $10,393.75; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 48 | $26.00 | $2.13 | $+43.68 | $15,660.52 | ▲ +43.68 after sell → book $10,391.62; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 212 | $12.22 | $2.86 | — | $18,248.30 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $2597.90 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 511 | $5.08 | $6.77 | — | $20,837.41 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $2597.90 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,837.41 | ▼ close $10,230.92 vs 09:30 $10,403.33 (session -151.07) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,837.41 | ▼ 09:30 equity $10,228.80 vs yday $10,230.92 (-2.12) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 22 | $74.54 | $2.12 | — | $22,475.16 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1704.80 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MU` | 1 | $967.01 | $2.04 | — | $23,440.13 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ret5=+0.1; leftover $1704.80 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 30 | $55.25 | $2.15 | — | $25,095.49 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1704.80 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,095.49 | ▲ close $10,243.26 vs 09:30 $10,228.80 (session +20.77) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25,095.49 | ▲ 09:30 equity $10,253.25 vs yday $10,243.26 (+9.99) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 386 | $13.90 | $4.98 | $-116.36 | $19,725.11 | ▼ -116.36 after sell → book $10,248.27; vs 09:30 mark -4.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 20 | $252.24 | $2.24 | — | $24,767.67 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $5124.13 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,767.67 | ▲ close $10,665.41 vs 09:30 $10,253.25 (session +419.38) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,767.67 | ▲ 09:30 equity $10,688.09 vs yday $10,665.41 (+22.68) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 212 | $11.10 | $2.73 | $+231.84 | $22,411.73 | ▲ +231.84 after sell → book $10,685.36; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 511 | $4.97 | $6.59 | $+40.29 | $19,862.92 | ▲ +40.29 after sell → book $10,678.77; vs 09:30 mark -6.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,862.92 | ▲ close $10,681.59 vs 09:30 $10,688.09 (session +2.82) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,862.92 | ▲ 09:30 equity $10,866.35 vs yday $10,681.59 (+184.76) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 22 | $73.22 | $2.06 | $+24.86 | $18,250.02 | ▲ +24.86 after sell → book $10,864.29; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MU` | 1 | $941.13 | $1.99 | $+21.85 | $17,306.90 | ▲ +21.85 after sell → book $10,862.30; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 30 | $54.76 | $2.08 | $+10.47 | $15,662.02 | ▲ +10.47 after sell → book $10,860.22; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,662.02 | ▲ close $10,915.02 vs 09:30 $10,866.35 (session +54.80) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,662.02 | ▲ 09:30 equity $10,947.82 vs yday $10,915.02 (+32.80) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 20 | $235.71 | $2.05 | $+326.31 | $10,945.77 | ▲ +326.31 after sell → book $10,945.77; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,945.77 | ▲ close $10,945.77 vs 09:30 $10,947.82 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,945.77 | ▲ 09:30 equity $10,945.77 vs yday $10,945.77 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 184 | $14.85 | $2.67 | — | $13,675.50 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2736.44 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1600 | $1.71 | $21.00 | — | $16,390.50 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2736.44 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,390.50 | ▲ close $11,093.14 vs 09:30 $10,945.77 (session +171.04) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,390.50 | ▲ 09:30 equity $11,154.58 vs yday $11,093.14 (+61.44) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 597 | $4.67 | $7.90 | — | $19,170.59 | — | short prior-export headline🔴; gate headline=bad; list yday_gainer; ret5=+11.9; leftover $2788.64 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 36 | $76.55 | $2.21 | — | $21,924.18 | — | short prior-export headline🔴; gate headline=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2788.64 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,924.18 | ▼ close $11,054.19 vs 09:30 $11,154.58 (session -90.28) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,924.18 | ▲ 09:30 equity $11,101.23 vs yday $11,054.19 (+47.04) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,924.18 | ▲ close $11,378.54 vs 09:30 $11,101.23 (session +277.31) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,924.18 | ▲ 09:30 equity $11,414.70 vs yday $11,378.54 (+36.16) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 184 | $13.60 | $2.54 | $+224.79 | $19,419.24 | ▲ +224.79 after sell → book $11,412.16; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1600 | $1.58 | $20.64 | $+166.36 | $16,870.60 | ▲ +166.36 after sell → book $11,391.52; vs 09:30 mark -20.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,870.60 | ▲ close $11,419.51 vs 09:30 $11,414.70 (session +27.99) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,870.60 | ▲ 09:30 equity $11,503.24 vs yday $11,419.51 (+83.73) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 597 | $4.36 | $7.70 | $+169.47 | $14,259.98 | ▲ +169.47 after sell → book $11,495.54; vs 09:30 mark -7.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 36 | $76.79 | $2.10 | $-12.94 | $11,493.44 | ▼ -12.94 after sell → book $11,493.44; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,493.44 | ▲ close $11,493.44 vs 09:30 $11,503.24 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,493.44 | ▲ 09:30 equity $11,493.44 vs yday $11,493.44 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 408 | $3.52 | $5.38 | — | $12,924.22 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ret5=-19.2; leftover $1436.68 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 707 | $2.03 | $9.29 | — | $14,350.14 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=-8.8; leftover $1436.68 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 57 | $24.97 | $2.22 | — | $15,771.21 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+10.8; leftover $1436.68 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 426 | $3.37 | $5.62 | — | $17,201.21 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $1436.68 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,201.21 | ▲ close $11,504.32 vs 09:30 $11,493.44 (session +33.38) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,201.21 | ▲ 09:30 equity $11,517.11 vs yday $11,504.32 (+12.79) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,201.21 | ▼ close $11,259.36 vs 09:30 $11,517.11 (session -257.75) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,201.21 | ▲ 09:30 equity $11,264.04 vs yday $11,259.36 (+4.68) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,201.21 | ▼ close $11,257.15 vs 09:30 $11,264.04 (session -6.89) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,201.21 | ▲ 09:30 equity $11,279.98 vs yday $11,257.15 (+22.83) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 408 | $3.98 | $5.26 | $-198.32 | $15,572.11 | ▼ -198.32 after sell → book $11,274.72; vs 09:30 mark -5.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 707 | $1.85 | $9.12 | $+108.85 | $14,255.04 | ▲ +108.85 after sell → book $11,265.60; vs 09:30 mark -9.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 57 | $24.42 | $2.16 | $+26.97 | $12,860.94 | ▲ +26.97 after sell → book $11,263.44; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 426 | $3.75 | $5.50 | $-172.99 | $11,257.94 | ▼ -172.99 after sell → book $11,257.94; vs 09:30 mark -5.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 151 | $18.61 | $2.57 | — | $14,065.48 | — | short prior-export headline🔴; gate headline=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $2814.49 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 412 | $6.83 | $5.48 | — | $16,873.96 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+11.2; leftover $2814.49 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,873.96 | ▼ close $10,850.90 vs 09:30 $11,279.98 (session -398.99) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,873.96 | ▼ 09:30 equity $10,812.74 vs yday $10,850.90 (-38.16) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 340 | $7.95 | $4.54 | — | $19,572.42 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $2703.19 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 33 | $81.00 | $2.19 | — | $22,243.23 | — | short prior-export headline🔴; gate headline=bad; list earn_react; 🔵; ret5=-3.0; leftover $2703.19 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,243.23 | ▲ close $11,011.88 vs 09:30 $10,812.74 (session +205.87) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,243.23 | ▲ 09:30 equity $11,040.00 vs yday $11,011.88 (+28.12) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 160 | $34.44 | $2.69 | — | $27,750.94 | — | short prior-export headline🔴; gate headline=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $5520.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27,750.94 | ▲ close $11,179.24 vs 09:30 $11,040.00 (session +141.93) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27,750.94 | ▼ 09:30 equity $10,979.59 vs yday $11,179.24 (-199.65) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 151 | $22.11 | $2.44 | $-533.51 | $24,409.88 | ▼ -533.51 after sell → book $10,977.14; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 412 | $6.55 | $5.31 | $+104.56 | $21,705.97 | ▲ +104.56 after sell → book $10,971.83; vs 09:30 mark -5.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 332 | $8.26 | $4.44 | — | $24,443.86 | — | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=+7.7; leftover $2742.96 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $26,777.28 | — | short prior-export headline🔴; gate headline=bad; list ohlc_hot; ret5=+8.5; leftover $2742.96 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,777.28 | ▼ close $10,714.52 vs 09:30 $10,979.59 (session -250.78) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,777.28 | ▲ 09:30 equity $10,741.82 vs yday $10,714.52 (+27.30) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 340 | $8.28 | $4.39 | $-119.42 | $23,959.39 | ▼ -119.42 after sell → book $10,737.43; vs 09:30 mark -4.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 28 | $93.97 | $2.18 | — | $26,588.38 | — | short prior-export headline🔴; gate headline=bad; list flatten; ret5=-0.6; leftover $2684.36 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,588.38 | ▼ close $10,667.58 vs 09:30 $10,741.82 (session -67.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26,588.38 | ▼ 09:30 equity $10,010.58 vs yday $10,667.58 (-657.00) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 33 | $82.00 | $2.09 | $-37.28 | $23,880.29 | ▼ -37.28 after sell → book $10,008.49; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 42 | $116.85 | $2.30 | — | $28,785.69 | — | short prior-export headline🔴; gate headline=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $5004.24 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,785.69 | ▲ close $10,213.57 vs 09:30 $10,010.58 (session +207.38) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28,785.69 | ▲ 09:30 equity $10,306.17 vs yday $10,213.57 (+92.60) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $26,382.61 | ▼ -69.66 after sell → book $10,304.17; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,382.61 | ▼ close $10,076.09 vs 09:30 $10,306.17 (session -228.08) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,790.31 | ▼ 09:30 equity $9,112.90 vs yday $9,142.38 (-29.48) | 09:30 open · cash $20,790.31 (unchanged overnight, no fees) · equity $9,112.90 vs prior close $9,142.38 (-29.48) · 5 name(s) re-marked at the open (per-name table). AEHL×298 yday $8.96 → 09:30 $9.05 -26.82; BAND×39 yday $61.83 → 09:30 $61.83 -0.00; HALO×19 yday $115.22 → 09:30 $115.36 -2.66; PAYX×20 yday $101.59 → 09:30 $101.59 -0.00; USFD×25 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 580 | $7.85 | $7.74 | — | $25,335.57 | — | short prior-export headline🔴; gate headline=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $4556.45 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,335.57 | ▼ close $9,081.12 vs 09:30 $9,112.90 (session -24.04) | 16:00 close · cash $25,335.57 · equity $9,081.12 vs 09:30 $9,112.90 (-31.78; session marks -24.04) · 6 name(s) marked open→close (per-name table). AEHL×298 09:30 $9.05 → close $9.36 -92.38; BAND×39 09:30 $61.83 → close $61.83 -0.00; HALO×19 09:30 $115.36 → close $113.90 +27.74; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; RSKD×580 09:30 $7.85 → close $7.78 +40.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 160 | 2026-09-18 @ $34.44 | short prior-export headline🔴; gate headline=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $5520.00 |
| `AEHL` | 332 | 2026-09-21 @ $8.26 | short prior-export headline🔴; gate headline=bad; list yday_mover; ret5=+7.7; leftover $2742.96 |
| `USFD` | 28 | 2026-09-22 @ $93.97 | short prior-export headline🔴; gate headline=bad; list flatten; ret5=-0.6; leftover $2684.36 |
| `HALO` | 42 | 2026-09-23 @ $116.85 | short prior-export headline🔴; gate headline=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $5004.24 |
