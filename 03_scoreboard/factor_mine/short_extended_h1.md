# Factor mine action — `short_extended_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · ret_5>15

Cash book **-5.30%** ($9,470) · signal-only (no cash/fees) was -19.86%. Starts YES **3/30**. Fills 236 · skips 88 · realized $-1484.16.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 15%.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=15.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,515.82.

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
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,989.65 | ▲ close $10,039.83 vs 09:30 $10,000.00 (session +42.80) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,989.65 | ▲ 09:30 equity $10,084.77 vs yday $10,039.83 (+44.94) | — | — |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 214 | $22.92 | $2.76 | $+82.01 | $10,082.01 | ▲ +82.01 after sell → book $10,082.01; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $10,706.12 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $11,328.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $11,949.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $12,572.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $13,197.49 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,814.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $14,398.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.13 | — |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $15,023.36 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.13 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,023.36 | ▲ close $10,193.38 vs 09:30 $10,084.77 (session +132.73) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,023.36 | ▲ 09:30 equity $10,201.66 vs yday $10,193.38 (+8.28) | — | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 32 | $19.57 | $2.09 | $-4.21 | $14,395.04 | ▼ -4.21 after sell → book $10,199.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $13,774.82 | ▲ +2.25 after sell → book $10,197.48; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 56 | $9.57 | $2.16 | $+82.45 | $13,236.74 | ▲ +82.45 after sell → book $10,195.32; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 453 | $1.32 | $5.84 | $+19.92 | $12,632.94 | ▲ +19.92 after sell → book $10,189.48; vs 09:30 mark -5.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `QMLS` | 86 | $7.24 | $2.25 | $-0.24 | $12,008.05 | ▼ -0.24 after sell → book $10,187.23; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,370.98 | ▼ -19.93 after sell → book $10,185.08; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $10,800.28 | ▲ +13.07 after sell → book $10,183.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 127 | $4.86 | $2.37 | $+5.37 | $10,180.69 | ▲ +5.37 after sell → book $10,180.69; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 92 | $6.87 | $2.31 | — | $10,810.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $11,426.80 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $12,043.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 331 | $1.92 | $4.35 | — | $12,674.33 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 42 | $14.94 | $2.15 | — | $13,299.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $13,933.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $14,561.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $636.29 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 594 | $1.07 | $7.79 | — | $15,189.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $636.29 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,189.73 | ▲ close $10,228.41 vs 09:30 $10,201.66 (session +72.83) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,189.73 | ▲ 09:30 equity $10,340.59 vs yday $10,228.41 (+112.18) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 92 | $7.50 | $2.27 | $-62.53 | $14,497.46 | ▼ -62.53 after sell → book $10,338.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $13,872.93 | ▼ -8.16 after sell → book $10,336.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,327.67 | ▲ +71.11 after sell → book $10,334.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 331 | $1.70 | $4.27 | $+64.20 | $12,760.70 | ▲ +64.20 after sell → book $10,329.97; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LPTH` | 42 | $14.01 | $2.12 | $+34.79 | $12,170.17 | ▲ +34.79 after sell → book $10,327.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 58 | $10.31 | $2.16 | $+33.91 | $11,570.02 | ▲ +33.91 after sell → book $10,325.69; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ALOY` | 43 | $13.19 | $2.12 | $+58.93 | $11,000.73 | ▲ +58.93 after sell → book $10,323.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 594 | $1.14 | $7.66 | $-57.03 | $10,315.91 | ▼ -57.03 after sell → book $10,315.91; vs 09:30 mark -7.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,315.91 | ▲ close $10,315.91 vs 09:30 $10,340.59 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,315.91 | ▲ close $10,315.91 vs 09:30 $10,315.91 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $10,914.43 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 470 | $1.37 | $6.17 | — | $11,552.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 560 | $1.15 | $7.34 | — | $12,188.82 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,732.08 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 97 | $6.61 | $2.32 | — | $13,370.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,008.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,649.85 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $644.74 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $15,285.67 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $644.74 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,285.67 | ▼ close $10,288.52 vs 09:30 $10,315.91 (session -1.04) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,285.67 | ▼ 09:30 equity $10,114.17 vs yday $10,288.52 (-174.35) | — | — |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $14,751.23 | ▲ +64.08 after sell → book $10,112.17; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AZI` | 470 | $1.46 | $6.06 | $-54.53 | $14,058.97 | ▼ -54.53 after sell → book $10,106.11; vs 09:30 mark -6.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $13,502.36 | ▼ -13.34 after sell → book $10,104.10; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BTGO` | 97 | $6.95 | $2.28 | $-38.07 | $12,825.93 | ▼ -38.07 after sell → book $10,101.82; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ASST` | 40 | $17.66 | $2.11 | $-70.66 | $12,117.42 | ▼ -70.66 after sell → book $10,099.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `PPC` | 21 | $31.13 | $2.05 | $-14.22 | $11,461.64 | ▼ -14.22 after sell → book $10,097.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 54 | $11.57 | $2.15 | $+8.89 | $10,834.71 | ▲ +8.89 after sell → book $10,095.51; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,549.24 | — | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $721.11 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $12,196.10 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $721.11 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 64 | $11.13 | $2.22 | — | $12,906.20 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $721.11 | — |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 518 | $1.39 | $6.80 | — | $13,619.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $721.11 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2452 | $0.29 | $15.00 | — | $14,325.31 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $721.11 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRVI` | 87 | $8.28 | $2.30 | — | $15,043.37 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $721.11 | — |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 178 | $4.04 | $2.58 | — | $15,759.91 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $721.11 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,759.91 | ▼ close $9,736.73 vs 09:30 $10,114.17 (session -325.79) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,759.91 | ▼ 09:30 equity $9,438.59 vs yday $9,736.73 (-298.14) | — | — |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 560 | $1.83 | $7.22 | $-395.37 | $14,727.89 | ▼ -395.37 after sell → book $9,431.37; vs 09:30 mark -7.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.51 | $2.01 | $-10.54 | $14,002.82 | ▼ -10.54 after sell → book $9,429.36; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-6.23 | $13,349.73 | ▼ -6.23 after sell → book $9,427.36; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `INDP` | 518 | $1.24 | $6.68 | $+64.22 | $12,700.73 | ▲ +64.22 after sell → book $9,420.68; vs 09:30 mark -6.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRVI` | 87 | $8.59 | $2.25 | $-31.52 | $11,951.15 | ▼ -31.52 after sell → book $9,418.43; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 178 | $4.16 | $2.52 | $-26.47 | $11,208.14 | ▼ -26.47 after sell → book $9,415.91; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,208.14 | ▼ close $9,407.66 vs 09:30 $9,438.59 (session -8.24) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,208.14 | ▲ 09:30 equity $9,421.74 vs yday $9,407.66 (+14.08) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 64 | $14.12 | $2.18 | $-195.76 | $10,302.28 | ▼ -195.76 after sell → book $9,419.56; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `CAN` | 2452 | $0.36 | $16.18 | $-193.02 | $9,403.38 | ▼ -193.02 after sell → book $9,403.38; vs 09:30 mark -16.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMEA` | 360 | $1.63 | $4.73 | — | $9,985.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `NPWR` | 293 | $2.00 | $3.85 | — | $10,567.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ALVO` | 112 | $5.24 | $2.37 | — | $11,152.10 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 66 | $8.79 | $2.22 | — | $11,730.02 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 376 | $1.56 | $4.94 | — | $12,311.64 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 102 | $5.71 | $2.34 | — | $12,891.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 947 | $0.62 | $8.90 | — | $13,469.96 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $587.71 | — |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 165 | $3.55 | $2.54 | — | $14,053.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+27.9; leftover $587.71 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,053.17 | ▼ close $9,233.37 vs 09:30 $9,421.74 (session -138.12) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,053.17 | ▲ 09:30 equity $9,279.49 vs yday $9,233.37 (+46.12) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `BMEA` | 360 | $1.75 | $4.64 | $-54.37 | $13,416.73 | ▼ -54.37 after sell → book $9,274.84; vs 09:30 mark -4.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `NPWR` | 293 | $1.93 | $3.78 | $+12.88 | $12,847.46 | ▲ +12.88 after sell → book $9,271.06; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ALVO` | 112 | $4.98 | $2.33 | $+24.42 | $12,287.37 | ▲ +24.42 after sell → book $9,268.74; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 376 | $1.60 | $4.85 | $-24.83 | $11,680.92 | ▼ -24.83 after sell → book $9,263.89; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `FWDI` | 102 | $5.97 | $2.30 | $-31.15 | $11,069.69 | ▼ -31.15 after sell → book $9,261.59; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `DEFT` | 947 | $0.60 | $8.50 | $+3.43 | $10,494.88 | ▲ +3.43 after sell → book $9,253.09; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `GORO` | 165 | $3.77 | $2.48 | $-41.32 | $9,870.34 | ▼ -41.32 after sell → book $9,250.60; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `INDP` | 606 | $1.09 | $7.95 | — | $10,522.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $660.76 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CAPR` | 79 | $8.29 | $2.27 | — | $11,175.58 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $660.76 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BRR` | 300 | $2.20 | $3.95 | — | $11,831.63 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+17.8; leftover $660.76 | — |
| 2026-08-26 09:30 ET | **SHORT** | `USDE` | 113 | $5.81 | $2.38 | — | $12,485.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $660.76 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FIGR` | 16 | $40.50 | $2.08 | — | $13,131.71 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.8; leftover $660.76 | — |
| 2026-08-26 09:30 ET | **SHORT** | `MNRO` | 47 | $14.00 | $2.17 | — | $13,787.54 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.8; leftover $660.76 | — |
| 2026-08-26 09:30 ET | **SHORT** | `FUTU` | 5 | $124.67 | $2.04 | — | $14,408.85 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.7; leftover $660.76 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,408.85 | ▼ close $9,206.14 vs 09:30 $9,279.49 (session -21.64) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,408.85 | ▼ 09:30 equity $9,156.46 vs yday $9,206.14 (-49.68) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `BRR` | 300 | $2.19 | $3.87 | $-4.82 | $13,747.98 | ▼ -4.82 after sell → book $9,152.59; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FIGR` | 16 | $37.42 | $2.04 | $+45.17 | $13,147.22 | ▲ +45.17 after sell → book $9,150.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `MNRO` | 47 | $12.56 | $2.13 | $+63.38 | $12,554.77 | ▲ +63.38 after sell → book $9,148.42; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `BZ` | 82 | $18.50 | $2.30 | — | $14,069.47 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.2; leftover $1524.74 | — |
| 2026-08-27 09:30 ET | **SHORT** | `AQST` | 282 | $5.39 | $3.74 | — | $15,585.71 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.4; leftover $1524.74 | — |
| 2026-08-27 09:30 ET | **SHORT** | `VYX` | 170 | $8.95 | $2.58 | — | $17,104.63 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.2; leftover $1524.74 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,104.63 | ▼ close $8,986.45 vs 09:30 $9,156.46 (session -153.35) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,104.63 | ▲ 09:30 equity $9,112.83 vs yday $8,986.45 (+126.38) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 66 | $9.08 | $2.19 | $-23.55 | $16,503.16 | ▼ -23.55 after sell → book $9,110.64; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `INDP` | 606 | $1.16 | $7.82 | $-58.18 | $15,792.38 | ▼ -58.18 after sell → book $9,102.82; vs 09:30 mark -7.82 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `USDE` | 113 | $7.24 | $2.33 | $-166.29 | $14,971.93 | ▼ -166.29 after sell → book $9,100.49; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `FUTU` | 5 | $124.27 | $2.00 | $-2.05 | $14,348.58 | ▼ -2.05 after sell → book $9,098.49; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BZ` | 82 | $18.15 | $2.24 | $+24.16 | $12,858.04 | ▲ +24.16 after sell → book $9,096.25; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AQST` | 282 | $5.11 | $3.64 | $+71.58 | $11,413.38 | ▲ +71.58 after sell → book $9,092.61; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SLI` | 282 | $2.68 | $3.72 | — | $12,165.43 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+16.3; leftover $757.72 | — |
| 2026-08-28 09:30 ET | **SHORT** | `ANF` | 5 | $146.07 | $2.05 | — | $12,893.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $757.72 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 47 | $15.88 | $2.17 | — | $13,637.92 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+19.4; leftover $757.72 | — |
| 2026-08-28 09:30 ET | **SHORT** | `LVWR` | 545 | $1.39 | $7.15 | — | $14,388.32 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+20.4; leftover $757.72 | — |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 757 | $1.00 | $9.92 | — | $15,135.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+16.8; leftover $757.72 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 87 | $8.65 | $2.30 | — | $15,885.65 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.0; leftover $757.72 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,885.65 | ▲ close $9,228.68 vs 09:30 $9,112.83 (session +163.37) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,885.65 | ▲ 09:30 equity $9,240.82 vs yday $9,228.68 (+12.14) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 79 | $9.50 | $2.23 | $-100.09 | $15,132.92 | ▼ -100.09 after sell → book $9,238.59; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `VYX` | 170 | $8.66 | $2.50 | $+44.22 | $13,658.22 | ▲ +44.22 after sell → book $9,236.09; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SLI` | 282 | $2.58 | $3.64 | $+20.85 | $12,927.03 | ▲ +20.85 after sell → book $9,232.46; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ANF` | 5 | $148.03 | $2.00 | $-13.85 | $12,184.87 | ▼ -13.85 after sell → book $9,230.45; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BHVN` | 47 | $15.46 | $2.13 | $+15.44 | $11,456.12 | ▲ +15.44 after sell → book $9,228.32; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `LVWR` | 545 | $1.30 | $7.03 | $+34.87 | $10,740.59 | ▲ +34.87 after sell → book $9,221.29; vs 09:30 mark -7.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTU` | 757 | $1.06 | $9.77 | $-65.11 | $9,928.40 | ▼ -65.11 after sell → book $9,211.52; vs 09:30 mark -9.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 87 | $8.24 | $2.25 | $+31.12 | $9,209.27 | ▲ +31.12 after sell → book $9,209.27; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,209.27 | ▲ close $9,209.27 vs 09:30 $9,240.82 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,209.27 | ▲ 09:30 equity $9,209.27 vs yday $9,209.27 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,209.27 | ▲ close $9,209.27 vs 09:30 $9,209.27 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,209.27 | ▲ 09:30 equity $9,209.27 vs yday $9,209.27 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,209.27 | ▲ close $9,209.27 vs 09:30 $9,209.27 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,209.27 | ▲ 09:30 equity $9,209.27 vs yday $9,209.27 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `GPRO` | 323 | $1.78 | $4.25 | — | $9,779.97 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+183.1; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.28 | $2.12 | — | $10,344.53 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.5; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `MMED` | 24 | $23.88 | $2.10 | — | $10,915.55 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CNXC` | 17 | $32.88 | $2.08 | — | $11,472.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+16.2; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 78 | $7.31 | $2.26 | — | $12,040.35 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+18.5; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CNH` | 41 | $13.71 | $2.15 | — | $12,600.31 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+17.5; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `TARS` | 6 | $82.76 | $2.04 | — | $13,094.83 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.1; leftover $575.58 | — |
| 2026-09-03 09:30 ET | **SHORT** | `DFDV` | 102 | $5.59 | $2.34 | — | $13,663.18 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.0; leftover $575.58 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,663.18 | ▲ close $9,338.31 vs 09:30 $9,209.27 (session +148.37) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,663.18 | ▲ 09:30 equity $9,348.14 vs yday $9,338.31 (+9.83) | — | — |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $17.27 | $2.08 | $+27.11 | $13,125.73 | ▲ +27.11 after sell → book $9,346.06; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MMED` | 24 | $23.84 | $2.06 | $-3.20 | $12,551.51 | ▼ -3.20 after sell → book $9,344.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CNXC` | 17 | $32.48 | $2.04 | $+2.68 | $11,997.31 | ▲ +2.68 after sell → book $9,341.96; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SION` | 78 | $6.68 | $2.22 | $+44.65 | $11,474.04 | ▲ +44.65 after sell → book $9,339.73; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CNH` | 41 | $13.89 | $2.11 | $-11.64 | $10,902.44 | ▼ -11.64 after sell → book $9,337.62; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DFDV` | 102 | $5.79 | $2.30 | $-24.52 | $10,309.56 | ▼ -24.52 after sell → book $9,335.32; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `BAK` | 401 | $1.94 | $5.27 | — | $11,082.23 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+18.3; leftover $777.94 | — |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 246 | $3.15 | $3.25 | — | $11,853.89 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $777.94 | — |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 171 | $4.53 | $2.56 | — | $12,625.96 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $777.94 | — |
| 2026-09-04 09:30 ET | **SHORT** | `FMC` | 60 | $12.95 | $2.21 | — | $13,400.74 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+21.8; leftover $777.94 | — |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 309 | $2.51 | $4.07 | — | $14,172.27 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $777.94 | — |
| 2026-09-04 09:30 ET | **SHORT** | `LENZ` | 135 | $5.75 | $2.45 | — | $14,946.07 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $777.94 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,946.07 | ▼ close $9,182.61 vs 09:30 $9,348.14 (session -132.91) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,946.07 | ▲ 09:30 equity $9,229.71 vs yday $9,182.61 (+47.10) | — | — |
| 2026-09-08 09:30 ET | **COVER** | `GPRO` | 323 | $1.56 | $4.17 | $+61.03 | $14,436.41 | ▲ +61.03 after sell → book $9,225.55; vs 09:30 mark -4.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `TARS` | 6 | $89.67 | $2.01 | $-45.51 | $13,896.38 | ▼ -45.51 after sell → book $9,223.54; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BAK` | 401 | $1.94 | $5.17 | $-10.44 | $13,113.26 | ▼ -10.44 after sell → book $9,218.36; vs 09:30 mark -5.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `SLBT` | 246 | $2.88 | $3.17 | $+60.00 | $12,401.61 | ▲ +60.00 after sell → book $9,215.19; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `IRD` | 171 | $4.53 | $2.50 | $-5.07 | $11,624.48 | ▼ -5.07 after sell → book $9,212.69; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `FMC` | 60 | $13.11 | $2.17 | $-13.98 | $10,835.71 | ▼ -13.98 after sell → book $9,210.52; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BRR` | 309 | $2.66 | $3.99 | $-54.40 | $10,009.78 | ▼ -54.40 after sell → book $9,206.53; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `LENZ` | 135 | $5.95 | $2.40 | $-31.84 | $9,204.14 | ▼ -31.84 after sell → book $9,204.14; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,204.14 | ▲ close $9,204.14 vs 09:30 $9,229.71 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,204.14 | ▲ 09:30 equity $9,204.14 vs yday $9,204.14 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,204.14 | ▲ close $9,204.14 vs 09:30 $9,204.14 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,204.14 | ▲ 09:30 equity $9,204.14 vs yday $9,204.14 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,204.14 | ▲ close $9,204.14 vs 09:30 $9,204.14 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,204.14 | ▲ 09:30 equity $9,204.14 vs yday $9,204.14 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `COHU` | 10 | $56.09 | $2.06 | — | $9,762.98 | — | ret_5>15; gate ret_5_min=15.0; list flatten; 🔵; ret5=+19.6; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `INDP` | 213 | $2.70 | $2.81 | — | $10,335.27 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CMRC` | 183 | $3.13 | $2.60 | — | $10,905.47 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+24.2; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `WLTH` | 52 | $10.95 | $2.18 | — | $11,472.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BNC` | 117 | $4.91 | $2.39 | — | $12,044.77 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `SWKS` | 6 | $84.27 | $2.04 | — | $12,548.35 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ANGX` | 106 | $5.38 | $2.35 | — | $13,116.28 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+19.8; leftover $575.26 | — |
| 2026-09-11 09:30 ET | **SHORT** | `APPS` | 48 | $11.88 | $2.17 | — | $13,684.35 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.7; leftover $575.26 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,684.35 | ▼ close $9,106.08 vs 09:30 $9,204.14 (session -79.46) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,684.35 | ▲ 09:30 equity $9,128.95 vs yday $9,106.08 (+22.87) | — | — |
| 2026-09-14 09:30 ET | **COVER** | `COHU` | 10 | $52.23 | $2.02 | $+34.52 | $13,160.03 | ▲ +34.52 after sell → book $9,126.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `WLTH` | 52 | $10.29 | $2.15 | $+29.99 | $12,622.80 | ▲ +29.99 after sell → book $9,124.78; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `BNC` | 117 | $5.03 | $2.34 | $-18.77 | $12,031.95 | ▼ -18.77 after sell → book $9,122.44; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `SWKS` | 6 | $86.06 | $2.01 | $-14.79 | $11,513.58 | ▼ -14.79 after sell → book $9,120.43; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `ANGX` | 106 | $5.57 | $2.31 | $-24.80 | $10,920.85 | ▼ -24.80 after sell → book $9,118.12; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `APPS` | 48 | $11.75 | $2.13 | $+1.94 | $10,354.72 | ▲ +1.94 after sell → book $9,115.99; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,354.72 | ▼ close $9,019.78 vs 09:30 $9,128.95 (session -96.21) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,354.72 | ▼ 09:30 equity $8,964.40 vs yday $9,019.78 (-55.38) | — | — |
| 2026-09-15 09:30 ET | **COVER** | `CMRC` | 183 | $3.64 | $2.54 | $-98.46 | $9,686.06 | ▼ -98.46 after sell → book $8,961.86; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,686.06 | ▼ close $8,910.74 vs 09:30 $8,964.40 (session -51.12) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,686.06 | ▼ 09:30 equity $8,906.48 vs yday $8,910.74 (-4.26) | — | — |
| 2026-09-16 09:30 ET | **SHORT** | `HLP` | 412 | $1.80 | $5.41 | — | $10,422.25 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $742.21 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SWKS` | 8 | $89.38 | $2.05 | — | $11,135.23 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $742.21 | — |
| 2026-09-16 09:30 ET | **SHORT** | `FTRE` | 37 | $19.75 | $2.14 | — | $11,863.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $742.21 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SDGR` | 31 | $23.29 | $2.12 | — | $12,583.71 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+16.1; leftover $742.21 | — |
| 2026-09-16 09:30 ET | **SHORT** | `REF` | 47 | $15.75 | $2.17 | — | $13,321.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $742.21 | — |
| 2026-09-16 09:30 ET | **SHORT** | `CRWD` | 3 | $236.92 | $2.04 | — | $14,030.51 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.5; leftover $742.21 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,030.51 | ▼ close $8,865.11 vs 09:30 $8,906.48 (session -25.43) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,030.51 | ▼ 09:30 equity $8,817.00 vs yday $8,865.11 (-48.11) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `SWKS` | 8 | $86.76 | $2.01 | $+16.89 | $13,334.42 | ▲ +16.89 after sell → book $8,814.99; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `FTRE` | 37 | $20.31 | $2.10 | $-24.96 | $12,580.85 | ▼ -24.96 after sell → book $8,812.89; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `SDGR` | 31 | $24.09 | $2.08 | $-29.01 | $11,831.97 | ▼ -29.01 after sell → book $8,810.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `REF` | 47 | $15.85 | $2.13 | $-9.00 | $11,084.89 | ▼ -9.00 after sell → book $8,808.67; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `CRWD` | 3 | $236.04 | $2.00 | $-1.40 | $10,374.77 | ▼ -1.40 after sell → book $8,806.67; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `RVTY` | 5 | $147.61 | $2.05 | — | $11,110.78 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+17.7; leftover $880.67 | — |
| 2026-09-17 09:30 ET | **SHORT** | `IOVA` | 85 | $10.25 | $2.29 | — | $11,979.73 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $880.67 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BBNX` | 39 | $22.46 | $2.15 | — | $12,853.52 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $880.67 | — |
| 2026-09-17 09:30 ET | **SHORT** | `EMAT` | 228 | $3.86 | $3.01 | — | $13,730.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $880.67 | — |
| 2026-09-17 09:30 ET | **SHORT** | `IQ` | 823 | $1.07 | $10.79 | — | $14,600.41 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $880.67 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,600.41 | ▼ close $8,688.10 vs 09:30 $8,817.00 (session -98.28) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,600.41 | ▲ 09:30 equity $8,722.52 vs yday $8,688.10 (+34.42) | — | — |
| 2026-09-18 09:30 ET | **COVER** | `HLP` | 412 | $1.96 | $5.31 | $-76.65 | $13,787.58 | ▼ -76.65 after sell → book $8,717.21; vs 09:30 mark -5.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `RVTY` | 5 | $146.50 | $2.00 | $+1.50 | $13,053.07 | ▲ +1.50 after sell → book $8,715.20; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `IOVA` | 85 | $10.12 | $2.25 | $+6.51 | $12,190.63 | ▲ +6.51 after sell → book $8,712.96; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BBNX` | 39 | $21.30 | $2.11 | $+40.98 | $11,357.82 | ▲ +40.98 after sell → book $8,710.85; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `EMAT` | 228 | $3.97 | $2.94 | $-31.03 | $10,449.72 | ▼ -31.03 after sell → book $8,707.91; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `IQ` | 823 | $1.12 | $10.62 | $-62.55 | $9,517.34 | ▼ -62.55 after sell → book $8,697.29; vs 09:30 mark -10.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `RBRK` | 5 | $108.55 | $2.04 | — | $10,058.05 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+21.3; leftover $621.24 | — |
| 2026-09-18 09:30 ET | **SHORT** | `DELL` | 1 | $593.15 | $2.03 | — | $10,649.17 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+16.1; leftover $621.24 | — |
| 2026-09-18 09:30 ET | **SHORT** | `VICR` | 2 | $219.62 | $2.03 | — | $11,086.39 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $621.24 | — |
| 2026-09-18 09:30 ET | **SHORT** | `ECO` | 7 | $85.00 | $2.05 | — | $11,679.34 | — | ret_5>15; gate ret_5_min=15.0; list flatten; 🔵; ⚪; ret5=+18.3; leftover $621.24 | — |
| 2026-09-18 09:30 ET | **SHORT** | `SDGR` | 21 | $29.32 | $2.09 | — | $12,292.97 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $621.24 | — |
| 2026-09-18 09:30 ET | **SHORT** | `CYPH` | 204 | $3.04 | $2.69 | — | $12,909.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $621.24 | — |
| 2026-09-18 09:30 ET | **SHORT** | `USDE` | 65 | $9.54 | $2.22 | — | $13,527.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+15.8; leftover $621.24 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,527.29 | ▼ close $8,623.27 vs 09:30 $8,722.52 (session -58.87) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,527.29 | ▼ 09:30 equity $8,323.93 vs yday $8,623.27 (-299.34) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `INDP` | 213 | $3.55 | $2.75 | $-186.61 | $12,768.40 | ▼ -186.61 after sell → book $8,321.19; vs 09:30 mark -2.74 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `RBRK` | 5 | $107.57 | $2.00 | $+0.86 | $12,228.54 | ▲ +0.86 after sell → book $8,319.18; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `DELL` | 1 | $586.77 | $1.99 | $+2.36 | $11,639.78 | ▲ +2.36 after sell → book $8,317.19; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `VICR` | 2 | $230.25 | $2.00 | $-25.28 | $11,177.28 | ▼ -25.28 after sell → book $8,315.19; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `ECO` | 7 | $82.83 | $2.01 | $+11.13 | $10,595.46 | ▲ +11.13 after sell → book $8,313.18; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `SDGR` | 21 | $29.43 | $2.05 | $-6.45 | $9,975.38 | ▼ -6.45 after sell → book $8,311.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `IOVA` | 66 | $10.43 | $2.23 | — | $10,661.53 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ret5=+19.2; leftover $692.59 | — |
| 2026-09-21 09:30 ET | **SHORT** | `TJGC` | 40 | $16.91 | $2.15 | — | $11,335.78 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+50.5; leftover $692.59 | — |
| 2026-09-21 09:30 ET | **SHORT** | `GEMI` | 120 | $5.75 | $2.40 | — | $12,023.98 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $692.59 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FWDI` | 84 | $8.22 | $2.28 | — | $12,712.18 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $692.59 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SECZ` | 59 | $11.67 | $2.21 | — | $13,398.50 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+31.3; leftover $692.59 | — |
| 2026-09-21 09:30 ET | **SHORT** | `FEAM` | 280 | $2.47 | $3.69 | — | $14,086.41 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+73.6; leftover $692.59 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,086.41 | ▲ close $8,302.09 vs 09:30 $8,323.93 (session +5.92) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,086.41 | ▼ 09:30 equity $8,279.02 vs yday $8,302.09 (-23.07) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `CYPH` | 204 | $3.51 | $2.63 | $-102.22 | $13,367.74 | ▼ -102.22 after sell → book $8,276.39; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `IOVA` | 66 | $10.18 | $2.19 | $+12.08 | $12,693.68 | ▲ +12.08 after sell → book $8,274.21; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `GEMI` | 120 | $6.05 | $2.35 | $-40.75 | $11,964.73 | ▼ -40.75 after sell → book $8,271.86; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 234 | $2.94 | $3.09 | — | $12,649.60 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+136.1; leftover $689.32 | — |
| 2026-09-22 09:30 ET | **SHORT** | `CRML` | 75 | $9.11 | $2.26 | — | $13,330.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+44.4; leftover $689.32 | — |
| 2026-09-22 09:30 ET | **SHORT** | `NUAI` | 95 | $7.23 | $2.32 | — | $14,015.12 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+36.6; leftover $689.32 | — |
| 2026-09-22 09:30 ET | **SHORT** | `VGZ` | 260 | $2.65 | $3.43 | — | $14,700.70 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.3; leftover $689.32 | — |
| 2026-09-22 09:30 ET | **SHORT** | `ARM` | 2 | $319.41 | $2.03 | — | $15,337.48 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+35.1; leftover $689.32 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,337.48 | ▲ close $8,316.44 vs 09:30 $8,279.02 (session +57.71) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,337.48 | ▼ 09:30 equity $8,256.52 vs yday $8,316.44 (-59.92) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 65 | $13.22 | $2.19 | $-243.61 | $14,476.00 | ▼ -243.61 after sell → book $8,254.34; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `TJGC` | 40 | $16.92 | $2.11 | $-4.66 | $13,797.09 | ▼ -4.66 after sell → book $8,252.23; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `FWDI` | 84 | $8.20 | $2.24 | $-2.85 | $13,106.05 | ▼ -2.85 after sell → book $8,249.99; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `SECZ` | 59 | $12.80 | $2.17 | $-71.04 | $12,348.68 | ▼ -71.04 after sell → book $8,247.82; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `GLND` | 234 | $2.70 | $3.02 | $+50.06 | $11,713.86 | ▲ +50.06 after sell → book $8,244.80; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `CRML` | 75 | $8.39 | $2.21 | $+49.53 | $11,082.40 | ▲ +49.53 after sell → book $8,242.59; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `NUAI` | 95 | $6.83 | $2.27 | $+33.41 | $10,431.27 | ▲ +33.41 after sell → book $8,240.31; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `VGZ` | 260 | $2.73 | $3.35 | $-27.58 | $9,718.12 | ▼ -27.58 after sell → book $8,236.96; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `ARM` | 2 | $331.78 | $2.00 | $-28.77 | $9,052.56 | ▼ -28.77 after sell → book $8,234.96; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `VKTX` | 14 | $41.76 | $2.07 | — | $9,635.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $588.21 | — |
| 2026-09-23 09:30 ET | **SHORT** | `BFLY` | 59 | $9.90 | $2.20 | — | $10,217.03 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $588.21 | — |
| 2026-09-23 09:30 ET | **SHORT** | `VICR` | 2 | $266.50 | $2.03 | — | $10,748.00 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $588.21 | — |
| 2026-09-23 09:30 ET | **SHORT** | `EVTL` | 801 | $0.73 | $8.44 | — | $11,327.49 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $588.21 | — |
| 2026-09-23 09:30 ET | **SHORT** | `INOD` | 8 | $70.84 | $2.05 | — | $11,892.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $588.21 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SVIA` | 131 | $4.49 | $2.43 | — | $12,477.92 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+26.4; leftover $588.21 | — |
| 2026-09-23 09:30 ET | **SHORT** | `THM` | 205 | $2.86 | $2.70 | — | $13,061.52 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+25.5; leftover $588.21 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,061.52 | ▲ close $8,403.11 vs 09:30 $8,256.52 (session +190.08) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,061.52 | ▲ 09:30 equity $8,540.36 vs yday $8,403.11 (+137.25) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `FEAM` | 280 | $2.68 | $3.61 | $-66.10 | $12,307.51 | ▼ -66.10 after sell → book $8,536.75; vs 09:30 mark -3.61 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `VKTX` | 14 | $36.02 | $2.03 | $+76.19 | $11,801.12 | ▲ +76.19 after sell → book $8,534.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `BFLY` | 59 | $9.12 | $2.17 | $+41.65 | $11,260.88 | ▲ +41.65 after sell → book $8,532.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `VICR` | 2 | $274.61 | $2.00 | $-20.25 | $10,709.66 | ▼ -20.25 after sell → book $8,530.55; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `EVTL` | 801 | $0.66 | $7.69 | $+42.82 | $10,172.99 | ▲ +42.82 after sell → book $8,522.86; vs 09:30 mark -7.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `INOD` | 8 | $70.50 | $2.01 | $-1.34 | $9,606.97 | ▼ -1.34 after sell → book $8,520.85; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `SVIA` | 131 | $3.92 | $2.38 | $+69.20 | $9,090.42 | ▲ +69.20 after sell → book $8,518.47; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `THM` | 205 | $2.79 | $2.64 | $+9.00 | $8,515.82 | ▲ +9.00 after sell → book $8,515.82; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,515.82 | ▲ close $8,515.82 vs 09:30 $8,540.36 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,420.94 | ▲ 09:30 equity $9,420.94 vs yday $9,420.94 (+0.00) | 09:30 open · cash $9,420.94 · no holdings · equity $9,420.94 vs prior close $9,420.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **SHORT** | `GLND` | 97 | $6.06 | $2.32 | — | $10,006.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+342.1; leftover $588.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `ZSQR` | 152 | $3.86 | $2.50 | — | $10,590.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $588.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TJGC` | 19 | $29.76 | $2.08 | — | $11,154.02 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $588.81 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `DNA` | 57 | $10.20 | $2.20 | — | $11,733.22 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $588.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `TWST` | 3 | $184.00 | $2.03 | — | $12,283.19 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $588.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SECZ` | 36 | $16.21 | $2.13 | — | $12,864.61 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $588.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `GRAL` | 4 | $123.50 | $2.04 | — | $13,356.58 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $588.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `QMCO` | 19 | $29.80 | $2.08 | — | $13,920.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $588.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,920.69 | ▲ close $9,470.04 vs 09:30 $9,420.94 (session +66.49) | 16:00 close · cash $13,920.69 · equity $9,470.04 vs 09:30 $9,420.94 (+49.10; session marks +66.49) · 8 name(s) marked open→close (per-name table). GLND×97 09:30 $6.06 → close $5.54 +50.44; ZSQR×152 09:30 $3.86 → close $3.78 +12.16; TJGC×19 09:30 $29.76 → close $26.24 +66.88; DNA×57 09:30 $10.20 → close $10.66 -26.22; TWST×3 09:30 $184.00 → close $182.83 +3.51; SECZ×36 09:30 $16.21 → close $15.96 +9.00; GRAL×4 09:30 $123.50 → close $126.89 -13.56; QMCO×19 09:30 $29.80 → close $31.68 -35.72 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `COIN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CNXC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NABL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RXST` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RZLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ORBS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PAYP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HYLN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LIFE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XHLD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BAND` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
