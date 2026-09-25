# Factor mine action — `union_h5_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **-12.89%** ($8,711) · signal-only (no cash/fees) was +16.74%. Starts YES **6/30**. Fills 166 · skips 458 · realized $-167.75.

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
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $204.44.

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
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.16 | ▲ close $10,194.81 vs 09:30 $10,157.73 (session +37.08) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▲ 09:30 equity $10,296.33 vs yday $10,194.81 (+101.52) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.16 | ▲ close $10,540.20 vs 09:30 $10,296.33 (session +243.87) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▼ 09:30 equity $10,477.31 vs yday $10,540.20 (-62.89) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 37 | $58.64 | $2.13 | $-47.15 | $2,252.71 | ▼ -47.15 after sell → book $10,475.18; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 42 | $42.46 | $2.14 | $-152.10 | $4,033.89 | ▼ -152.10 after sell → book $10,473.04; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 32 | $53.06 | $2.11 | $+73.78 | $5,729.70 | ▲ +73.78 after sell → book $10,470.93; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 27 | $51.65 | $2.09 | $+48.49 | $7,122.16 | ▲ +48.49 after sell → book $10,468.84; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $8,420.82 | ▲ +196.59 after sell → book $10,466.54; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $9,277.21 | ▲ +21.59 after sell → book $10,464.45; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 685 | $1.30 | $8.96 | $+319.09 | $10,158.75 | ▲ +319.09 after sell → book $10,455.49; vs 09:30 mark -8.96 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 11 | $23.11 | $2.04 | $-6.49 | $10,410.92 | ▼ -6.49 after sell → book $10,453.44; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 112 | $20.55 | $2.33 | — | $8,106.99 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2313.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 22 | $91.01 | $2.06 | — | $6,102.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2024.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 84 | $20.65 | $2.24 | — | $4,365.87 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1735.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 250 | $5.77 | $3.23 | — | $2,920.15 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1445.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,779.44 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1156.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $918.10 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $867.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 330 | $1.75 | $4.26 | — | $336.34 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $578.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $45.26 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $289.19 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.26 | ▲ close $10,666.78 vs 09:30 $10,477.31 (session +233.68) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.26 | ▲ 09:30 equity $10,954.91 vs yday $10,666.78 (+288.13) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $56.82 | ▲ +2.46 after sell → book $10,954.77; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $62.79 | ▼ -0.68 after sell → book $10,954.67; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 2 | $1.66 | $0.06 | $+0.22 | $66.05 | ▲ +0.22 after sell → book $10,954.61; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 2 | $2.47 | $0.06 | — | $61.06 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $7.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 2 | $1.93 | $0.04 | — | $57.15 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $5.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1 | $1.32 | $0.02 | — | $55.82 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1.83 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.82 | ▼ close $10,857.48 vs 09:30 $10,954.91 (session -97.02) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.82 | ▲ 09:30 equity $10,958.31 vs yday $10,857.48 (+100.83) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 3 | $4.62 | $0.17 | $+1.43 | $69.52 | ▲ +1.43 after sell → book $10,958.14; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $78.67 | ▲ +0.60 after sell → book $10,958.03; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $82.11 | ▲ +0.17 after sell → book $10,957.97; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.11 | ▼ close $10,864.13 vs 09:30 $10,958.31 (session -93.84) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.11 | ▼ 09:30 equity $10,686.43 vs yday $10,864.13 (-177.70) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $71.02 | — | rank-weighted leftover; list flatten; 🔵; ret5=+1.2; leftover $15.97 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $62.58 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $11.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $4.94 | $0.05 | — | $57.59 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $9.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 6 | $0.36 | $0.04 | — | $55.40 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-15.6; leftover $2.28 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.40 | ▲ close $11,081.03 vs 09:30 $10,686.43 (session +394.90) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.40 | ▼ 09:30 equity $10,869.32 vs yday $11,081.03 (-211.71) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.40 | ▼ close $10,811.73 vs 09:30 $10,869.32 (session -57.59) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.40 | ▲ 09:30 equity $10,825.27 vs yday $10,811.73 (+13.54) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 112 | $20.93 | $2.36 | $+37.87 | $2,397.20 | ▲ +37.87 after sell → book $10,822.91; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 22 | $95.52 | $2.08 | $+95.08 | $4,496.55 | ▲ +95.08 after sell → book $10,820.83; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 84 | $21.31 | $2.27 | $+50.93 | $6,284.32 | ▲ +50.93 after sell → book $10,818.56; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 250 | $5.49 | $3.28 | $-76.50 | $7,653.55 | ▼ -76.50 after sell → book $10,815.28; vs 09:30 mark -3.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 58 | $21.47 | $2.18 | $+102.37 | $8,896.62 | ▲ +102.37 after sell → book $10,813.09; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 29 | $32.32 | $2.10 | $+73.84 | $9,831.81 | ▲ +73.84 after sell → book $10,811.00; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 330 | $1.91 | $4.32 | $+44.22 | $10,457.78 | ▲ +44.22 after sell → book $10,806.68; vs 09:30 mark -4.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 2 | $155.89 | $2.02 | $+18.69 | $10,767.55 | ▲ +18.69 after sell → book $10,804.66; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 57 | $41.44 | $2.16 | — | $8,403.31 | — | rank-weighted leftover; list flatten; ret5=+3.1; leftover $2392.79 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 145 | $14.42 | $2.42 | — | $6,309.98 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $2093.69 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 74 | $24.00 | $2.21 | — | $4,531.77 | — | rank-weighted leftover; list flatten; ret5=+8.7; leftover $1794.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 575 | $2.60 | $7.42 | — | $3,029.35 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+13.0; leftover $1495.49 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 92 | $12.98 | $2.27 | — | $1,832.93 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1196.39 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 29 | $30.79 | $2.08 | — | $937.94 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $897.30 | — |
| 2026-08-27 09:30 ET | **BUY** | `FLNC` | 51 | $11.52 | $2.14 | — | $348.28 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-8.2; leftover $598.20 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 30 | $9.68 | $2.08 | — | $55.80 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $299.10 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.80 | ▲ close $10,851.86 vs 09:30 $10,825.27 (session +69.98) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.80 | ▲ 09:30 equity $10,863.80 vs yday $10,851.86 (+11.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 2 | $2.35 | $0.07 | $-0.37 | $60.42 | ▼ -0.37 after sell → book $10,863.72; vs 09:30 mark -0.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 2 | $2.06 | $0.07 | $+0.15 | $64.48 | ▲ +0.15 after sell → book $10,863.66; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 1 | $1.82 | $0.04 | $+0.44 | $66.25 | ▲ +0.44 after sell → book $10,863.61; vs 09:30 mark -0.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 1 | $15.66 | $0.16 | — | $50.44 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $19.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 1 | $3.32 | $0.04 | — | $47.08 | — | rank-weighted leftover; list probable,yday_gainer; ret5=+6.4; leftover $6.63 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.08 | ▼ close $10,624.80 vs 09:30 $10,863.80 (session -238.62) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.08 | ▲ 09:30 equity $10,709.82 vs yday $10,624.80 (+85.02) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.08 | ▲ close $10,733.22 vs 09:30 $10,709.82 (session +23.39) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.08 | ▲ 09:30 equity $10,931.02 vs yday $10,733.22 (+197.80) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $57.37 | ▼ -0.80 after sell → book $10,930.89; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $65.52 | ▼ -0.29 after sell → book $10,930.79; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.64 | $0.07 | $-0.42 | $70.09 | ▼ -0.42 after sell → book $10,930.72; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 6 | $0.36 | $0.06 | $-0.06 | $72.22 | ▼ -0.06 after sell → book $10,930.66; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.22 | ▲ close $10,962.87 vs 09:30 $10,931.02 (session +32.21) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.22 | ▼ 09:30 equity $10,900.32 vs yday $10,962.87 (-62.55) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.22 | ▲ close $10,974.39 vs 09:30 $10,900.32 (session +74.07) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.22 | ▲ 09:30 equity $11,019.93 vs yday $10,974.39 (+45.54) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 57 | $42.43 | $2.19 | $+52.08 | $2,488.54 | ▲ +52.08 after sell → book $11,017.74; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 74 | $26.12 | $2.24 | $+152.43 | $4,419.18 | ▲ +152.43 after sell → book $11,015.50; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 575 | $2.49 | $7.52 | $-78.19 | $5,843.40 | ▼ -78.19 after sell → book $11,007.97; vs 09:30 mark -7.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 92 | $13.25 | $2.29 | $+20.28 | $7,060.11 | ▲ +20.28 after sell → book $11,005.68; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AVBP` | 29 | $30.58 | $2.10 | $-10.26 | $7,944.83 | ▼ -10.26 after sell → book $11,003.59; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `FLNC` | 51 | $10.01 | $2.16 | $-81.21 | $8,453.28 | ▼ -81.21 after sell → book $11,001.42; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 30 | $9.68 | $2.10 | $-4.18 | $8,741.58 | ▼ -4.18 after sell → book $10,999.32; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 41 | $52.88 | $2.11 | — | $6,571.39 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2185.40 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 43 | $42.93 | $2.12 | — | $4,723.28 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1873.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 430 | $3.63 | $5.55 | — | $3,156.83 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1561.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 155 | $8.03 | $2.46 | — | $1,909.73 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1248.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $132.45 | $2.01 | — | $980.57 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $936.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 4 | $145.94 | $2.00 | — | $394.79 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $624.40 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 18 | $16.77 | $2.04 | — | $90.88 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $312.20 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.88 | ▼ close $10,750.03 vs 09:30 $11,019.93 (session -231.01) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.88 | ▼ 09:30 equity $10,720.22 vs yday $10,750.03 (-29.81) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 145 | $15.00 | $2.47 | $+79.21 | $2,263.42 | ▲ +79.21 after sell → book $10,717.76; vs 09:30 mark -2.46 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 1 | $13.56 | $0.16 | $-2.42 | $2,276.82 | ▼ -2.42 after sell → book $10,717.60; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 1 | $3.53 | $0.06 | $+0.12 | $2,280.29 | ▲ +0.12 after sell → book $10,717.54; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 258 | $2.52 | $3.33 | — | $1,626.80 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $651.51 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 80 | $6.71 | $2.23 | — | $1,087.77 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $542.93 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 228 | $1.90 | $2.94 | — | $651.63 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $434.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 68 | $4.78 | $2.19 | — | $324.40 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $325.76 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 136 | $1.59 | $2.40 | — | $105.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $217.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 9 | $11.31 | $1.04 | — | $2.92 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $108.59 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.92 | ▲ close $10,715.86 vs 09:30 $10,720.22 (session +12.45) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.92 | ▲ 09:30 equity $10,762.71 vs yday $10,715.86 (+46.85) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.92 | ▼ close $10,602.46 vs 09:30 $10,762.71 (session -160.25) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.92 | ▼ 09:30 equity $10,551.21 vs yday $10,602.46 (-51.25) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.92 | ▼ close $10,202.39 vs 09:30 $10,551.21 (session -348.82) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.92 | ▼ 09:30 equity $10,078.12 vs yday $10,202.39 (-124.27) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.92 | ▼ close $9,937.36 vs 09:30 $10,078.12 (session -140.76) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.92 | ▲ 09:30 equity $10,037.82 vs yday $9,937.36 (+100.46) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 41 | $53.53 | $2.14 | $+22.40 | $2,195.51 | ▲ +22.40 after sell → book $10,035.68; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 43 | $41.30 | $2.14 | $-74.35 | $3,969.27 | ▼ -74.35 after sell → book $10,033.54; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 430 | $2.77 | $5.63 | $-380.98 | $5,154.74 | ▼ -380.98 after sell → book $10,027.91; vs 09:30 mark -5.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 155 | $7.70 | $2.49 | $-56.10 | $6,345.75 | ▼ -56.10 after sell → book $10,025.42; vs 09:30 mark -2.49 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 7 | $122.40 | $2.03 | $-74.39 | $7,200.52 | ▼ -74.39 after sell → book $10,023.39; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 4 | $137.91 | $2.02 | $-36.18 | $7,750.12 | ▼ -36.18 after sell → book $10,021.37; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 18 | $14.06 | $2.06 | $-52.89 | $8,001.13 | ▼ -52.89 after sell → book $10,019.30; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 109 | $16.28 | $2.32 | — | $6,224.30 | — | rank-weighted leftover; list flatten; 🔵; ret5=-1.1; leftover $1778.03 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 569 | $2.73 | $7.34 | — | $4,663.59 | — | rank-weighted leftover; list flatten; 🔵; ret5=-3.0; leftover $1555.78 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $3,420.54 | — | rank-weighted leftover; list flatten; ret5=+8.3; leftover $1333.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $2,431.95 | — | rank-weighted leftover; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1111.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 5 | $157.78 | $2.00 | — | $1,641.04 | — | rank-weighted leftover; list flatten; 🔵; ret5=+4.7; leftover $889.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 11 | $56.09 | $2.02 | — | $1,022.03 | — | rank-weighted leftover; list flatten; 🔵; ret5=+19.6; leftover $666.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 217 | $2.04 | $2.80 | — | $576.55 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $444.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 46 | $4.75 | $2.13 | — | $355.92 | — | rank-weighted leftover; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $222.25 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $355.92 | ▼ close $9,940.28 vs 09:30 $10,037.82 (session -56.39) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $355.92 | ▼ 09:30 equity $9,754.50 vs yday $9,940.28 (-185.78) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 258 | $2.15 | $3.38 | $-102.17 | $907.24 | ▼ -102.17 after sell → book $9,751.12; vs 09:30 mark -3.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 80 | $5.93 | $2.25 | $-66.88 | $1,379.39 | ▼ -66.88 after sell → book $9,748.87; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 228 | $1.72 | $2.99 | $-48.11 | $1,767.42 | ▼ -48.11 after sell → book $9,745.88; vs 09:30 mark -2.99 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 68 | $4.13 | $2.22 | $-48.61 | $2,046.05 | ▼ -48.61 after sell → book $9,743.66; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 136 | $1.59 | $2.43 | $-4.83 | $2,259.86 | ▼ -4.83 after sell → book $9,741.23; vs 09:30 mark -2.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 9 | $10.73 | $1.01 | $-7.28 | $2,355.41 | ▼ -7.28 after sell → book $9,740.22; vs 09:30 mark -1.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,355.41 | ▼ close $9,688.76 vs 09:30 $9,754.50 (session -51.45) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,355.41 | ▲ 09:30 equity $9,710.55 vs yday $9,688.76 (+21.79) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,355.41 | ▼ close $9,559.83 vs 09:30 $9,710.55 (session -150.72) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,355.41 | ▲ 09:30 equity $9,604.60 vs yday $9,559.83 (+44.77) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 1 | $270.89 | $1.99 | — | $2,082.53 | — | rank-weighted leftover; list flatten; ret5=+4.0; leftover $523.42 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 5 | $77.12 | $2.00 | — | $1,694.92 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+7.2; leftover $458.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 27 | $14.31 | $2.07 | — | $1,306.48 | — | rank-weighted leftover; list flatten; ret5=+4.8; leftover $392.57 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 8 | $36.46 | $2.01 | — | $1,012.79 | — | rank-weighted leftover; list flatten; 🔵; ret5=+2.9; leftover $327.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 14 | $18.61 | $2.03 | — | $750.22 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $261.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 10 | $18.21 | $1.85 | — | $566.27 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-19.1; leftover $196.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 1 | $68.79 | $0.69 | — | $496.79 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $130.86 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 11 | $5.87 | $0.68 | — | $431.54 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $65.43 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $431.54 | ▲ close $9,713.68 vs 09:30 $9,604.60 (session +122.41) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $431.54 | ▲ 09:30 equity $9,886.32 vs yday $9,713.68 (+172.64) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 5 | $10.25 | $0.53 | — | $379.76 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $59.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 6 | $7.59 | $0.47 | — | $333.75 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $47.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $298.46 | — | rank-weighted leftover; list flatten; ret5=+1.6; leftover $35.96 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 141 | $0.17 | $0.66 | — | $273.83 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $23.97 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.83 | ▼ close $9,839.72 vs 09:30 $9,886.32 (session -44.58) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.83 | ▲ 09:30 equity $9,854.02 vs yday $9,839.72 (+14.30) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 109 | $16.93 | $2.35 | $+66.18 | $2,116.85 | ▲ +66.18 after sell → book $9,851.67; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 569 | $2.68 | $7.45 | $-43.24 | $3,634.32 | ▼ -43.24 after sell → book $9,844.22; vs 09:30 mark -7.45 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 6 | $197.76 | $2.03 | $-58.52 | $4,818.86 | ▼ -58.52 after sell → book $9,842.20; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 6 | $150.47 | $2.03 | $-87.80 | $5,719.65 | ▼ -87.80 after sell → book $9,840.17; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 5 | $152.71 | $2.02 | $-29.38 | $6,481.17 | ▼ -29.38 after sell → book $9,838.14; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 11 | $55.80 | $2.04 | $-7.26 | $7,092.93 | ▼ -7.26 after sell → book $9,836.10; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 217 | $1.90 | $2.85 | $-36.02 | $7,502.38 | ▼ -36.02 after sell → book $9,833.25; vs 09:30 mark -2.85 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 46 | $4.50 | $2.15 | $-15.78 | $7,707.24 | ▼ -15.78 after sell → book $9,831.11; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 15 | $108.55 | $2.04 | — | $6,076.95 | — | rank-weighted leftover; list flatten; ⚪; ret5=+21.3; leftover $1712.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $4,888.66 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+16.1; leftover $1498.63 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $3,629.53 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1284.54 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $2,749.05 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1070.45 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 10 | $85.00 | $2.02 | — | $1,897.03 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $856.36 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 18 | $34.44 | $2.04 | — | $1,275.06 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $642.27 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 441 | $0.97 | $5.60 | — | $841.69 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $428.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 102 | $2.08 | $2.30 | — | $627.24 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $214.09 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $627.24 | ▼ close $9,664.11 vs 09:30 $9,854.02 (session -147.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $627.24 | ▲ 09:30 equity $9,773.89 vs yday $9,664.11 (+109.78) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $467.78 | — | rank-weighted leftover; list flatten; ret5=+6.5; leftover $179.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 1 | $88.83 | $0.89 | — | $378.06 | — | rank-weighted leftover; list flatten; ret5=+7.6; leftover $119.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 6 | $13.47 | $0.83 | — | $296.42 | — | rank-weighted leftover; list flatten; ret5=+3.6; leftover $89.61 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 14 | $4.00 | $0.60 | — | $239.81 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $59.74 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 3 | $9.31 | $0.29 | — | $211.60 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $29.87 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.60 | ▲ close $9,808.29 vs 09:30 $9,773.89 (session +38.59) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.60 | ▼ 09:30 equity $9,797.73 vs yday $9,808.29 (-10.56) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 10 | $0.58 | $0.09 | — | $205.71 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $5.88 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.71 | ▼ close $9,762.80 vs 09:30 $9,797.73 (session -34.84) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.71 | ▲ 09:30 equity $9,956.94 vs yday $9,762.80 (+194.14) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 1 | $270.66 | $2.01 | $-4.24 | $474.35 | ▼ -4.24 after sell → book $9,954.92; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 5 | $73.61 | $2.02 | $-21.58 | $840.38 | ▼ -21.58 after sell → book $9,952.90; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 27 | $13.12 | $2.09 | $-36.29 | $1,192.53 | ▼ -36.29 after sell → book $9,950.81; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 8 | $38.04 | $2.03 | $+8.59 | $1,494.81 | ▲ +8.59 after sell → book $9,948.77; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 14 | $23.00 | $2.05 | $+57.38 | $1,814.76 | ▲ +57.38 after sell → book $9,946.72; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 10 | $23.30 | $2.04 | $+47.01 | $2,045.72 | ▲ +47.01 after sell → book $9,944.68; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 1 | $76.47 | $0.79 | $+6.20 | $2,121.41 | ▲ +6.20 after sell → book $9,943.90; vs 09:30 mark -0.78 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 11 | $5.53 | $0.66 | $-5.08 | $2,181.57 | ▼ -5.08 after sell → book $9,943.23; vs 09:30 mark -0.67 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 6 | $116.85 | $2.01 | — | $1,478.47 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $727.19 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 20 | $27.79 | $2.05 | — | $920.62 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $581.75 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 44 | $9.81 | $2.12 | — | $486.85 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $436.31 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 14 | $20.25 | $2.03 | — | $201.32 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $290.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 7 | $20.65 | $1.47 | — | $55.31 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $145.44 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.31 | ▲ close $9,990.12 vs 09:30 $9,956.94 (session +56.56) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.31 | ▼ 09:30 equity $9,874.19 vs yday $9,990.12 (-115.93) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 5 | $10.39 | $0.55 | $-0.38 | $106.70 | ▼ -0.38 after sell → book $9,873.64; vs 09:30 mark -0.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 6 | $7.38 | $0.48 | $-2.21 | $150.50 | ▼ -2.21 after sell → book $9,873.16; vs 09:30 mark -0.48 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 1 | $33.82 | $0.36 | $-1.82 | $183.96 | ▼ -1.82 after sell → book $9,872.80; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 141 | $0.15 | $0.67 | $-4.15 | $204.44 | ▼ -4.15 after sell → book $9,872.13; vs 09:30 mark -0.67 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.44 | ▲ close $9,936.44 vs 09:30 $9,874.19 (session +64.32) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.55 | ▼ 09:30 equity $8,729.57 vs yday $8,730.16 (-0.59) | 09:30 open · cash $184.55 (unchanged overnight, no fees) · equity $8,729.57 vs prior close $8,730.16 (-0.59) · 12 name(s) re-marked at the open (per-name table). A×11 yday $172.84 → 09:30 $171.98 -9.46; ADMA×96 yday $9.52 → 09:30 $9.52 +0.00; ARQT×45 yday $26.27 → 09:30 $26.27 +0.00; CYPH×2 yday $4.08 → 09:30 $4.00 -0.15; DEFT×4 yday $0.53 → 09:30 $0.53 +0.00; DXCM×24 yday $87.47 → 09:30 $87.47 +0.00; EYPT×1 yday $3.65 → 09:30 $3.65 +0.00; FJET×2 yday $1.80 → 09:30 $1.80 +0.00; FTRE×31 yday $20.02 → 09:30 $20.02 +0.00; HALO×13 yday $115.22 → 09:30 $115.36 +1.82; MGTX×1 yday $11.05 → 09:30 $11.05 +0.00; OMER×15 yday $20.13 → 09:30 $20.61 +7.20 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 1 | $38.51 | $0.39 | — | $145.65 | — | rank-weighted leftover; list flatten; ret5=+4.7; leftover $43.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 4 | $7.65 | $0.32 | — | $114.73 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.2; leftover $35.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 1 | $26.27 | $0.27 | — | $88.20 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $26.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.20 | ▼ close $8,711.03 vs 09:30 $8,729.57 (session -17.57) | 16:00 close · cash $88.20 · equity $8,711.03 vs 09:30 $8,729.57 (-18.54; session marks -17.57) · 15 name(s) marked open→close (per-name table). A×11 09:30 $171.98 → close $172.79 +8.91; ADMA×96 09:30 $9.52 → close $9.52 +0.00; ARQT×45 09:30 $26.27 → close $26.27 +0.00; CYPH×2 09:30 $4.00 → close $4.12 +0.23; DEFT×4 09:30 $0.53 → close $0.53 +0.00; DXCM×24 09:30 $87.47 → close $87.47 +0.00; EYPT×1 09:30 $3.65 → close $3.65 +0.00; FJET×2 09:30 $1.80 → close $1.80 -0.00; FTRE×31 09:30 $20.02 → close $20.02 +0.00; HALO×13 09:30 $115.36 → close $113.90 -18.98; MGTX×1 09:30 $11.05 → close $11.05 +0.00; OMER×15 09:30 $20.61 → close $20.08 -7.95; BLFS×1 09:30 $38.51 → close $38.49 -0.02; MRVI×4 09:30 $7.65 → close $7.60 -0.20; WRBY×1 09:30 $26.27 → close $26.71 +0.44 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
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
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
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
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 14.68 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 12.84 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 11.01 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.17 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 3.67 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 18.25 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 13.68 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 6.84 < 1 share @ 426.97 |
| 2026-08-25 | `CAPR` | cash | leftover split 4.56 < 1 share @ 7.25 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 18.47 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 14.77 < 1 share @ 24.84 |
| 2026-08-26 | `INSP` | cash | leftover split 11.08 < 1 share @ 60.07 |
| 2026-08-26 | `AVBP` | cash | leftover split 7.39 < 1 share @ 31.21 |
| 2026-08-26 | `FLNC` | cash | leftover split 3.69 < 1 share @ 11.12 |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `SEDG` | cash | leftover split 26.50 < 1 share @ 32.90 |
| 2026-08-28 | `URBN` | cash | leftover split 13.25 < 1 share @ 79.42 |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-17 | `ILMN` | cash | leftover split 95.90 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 83.91 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 71.92 < 1 share @ 147.61 |
| 2026-09-17 | `BRUN` | cash | leftover split 11.99 < 1 share @ 15.87 |
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
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `IQV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `HUM` | cash | leftover split 149.34 < 1 share @ 386.20 |
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
| 2026-09-22 | `AMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 29.39 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `AMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| `RBRK` | 15 | 2026-09-18 @ $108.55 | rank-weighted leftover; list flatten; ⚪; ret5=+21.3; leftover $1712.72 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | rank-weighted leftover; list flatten,ohlc_hot; ret5=+16.1; leftover $1498.63 |
| `GNRC` | 6 | 2026-09-18 @ $209.52 | rank-weighted leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1284.54 |
| `VICR` | 4 | 2026-09-18 @ $219.62 | rank-weighted leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1070.45 |
| `ECO` | 10 | 2026-09-18 @ $85.00 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $856.36 |
| `FIVN` | 18 | 2026-09-18 @ $34.44 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $642.27 |
| `TLSA` | 441 | 2026-09-18 @ $0.97 | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $428.18 |
| `SWRD` | 102 | 2026-09-18 @ $2.08 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $214.09 |
| `A` | 1 | 2026-09-21 @ $157.87 | rank-weighted leftover; list flatten; ret5=+6.5; leftover $179.21 |
| `DXCM` | 1 | 2026-09-21 @ $88.83 | rank-weighted leftover; list flatten; ret5=+7.6; leftover $119.47 |
| `MGTX` | 6 | 2026-09-21 @ $13.47 | rank-weighted leftover; list flatten; ret5=+3.6; leftover $89.61 |
| `CYPH` | 14 | 2026-09-21 @ $4.00 | rank-weighted leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $59.74 |
| `BKKT` | 3 | 2026-09-21 @ $9.31 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $29.87 |
| `DEFT` | 10 | 2026-09-22 @ $0.58 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $5.88 |
| `HALO` | 6 | 2026-09-23 @ $116.85 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $727.19 |
| `ARQT` | 20 | 2026-09-23 @ $27.79 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $581.75 |
| `ADMA` | 44 | 2026-09-23 @ $9.81 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $436.31 |
| `FTRE` | 14 | 2026-09-23 @ $20.25 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $290.88 |
| `OMER` | 7 | 2026-09-23 @ $20.65 | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $145.44 |
