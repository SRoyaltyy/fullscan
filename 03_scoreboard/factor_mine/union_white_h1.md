# Factor mine action — `union_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **-13.28%** ($8,672) · signal-only (no cash/fees) was -8.86%. Starts YES **3/30**. Fills 198 · skips 0 · realized $-116.79.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: no morning camera is red (the 'white' / all-clear row).
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
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,883.18.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,149.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $7,879.70 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,615.89 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $5,331.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $4,053.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 85 | $14.80 | $2.25 | — | $2,792.75 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 294 | $4.31 | $3.79 | — | $1,521.82 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 303 | $4.18 | $3.91 | — | $251.37 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1267.99 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.37 | ▼ close $9,999.36 vs 09:30 $10,178.12 (session -100.50) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.37 | ▲ 09:30 equity $10,039.35 vs yday $9,999.36 (+39.99) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,260.17 | ▲ +14.07 after sell → book $10,037.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,476.23 | ▼ -53.41 after sell → book $10,035.26; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $3,764.59 | ▲ +24.55 after sell → book $10,032.81; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $4,975.20 | ▼ -73.89 after sell → book $10,016.25; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $6,248.55 | ▼ -5.05 after sell → book $10,005.20; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 85 | $13.67 | $2.27 | $-100.56 | $7,408.23 | ▼ -100.56 after sell → book $10,002.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 294 | $4.60 | $3.85 | $+77.62 | $8,756.77 | ▲ +77.62 after sell → book $9,999.07; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 303 | $4.10 | $3.97 | $-32.12 | $9,995.11 | ▼ -32.12 after sell → book $9,995.11; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 308 | $4.05 | $3.97 | — | $8,743.73 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $7,497.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 385 | $3.24 | $4.97 | — | $6,245.31 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,007.88 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $3,765.16 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $2,522.65 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,273.03 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1249.39 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $34.02 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1249.39 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.02 | ▼ close $9,738.55 vs 09:30 $10,039.35 (session -234.18) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.02 | ▼ 09:30 equity $9,553.81 vs yday $9,738.55 (-184.74) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 308 | $3.72 | $4.03 | $-109.65 | $1,175.75 | ▼ -109.65 after sell → book $9,549.78; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $2,430.13 | ▲ +8.33 after sell → book $9,547.31; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 385 | $3.11 | $5.04 | $-60.06 | $3,622.44 | ▼ -60.06 after sell → book $9,542.27; vs 09:30 mark -5.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,909.01 | ▲ +49.13 after sell → book $9,540.17; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $6,134.66 | ▼ -17.07 after sell → book $9,537.74; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $7,234.05 | ▼ -143.13 after sell → book $9,535.53; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,446.86 | ▼ -36.80 after sell → book $9,533.28; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $9,531.16 | ▼ -154.71 after sell → book $9,531.16; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,531.16 | ▲ close $9,531.16 vs 09:30 $9,553.81 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,531.16 | ▲ 09:30 equity $9,531.16 vs yday $9,531.16 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,531.16 | ▲ close $9,531.16 vs 09:30 $9,531.16 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,531.16 | ▲ 09:30 equity $9,531.16 vs yday $9,531.16 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,357.65 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,172.49 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,993.28 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $4,802.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,622.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,434.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 680 | $1.75 | $8.77 | — | $1,235.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1191.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $77.61 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1191.39 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.61 | ▲ close $9,732.52 vs 09:30 $9,531.16 (session +225.44) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.61 | ▲ 09:30 equity $9,989.84 vs yday $9,732.52 (+257.32) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,323.73 | ▲ +72.61 after sell → book $9,987.66; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,566.04 | ▲ +57.15 after sell → book $9,985.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,803.61 | ▲ +58.36 after sell → book $9,983.43; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 206 | $5.67 | $2.70 | $-25.96 | $4,968.93 | ▼ -25.96 after sell → book $9,980.73; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,236.94 | ▲ +88.04 after sell → book $9,978.54; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,521.61 | ▲ +97.36 after sell → book $9,976.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 680 | $1.79 | $8.89 | $+9.53 | $8,729.91 | ▲ +9.53 after sell → book $9,967.51; vs 09:30 mark -8.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,965.48 | ▲ +77.23 after sell → book $9,965.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,769.16 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,528.55 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,445.05 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,207.30 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 504 | $2.47 | $6.50 | — | $3,955.92 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 645 | $1.93 | $8.32 | — | $2,702.74 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,506.29 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1245.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 943 | $1.32 | $12.16 | — | $249.37 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1245.69 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.37 | ▲ close $10,177.02 vs 09:30 $9,989.84 (session +249.13) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $249.37 | ▲ 09:30 equity $10,535.18 vs yday $10,177.02 (+358.16) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,452.43 | ▲ +6.74 after sell → book $10,533.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,643.24 | ▼ -49.79 after sell → book $10,530.91; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,726.37 | ▼ -0.38 after sell → book $10,528.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.33 | $2.35 | $+239.52 | $5,203.64 | ▲ +239.52 after sell → book $10,526.53; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 504 | $2.40 | $6.60 | $-48.38 | $6,406.65 | ▼ -48.38 after sell → book $10,519.94; vs 09:30 mark -6.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 645 | $1.88 | $8.44 | $-49.01 | $7,610.81 | ▼ -49.01 after sell → book $10,511.50; vs 09:30 mark -8.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,783.74 | ▼ -23.52 after sell → book $10,509.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 943 | $1.83 | $12.34 | $+456.43 | $10,497.10 | ▲ +456.43 after sell → book $10,497.10; vs 09:30 mark -12.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,497.10 | ▲ close $10,497.10 vs 09:30 $10,535.18 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,497.10 | ▲ 09:30 equity $10,497.10 vs yday $10,497.10 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,187.59 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 157 | $8.35 | $2.46 | — | $7,874.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 804 | $1.63 | $10.37 | — | $6,553.29 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 250 | $5.24 | $3.23 | — | $5,240.06 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 149 | $8.79 | $2.44 | — | $3,927.92 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 841 | $1.56 | $10.85 | — | $2,605.11 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2116 | $0.62 | $19.47 | — | $1,273.72 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1312.14 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.37 | $2.59 | — | $3.50 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1312.14 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.50 | ▲ close $10,640.85 vs 09:30 $10,497.10 (session +197.30) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.50 | ▼ 09:30 equity $10,605.87 vs yday $10,640.85 (-34.98) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 55 | $24.84 | $2.18 | $+54.52 | $1,367.53 | ▲ +54.52 after sell → book $10,603.70; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 157 | $8.60 | $2.50 | $+34.29 | $2,715.23 | ▲ +34.29 after sell → book $10,601.20; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 804 | $1.75 | $10.52 | $+79.61 | $4,115.73 | ▲ +79.61 after sell → book $10,590.68; vs 09:30 mark -10.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 250 | $4.98 | $3.28 | $-71.50 | $5,357.46 | ▼ -71.50 after sell → book $10,587.40; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 149 | $9.39 | $2.47 | $+84.49 | $6,754.09 | ▲ +84.49 after sell → book $10,584.93; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 841 | $1.60 | $11.00 | $+11.79 | $8,088.69 | ▲ +11.79 after sell → book $10,573.93; vs 09:30 mark -11.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2116 | $0.60 | $19.36 | $-85.38 | $9,334.70 | ▼ -85.38 after sell → book $10,554.57; vs 09:30 mark -19.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-52.98 | $10,551.94 | ▼ -52.98 after sell → book $10,551.94; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1812 | $5.81 | $23.37 | — | $0.84 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10551.94 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.84 | ▲ close $10,836.60 vs 09:30 $10,605.87 (session +308.04) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▲ 09:30 equity $11,778.84 vs yday $10,836.60 (+942.24) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1812 | $6.50 | $23.77 | $+1203.14 | $11,755.07 | ▲ +1,203.14 after sell → book $11,755.07; vs 09:30 mark -23.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,755.07 | ▲ close $11,755.07 vs 09:30 $11,778.84 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,755.07 | ▲ 09:30 equity $11,755.07 vs yday $11,755.07 (+0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,491.87 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $9,072.25 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $7,719.32 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,419.67 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 16 | $91.49 | $2.04 | — | $4,953.80 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 23 | $62.82 | $2.06 | — | $3,506.88 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 5 | $289.44 | $2.00 | — | $2,057.67 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1469.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 12 | $119.76 | $2.03 | — | $618.53 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1469.38 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $618.53 | ▼ close $11,321.77 vs 09:30 $11,755.07 (session -417.13) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $618.53 | ▲ 09:30 equity $11,382.66 vs yday $11,321.77 (+60.89) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $1,851.75 | ▼ -29.98 after sell → book $11,380.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $3,172.71 | ▼ -98.66 after sell → book $11,378.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 11 | $118.83 | $2.04 | $-47.85 | $4,477.80 | ▼ -47.85 after sell → book $11,376.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,765.74 | ▼ -11.70 after sell → book $11,374.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 16 | $89.39 | $2.06 | $-37.70 | $7,193.92 | ▼ -37.70 after sell → book $11,372.47; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 23 | $60.46 | $2.08 | $-58.42 | $8,582.42 | ▼ -58.42 after sell → book $11,370.39; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 5 | $280.25 | $2.03 | $-49.98 | $9,981.64 | ▼ -49.98 after sell → book $11,368.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 12 | $115.56 | $2.05 | $-54.47 | $11,366.31 | ▼ -54.47 after sell → book $11,366.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,366.31 | ▲ close $11,366.31 vs 09:30 $11,382.66 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,366.31 | ▲ 09:30 equity $11,366.31 vs yday $11,366.31 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,366.31 | ▲ close $11,366.31 vs 09:30 $11,366.31 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,366.31 | ▲ 09:30 equity $11,366.31 vs yday $11,366.31 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,366.31 | ▲ close $11,366.31 vs 09:30 $11,366.31 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,366.31 | ▲ 09:30 equity $11,366.31 vs yday $11,366.31 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $9,989.36 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $42.93 | $2.09 | — | $8,570.59 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 391 | $3.63 | $5.04 | — | $7,146.21 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 176 | $8.03 | $2.52 | — | $5,730.41 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,403.89 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 84 | $16.77 | $2.24 | — | $2,992.97 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 95 | $14.85 | $2.27 | — | $1,579.95 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1420.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 651 | $2.18 | $8.40 | — | $152.37 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1420.79 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.37 | ▼ close $11,087.42 vs 09:30 $11,366.31 (session -252.24) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.37 | ▼ 09:30 equity $11,027.22 vs yday $11,087.42 (-60.20) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 33 | $41.50 | $2.11 | $-51.39 | $1,519.76 | ▼ -51.39 after sell → book $11,025.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 176 | $7.91 | $2.56 | $-26.20 | $2,909.36 | ▼ -26.20 after sell → book $11,022.55; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,207.62 | ▼ -28.26 after sell → book $11,020.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 84 | $15.61 | $2.27 | $-101.95 | $5,516.59 | ▼ -101.95 after sell → book $11,018.24; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 95 | $14.63 | $2.30 | $-25.48 | $6,904.14 | ▼ -25.48 after sell → book $11,015.94; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 651 | $2.16 | $8.52 | $-29.94 | $8,301.78 | ▼ -29.94 after sell → book $11,007.42; vs 09:30 mark -8.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 549 | $2.52 | $7.08 | — | $6,911.22 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1383.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 206 | $6.71 | $2.66 | — | $5,526.30 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1383.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 728 | $1.90 | $9.39 | — | $4,133.71 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1383.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 289 | $4.78 | $3.73 | — | $2,748.57 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1383.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 870 | $1.59 | $11.22 | — | $1,354.04 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1383.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 119 | $11.31 | $2.35 | — | $5.81 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1383.63 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.81 | ▼ close $10,914.82 vs 09:30 $11,027.22 (session -56.18) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.81 | ▼ 09:30 equity $10,871.02 vs yday $10,914.82 (-43.80) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 26 | $54.31 | $2.09 | $+33.02 | $1,415.78 | ▲ +33.02 after sell → book $10,868.93; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 391 | $3.43 | $5.12 | $-88.36 | $2,751.79 | ▼ -88.36 after sell → book $10,863.81; vs 09:30 mark -5.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 549 | $2.38 | $7.18 | $-91.13 | $4,051.22 | ▼ -91.13 after sell → book $10,856.62; vs 09:30 mark -7.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 206 | $6.57 | $2.70 | $-34.20 | $5,401.94 | ▼ -34.20 after sell → book $10,853.92; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 728 | $2.00 | $9.52 | $+53.89 | $6,848.42 | ▲ +53.89 after sell → book $10,844.40; vs 09:30 mark -9.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 289 | $4.30 | $3.79 | $-146.23 | $8,087.33 | ▼ -146.23 after sell → book $10,840.61; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 870 | $1.63 | $11.38 | $+12.20 | $9,494.05 | ▲ +12.20 after sell → book $10,829.23; vs 09:30 mark -11.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 119 | $11.22 | $2.38 | $-15.43 | $10,826.85 | ▼ -15.43 after sell → book $10,826.85; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.85 | ▲ close $10,826.85 vs 09:30 $10,871.02 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,826.85 | ▲ 09:30 equity $10,826.85 vs yday $10,826.85 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.85 | ▲ close $10,826.85 vs 09:30 $10,826.85 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,826.85 | ▲ 09:30 equity $10,826.85 vs yday $10,826.85 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.85 | ▲ close $10,826.85 vs 09:30 $10,826.85 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,826.85 | ▲ 09:30 equity $10,826.85 vs yday $10,826.85 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 13 | $164.43 | $2.03 | — | $8,687.24 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2165.37 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 41 | $52.55 | $2.11 | — | $6,530.57 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2165.37 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 214 | $10.11 | $2.76 | — | $4,364.27 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2165.37 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 666 | $3.25 | $8.59 | — | $2,191.18 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $2165.37 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 118 | $18.30 | $2.34 | — | $29.44 | — | union ∩ white, no 🚨; gate zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2165.37 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.44 | ▼ close $10,702.15 vs 09:30 $10,826.85 (session -106.87) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.44 | ▼ 09:30 equity $10,535.80 vs yday $10,702.15 (-166.35) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 13 | $141.42 | $2.05 | $-303.21 | $1,865.84 | ▼ -303.21 after sell → book $10,533.74; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 41 | $56.90 | $2.14 | $+174.10 | $4,196.60 | ▲ +174.10 after sell → book $10,531.60; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 214 | $10.00 | $2.81 | $-29.11 | $6,333.79 | ▼ -29.11 after sell → book $10,528.79; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 666 | $3.06 | $8.72 | $-143.85 | $8,363.03 | ▼ -143.85 after sell → book $10,520.07; vs 09:30 mark -8.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 118 | $18.28 | $2.38 | $-7.08 | $10,517.69 | ▼ -7.08 after sell → book $10,517.69; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,517.69 | ▲ close $10,517.69 vs 09:30 $10,535.80 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,517.69 | ▲ 09:30 equity $10,517.69 vs yday $10,517.69 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,517.69 | ▲ close $10,517.69 vs 09:30 $10,517.69 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,517.69 | ▲ 09:30 equity $10,517.69 vs yday $10,517.69 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 58 | $89.38 | $2.16 | — | $5,331.48 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5258.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 44 | $118.18 | $2.12 | — | $129.44 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5258.84 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.44 | ▼ close $10,108.34 vs 09:30 $10,517.69 (session -405.06) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.44 | ▲ 09:30 equity $10,217.12 vs yday $10,108.34 (+108.78) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 58 | $86.76 | $2.21 | $-156.34 | $5,159.31 | ▼ -156.34 after sell → book $10,214.91; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 44 | $114.90 | $2.17 | $-148.61 | $10,212.74 | ▼ -148.61 after sell → book $10,212.74; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 1282 | $7.95 | $16.54 | — | $4.30 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $10212.74 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.30 | ▼ close $9,888.52 vs 09:30 $10,217.12 (session -307.68) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.30 | ▲ 09:30 equity $10,068.00 vs yday $9,888.52 (+179.48) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 1282 | $7.85 | $16.83 | $-161.57 | $10,051.17 | ▼ -161.57 after sell → book $10,051.17; vs 09:30 mark -16.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $8,855.09 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+21.3; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $7,663.06 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 36 | $34.44 | $2.10 | — | $6,421.12 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 84 | $14.79 | $2.24 | — | $5,176.52 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $3,942.97 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 413 | $3.04 | $5.33 | — | $2,684.18 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 15 | $81.40 | $2.04 | — | $1,461.15 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1256.40 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 318 | $3.94 | $4.10 | — | $204.13 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1256.40 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.13 | ▲ close $10,036.64 vs 09:30 $10,068.00 (session +7.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.13 | ▲ 09:30 equity $10,274.20 vs yday $10,036.64 (+237.56) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $1,385.35 | ▼ -14.85 after sell → book $10,272.15; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $2,542.92 | ▼ -34.46 after sell → book $10,270.10; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 36 | $33.00 | $2.12 | $-56.06 | $3,728.80 | ▼ -56.06 after sell → book $10,267.98; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 84 | $14.58 | $2.27 | $-22.15 | $4,951.26 | ▼ -22.15 after sell → book $10,265.72; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+0.37 | $6,185.18 | ▲ +0.37 after sell → book $10,263.58; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 413 | $4.00 | $5.41 | $+387.81 | $7,831.77 | ▲ +387.81 after sell → book $10,258.17; vs 09:30 mark -5.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 15 | $79.08 | $2.06 | $-38.89 | $9,015.92 | ▼ -38.89 after sell → book $10,256.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 318 | $3.90 | $4.17 | $-20.99 | $10,251.95 | ▼ -20.99 after sell → book $10,251.95; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,251.95 | ▲ close $10,251.95 vs 09:30 $10,274.20 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,251.95 | ▲ 09:30 equity $10,251.95 vs yday $10,251.95 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,251.95 | ▲ close $10,251.95 vs 09:30 $10,251.95 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,251.95 | ▲ 09:30 equity $10,251.95 vs yday $10,251.95 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 14 | $89.50 | $2.03 | — | $8,996.92 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $7,829.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,658.61 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 46 | $27.79 | $2.13 | — | $5,378.14 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 161 | $7.95 | $2.47 | — | $4,095.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 130 | $9.81 | $2.38 | — | $2,818.04 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 63 | $20.25 | $2.18 | — | $1,540.11 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1281.49 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 62 | $20.65 | $2.18 | — | $257.63 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1281.49 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $257.63 | ▼ close $9,955.45 vs 09:30 $10,251.95 (session -279.10) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $257.63 | ▼ 09:30 equity $9,900.77 vs yday $9,955.45 (-54.68) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-29.63 | $1,483.03 | ▼ -29.63 after sell → book $9,898.72; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $-22.17 | $2,628.65 | ▼ -22.17 after sell → book $9,896.69; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $3,748.81 | ▼ -50.36 after sell → book $9,894.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 46 | $26.22 | $2.15 | $-76.50 | $4,952.78 | ▼ -76.50 after sell → book $9,892.50; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 161 | $7.38 | $2.51 | $-96.75 | $6,138.45 | ▼ -96.75 after sell → book $9,889.99; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 130 | $9.67 | $2.41 | $-22.99 | $7,393.14 | ▼ -22.99 after sell → book $9,887.58; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 63 | $19.40 | $2.20 | $-57.93 | $8,613.14 | ▼ -57.93 after sell → book $9,885.38; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 62 | $20.52 | $2.20 | $-12.43 | $9,883.18 | ▼ -12.43 after sell → book $9,883.18; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,883.18 | ▲ close $9,883.18 vs 09:30 $9,900.77 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,689.09 | ▲ 09:30 equity $8,689.09 vs yday $8,689.09 (+0.00) | 09:30 open · cash $8,689.09 · no holdings · equity $8,689.09 vs prior close $8,689.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,648.83 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 141 | $7.65 | $2.41 | — | $6,567.77 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $5,560.62 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $4,554.26 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1086.14 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 281 | $3.86 | $3.62 | — | $3,465.97 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $2,543.97 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 67 | $16.21 | $2.19 | — | $1,455.71 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $465.69 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $465.69 | ▲ close $8,671.80 vs 09:30 $8,689.09 (session +1.03) | 16:00 close · cash $465.69 · equity $8,671.80 vs 09:30 $8,689.09 (-17.29; session marks +1.03) · 8 name(s) marked open→close (per-name table). HALO×9 09:30 $115.36 → close $113.90 -13.14; MRVI×141 09:30 $7.65 → close $7.60 -7.05; TXG×12 09:30 $83.76 → close $85.71 +23.40; TEM×12 09:30 $83.69 → close $85.01 +15.78; ZSQR×281 09:30 $3.86 → close $3.78 -22.48; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×67 09:30 $16.21 → close $15.96 -16.75; GRAL×8 09:30 $123.50 → close $126.89 +27.12 | — |
