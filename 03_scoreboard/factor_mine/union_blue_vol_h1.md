# Factor mine action — `union_blue_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-25.45%** ($7,455) · signal-only (no cash/fees) was -5.26%. Starts YES **0/30**. Fills 232 · skips 56 · realized $-1650.33.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the name is painted 🔵 (a turn higher on a still-red row).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `vol=good,blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,349.75.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,265.54 | ▼ -4.98 after sell → book $9,757.42; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,411.56 | ▼ -99.43 after sell → book $9,755.16; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,741.76 | ▲ +76.56 after sell → book $9,751.36; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,963.74 | ▼ -31.69 after sell → book $9,747.44; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,141.25 | ▼ -62.20 after sell → book $9,745.20; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,371.97 | ▼ -4.38 after sell → book $9,743.01; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $8,441.45 | ▼ -178.28 after sell → book $9,740.65; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $9,734.58 | ▲ +38.98 after sell → book $9,734.58; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 300 | $4.05 | $3.87 | — | $8,515.71 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,300.36 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 83 | $14.66 | $2.24 | — | $6,081.34 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $4,862.51 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 751 | $1.62 | $9.69 | — | $3,636.20 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 464 | $2.62 | $5.99 | — | $2,414.54 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `ENHA` | 605 | $2.01 | $7.80 | — | $1,190.68 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-26.0; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 20 | $58.01 | $2.05 | — | $28.43 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1216.82 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.43 | ▼ close $9,119.54 vs 09:30 $9,768.32 (session -578.78) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.43 | ▼ 09:30 equity $8,907.92 vs yday $9,119.54 (-211.62) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 300 | $3.72 | $3.93 | $-106.80 | $1,140.50 | ▼ -106.80 after sell → book $8,903.99; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,339.07 | ▼ -16.78 after sell → book $8,901.57; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 83 | $13.19 | $2.26 | $-126.51 | $3,431.58 | ▼ -126.51 after sell → book $8,899.31; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $4,577.20 | ▼ -73.21 after sell → book $8,897.06; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 751 | $1.32 | $9.82 | $-241.06 | $5,562.45 | ▼ -241.06 after sell → book $8,887.23; vs 09:30 mark -9.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 464 | $2.52 | $6.07 | $-58.46 | $6,725.66 | ▼ -58.46 after sell → book $8,881.16; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ENHA` | 605 | $1.70 | $7.91 | $-203.27 | $7,746.25 | ▼ -203.27 after sell → book $8,873.25; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 20 | $56.35 | $2.07 | $-37.32 | $8,871.18 | ▼ -37.32 after sell → book $8,871.18; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,871.18 | ▲ close $8,871.18 vs 09:30 $8,907.92 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,871.18 | ▲ 09:30 equity $8,871.18 vs yday $8,871.18 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,871.18 | ▲ close $8,871.18 vs 09:30 $8,871.18 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,871.18 | ▲ 09:30 equity $8,871.18 vs yday $8,871.18 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 53 | $20.55 | $2.15 | — | $7,779.88 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,685.73 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 53 | $20.65 | $2.15 | — | $5,589.13 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 192 | $5.77 | $2.57 | — | $4,478.73 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $3,377.29 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $2,278.88 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 633 | $1.75 | $8.17 | — | $1,162.96 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1108.90 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $149.17 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1108.90 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.17 | ▲ close $9,054.48 vs 09:30 $8,871.18 (session +206.63) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.17 | ▲ 09:30 equity $9,291.68 vs yday $9,054.48 (+237.20) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 53 | $21.90 | $2.17 | $+67.23 | $1,307.70 | ▲ +67.23 after sell → book $9,289.51; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,454.30 | ▲ +52.45 after sell → book $9,287.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 53 | $21.75 | $2.17 | $+53.98 | $3,604.88 | ▲ +53.98 after sell → book $9,285.30; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 192 | $5.67 | $2.61 | $-24.37 | $4,690.91 | ▼ -24.37 after sell → book $9,282.69; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $5,874.25 | ▲ +81.90 after sell → book $9,280.51; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 37 | $32.17 | $2.12 | $+89.76 | $7,062.42 | ▲ +89.76 after sell → book $9,278.39; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 633 | $1.79 | $8.28 | $+8.87 | $8,187.21 | ▲ +8.87 after sell → book $9,270.11; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $9,268.08 | ▲ +67.08 after sell → book $9,268.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,191.19 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 67 | $17.20 | $2.19 | — | $7,036.60 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,953.09 | — | combo gate; gate vol=good,blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $4,793.27 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 469 | $2.47 | $6.05 | — | $3,628.79 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 600 | $1.93 | $7.74 | — | $2,463.05 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $1,326.33 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1158.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 877 | $1.32 | $11.31 | — | $157.37 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1158.51 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.37 | ▲ close $9,465.13 vs 09:30 $9,291.68 (session +232.72) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.37 | ▲ 09:30 equity $9,798.38 vs yday $9,465.13 (+333.25) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $1,239.92 | ▲ +5.67 after sell → book $9,796.34; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 67 | $16.57 | $2.21 | $-46.61 | $2,347.90 | ▼ -46.61 after sell → book $9,794.13; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,431.03 | ▼ -0.38 after sell → book $9,792.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.33 | $2.33 | $+224.17 | $4,815.02 | ▲ +224.17 after sell → book $9,789.78; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 469 | $2.40 | $6.14 | $-45.02 | $5,934.48 | ▼ -45.02 after sell → book $9,783.64; vs 09:30 mark -6.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 600 | $1.88 | $7.85 | $-45.59 | $7,054.63 | ▼ -45.59 after sell → book $9,775.79; vs 09:30 mark -7.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 19 | $58.75 | $2.07 | $-22.54 | $8,168.81 | ▼ -22.54 after sell → book $9,773.72; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 877 | $1.83 | $11.47 | $+424.48 | $9,762.25 | ▲ +424.48 after sell → book $9,762.25; vs 09:30 mark -11.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,762.25 | ▲ close $9,762.25 vs 09:30 $9,798.38 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,762.25 | ▲ 09:30 equity $9,762.25 vs yday $9,762.25 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 168 | $7.25 | $2.49 | — | $8,541.76 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 89 | $13.59 | $2.26 | — | $7,329.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 128 | $9.49 | $2.37 | — | $6,112.90 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $4,891.13 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 268 | $4.55 | $3.46 | — | $3,668.27 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 748 | $1.63 | $9.65 | — | $2,439.38 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 610 | $2.00 | $7.87 | — | $1,211.51 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1220.28 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 317 | $3.80 | $4.09 | — | $2.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1220.28 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.82 | ▲ close $9,989.77 vs 09:30 $9,762.25 (session +261.80) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.82 | ▲ 09:30 equity $9,997.27 vs yday $9,989.77 (+7.50) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 168 | $8.29 | $2.53 | $+169.69 | $1,393.01 | ▲ +169.69 after sell → book $9,994.73; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 89 | $13.63 | $2.28 | $-0.98 | $2,603.80 | ▼ -0.98 after sell → book $9,992.45; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 128 | $9.89 | $2.41 | $+46.42 | $3,867.31 | ▲ +46.42 after sell → book $9,990.05; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $5,127.12 | ▲ +38.04 after sell → book $9,987.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 268 | $4.31 | $3.51 | $-71.29 | $6,278.69 | ▼ -71.29 after sell → book $9,984.43; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 748 | $1.75 | $9.78 | $+74.07 | $7,581.65 | ▲ +74.07 after sell → book $9,974.64; vs 09:30 mark -9.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 610 | $1.93 | $7.98 | $-58.55 | $8,750.97 | ▼ -58.55 after sell → book $9,966.66; vs 09:30 mark -7.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 317 | $3.83 | $4.15 | $+2.85 | $9,962.51 | ▲ +2.85 after sell → book $9,962.51; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 5696 | $0.58 | $50.30 | — | $6,591.45 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-27.5; leftover $3320.84 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 571 | $5.81 | $7.37 | — | $3,266.57 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3320.84 | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 26 | $121.87 | $2.07 | — | $95.88 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-35.1; leftover $3320.84 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.88 | ▲ close $10,014.42 vs 09:30 $9,997.27 (session +111.64) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.88 | ▲ 09:30 equity $10,173.24 vs yday $10,014.42 (+158.82) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 5696 | $0.53 | $48.25 | $-400.43 | $3,066.52 | ▼ -400.43 after sell → book $10,125.00; vs 09:30 mark -48.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 571 | $6.50 | $7.49 | $+379.13 | $6,770.53 | ▲ +379.13 after sell → book $10,117.51; vs 09:30 mark -7.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 26 | $128.73 | $2.10 | $+174.19 | $10,115.40 | ▲ +174.19 after sell → book $10,115.40; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,115.40 | ▲ close $10,115.40 vs 09:30 $10,173.24 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,115.40 | ▲ 09:30 equity $10,115.40 vs yday $10,115.40 (+0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 61 | $32.90 | $2.17 | — | $8,106.33 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $2023.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 25 | $79.42 | $2.06 | — | $6,118.76 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $2023.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,217.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2023.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $2,231.15 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2023.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 86 | $23.30 | $2.25 | — | $225.10 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $2023.08 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.10 | ▼ close $9,911.34 vs 09:30 $10,115.40 (session -193.51) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.10 | ▼ 09:30 equity $9,861.60 vs yday $9,911.34 (-49.74) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 61 | $31.15 | $2.20 | $-111.12 | $2,123.06 | ▼ -111.12 after sell → book $9,859.41; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 25 | $80.44 | $2.09 | $+21.34 | $4,131.96 | ▲ +21.34 after sell → book $9,857.31; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.03 | $2.05 | $+21.40 | $6,054.30 | ▲ +21.40 after sell → book $9,855.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 14 | $132.30 | $2.06 | $-136.53 | $7,904.44 | ▼ -136.53 after sell → book $9,853.20; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 86 | $22.66 | $2.28 | $-59.57 | $9,850.93 | ▼ -59.57 after sell → book $9,850.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.93 | ▲ close $9,850.93 vs 09:30 $9,861.60 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.93 | ▲ 09:30 equity $9,850.93 vs yday $9,850.93 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.93 | ▲ close $9,850.93 vs 09:30 $9,850.93 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.93 | ▲ 09:30 equity $9,850.93 vs yday $9,850.93 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.93 | ▲ close $9,850.93 vs 09:30 $9,850.93 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.93 | ▲ 09:30 equity $9,850.93 vs yday $9,850.93 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,656.86 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.45 | $2.23 | — | $7,434.08 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,264.51 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $5,038.09 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 564 | $2.18 | $7.28 | — | $3,801.29 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $2,581.27 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1894 | $0.65 | $17.99 | — | $1,332.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1231.37 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 330 | $3.73 | $4.26 | — | $97.02 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1231.37 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.02 | ▼ close $9,715.09 vs 09:30 $9,850.93 (session -95.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.02 | ▲ 09:30 equity $9,762.92 vs yday $9,715.09 (+47.83) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,265.25 | ▼ -25.83 after sell → book $9,760.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 79 | $15.00 | $2.25 | $-40.03 | $2,448.00 | ▼ -40.03 after sell → book $9,758.63; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,674.93 | ▲ +57.35 after sell → book $9,756.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $15.61 | $2.23 | $-89.12 | $4,812.23 | ▼ -89.12 after sell → book $9,754.37; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 564 | $2.16 | $7.38 | $-25.93 | $6,023.09 | ▼ -25.93 after sell → book $9,746.99; vs 09:30 mark -7.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.84 | $2.16 | $-6.35 | $7,236.77 | ▼ -6.35 after sell → book $9,744.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1894 | $0.69 | $19.08 | $+38.69 | $8,524.55 | ▲ +38.69 after sell → book $9,725.75; vs 09:30 mark -19.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 330 | $3.64 | $4.32 | $-38.28 | $9,721.43 | ▼ -38.28 after sell → book $9,721.43; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 351 | $3.46 | $4.53 | — | $8,502.44 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 482 | $2.52 | $6.22 | — | $7,281.58 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 181 | $6.71 | $2.53 | — | $6,064.54 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 639 | $1.90 | $8.24 | — | $4,842.20 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 254 | $4.78 | $3.28 | — | $3,624.80 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 107 | $11.31 | $2.31 | — | $2,412.32 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 345 | $3.52 | $4.45 | — | $1,193.47 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1215.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $163.91 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1215.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.91 | ▲ close $9,757.03 vs 09:30 $9,762.92 (session +69.15) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.91 | ▼ 09:30 equity $9,693.76 vs yday $9,757.03 (-63.27) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 351 | $3.43 | $4.60 | $-19.65 | $1,363.25 | ▼ -19.65 after sell → book $9,689.17; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 482 | $2.38 | $6.31 | $-80.01 | $2,504.10 | ▼ -80.01 after sell → book $9,682.86; vs 09:30 mark -6.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 181 | $6.57 | $2.57 | $-30.45 | $3,690.70 | ▼ -30.45 after sell → book $9,680.29; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 639 | $2.00 | $8.36 | $+47.30 | $4,960.34 | ▲ +47.30 after sell → book $9,671.93; vs 09:30 mark -8.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 254 | $4.30 | $3.33 | $-128.53 | $6,049.21 | ▼ -128.53 after sell → book $9,668.60; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-14.28 | $7,247.41 | ▼ -14.28 after sell → book $9,666.26; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 345 | $3.99 | $4.52 | $+153.18 | $8,619.44 | ▲ +153.18 after sell → book $9,661.74; vs 09:30 mark -4.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $9,659.72 | ▲ +10.73 after sell → book $9,659.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.72 | ▲ close $9,659.72 vs 09:30 $9,693.76 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.72 | ▲ 09:30 equity $9,659.72 vs yday $9,659.72 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.72 | ▲ close $9,659.72 vs 09:30 $9,659.72 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.72 | ▲ 09:30 equity $9,659.72 vs yday $9,659.72 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.72 | ▲ close $9,659.72 vs 09:30 $9,659.72 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.72 | ▲ 09:30 equity $9,659.72 vs yday $9,659.72 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 447 | $2.70 | $5.77 | — | $8,447.06 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 110 | $10.95 | $2.32 | — | $7,240.24 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 245 | $4.91 | $3.16 | — | $6,034.13 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 224 | $5.38 | $2.89 | — | $4,826.12 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 134 | $8.98 | $2.39 | — | $3,620.41 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+14.1; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `LDI` | 1420 | $0.85 | $16.33 | — | $2,397.08 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 21 | $54.91 | $2.05 | — | $1,241.91 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+24.3; leftover $1207.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 196 | $6.16 | $2.58 | — | $31.97 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1207.47 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.97 | ▼ close $9,537.48 vs 09:30 $9,659.72 (session -84.75) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.97 | ▲ 09:30 equity $9,563.51 vs yday $9,537.48 (+26.03) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 447 | $2.80 | $5.85 | $+33.08 | $1,277.72 | ▲ +33.08 after sell → book $9,557.66; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 110 | $10.29 | $2.35 | $-77.27 | $2,407.28 | ▼ -77.27 after sell → book $9,555.32; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 245 | $5.03 | $3.21 | $+23.03 | $3,636.41 | ▲ +23.03 after sell → book $9,552.10; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 224 | $5.57 | $2.94 | $+36.73 | $4,881.16 | ▲ +36.73 after sell → book $9,549.17; vs 09:30 mark -2.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TSSI` | 134 | $8.57 | $2.42 | $-59.76 | $6,027.11 | ▼ -59.76 after sell → book $9,546.74; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LDI` | 1420 | $0.84 | $16.41 | $-49.78 | $7,200.67 | ▼ -49.78 after sell → book $9,530.34; vs 09:30 mark -16.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 21 | $54.75 | $2.07 | $-7.49 | $8,348.34 | ▼ -7.49 after sell → book $9,528.26; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 196 | $6.02 | $2.62 | $-32.64 | $9,525.64 | ▼ -32.64 after sell → book $9,525.64; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,525.64 | ▲ close $9,525.64 vs 09:30 $9,563.51 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,525.64 | ▲ 09:30 equity $9,525.64 vs yday $9,525.64 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,525.64 | ▲ close $9,525.64 vs 09:30 $9,525.64 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,525.64 | ▲ 09:30 equity $9,525.64 vs yday $9,525.64 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 202 | $5.87 | $2.61 | — | $8,337.30 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $7,199.07 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 43 | $27.09 | $2.12 | — | $6,032.08 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $4,868.11 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 51 | $23.29 | $2.14 | — | $3,678.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 35 | $33.14 | $2.10 | — | $2,516.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-2.9; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 10 | $118.18 | $2.02 | — | $1,332.36 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1190.71 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $203.31 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1190.71 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.31 | ▼ close $9,443.00 vs 09:30 $9,525.64 (session -65.59) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.31 | ▲ 09:30 equity $9,598.91 vs yday $9,443.00 (+155.91) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 202 | $5.58 | $2.65 | $-63.84 | $1,327.82 | ▼ -63.84 after sell → book $9,596.26; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $2,407.37 | ▼ -58.68 after sell → book $9,594.21; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 43 | $28.23 | $2.14 | $+44.76 | $3,619.12 | ▲ +44.76 after sell → book $9,592.07; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $4,744.95 | ▼ -38.14 after sell → book $9,590.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 51 | $24.09 | $2.16 | $+36.49 | $5,971.38 | ▲ +36.49 after sell → book $9,587.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 35 | $36.76 | $2.12 | $+122.49 | $7,255.86 | ▲ +122.49 after sell → book $9,585.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $-36.86 | $8,402.82 | ▼ -36.86 after sell → book $9,583.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $9,581.67 | ▲ +49.79 after sell → book $9,581.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 157 | $7.59 | $2.46 | — | $8,387.58 | — | combo gate; gate vol=good,blue=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 46 | $25.95 | $2.13 | — | $7,191.75 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $5,993.79 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 499 | $2.40 | $6.44 | — | $4,789.75 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 66 | $18.04 | $2.19 | — | $3,597.26 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 310 | $3.86 | $4.00 | — | $2,396.66 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 447 | $2.67 | $5.77 | — | $1,195.17 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-0.4; leftover $1197.71 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 19 | $61.90 | $2.05 | — | $17.02 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1197.71 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.02 | ▲ close $9,809.26 vs 09:30 $9,598.91 (session +254.62) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.02 | ▲ 09:30 equity $9,857.51 vs yday $9,809.26 (+48.25) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 157 | $7.98 | $2.50 | $+56.27 | $1,267.38 | ▲ +56.27 after sell → book $9,855.02; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 46 | $26.14 | $2.15 | $+4.46 | $2,467.67 | ▲ +4.46 after sell → book $9,852.87; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $3,741.95 | ▲ +76.32 after sell → book $9,850.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 499 | $2.29 | $6.53 | $-67.86 | $4,878.13 | ▼ -67.86 after sell → book $9,844.31; vs 09:30 mark -6.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 66 | $17.80 | $2.21 | $-19.91 | $6,050.72 | ▼ -19.91 after sell → book $9,842.10; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EMAT` | 310 | $3.97 | $4.06 | $+26.04 | $7,277.36 | ▲ +26.04 after sell → book $9,838.04; vs 09:30 mark -4.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CYPH` | 447 | $3.04 | $5.85 | $+149.30 | $8,628.16 | ▲ +149.30 after sell → book $9,832.19; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 19 | $63.37 | $2.07 | $+23.82 | $9,830.12 | ▲ +23.82 after sell → book $9,830.12; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $8,780.51 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $7,680.41 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $6,488.38 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1266 | $0.97 | $16.08 | — | $5,244.28 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 311 | $3.95 | $4.01 | — | $4,011.82 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 87 | $14.07 | $2.25 | — | $2,785.48 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 83 | $14.79 | $2.24 | — | $1,555.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1228.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 3471 | $0.35 | $22.70 | — | $304.23 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $1228.76 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.23 | ▼ close $8,844.66 vs 09:30 $9,857.51 (session -932.14) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $304.23 | ▲ 09:30 equity $8,967.56 vs yday $8,844.66 (+122.90) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $1,352.21 | ▼ -1.63 after sell → book $8,965.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $2,501.43 | ▲ +49.12 after sell → book $8,963.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $3,659.00 | ▼ -34.46 after sell → book $8,961.46; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1266 | $0.94 | $15.92 | $-69.98 | $4,833.12 | ▼ -69.98 after sell → book $8,945.54; vs 09:30 mark -15.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 311 | $3.87 | $4.07 | $-32.97 | $6,032.62 | ▼ -32.97 after sell → book $8,941.47; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 87 | $13.90 | $2.28 | $-19.32 | $7,239.64 | ▼ -19.32 after sell → book $8,939.19; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 83 | $14.58 | $2.26 | $-21.93 | $8,447.52 | ▼ -21.93 after sell → book $8,936.93; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DCX` | 3471 | $0.14 | $15.89 | $-777.92 | $8,921.04 | ▼ -777.92 after sell → book $8,921.04; vs 09:30 mark -15.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 111 | $9.99 | $2.32 | — | $7,809.83 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 85 | $13.05 | $2.25 | — | $6,698.33 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 193 | $5.75 | $2.57 | — | $5,585.05 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 135 | $8.22 | $2.40 | — | $4,472.95 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `DFDV` | 171 | $6.51 | $2.50 | — | $3,357.24 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `CAN` | 2667 | $0.42 | $19.15 | — | $2,223.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.7; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 518 | $2.15 | $6.68 | — | $1,102.90 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1115.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 102 | $10.71 | $2.30 | — | $8.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1115.13 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.18 | ▼ close $8,804.55 vs 09:30 $8,967.56 (session -76.32) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.18 | ▲ 09:30 equity $8,810.47 vs yday $8,804.55 (+5.92) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 111 | $9.91 | $2.35 | $-13.55 | $1,105.84 | ▼ -13.55 after sell → book $8,808.12; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 85 | $12.99 | $2.27 | $-9.61 | $2,207.72 | ▼ -9.61 after sell → book $8,805.85; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 193 | $6.05 | $2.61 | $+52.72 | $3,373.73 | ▲ +52.72 after sell → book $8,803.24; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CAN` | 2667 | $0.41 | $19.26 | $-73.08 | $4,434.61 | ▼ -73.08 after sell → book $8,783.99; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,434.61 | ▲ close $8,783.99 vs 09:30 $8,810.47 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,434.61 | ▼ 09:30 equity $8,696.84 vs yday $8,783.99 (-87.15) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `FWDI` | 135 | $8.20 | $2.43 | $-7.52 | $5,539.18 | ▼ -7.52 after sell → book $8,694.41; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DFDV` | 171 | $6.09 | $2.54 | $-76.86 | $6,578.03 | ▼ -76.86 after sell → book $8,691.87; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 518 | $2.09 | $6.78 | $-44.54 | $7,653.87 | ▼ -44.54 after sell → book $8,685.09; vs 09:30 mark -6.78 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 102 | $10.11 | $2.32 | $-65.82 | $8,682.77 | ▼ -65.82 after sell → book $8,682.77; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 12 | $89.50 | $2.03 | — | $7,606.74 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 6 | $166.54 | $2.01 | — | $6,605.49 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 52 | $20.65 | $2.15 | — | $5,529.55 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 276 | $3.93 | $3.56 | — | $4,441.31 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 38 | $28.30 | $2.10 | — | $3,363.80 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 69 | $15.72 | $2.20 | — | $2,276.93 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 42 | $25.40 | $2.12 | — | $1,208.01 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1085.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 69 | $15.55 | $2.20 | — | $132.86 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1085.35 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.86 | ▼ close $8,428.43 vs 09:30 $8,696.84 (session -235.98) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.86 | ▼ 09:30 equity $8,368.30 vs yday $8,428.43 (-60.13) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-25.97 | $1,182.92 | ▼ -25.97 after sell → book $8,366.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $-19.58 | $2,164.59 | ▼ -19.58 after sell → book $8,364.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 52 | $20.52 | $2.17 | $-11.07 | $3,229.46 | ▼ -11.07 after sell → book $8,362.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 276 | $3.77 | $3.62 | $-51.34 | $4,266.37 | ▼ -51.34 after sell → book $8,358.45; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MAZE` | 38 | $28.15 | $2.12 | $-9.93 | $5,333.94 | ▼ -9.93 after sell → book $8,356.32; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 69 | $14.38 | $2.22 | $-96.88 | $6,323.95 | ▼ -96.88 after sell → book $8,354.11; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 42 | $23.99 | $2.14 | $-63.47 | $7,329.39 | ▼ -63.47 after sell → book $8,351.97; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 69 | $14.82 | $2.22 | $-54.79 | $8,349.75 | ▼ -54.79 after sell → book $8,349.75; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,349.75 | ▲ close $8,349.75 vs 09:30 $8,368.30 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,397.53 | ▲ 09:30 equity $7,397.53 vs yday $7,397.53 (+0.00) | 09:30 open · cash $7,397.53 · no holdings · equity $7,397.53 vs prior close $7,397.53 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 35 | $26.27 | $2.10 | — | $6,475.98 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $924.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 51 | $17.91 | $2.14 | — | $5,560.43 | — | combo gate; gate vol=good,blue=True; list probable; 🔵; ret5=+3.7; leftover $924.69 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 239 | $3.86 | $3.08 | — | $4,634.81 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $924.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 90 | $10.20 | $2.26 | — | $3,714.55 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $924.69 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $2,792.54 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $924.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 57 | $16.21 | $2.16 | — | $1,866.41 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $924.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 7 | $123.50 | $2.01 | — | $999.90 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $924.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 31 | $29.80 | $2.08 | — | $74.02 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $924.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.02 | ▲ close $7,454.80 vs 09:30 $7,397.53 (session +75.11) | 16:00 close · cash $74.02 · equity $7,454.80 vs 09:30 $7,397.53 (+57.27; session marks +75.11) · 8 name(s) marked open→close (per-name table). WRBY×35 09:30 $26.27 → close $26.71 +15.40; PL×51 09:30 $17.91 → close $17.43 -24.48; ZSQR×239 09:30 $3.86 → close $3.78 -19.12; DNA×90 09:30 $10.20 → close $10.66 +41.40; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×57 09:30 $16.21 → close $15.96 -14.25; GRAL×7 09:30 $123.50 → close $126.89 +23.73; QMCO×31 09:30 $29.80 → close $31.68 +58.28 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
