# Factor mine action — `union_break10_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **-8.34%** ($9,166) · signal-only (no cash/fees) was +11.56%. Starts YES **14/30**. Fills 261 · skips 100 · realized $-529.03.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name broke its prior 10-session range.
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
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,402.93.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,732.46 vs 09:30 $10,000.00 (session -264.74) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 217 | $44.09 | $2.91 | $-415.84 | $9,584.16 | ▼ -415.84 after sell → book $9,584.16; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 20 | $57.61 | $2.05 | — | $8,429.91 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1198.02 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 72 | $16.50 | $2.21 | — | $7,239.70 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1198.02 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 61 | $19.57 | $2.17 | — | $6,043.76 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1198.02 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 107 | $11.12 | $2.31 | — | $4,851.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1198.02 | — |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 24 | $48.82 | $2.06 | — | $3,677.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1198.02 | — |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 242 | $4.94 | $3.12 | — | $2,479.27 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1198.02 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 88 | $13.55 | $2.25 | — | $1,284.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1198.02 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,284.61 | ▼ close $9,245.54 vs 09:30 $9,587.07 (session -322.44) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,284.61 | ▼ 09:30 equity $9,213.89 vs yday $9,245.54 (-31.65) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 20 | $55.37 | $2.07 | $-48.92 | $2,389.94 | ▼ -48.92 after sell → book $9,211.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 72 | $15.73 | $2.23 | $-59.87 | $3,520.27 | ▼ -59.87 after sell → book $9,209.59; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 61 | $19.57 | $2.19 | $-4.37 | $4,711.85 | ▼ -4.37 after sell → book $9,207.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 107 | $9.57 | $2.34 | $-170.50 | $5,733.50 | ▼ -170.50 after sell → book $9,205.06; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 24 | $47.39 | $2.08 | $-38.46 | $6,868.78 | ▼ -38.46 after sell → book $9,202.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 242 | $4.86 | $3.17 | $-25.65 | $8,041.73 | ▼ -25.65 after sell → book $9,199.81; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 88 | $13.16 | $2.28 | $-38.85 | $9,197.53 | ▼ -38.85 after sell → book $9,197.53; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 24 | $46.18 | $2.06 | — | $8,087.15 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+6.7; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 63 | $18.24 | $2.18 | — | $6,935.85 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 70 | $16.20 | $2.20 | — | $5,799.65 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 167 | $6.87 | $2.49 | — | $4,649.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 27 | $41.23 | $2.07 | — | $3,534.59 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 35 | $32.55 | $2.10 | — | $2,393.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 598 | $1.92 | $7.71 | — | $1,237.37 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1149.69 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 76 | $14.94 | $2.22 | — | $99.71 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1149.69 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.71 | ▼ close $9,056.27 vs 09:30 $9,213.89 (session -118.23) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.71 | ▼ 09:30 equity $8,831.92 vs yday $9,056.27 (-224.35) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 24 | $48.00 | $2.08 | $+39.54 | $1,249.63 | ▲ +39.54 after sell → book $8,829.84; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 63 | $16.20 | $2.20 | $-132.90 | $2,268.03 | ▼ -132.90 after sell → book $8,827.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 70 | $15.78 | $2.22 | $-33.82 | $3,370.41 | ▼ -33.82 after sell → book $8,825.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 167 | $7.50 | $2.53 | $+100.19 | $4,620.38 | ▲ +100.19 after sell → book $8,822.89; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 27 | $41.50 | $2.09 | $+3.13 | $5,738.79 | ▲ +3.13 after sell → book $8,820.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 35 | $28.59 | $2.12 | $-142.81 | $6,737.32 | ▼ -142.81 after sell → book $8,818.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 598 | $1.70 | $7.82 | $-147.10 | $7,746.10 | ▼ -147.10 after sell → book $8,810.86; vs 09:30 mark -7.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 76 | $14.01 | $2.24 | $-75.14 | $8,808.62 | ▼ -75.14 after sell → book $8,808.62; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,808.62 | ▲ close $8,808.62 vs 09:30 $8,831.92 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,808.62 | ▲ 09:30 equity $8,808.62 vs yday $8,808.62 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,808.62 | ▲ close $8,808.62 vs 09:30 $8,808.62 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,808.62 | ▲ 09:30 equity $8,808.62 vs yday $8,808.62 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 53 | $20.55 | $2.15 | — | $7,717.32 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,623.17 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 53 | $20.65 | $2.15 | — | $5,526.57 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $4,425.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $3,326.72 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 629 | $1.75 | $8.11 | — | $2,217.86 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,204.07 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1101.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 223 | $4.92 | $2.88 | — | $104.03 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1101.08 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.03 | ▲ close $8,996.61 vs 09:30 $8,808.62 (session +211.58) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.03 | ▲ 09:30 equity $9,310.34 vs yday $8,996.61 (+313.73) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 53 | $21.90 | $2.17 | $+67.23 | $1,262.56 | ▲ +67.23 after sell → book $9,308.17; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,409.16 | ▲ +52.45 after sell → book $9,306.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 53 | $21.75 | $2.17 | $+53.98 | $3,559.74 | ▲ +53.98 after sell → book $9,303.96; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $4,743.08 | ▲ +81.90 after sell → book $9,301.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 37 | $32.17 | $2.12 | $+89.76 | $5,931.25 | ▲ +89.76 after sell → book $9,299.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 629 | $1.79 | $8.23 | $+8.82 | $7,048.93 | ▲ +8.82 after sell → book $9,291.43; vs 09:30 mark -8.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $8,129.80 | ▲ +67.08 after sell → book $9,289.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 223 | $5.20 | $2.92 | $+56.64 | $9,286.48 | ▲ +56.64 after sell → book $9,286.48; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,209.59 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 67 | $17.20 | $2.19 | — | $7,055.00 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,971.49 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 879 | $1.32 | $11.34 | — | $4,799.87 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1343 | $0.86 | $15.63 | — | $3,623.89 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $2,475.00 | — | union ∩ break10, no 🚨; gate break_10=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3948 | $0.29 | $23.45 | — | $1,290.84 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 140 | $8.28 | $2.41 | — | $129.23 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1160.81 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.23 | ▲ close $9,623.62 vs 09:30 $9,310.34 (session +398.21) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.23 | ▲ 09:30 equity $10,097.59 vs yday $9,623.62 (+473.97) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $1,211.78 | ▲ +5.67 after sell → book $10,095.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 67 | $16.57 | $2.21 | $-46.61 | $2,319.76 | ▼ -46.61 after sell → book $10,093.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,402.88 | ▼ -0.38 after sell → book $10,091.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 879 | $1.83 | $11.50 | $+425.45 | $4,999.96 | ▲ +425.45 after sell → book $10,079.82; vs 09:30 mark -11.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1343 | $0.89 | $16.21 | $+3.07 | $6,179.01 | ▲ +3.07 after sell → book $10,063.61; vs 09:30 mark -16.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $7,346.89 | ▲ +18.99 after sell → book $10,061.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3948 | $0.38 | $27.63 | $+300.29 | $8,831.34 | ▲ +300.29 after sell → book $10,033.94; vs 09:30 mark -27.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 140 | $8.59 | $2.44 | $+38.55 | $10,031.49 | ▲ +38.55 after sell → book $10,031.49; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,031.49 | ▲ close $10,031.49 vs 09:30 $10,097.59 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,031.49 | ▲ 09:30 equity $10,031.49 vs yday $10,031.49 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $8,809.72 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 769 | $1.63 | $9.92 | — | $7,546.33 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 239 | $5.24 | $3.08 | — | $6,290.89 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 142 | $8.79 | $2.42 | — | $5,040.30 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 803 | $1.56 | $10.36 | — | $3,777.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 219 | $5.71 | $2.83 | — | $2,523.94 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 353 | $3.55 | $4.55 | — | $1,266.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1253.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 65 | $19.04 | $2.19 | — | $26.45 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+49.5; leftover $1253.94 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.45 | ▲ close $10,559.44 vs 09:30 $10,031.49 (session +565.38) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.45 | ▼ 09:30 equity $10,431.41 vs yday $10,559.44 (-128.03) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $1,286.26 | ▲ +38.04 after sell → book $10,429.30; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 769 | $1.75 | $10.06 | $+76.15 | $2,625.80 | ▲ +76.15 after sell → book $10,419.24; vs 09:30 mark -10.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 239 | $4.98 | $3.13 | $-68.36 | $3,812.89 | ▼ -68.36 after sell → book $10,416.11; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 803 | $1.60 | $10.50 | $+11.26 | $5,087.19 | ▲ +11.26 after sell → book $10,405.61; vs 09:30 mark -10.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 219 | $5.97 | $2.87 | $+51.24 | $6,391.74 | ▲ +51.24 after sell → book $10,402.73; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 353 | $3.77 | $4.62 | $+68.48 | $7,717.93 | ▲ +68.48 after sell → book $10,398.11; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 65 | $20.72 | $2.21 | $+104.81 | $9,062.52 | ▲ +104.81 after sell → book $10,395.90; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 131 | $9.83 | $2.38 | — | $7,772.41 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1294.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 94 | $13.63 | $2.27 | — | $6,488.92 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1294.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 198 | $6.53 | $2.58 | — | $5,193.39 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+3.6; leftover $1294.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 565 | $2.29 | $7.29 | — | $3,892.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $1294.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 31 | $40.50 | $2.08 | — | $2,634.67 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $1294.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 92 | $14.00 | $2.27 | — | $1,344.41 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+17.8; leftover $1294.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $95.69 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+15.7; leftover $1294.65 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.69 | ▼ close $10,013.56 vs 09:30 $10,431.41 (session -361.45) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.69 | ▼ 09:30 equity $9,982.00 vs yday $10,013.56 (-31.56) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 142 | $9.41 | $2.45 | $+83.17 | $1,429.46 | ▲ +83.17 after sell → book $9,979.55; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 131 | $9.68 | $2.41 | $-24.45 | $2,695.12 | ▼ -24.45 after sell → book $9,977.13; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 94 | $12.98 | $2.30 | $-65.67 | $3,912.94 | ▼ -65.67 after sell → book $9,974.83; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 198 | $6.15 | $2.63 | $-80.45 | $5,128.02 | ▼ -80.45 after sell → book $9,972.21; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CNTN` | 565 | $2.21 | $7.39 | $-59.88 | $6,369.28 | ▼ -59.88 after sell → book $9,964.82; vs 09:30 mark -7.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 31 | $37.42 | $2.10 | $-99.67 | $7,527.19 | ▼ -99.67 after sell → book $9,962.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 92 | $12.56 | $2.29 | $-137.04 | $8,680.42 | ▼ -137.04 after sell → book $9,960.42; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $9,958.38 | ▲ +29.24 after sell → book $9,958.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $41.44 | $2.08 | — | $8,713.10 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+3.1; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 478 | $2.60 | $6.17 | — | $7,464.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+13.0; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 135 | $9.19 | $2.40 | — | $6,221.09 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 67 | $18.50 | $2.19 | — | $4,979.40 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 258 | $4.81 | $3.33 | — | $3,735.09 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+14.8; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 230 | $5.39 | $2.97 | — | $2,492.42 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+17.4; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `VERA` | 33 | $36.70 | $2.09 | — | $1,279.23 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+14.1; leftover $1244.80 | — |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 139 | $8.95 | $2.41 | — | $32.78 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $1244.80 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.78 | ▼ close $9,902.00 vs 09:30 $9,982.00 (session -32.76) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.78 | ▼ 09:30 equity $9,846.51 vs yday $9,902.00 (-55.49) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.74 | $2.10 | $+4.82 | $1,282.88 | ▲ +4.82 after sell → book $9,844.41; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 67 | $18.15 | $2.21 | $-27.85 | $2,496.72 | ▼ -27.85 after sell → book $9,842.20; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OABI` | 258 | $4.54 | $3.38 | $-76.37 | $3,664.65 | ▼ -76.37 after sell → book $9,838.81; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 230 | $5.11 | $3.02 | $-70.38 | $4,836.94 | ▼ -70.38 after sell → book $9,835.80; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `VERA` | 33 | $34.40 | $2.11 | $-80.10 | $5,970.03 | ▼ -80.10 after sell → book $9,833.69; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $5,044.33 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.8; leftover $1194.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 55 | $21.49 | $2.15 | — | $3,860.23 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.3; leftover $1194.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 65 | $18.36 | $2.19 | — | $2,664.64 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.8; leftover $1194.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 51 | $23.30 | $2.14 | — | $1,474.20 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1194.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `DJT` | 122 | $9.72 | $2.36 | — | $286.01 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+14.8; leftover $1194.01 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.01 | ▼ close $9,573.71 vs 09:30 $9,846.51 (session -249.15) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.01 | ▼ 09:30 equity $9,488.00 vs yday $9,573.71 (-85.71) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 478 | $2.58 | $6.26 | $-21.98 | $1,512.99 | ▼ -21.98 after sell → book $9,481.74; vs 09:30 mark -6.26 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 135 | $9.50 | $2.43 | $+37.03 | $2,793.06 | ▲ +37.03 after sell → book $9,479.31; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 139 | $8.66 | $2.44 | $-45.16 | $3,994.36 | ▼ -45.16 after sell → book $9,476.87; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $4,868.25 | ▼ -51.81 after sell → book $9,474.86; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 55 | $20.56 | $2.17 | $-55.48 | $5,996.87 | ▼ -55.48 after sell → book $9,472.68; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 65 | $17.77 | $2.21 | $-42.74 | $7,149.71 | ▼ -42.74 after sell → book $9,470.47; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 51 | $22.66 | $2.16 | $-36.95 | $8,303.21 | ▼ -36.95 after sell → book $9,468.31; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DJT` | 122 | $9.55 | $2.39 | $-25.48 | $9,465.93 | ▼ -25.48 after sell → book $9,465.93; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,465.93 | ▲ close $9,465.93 vs 09:30 $9,488.00 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,465.93 | ▲ 09:30 equity $9,465.93 vs yday $9,465.93 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,465.93 | ▲ close $9,465.93 vs 09:30 $9,465.93 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,465.93 | ▲ 09:30 equity $9,465.93 vs yday $9,465.93 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,465.93 | ▲ close $9,465.93 vs 09:30 $9,465.93 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,465.93 | ▲ 09:30 equity $9,465.93 vs yday $9,465.93 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $52.88 | $2.06 | — | $8,300.51 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 27 | $42.93 | $2.07 | — | $7,139.33 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 325 | $3.63 | $4.19 | — | $5,955.39 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 147 | $8.03 | $2.43 | — | $4,772.55 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $3,710.93 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 664 | $1.78 | $8.57 | — | $2,520.45 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 49 | $23.88 | $2.14 | — | $1,348.19 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1183.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 870 | $1.36 | $11.22 | — | $153.77 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1183.24 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.77 | ▼ close $8,974.53 vs 09:30 $9,465.93 (session -456.71) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.77 | ▼ 09:30 equity $8,967.42 vs yday $8,974.53 (-7.11) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 22 | $52.03 | $2.08 | $-22.83 | $1,296.35 | ▼ -22.83 after sell → book $8,965.34; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 27 | $41.50 | $2.09 | $-42.77 | $2,414.76 | ▼ -42.77 after sell → book $8,963.25; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 325 | $3.46 | $4.26 | $-63.70 | $3,535.00 | ▼ -63.70 after sell → book $8,958.99; vs 09:30 mark -4.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 147 | $7.91 | $2.47 | $-22.54 | $4,695.31 | ▼ -22.54 after sell → book $8,956.53; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 8 | $130.03 | $2.03 | $-23.41 | $5,733.51 | ▼ -23.41 after sell → book $8,954.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GPRO` | 664 | $1.48 | $8.69 | $-216.45 | $6,707.55 | ▼ -216.45 after sell → book $8,945.81; vs 09:30 mark -8.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 49 | $23.84 | $2.16 | $-6.25 | $7,873.55 | ▼ -6.25 after sell → book $8,943.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 870 | $1.23 | $11.38 | $-135.70 | $8,932.27 | ▼ -135.70 after sell → book $8,932.27; vs 09:30 mark -11.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $7,902.72 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 246 | $4.53 | $3.17 | — | $6,785.16 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 13 | $82.70 | $2.03 | — | $5,708.03 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 444 | $2.51 | $5.73 | — | $4,587.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 194 | $5.75 | $2.57 | — | $3,469.79 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 111 | $10.03 | $2.32 | — | $2,354.14 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; ret5=+4.0; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 44 | $25.18 | $2.12 | — | $1,244.10 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1116.53 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 192 | $5.79 | $2.57 | — | $129.85 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1116.53 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.85 | ▲ close $9,268.91 vs 09:30 $8,967.42 (session +359.15) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.85 | ▼ 09:30 equity $9,165.36 vs yday $9,268.91 (-103.55) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,170.14 | ▲ +10.73 after sell → book $9,163.35; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 246 | $4.53 | $3.22 | $-6.40 | $2,281.29 | ▼ -6.40 after sell → book $9,160.12; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 13 | $89.67 | $2.05 | $+86.53 | $3,444.95 | ▲ +86.53 after sell → book $9,158.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 444 | $2.66 | $5.81 | $+55.06 | $4,620.18 | ▲ +55.06 after sell → book $9,152.26; vs 09:30 mark -5.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 194 | $5.95 | $2.61 | $+33.61 | $5,771.87 | ▲ +33.61 after sell → book $9,149.65; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SCZM` | 111 | $9.90 | $2.35 | $-19.10 | $6,868.42 | ▼ -19.10 after sell → book $9,147.30; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 44 | $26.44 | $2.14 | $+51.18 | $8,029.64 | ▲ +51.18 after sell → book $9,145.16; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 192 | $5.81 | $2.61 | $-1.33 | $9,142.55 | ▼ -1.33 after sell → book $9,142.55; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,142.55 | ▲ close $9,142.55 vs 09:30 $9,165.36 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,142.55 | ▲ 09:30 equity $9,142.55 vs yday $9,142.55 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,142.55 | ▲ close $9,142.55 vs 09:30 $9,142.55 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,142.55 | ▲ 09:30 equity $9,142.55 vs yday $9,142.55 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,142.55 | ▲ close $9,142.55 vs 09:30 $9,142.55 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,142.55 | ▲ 09:30 equity $9,142.55 vs yday $9,142.55 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 560 | $2.04 | $7.22 | — | $7,992.92 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 240 | $4.75 | $3.10 | — | $6,849.83 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 539 | $2.12 | $6.95 | — | $5,700.19 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 423 | $2.70 | $5.46 | — | $4,552.64 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 365 | $3.13 | $4.71 | — | $3,405.48 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+24.2; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 104 | $10.95 | $2.30 | — | $2,264.38 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 193 | $5.91 | $2.57 | — | $1,121.18 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1142.82 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $23.64 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1142.82 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.64 | ▲ close $9,241.10 vs 09:30 $9,142.55 (session +132.90) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.64 | ▼ 09:30 equity $9,196.46 vs yday $9,241.10 (-44.64) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 560 | $2.01 | $7.33 | $-31.35 | $1,141.91 | ▼ -31.35 after sell → book $9,189.13; vs 09:30 mark -7.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 240 | $4.82 | $3.15 | $+10.56 | $2,295.57 | ▲ +10.56 after sell → book $9,185.99; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 539 | $2.05 | $7.05 | $-51.74 | $3,393.46 | ▼ -51.74 after sell → book $9,178.93; vs 09:30 mark -7.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 104 | $10.29 | $2.33 | $-73.27 | $4,461.29 | ▼ -73.27 after sell → book $9,176.60; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 193 | $5.86 | $2.61 | $-14.83 | $5,589.66 | ▼ -14.83 after sell → book $9,173.99; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 13 | $86.06 | $2.05 | $+19.19 | $6,706.39 | ▲ +19.19 after sell → book $9,171.94; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,706.39 | ▲ close $9,363.21 vs 09:30 $9,196.46 (session +191.27) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,706.39 | ▲ 09:30 equity $9,473.19 vs yday $9,363.21 (+109.98) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 365 | $3.64 | $4.78 | $+176.66 | $8,030.21 | ▲ +176.66 after sell → book $9,468.41; vs 09:30 mark -4.78 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,030.21 | ▲ close $9,569.93 vs 09:30 $9,473.19 (session +101.52) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,030.21 | ▲ 09:30 equity $9,578.39 vs yday $9,569.93 (+8.46) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $6,948.50 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1147.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $5,845.82 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1147.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $4,705.93 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1147.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 637 | $1.80 | $8.22 | — | $3,551.11 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1147.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 58 | $19.75 | $2.16 | — | $2,403.45 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1147.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 49 | $23.29 | $2.14 | — | $1,260.10 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1147.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 72 | $15.75 | $2.21 | — | $123.89 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1147.17 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.89 | ▲ close $9,606.28 vs 09:30 $9,578.39 (session +48.80) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.89 | ▲ 09:30 equity $9,776.10 vs yday $9,606.28 (+169.82) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 423 | $3.30 | $5.54 | $+242.81 | $1,514.26 | ▲ +242.81 after sell → book $9,770.57; vs 09:30 mark -5.53 | dropped from list after 4 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 14 | $76.44 | $2.05 | $-13.60 | $2,582.36 | ▼ -13.60 after sell → book $9,768.51; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 16 | $72.70 | $2.06 | $+58.46 | $3,743.51 | ▲ +58.46 after sell → book $9,766.46; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 42 | $28.23 | $2.14 | $+43.63 | $4,927.03 | ▲ +43.63 after sell → book $9,764.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HLP` | 637 | $2.10 | $8.33 | $+174.55 | $6,256.40 | ▲ +174.55 after sell → book $9,755.99; vs 09:30 mark -8.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 58 | $20.31 | $2.18 | $+28.13 | $7,432.19 | ▲ +28.13 after sell → book $9,753.80; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 49 | $24.09 | $2.16 | $+34.91 | $8,610.45 | ▲ +34.91 after sell → book $9,751.65; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `REF` | 72 | $15.85 | $2.23 | $+2.77 | $9,749.42 | ▲ +2.77 after sell → book $9,749.42; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $8,578.16 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $7,364.71 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,181.81 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 118 | $10.25 | $2.34 | — | $4,969.97 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 160 | $7.59 | $2.47 | — | $3,753.10 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 34 | $34.93 | $2.09 | — | $2,563.39 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+1.6; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 46 | $25.95 | $2.13 | — | $1,367.56 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1218.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 507 | $2.40 | $6.54 | — | $144.22 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1218.68 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.22 | ▲ close $9,798.10 vs 09:30 $9,776.10 (session +70.29) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.22 | ▲ 09:30 equity $9,834.30 vs yday $9,798.10 (+36.20) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,387.85 | ▲ +72.37 after sell → book $9,832.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,650.13 | ▲ +48.83 after sell → book $9,830.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $3,820.10 | ▼ -12.93 after sell → book $9,828.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 118 | $10.12 | $2.37 | $-20.06 | $5,011.88 | ▼ -20.06 after sell → book $9,825.83; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 160 | $7.98 | $2.51 | $+57.42 | $6,286.18 | ▲ +57.42 after sell → book $9,823.33; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 34 | $34.52 | $2.11 | $-18.14 | $7,457.74 | ▼ -18.14 after sell → book $9,821.21; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 46 | $26.14 | $2.15 | $+4.46 | $8,658.04 | ▲ +4.46 after sell → book $9,819.07; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 507 | $2.29 | $6.63 | $-68.94 | $9,812.43 | ▼ -68.94 after sell → book $9,812.43; vs 09:30 mark -6.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $8,616.36 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+21.3; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $7,566.75 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $6,466.65 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 342 | $3.58 | $4.41 | — | $5,237.88 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 41 | $29.32 | $2.11 | — | $4,033.64 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `INDP` | 318 | $3.85 | $4.10 | — | $2,805.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+53.5; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 823 | $1.49 | $10.62 | — | $1,568.36 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1226.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 107 | $11.38 | $2.31 | — | $348.38 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $1226.55 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $348.38 | ▲ close $9,857.59 vs 09:30 $9,834.30 (session +74.75) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $348.38 | ▲ 09:30 equity $9,984.55 vs yday $9,857.59 (+126.96) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $1,529.61 | ▼ -14.85 after sell → book $9,982.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $2,577.59 | ▼ -1.63 after sell → book $9,980.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $3,726.81 | ▲ +49.12 after sell → book $9,978.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 342 | $3.71 | $4.48 | $+35.57 | $4,991.15 | ▲ +35.57 after sell → book $9,973.98; vs 09:30 mark -4.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 41 | $29.43 | $2.13 | $+0.26 | $6,195.65 | ▲ +0.26 after sell → book $9,971.85; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 318 | $3.55 | $4.17 | $-103.67 | $7,320.38 | ▼ -103.67 after sell → book $9,967.68; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `LVWR` | 823 | $1.65 | $10.76 | $+110.30 | $8,667.57 | ▲ +110.30 after sell → book $9,956.92; vs 09:30 mark -10.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 107 | $12.05 | $2.34 | $+67.04 | $9,954.58 | ▲ +67.04 after sell → book $9,954.58; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 92 | $13.47 | $2.27 | — | $8,712.62 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 73 | $16.91 | $2.21 | — | $7,475.98 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 95 | $13.05 | $2.27 | — | $6,233.95 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 216 | $5.75 | $2.79 | — | $4,988.09 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 151 | $8.22 | $2.44 | — | $3,744.42 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 106 | $11.67 | $2.31 | — | $2,505.09 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 503 | $2.47 | $6.49 | — | $1,256.20 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+73.6; leftover $1244.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 7 | $164.58 | $2.01 | — | $102.12 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1244.32 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.12 | ▲ close $10,176.79 vs 09:30 $9,984.55 (session +245.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.12 | ▼ 09:30 equity $10,163.99 vs yday $10,176.79 (-12.80) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 95 | $12.99 | $2.30 | $-10.28 | $1,333.87 | ▼ -10.28 after sell → book $10,161.69; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 216 | $6.05 | $2.83 | $+59.18 | $2,638.92 | ▲ +59.18 after sell → book $10,158.86; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `MSTR` | 7 | $167.55 | $2.03 | $+16.75 | $3,809.74 | ▲ +16.75 after sell → book $10,156.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 59 | $9.11 | $2.17 | — | $3,270.08 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $544.25 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 538 | $1.01 | $6.94 | — | $2,719.76 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $544.25 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 75 | $7.23 | $2.21 | — | $2,175.30 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $544.25 | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $1,853.89 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+35.1; leftover $544.25 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,853.89 | ▼ close $10,084.04 vs 09:30 $10,163.99 (session -59.48) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,853.89 | ▲ 09:30 equity $10,184.23 vs yday $10,084.04 (+100.19) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 92 | $12.84 | $2.29 | $-62.98 | $3,032.88 | ▼ -62.98 after sell → book $10,181.94; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 73 | $16.92 | $2.23 | $-3.71 | $4,265.81 | ▼ -3.71 after sell → book $10,179.71; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FWDI` | 151 | $8.20 | $2.48 | $-7.94 | $5,501.53 | ▼ -7.94 after sell → book $10,177.23; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 106 | $12.80 | $2.34 | $+115.14 | $6,856.00 | ▲ +115.14 after sell → book $10,174.90; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FEAM` | 503 | $2.92 | $6.58 | $+213.28 | $8,318.17 | ▲ +213.28 after sell → book $10,168.31; vs 09:30 mark -6.59 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 59 | $8.39 | $2.19 | $-46.83 | $8,811.00 | ▼ -46.83 after sell → book $10,166.13; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 538 | $0.95 | $6.82 | $-46.04 | $9,315.27 | ▼ -46.04 after sell → book $10,159.30; vs 09:30 mark -6.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 75 | $6.83 | $2.24 | $-34.45 | $9,825.29 | ▼ -34.45 after sell → book $10,157.07; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $10,155.05 | ▲ +8.36 after sell → book $10,155.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $8,987.26 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 30 | $41.76 | $2.08 | — | $7,732.38 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 128 | $9.90 | $2.37 | — | $6,462.81 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `VICR` | 4 | $266.50 | $2.00 | — | $5,394.81 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 1729 | $0.73 | $17.88 | — | $4,107.84 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 17 | $70.84 | $2.04 | — | $2,901.52 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 282 | $4.49 | $3.64 | — | $1,631.70 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+26.4; leftover $1269.38 | — |
| 2026-09-23 09:30 ET | **BUY** | `THM` | 443 | $2.86 | $5.71 | — | $359.01 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+25.5; leftover $1269.38 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.01 | ▼ close $9,862.01 vs 09:30 $10,184.23 (session -255.30) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.01 | ▼ 09:30 equity $9,536.36 vs yday $9,862.01 (-325.65) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $-22.17 | $1,504.63 | ▼ -22.17 after sell → book $9,534.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 30 | $36.02 | $2.10 | $-176.23 | $2,583.28 | ▼ -176.23 after sell → book $9,532.23; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 128 | $9.12 | $2.41 | $-104.62 | $3,748.23 | ▼ -104.62 after sell → book $9,529.82; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVTL` | 1729 | $0.66 | $16.90 | $-162.03 | $4,873.16 | ▼ -162.03 after sell → book $9,512.92; vs 09:30 mark -16.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 17 | $70.50 | $2.06 | $-9.88 | $6,069.60 | ▼ -9.88 after sell → book $9,510.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 282 | $3.92 | $3.69 | $-166.66 | $7,172.76 | ▼ -166.66 after sell → book $9,507.17; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `THM` | 443 | $2.79 | $5.80 | $-42.52 | $8,402.93 | ▼ -42.52 after sell → book $9,501.37; vs 09:30 mark -5.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,402.93 | ▲ close $9,507.17 vs 09:30 $9,536.36 (session +5.80) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,195.59 | ▲ 09:30 equity $9,195.59 vs yday $9,195.59 (+0.00) | 09:30 open · cash $9,195.59 · no holdings · equity $9,195.59 vs prior close $9,195.59 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 29 | $38.51 | $2.08 | — | $8,076.72 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+4.7; leftover $1149.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 150 | $7.65 | $2.44 | — | $6,926.78 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1149.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 43 | $26.27 | $2.12 | — | $5,795.05 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1149.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $4,704.15 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1149.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 522 | $2.20 | $6.73 | — | $3,549.01 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1149.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 191 | $6.00 | $2.56 | — | $2,400.45 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1149.45 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 13 | $83.69 | $2.03 | — | $1,310.38 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1149.45 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 189 | $6.06 | $2.56 | — | $162.49 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+342.1; leftover $1149.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.49 | ▼ close $9,165.74 vs 09:30 $9,195.59 (session -7.30) | 16:00 close · cash $162.49 · equity $9,165.74 vs 09:30 $9,195.59 (-29.85; session marks -7.30) · 8 name(s) marked open→close (per-name table). BLFS×29 09:30 $38.51 → close $38.49 -0.58; MRVI×150 09:30 $7.65 → close $7.60 -7.50; WRBY×43 09:30 $26.27 → close $26.71 +18.92; TXG×13 09:30 $83.76 → close $85.71 +25.35; HLP×522 09:30 $2.20 → close $2.21 +5.22; SATL×191 09:30 $6.00 → close $6.17 +32.47; TEM×13 09:30 $83.69 → close $85.01 +17.10; GLND×189 09:30 $6.06 → close $5.54 -98.28 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1198.02 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CXM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GSM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 4 | 2026-09-23 @ $266.50 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1269.38 |
