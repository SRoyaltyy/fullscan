# Factor mine action — `union_vol_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g, no 🚨

Cash book **-19.01%** ($8,099) · signal-only (no cash/fees) was -4.96%. Starts YES **0/30**. Fills 242 · skips 87 · realized $-1273.21.

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
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,726.75.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
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
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 300 | $4.05 | $3.87 | — | $8,515.71 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $7,318.13 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $6,102.78 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $4,911.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-3.8; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $3,692.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+62.6; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,495.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+46.0; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,288.57 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1216.82 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $65.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1216.82 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.04 | ▼ close $9,533.39 vs 09:30 $9,768.32 (session -175.88) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.04 | ▼ 09:30 equity $9,483.84 vs yday $9,533.39 (-49.55) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 300 | $3.72 | $3.93 | $-106.80 | $1,177.11 | ▼ -106.80 after sell → book $9,479.91; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $2,422.11 | ▲ +47.42 after sell → book $9,477.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $3,620.68 | ▼ -16.78 after sell → book $9,475.39; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $4,808.34 | ▼ -3.85 after sell → book $9,473.27; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 177 | $7.50 | $2.56 | $+106.43 | $6,133.27 | ▲ +106.43 after sell → book $9,470.70; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,334.68 | ▲ +3.66 after sell → book $9,468.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,390.39 | ▼ -150.74 after sell → book $9,466.49; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $9,458.21 | ▼ -155.71 after sell → book $9,458.21; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.21 | ▲ close $9,458.21 vs 09:30 $9,483.84 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.21 | ▲ 09:30 equity $9,458.21 vs yday $9,458.21 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.21 | ▲ close $9,458.21 vs 09:30 $9,458.21 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.21 | ▲ 09:30 equity $9,458.21 vs yday $9,458.21 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,284.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,190.55 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $6,011.34 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 204 | $5.77 | $2.63 | — | $4,831.63 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,651.66 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,493.98 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 675 | $1.75 | $8.71 | — | $1,304.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1182.28 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $145.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1182.28 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.69 | ▲ close $9,655.65 vs 09:30 $9,458.21 (session +221.42) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.69 | ▲ 09:30 equity $9,909.74 vs yday $9,655.65 (+254.09) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,391.81 | ▲ +72.61 after sell → book $9,907.56; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,538.40 | ▲ +52.45 after sell → book $9,905.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,775.97 | ▲ +58.36 after sell → book $9,903.33; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 204 | $5.67 | $2.68 | $-25.71 | $4,929.97 | ▼ -25.71 after sell → book $9,900.65; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,197.98 | ▲ +88.04 after sell → book $9,898.46; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,450.49 | ▲ +94.83 after sell → book $9,896.34; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 675 | $1.79 | $8.83 | $+9.46 | $8,649.91 | ▲ +9.46 after sell → book $9,887.51; vs 09:30 mark -8.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,885.47 | ▲ +77.23 after sell → book $9,885.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,689.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 71 | $17.20 | $2.20 | — | $7,465.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,382.24 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,144.49 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 500 | $2.47 | $6.45 | — | $3,903.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 640 | $1.93 | $8.26 | — | $2,659.59 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,463.14 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1235.68 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 936 | $1.32 | $12.07 | — | $215.54 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1235.68 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.54 | ▲ close $10,097.66 vs 09:30 $9,909.74 (session +249.57) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.54 | ▲ 09:30 equity $10,452.97 vs yday $10,097.66 (+355.31) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,418.60 | ▲ +6.74 after sell → book $10,450.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 71 | $16.57 | $2.22 | $-49.16 | $2,592.85 | ▼ -49.16 after sell → book $10,448.71; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,675.97 | ▼ -0.38 after sell → book $10,446.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.33 | $2.35 | $+239.52 | $5,153.25 | ▲ +239.52 after sell → book $10,444.33; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 500 | $2.40 | $6.54 | $-47.99 | $6,346.71 | ▼ -47.99 after sell → book $10,437.79; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 640 | $1.88 | $8.37 | $-48.63 | $7,541.53 | ▼ -48.63 after sell → book $10,429.41; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,714.46 | ▼ -23.52 after sell → book $10,427.34; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 936 | $1.83 | $12.24 | $+453.04 | $10,415.10 | ▲ +453.04 after sell → book $10,415.10; vs 09:30 mark -12.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,415.10 | ▲ close $10,415.10 vs 09:30 $10,452.97 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.10 | ▲ 09:30 equity $10,415.10 vs yday $10,415.10 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 179 | $7.25 | $2.53 | — | $9,114.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 95 | $13.59 | $2.27 | — | $7,821.50 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 137 | $9.49 | $2.40 | — | $6,518.97 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 35 | $36.96 | $2.10 | — | $5,223.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 286 | $4.55 | $3.69 | — | $3,918.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 798 | $1.63 | $10.29 | — | $2,607.25 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 650 | $2.00 | $8.38 | — | $1,298.86 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1301.89 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 340 | $3.80 | $4.39 | — | $2.48 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1301.89 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.48 | ▲ close $10,657.94 vs 09:30 $10,415.10 (session +278.89) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.48 | ▲ 09:30 equity $10,666.12 vs yday $10,657.94 (+8.18) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 179 | $8.29 | $2.57 | $+181.06 | $1,483.82 | ▲ +181.06 after sell → book $10,663.55; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 95 | $13.63 | $2.30 | $-0.78 | $2,776.37 | ▼ -0.78 after sell → book $10,661.25; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 137 | $9.89 | $2.43 | $+49.96 | $4,128.86 | ▲ +49.96 after sell → book $10,658.81; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 35 | $38.24 | $2.12 | $+40.59 | $5,465.15 | ▲ +40.59 after sell → book $10,656.70; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 286 | $4.31 | $3.75 | $-76.08 | $6,694.06 | ▼ -76.08 after sell → book $10,652.95; vs 09:30 mark -3.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 798 | $1.75 | $10.44 | $+79.02 | $8,084.11 | ▲ +79.02 after sell → book $10,642.51; vs 09:30 mark -10.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 650 | $1.93 | $8.50 | $-62.39 | $9,330.11 | ▼ -62.39 after sell → book $10,634.01; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 340 | $3.83 | $4.45 | $+3.06 | $10,629.56 | ▲ +3.06 after sell → book $10,629.56; vs 09:30 mark -4.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 6077 | $0.58 | $53.66 | — | $7,033.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ret5=-27.5; leftover $3543.19 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 609 | $5.81 | $7.86 | — | $3,486.86 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3543.19 | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 28 | $121.87 | $2.07 | — | $72.43 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ret5=-35.1; leftover $3543.19 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.43 | ▲ close $10,687.08 vs 09:30 $10,666.12 (session +121.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.43 | ▲ 09:30 equity $10,856.18 vs yday $10,687.08 (+169.10) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 609 | $6.50 | $7.99 | $+404.37 | $4,022.94 | ▲ +404.37 after sell → book $10,848.19; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,022.94 | ▲ close $10,994.08 vs 09:30 $10,856.18 (session +145.89) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,022.94 | ▼ 09:30 equity $10,968.22 vs yday $10,994.08 (-25.86) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `SLQT` | 6077 | $0.53 | $51.53 | $-421.20 | $7,198.29 | ▼ -421.20 after sell → book $10,916.69; vs 09:30 mark -51.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 28 | $132.80 | $2.11 | $+301.85 | $10,914.58 | ▲ +301.85 after sell → book $10,914.58; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $9,563.56 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $8,211.38 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $6,894.73 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 85 | $15.88 | $2.25 | — | $5,542.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+19.4; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.15 | $2.21 | — | $4,179.22 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 140 | $9.73 | $2.41 | — | $2,814.61 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,536.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1364.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 70 | $19.25 | $2.20 | — | $187.06 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer; ret5=+14.1; leftover $1364.32 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.06 | ▼ close $10,619.26 vs 09:30 $10,968.22 (session -278.06) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.06 | ▼ 09:30 equity $10,577.16 vs yday $10,619.26 (-42.10) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $1,462.07 | ▼ -76.00 after sell → book $10,575.02; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $2,827.49 | ▲ +13.24 after sell → book $10,572.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $4,157.72 | ▲ +13.59 after sell → book $10,570.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 85 | $15.46 | $2.27 | $-40.21 | $5,469.56 | ▼ -40.21 after sell → book $10,568.66; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 75 | $17.70 | $2.24 | $-38.20 | $6,794.82 | ▼ -38.20 after sell → book $10,566.42; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 140 | $9.50 | $2.44 | $-37.05 | $8,122.37 | ▼ -37.05 after sell → book $10,563.97; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $9,311.04 | ▼ -89.19 after sell → book $10,561.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 70 | $17.87 | $2.22 | $-101.02 | $10,559.71 | ▼ -101.02 after sell → book $10,559.71; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,559.71 | ▲ close $10,559.71 vs 09:30 $10,577.16 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,559.71 | ▲ 09:30 equity $10,559.71 vs yday $10,559.71 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,559.71 | ▲ close $10,559.71 vs 09:30 $10,559.71 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,559.71 | ▲ 09:30 equity $10,559.71 vs yday $10,559.71 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,559.71 | ▲ close $10,559.71 vs 09:30 $10,559.71 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,559.71 | ▲ 09:30 equity $10,559.71 vs yday $10,559.71 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $9,365.65 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 85 | $15.45 | $2.25 | — | $8,050.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $6,734.63 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $5,424.35 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 23 | $55.42 | $2.06 | — | $4,147.63 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-25.9; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 605 | $2.18 | $7.80 | — | $2,820.92 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 741 | $1.78 | $9.56 | — | $1,492.38 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+183.1; leftover $1319.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 72 | $18.28 | $2.21 | — | $174.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+16.5; leftover $1319.96 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.02 | ▼ close $10,041.16 vs 09:30 $10,559.71 (session -488.42) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.02 | ▲ 09:30 equity $10,149.54 vs yday $10,041.16 (+108.38) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,342.25 | ▼ -25.83 after sell → book $10,147.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 85 | $15.00 | $2.27 | $-42.76 | $2,614.98 | ▼ -42.76 after sell → book $10,145.23; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $3,995.52 | ▲ +65.02 after sell → book $10,143.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $15.61 | $2.25 | $-94.95 | $5,210.86 | ▼ -94.95 after sell → book $10,140.95; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 23 | $55.79 | $2.08 | $+4.37 | $6,491.95 | ▲ +4.37 after sell → book $10,138.87; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 605 | $2.16 | $7.92 | $-27.82 | $7,790.83 | ▼ -27.82 after sell → book $10,130.95; vs 09:30 mark -7.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GPRO` | 741 | $1.48 | $9.69 | $-241.55 | $8,877.82 | ▼ -241.55 after sell → book $10,121.26; vs 09:30 mark -9.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 72 | $17.27 | $2.23 | $-77.15 | $10,119.03 | ▼ -77.15 after sell → book $10,119.03; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 365 | $3.46 | $4.71 | — | $8,851.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 501 | $2.52 | $6.46 | — | $7,582.44 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 188 | $6.71 | $2.55 | — | $6,318.41 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 665 | $1.90 | $8.58 | — | $5,046.33 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 264 | $4.78 | $3.41 | — | $3,781.00 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 795 | $1.59 | $10.26 | — | $2,506.70 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 111 | $11.31 | $2.32 | — | $1,248.96 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1264.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 353 | $3.52 | $4.55 | — | $1.85 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1264.88 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.85 | ▲ close $10,164.39 vs 09:30 $10,149.54 (session +88.19) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.85 | ▼ 09:30 equity $10,096.28 vs yday $10,164.39 (-68.11) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 365 | $3.43 | $4.78 | $-20.44 | $1,249.02 | ▼ -20.44 after sell → book $10,091.50; vs 09:30 mark -4.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 501 | $2.38 | $6.56 | $-83.16 | $2,434.85 | ▼ -83.16 after sell → book $10,084.95; vs 09:30 mark -6.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 188 | $6.57 | $2.60 | $-31.47 | $3,667.41 | ▼ -31.47 after sell → book $10,082.35; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 665 | $2.00 | $8.70 | $+49.22 | $4,988.71 | ▲ +49.22 after sell → book $10,073.65; vs 09:30 mark -8.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 264 | $4.30 | $3.46 | $-133.59 | $6,120.45 | ▼ -133.59 after sell → book $10,070.19; vs 09:30 mark -3.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 795 | $1.63 | $10.40 | $+11.15 | $7,405.90 | ▲ +11.15 after sell → book $10,059.79; vs 09:30 mark -10.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 111 | $11.22 | $2.35 | $-14.66 | $8,648.97 | ▼ -14.66 after sell → book $10,057.44; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 353 | $3.99 | $4.62 | $+156.73 | $10,052.82 | ▲ +156.73 after sell → book $10,052.82; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,052.82 | ▲ close $10,052.82 vs 09:30 $10,096.28 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,052.82 | ▲ 09:30 equity $10,052.82 vs yday $10,052.82 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,052.82 | ▲ close $10,052.82 vs 09:30 $10,052.82 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,052.82 | ▲ 09:30 equity $10,052.82 vs yday $10,052.82 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,052.82 | ▲ close $10,052.82 vs 09:30 $10,052.82 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,052.82 | ▲ 09:30 equity $10,052.82 vs yday $10,052.82 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,899.80 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 53 | $23.63 | $2.15 | — | $7,645.26 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-6.3; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 16 | $77.33 | $2.04 | — | $6,405.94 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+2.5; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 465 | $2.70 | $6.00 | — | $5,144.44 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 401 | $3.13 | $5.17 | — | $3,884.14 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+24.2; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 114 | $10.95 | $2.33 | — | $2,633.51 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 212 | $5.91 | $2.73 | — | $1,377.85 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1256.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 255 | $4.91 | $3.29 | — | $122.51 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1256.60 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.51 | ▼ close $9,909.82 vs 09:30 $10,052.82 (session -117.27) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.51 | ▲ 09:30 equity $9,983.19 vs yday $9,909.82 (+73.37) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $1,110.42 | ▼ -165.11 after sell → book $9,981.16; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 53 | $23.20 | $2.17 | $-27.11 | $2,337.85 | ▼ -27.11 after sell → book $9,978.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 16 | $77.10 | $2.06 | $-7.78 | $3,569.39 | ▼ -7.78 after sell → book $9,976.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 465 | $2.80 | $6.09 | $+34.42 | $4,865.31 | ▲ +34.42 after sell → book $9,970.85; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 114 | $10.29 | $2.36 | $-79.93 | $6,036.01 | ▼ -79.93 after sell → book $9,968.49; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 212 | $5.86 | $2.78 | $-16.11 | $7,275.55 | ▼ -16.11 after sell → book $9,965.71; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 255 | $5.03 | $3.34 | $+23.97 | $8,554.86 | ▲ +23.97 after sell → book $9,962.37; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,554.86 | ▲ close $10,014.50 vs 09:30 $9,983.19 (session +52.13) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,554.86 | ▲ 09:30 equity $10,014.50 vs yday $10,014.50 (-0.00) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 401 | $3.64 | $5.25 | $+194.09 | $10,009.24 | ▲ +194.09 after sell → book $10,009.24; vs 09:30 mark -5.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,009.24 | ▲ close $10,009.24 vs 09:30 $10,014.50 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,009.24 | ▲ 09:30 equity $10,009.24 vs yday $10,009.24 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $8,773.29 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 213 | $5.87 | $2.75 | — | $7,520.23 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 459 | $2.72 | $5.92 | — | $6,265.83 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-0.4; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $5,040.20 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 46 | $27.09 | $2.13 | — | $3,791.93 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 97 | $12.89 | $2.28 | — | $2,539.32 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=-18.2; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `INDP` | 341 | $3.66 | $4.40 | — | $1,286.86 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+96.8; leftover $1251.16 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $122.89 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1251.16 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.89 | ▼ close $9,801.78 vs 09:30 $10,009.24 (session -183.89) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.89 | ▲ 09:30 equity $9,915.81 vs yday $9,801.78 (+114.03) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $1,343.87 | ▼ -14.98 after sell → book $9,913.75; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 213 | $5.58 | $2.79 | $-67.31 | $2,529.62 | ▼ -67.31 after sell → book $9,910.96; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 459 | $2.94 | $6.01 | $+89.05 | $3,873.07 | ▲ +89.05 after sell → book $9,904.95; vs 09:30 mark -6.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $5,035.82 | ▼ -62.88 after sell → book $9,902.90; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 46 | $28.23 | $2.15 | $+48.16 | $6,332.25 | ▲ +48.16 after sell → book $9,900.75; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 97 | $13.56 | $2.31 | $+60.40 | $7,645.26 | ▲ +60.40 after sell → book $9,898.44; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 341 | $3.30 | $4.47 | $-131.62 | $8,766.10 | ▼ -131.62 after sell → book $9,893.98; vs 09:30 mark -4.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $9,891.93 | ▼ -38.14 after sell → book $9,891.93; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $8,720.67 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $7,537.78 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; ret5=+17.7; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 162 | $7.59 | $2.48 | — | $6,305.72 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7273 | $0.17 | $34.18 | — | $5,035.13 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 77 | $15.87 | $2.22 | — | $3,810.92 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 47 | $25.95 | $2.13 | — | $2,589.14 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $1,391.18 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1236.49 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 515 | $2.40 | $6.64 | — | $148.53 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1236.49 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.53 | ▲ close $9,957.00 vs 09:30 $9,915.81 (session +118.76) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.53 | ▲ 09:30 equity $10,122.47 vs yday $9,957.00 (+165.47) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,392.16 | ▲ +72.37 after sell → book $10,120.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $2,562.12 | ▼ -12.93 after sell → book $10,118.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 162 | $7.98 | $2.51 | $+58.19 | $3,852.37 | ▲ +58.19 after sell → book $10,115.90; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7273 | $0.17 | $35.40 | $-69.58 | $5,053.38 | ▼ -69.58 after sell → book $10,080.50; vs 09:30 mark -35.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 77 | $17.44 | $2.24 | $+116.42 | $6,394.02 | ▲ +116.42 after sell → book $10,078.26; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 47 | $26.14 | $2.15 | $+4.65 | $7,620.44 | ▲ +4.65 after sell → book $10,076.10; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $8,894.72 | ▲ +76.32 after sell → book $10,074.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 515 | $2.29 | $6.74 | $-70.03 | $10,067.33 | ▼ -70.03 after sell → book $10,067.33; vs 09:30 mark -6.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $8,808.21 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $7,708.10 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $6,516.07 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1297 | $0.97 | $16.47 | — | $5,241.51 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 318 | $3.95 | $4.10 | — | $3,981.31 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 89 | $14.07 | $2.26 | — | $2,726.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 85 | $14.79 | $2.25 | — | $1,467.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1258.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 3554 | $0.35 | $23.24 | — | $186.06 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $1258.42 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.06 | ▼ close $9,056.39 vs 09:30 $10,122.47 (session -956.58) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.06 | ▲ 09:30 equity $9,184.29 vs yday $9,056.39 (+127.90) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $1,444.04 | ▼ -1.16 after sell → book $9,182.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $2,593.26 | ▲ +49.12 after sell → book $9,180.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $3,750.83 | ▼ -34.46 after sell → book $9,178.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1297 | $0.94 | $16.31 | $-71.69 | $4,953.70 | ▼ -71.69 after sell → book $9,161.87; vs 09:30 mark -16.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 318 | $3.87 | $4.17 | $-33.71 | $6,180.20 | ▼ -33.71 after sell → book $9,157.71; vs 09:30 mark -4.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 89 | $13.90 | $2.28 | $-19.67 | $7,415.01 | ▼ -19.67 after sell → book $9,155.43; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 85 | $14.58 | $2.27 | $-22.36 | $8,652.04 | ▼ -22.36 after sell → book $9,153.16; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DCX` | 3554 | $0.14 | $16.27 | $-796.52 | $9,136.89 | ▼ -796.52 after sell → book $9,136.89; vs 09:30 mark -16.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,029.78 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; ret5=+6.5; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 12 | $88.83 | $2.03 | — | $6,961.80 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; ret5=+7.6; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 122 | $9.31 | $2.36 | — | $5,823.62 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 84 | $13.47 | $2.24 | — | $4,689.48 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 114 | $9.99 | $2.33 | — | $3,548.29 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 112 | $10.13 | $2.33 | — | $2,410.84 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+4.9; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 67 | $16.91 | $2.19 | — | $1,275.68 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+50.5; leftover $1142.11 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 87 | $13.05 | $2.25 | — | $138.08 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1142.11 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.08 | ▲ close $9,122.56 vs 09:30 $9,184.29 (session +3.41) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.08 | ▲ 09:30 equity $9,148.51 vs yday $9,122.56 (+25.95) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 114 | $9.91 | $2.36 | $-13.81 | $1,265.46 | ▼ -13.81 after sell → book $9,146.15; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 87 | $12.99 | $2.28 | $-9.75 | $2,393.31 | ▼ -9.75 after sell → book $9,143.87; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 515 | $0.58 | $4.53 | — | $2,090.08 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $299.16 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 32 | $9.11 | $2.09 | — | $1,796.48 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+44.4; leftover $299.16 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 296 | $1.01 | $3.82 | — | $1,493.70 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $299.16 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 41 | $7.23 | $2.11 | — | $1,195.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+36.6; leftover $299.16 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,195.15 | ▼ close $9,063.26 vs 09:30 $9,148.51 (session -68.07) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,195.15 | ▲ 09:30 equity $9,081.09 vs yday $9,063.26 (+17.83) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 122 | $9.50 | $2.39 | $+18.44 | $2,351.77 | ▲ +18.44 after sell → book $9,078.70; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 84 | $12.84 | $2.27 | $-57.85 | $3,428.06 | ▼ -57.85 after sell → book $9,076.44; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 112 | $10.26 | $2.35 | $+9.32 | $4,574.83 | ▲ +9.32 after sell → book $9,074.08; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 67 | $16.92 | $2.21 | $-3.73 | $5,706.26 | ▼ -3.73 after sell → book $9,071.87; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 515 | $0.57 | $4.60 | $-11.71 | $5,997.78 | ▼ -11.71 after sell → book $9,067.27; vs 09:30 mark -4.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 32 | $8.39 | $2.11 | $-27.23 | $6,264.15 | ▼ -27.23 after sell → book $9,065.16; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 296 | $0.95 | $3.76 | $-25.34 | $6,541.59 | ▼ -25.34 after sell → book $9,061.40; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 41 | $6.83 | $2.13 | $-20.65 | $6,819.49 | ▼ -20.65 after sell → book $9,059.27; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $5,765.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1136.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 55 | $20.65 | $2.15 | — | $4,627.92 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1136.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 289 | $3.93 | $3.73 | — | $3,488.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1136.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 40 | $28.30 | $2.11 | — | $2,354.31 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1136.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 72 | $15.72 | $2.21 | — | $1,220.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1136.58 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 44 | $25.40 | $2.12 | — | $100.54 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1136.58 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.54 | ▼ close $8,800.45 vs 09:30 $9,081.09 (session -244.48) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.54 | ▼ 09:30 equity $8,745.32 vs yday $8,800.45 (-55.13) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,246.16 | ▲ +38.52 after sell → book $8,743.29; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-17.93 | $2,296.22 | ▼ -17.93 after sell → book $8,741.25; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 9 | $112.22 | $2.04 | $-45.72 | $3,304.16 | ▼ -45.72 after sell → book $8,739.21; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 55 | $20.52 | $2.17 | $-11.48 | $4,430.58 | ▼ -11.48 after sell → book $8,737.03; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 289 | $3.77 | $3.79 | $-53.75 | $5,516.33 | ▼ -53.75 after sell → book $8,733.25; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MAZE` | 40 | $28.15 | $2.13 | $-10.24 | $6,640.20 | ▼ -10.24 after sell → book $8,731.12; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 72 | $14.38 | $2.23 | $-100.91 | $7,673.33 | ▼ -100.91 after sell → book $8,728.89; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 44 | $23.99 | $2.14 | $-66.30 | $8,726.75 | ▼ -66.30 after sell → book $8,726.75; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,726.75 | ▲ close $8,726.75 vs 09:30 $8,745.32 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,193.41 | ▲ 09:30 equity $8,193.41 vs yday $8,193.41 (+0.00) | 09:30 open · cash $8,193.41 · no holdings · equity $8,193.41 vs prior close $8,193.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 8 | $115.36 | $2.01 | — | $7,268.52 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1024.18 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 38 | $26.27 | $2.10 | — | $6,268.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1024.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 57 | $17.91 | $2.16 | — | $5,245.12 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable; 🔵; ret5=+3.7; leftover $1024.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $4,238.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1024.18 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 169 | $6.06 | $2.50 | — | $3,212.12 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+342.1; leftover $1024.18 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 265 | $3.86 | $3.42 | — | $2,185.80 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1024.18 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 100 | $10.20 | $2.29 | — | $1,163.51 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1024.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $241.50 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1024.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.50 | ▼ close $8,099.42 vs 09:30 $8,193.41 (session -75.47) | 16:00 close · cash $241.50 · equity $8,099.42 vs 09:30 $8,193.41 (-93.99; session marks -75.47) · 8 name(s) marked open→close (per-name table). HALO×8 09:30 $115.36 → close $113.90 -11.68; WRBY×38 09:30 $26.27 → close $26.71 +16.72; PL×57 09:30 $17.91 → close $17.43 -27.36; TEM×12 09:30 $83.69 → close $85.01 +15.78; GLND×169 09:30 $6.06 → close $5.54 -87.88; ZSQR×265 09:30 $3.86 → close $3.78 -21.20; DNA×100 09:30 $10.20 → close $10.66 +46.00; TWST×5 09:30 $184.00 → close $182.83 -5.85 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
