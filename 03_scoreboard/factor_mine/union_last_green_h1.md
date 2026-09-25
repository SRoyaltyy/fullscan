# Factor mine action — `union_last_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green, no 🚨

Cash book **-4.33%** ($9,567) · signal-only (no cash/fees) was +7.64%. Starts YES **9/30**. Fills 264 · skips 107 · realized $+492.16.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
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
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,492.19.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.25 | ▲ close $10,286.85 vs 09:30 $10,000.00 (session +322.82) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 33 | $59.65 | $2.11 | $-9.15 | $2,022.58 | ▼ -9.15 after sell → book $10,319.13; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $3,916.31 | ▼ -85.53 after sell → book $10,316.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $6,070.49 | ▲ +177.76 after sell → book $10,314.86; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2469 | $0.93 | $30.80 | $+238.08 | $8,335.86 | ▲ +238.08 after sell → book $10,284.06; vs 09:30 mark -30.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $10,281.78 | ▼ -39.37 after sell → book $10,281.78; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $9,104.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+3.6; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $8,109.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $6,840.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1371 | $0.94 | $16.96 | — | $5,538.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 856 | $1.50 | $11.04 | — | $4,243.74 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,968.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 298 | $4.31 | $3.84 | — | $1,680.46 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 307 | $4.18 | $3.96 | — | $393.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1285.22 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $393.24 | ▼ close $10,119.14 vs 09:30 $10,321.25 (session -118.52) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $393.24 | ▲ 09:30 equity $10,166.90 vs yday $10,119.14 (+47.76) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $1,586.17 | ▲ +15.71 after sell → book $10,164.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $2,594.97 | ▲ +14.07 after sell → book $10,162.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $3,811.04 | ▼ -53.41 after sell → book $10,160.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1371 | $0.91 | $16.79 | $-74.87 | $5,037.75 | ▼ -74.87 after sell → book $10,143.99; vs 09:30 mark -16.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 856 | $1.52 | $11.19 | $-5.12 | $6,327.67 | ▼ -5.12 after sell → book $10,132.79; vs 09:30 mark -11.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,501.02 | ▼ -101.70 after sell → book $10,130.52; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 298 | $4.60 | $3.90 | $+78.67 | $8,867.91 | ▲ +78.67 after sell → book $10,126.61; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 307 | $4.10 | $4.02 | $-32.54 | $10,122.59 | ▼ -32.54 after sell → book $10,122.59; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,873.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+6.7; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,729.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.8; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,511.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+8.3; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $5,245.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-4.7; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $4,008.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $2,747.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $1,493.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1265.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $282.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.8; leftover $1265.32 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.23 | ▲ close $10,112.86 vs 09:30 $10,166.90 (session +8.20) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.23 | ▲ 09:30 equity $10,164.62 vs yday $10,112.86 (+51.76) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,576.14 | ▲ +44.98 after sell → book $10,162.53; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,758.43 | ▲ +38.11 after sell → book $10,160.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $4,009.98 | ▲ +33.34 after sell → book $10,158.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $5,167.06 | ▼ -108.57 after sell → book $10,155.21; vs 09:30 mark -3.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $6,453.62 | ▲ +49.13 after sell → book $10,153.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $7,697.33 | ▼ -17.26 after sell → book $10,150.67; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $8,947.60 | ▼ -3.84 after sell → book $10,148.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $10,146.49 | ▼ -12.01 after sell → book $10,146.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.49 | ▲ close $10,146.49 vs 09:30 $10,164.62 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.49 | ▲ close $10,146.49 vs 09:30 $10,146.49 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,890.76 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $7,628.94 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 219 | $5.77 | $2.83 | — | $6,362.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,103.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,857.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 724 | $1.75 | $9.34 | — | $2,581.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,422.73 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1268.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 257 | $4.92 | $3.32 | — | $154.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1268.31 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.98 | ▲ close $10,282.06 vs 09:30 $10,146.49 (session +161.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.98 | ▲ 09:30 equity $10,635.34 vs yday $10,282.06 (+353.28) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,488.68 | ▲ +77.98 after sell → book $10,633.14; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $2,813.24 | ▲ +62.73 after sell → book $10,630.95; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 219 | $5.67 | $2.87 | $-27.60 | $4,052.10 | ▼ -27.60 after sell → book $10,628.08; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $5,404.78 | ▲ +94.17 after sell → book $10,625.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $6,753.78 | ▲ +102.43 after sell → book $10,623.74; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 724 | $1.79 | $9.47 | $+10.15 | $8,040.27 | ▲ +10.15 after sell → book $10,614.27; vs 09:30 mark -9.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,275.84 | ▲ +77.23 after sell → book $10,612.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 257 | $5.20 | $3.37 | $+65.28 | $10,608.87 | ▲ +65.28 after sell → book $10,608.87; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,293.11 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 77 | $17.20 | $2.22 | — | $7,966.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,666.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 119 | $11.13 | $2.35 | — | $5,339.87 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1004 | $1.32 | $12.95 | — | $4,001.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 798 | $1.66 | $10.29 | — | $2,666.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $1,418.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1326.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 88 | $14.96 | $2.25 | — | $99.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $1326.11 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.41 | ▲ close $10,850.47 vs 09:30 $10,635.34 (session +277.70) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.41 | ▲ 09:30 equity $11,266.78 vs yday $10,850.47 (+416.31) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,422.98 | ▲ +7.81 after sell → book $11,264.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 77 | $16.57 | $2.24 | $-52.97 | $2,696.62 | ▼ -52.97 after sell → book $11,262.49; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,996.78 | ▲ +0.34 after sell → book $11,260.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 119 | $13.33 | $2.38 | $+257.07 | $5,580.67 | ▲ +257.07 after sell → book $11,258.09; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1004 | $1.83 | $13.13 | $+485.96 | $7,404.85 | ▲ +485.96 after sell → book $11,244.95; vs 09:30 mark -13.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 798 | $1.55 | $10.44 | $-108.51 | $8,631.32 | ▼ -108.51 after sell → book $11,234.52; vs 09:30 mark -10.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $9,935.38 | ▲ +55.55 after sell → book $11,232.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 88 | $14.74 | $2.28 | $-23.89 | $11,230.22 | ▼ -23.89 after sell → book $11,230.22; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,230.22 | ▲ close $11,230.22 vs 09:30 $11,266.78 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,230.22 | ▲ 09:30 equity $11,230.22 vs yday $11,230.22 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3921 | $0.36 | $25.80 | — | $9,800.70 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-15.6; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 126 | $11.12 | $2.37 | — | $8,397.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 103 | $13.59 | $2.30 | — | $6,995.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 147 | $9.49 | $2.43 | — | $5,597.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $4,228.06 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 308 | $4.55 | $3.97 | — | $2,822.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 64 | $21.79 | $2.18 | — | $1,425.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable; 🔵; ret5=+3.1; leftover $1403.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 861 | $1.63 | $11.11 | — | $11.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1403.78 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.41 | ▲ close $11,332.77 vs 09:30 $11,230.22 (session +154.81) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.41 | ▼ 09:30 equity $11,290.36 vs yday $11,332.77 (-42.41) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3921 | $0.35 | $26.27 | $-71.67 | $1,369.26 | ▼ -71.67 after sell → book $11,264.09; vs 09:30 mark -26.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 126 | $11.03 | $2.40 | $-16.11 | $2,756.64 | ▼ -16.11 after sell → book $11,261.69; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 103 | $13.63 | $2.33 | $-0.51 | $4,158.20 | ▼ -0.51 after sell → book $11,259.37; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 147 | $9.89 | $2.47 | $+53.90 | $5,609.56 | ▲ +53.90 after sell → book $11,256.90; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 37 | $38.24 | $2.12 | $+43.14 | $7,022.32 | ▲ +43.14 after sell → book $11,254.78; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 308 | $4.31 | $4.03 | $-81.93 | $8,345.77 | ▼ -81.93 after sell → book $11,250.74; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 64 | $21.78 | $2.20 | $-5.03 | $9,737.48 | ▼ -5.03 after sell → book $11,248.54; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 861 | $1.75 | $11.26 | $+85.26 | $11,237.28 | ▲ +85.26 after sell → book $11,237.28; vs 09:30 mark -11.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 3 | $427.50 | $2.00 | — | $9,952.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.1; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `MOS` | 56 | $24.84 | $2.16 | — | $8,559.58 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+14.8; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 163 | $8.60 | $2.48 | — | $7,155.30 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+4.8; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 280 | $5.01 | $3.61 | — | $5,748.89 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 45 | $31.21 | $2.12 | — | $4,342.31 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 142 | $9.83 | $2.42 | — | $2,944.04 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 116 | $12.04 | $2.34 | — | $1,545.06 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1404.66 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 148 | $9.48 | $2.43 | — | $139.59 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1404.66 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.59 | ▼ close $11,169.38 vs 09:30 $11,290.36 (session -48.34) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.59 | ▼ 09:30 equity $11,135.60 vs yday $11,169.38 (-33.78) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-12.69 | $1,411.40 | ▼ -12.69 after sell → book $11,133.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 56 | $24.00 | $2.18 | $-51.38 | $2,753.22 | ▼ -51.38 after sell → book $11,131.40; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 163 | $8.49 | $2.52 | $-22.93 | $4,134.57 | ▼ -22.93 after sell → book $11,128.88; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 280 | $5.07 | $3.67 | $+9.52 | $5,550.50 | ▲ +9.52 after sell → book $11,125.21; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 45 | $30.79 | $2.15 | $-23.17 | $6,933.90 | ▼ -23.17 after sell → book $11,123.06; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 142 | $9.68 | $2.45 | $-26.17 | $8,306.01 | ▼ -26.17 after sell → book $11,120.61; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 148 | $9.33 | $2.47 | $-27.10 | $9,684.38 | ▼ -27.10 after sell → book $11,118.14; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $8,314.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+3.1; leftover $1383.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 95 | $14.42 | $2.27 | — | $6,942.60 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+7.1; leftover $1383.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 532 | $2.60 | $6.86 | — | $5,552.54 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+13.0; leftover $1383.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 6 | $227.10 | $2.01 | — | $4,187.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1383.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `INDP` | 1224 | $1.13 | $15.79 | — | $2,789.02 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; ret5=+21.3; leftover $1383.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 150 | $9.19 | $2.44 | — | $1,408.08 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1383.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $36.87 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $1383.48 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.87 | ▲ close $11,265.11 vs 09:30 $11,135.60 (session +180.64) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.87 | ▼ 09:30 equity $11,230.21 vs yday $11,265.11 (-34.90) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 116 | $12.79 | $2.37 | $+82.29 | $1,518.14 | ▲ +82.29 after sell → book $11,227.84; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 6 | $215.71 | $2.03 | $-72.41 | $2,810.34 | ▼ -72.41 after sell → book $11,225.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `INDP` | 1224 | $1.16 | $16.00 | $+4.93 | $4,214.18 | ▲ +4.93 after sell → book $11,209.81; vs 09:30 mark -16.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 74 | $18.15 | $2.23 | $-30.35 | $5,555.04 | ▼ -30.35 after sell → book $11,207.57; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 161 | $8.61 | $2.47 | — | $4,166.36 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1388.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $2,849.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1388.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 152 | $9.13 | $2.45 | — | $1,459.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1388.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 564 | $2.46 | $7.28 | — | $64.79 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; ret5=+7.9; leftover $1388.76 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.79 | ▼ close $10,975.64 vs 09:30 $11,230.21 (session -217.72) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.79 | ▲ 09:30 equity $10,997.92 vs yday $10,975.64 (+22.28) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+14.28 | $1,448.68 | ▲ +14.28 after sell → book $10,995.81; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 95 | $14.54 | $2.30 | $+6.82 | $2,827.68 | ▲ +6.82 after sell → book $10,993.51; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 532 | $2.58 | $6.96 | $-24.46 | $4,193.28 | ▼ -24.46 after sell → book $10,986.55; vs 09:30 mark -6.96 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 150 | $9.50 | $2.48 | $+41.58 | $5,615.80 | ▲ +41.58 after sell → book $10,984.07; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 161 | $8.52 | $2.51 | $-19.47 | $6,985.01 | ▼ -19.47 after sell → book $10,981.56; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $8,315.24 | ▲ +13.59 after sell → book $10,979.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 152 | $8.66 | $2.48 | $-76.37 | $9,629.08 | ▼ -76.37 after sell → book $10,977.04; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 564 | $2.39 | $7.38 | $-54.14 | $10,969.66 | ▼ -54.14 after sell → book $10,969.66; vs 09:30 mark -7.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,969.66 | ▲ close $10,969.66 vs 09:30 $10,997.92 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,969.66 | ▲ 09:30 equity $10,969.66 vs yday $10,969.66 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,969.66 | ▲ close $10,969.66 vs 09:30 $10,969.66 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,969.66 | ▲ 09:30 equity $10,969.66 vs yday $10,969.66 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,969.66 | ▲ close $10,969.66 vs 09:30 $10,969.66 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,969.66 | ▲ 09:30 equity $10,969.66 vs yday $10,969.66 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,645.59 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,312.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 377 | $3.63 | $4.86 | — | $6,939.31 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 170 | $8.03 | $2.50 | — | $5,571.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,245.19 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $2,884.58 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 92 | $14.85 | $2.27 | — | $1,516.12 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1371.21 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 628 | $2.18 | $8.10 | — | $138.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1371.21 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.98 | ▼ close $10,700.52 vs 09:30 $10,969.66 (session -243.01) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.98 | ▼ 09:30 equity $10,642.50 vs yday $10,700.52 (-58.02) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 25 | $52.03 | $2.09 | $-25.40 | $1,437.64 | ▼ -25.40 after sell → book $10,640.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $2,722.04 | ▼ -48.52 after sell → book $10,638.31; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 377 | $3.46 | $4.94 | $-73.89 | $4,021.52 | ▼ -73.89 after sell → book $10,633.37; vs 09:30 mark -4.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 170 | $7.91 | $2.54 | $-25.44 | $5,363.68 | ▼ -25.44 after sell → book $10,630.83; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $6,661.94 | ▼ -28.26 after sell → book $10,628.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $7,924.10 | ▼ -98.45 after sell → book $10,626.54; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 92 | $14.63 | $2.29 | $-24.80 | $9,267.76 | ▼ -24.80 after sell → book $10,624.24; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 628 | $2.16 | $8.22 | $-28.88 | $10,616.03 | ▼ -28.88 after sell → book $10,616.03; vs 09:30 mark -8.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 5 | $263.36 | $2.00 | — | $9,297.22 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $8,267.67 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 421 | $3.15 | $5.43 | — | $6,936.09 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $5,610.85 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 528 | $2.51 | $6.81 | — | $4,278.76 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `FCEL` | 91 | $14.52 | $2.26 | — | $2,955.17 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_mover; ret5=-24.1; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $1,818.16 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_mover; ret5=-12.7; leftover $1327.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 52 | $25.18 | $2.15 | — | $506.65 | — | union ∩ last_green, no 🚨; gate last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1327.00 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $506.65 | ▲ close $10,798.47 vs 09:30 $10,642.50 (session +207.13) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $506.65 | ▼ 09:30 equity $10,707.74 vs yday $10,798.47 (-90.73) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 5 | $253.72 | $2.03 | $-52.23 | $1,773.22 | ▼ -52.23 after sell → book $10,705.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $2,813.51 | ▲ +10.73 after sell → book $10,703.70; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SLBT` | 421 | $2.88 | $5.51 | $-124.61 | $4,020.48 | ▼ -124.61 after sell → book $10,698.19; vs 09:30 mark -5.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $5,453.14 | ▲ +107.42 after sell → book $10,696.13; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 528 | $2.66 | $6.91 | $+65.48 | $6,850.71 | ▲ +65.48 after sell → book $10,689.22; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FCEL` | 91 | $15.18 | $2.29 | $+55.51 | $8,229.80 | ▲ +55.51 after sell → book $10,686.93; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $9,310.03 | ▼ -56.79 after sell → book $10,684.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 52 | $26.44 | $2.17 | $+61.21 | $10,682.74 | ▲ +61.21 after sell → book $10,682.74; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,682.74 | ▲ close $10,682.74 vs 09:30 $10,707.74 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,682.74 | ▲ 09:30 equity $10,682.74 vs yday $10,682.74 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,682.74 | ▲ close $10,682.74 vs 09:30 $10,682.74 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,682.74 | ▲ 09:30 equity $10,682.74 vs yday $10,682.74 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,682.74 | ▲ close $10,682.74 vs 09:30 $10,682.74 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,682.74 | ▲ 09:30 equity $10,682.74 vs yday $10,682.74 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $9,439.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+8.3; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $8,147.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+19.6; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 654 | $2.04 | $8.44 | — | $6,804.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 281 | $4.75 | $3.62 | — | $5,466.59 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 629 | $2.12 | $8.11 | — | $4,125.00 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 56 | $23.63 | $2.16 | — | $2,799.56 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 115 | $11.55 | $2.33 | — | $1,468.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1335.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 8 | $157.55 | $2.01 | — | $206.56 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1335.34 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.56 | ▼ close $10,614.47 vs 09:30 $10,682.74 (session -37.52) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.56 | ▼ 09:30 equity $10,513.86 vs yday $10,614.47 (-100.61) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $1,443.53 | ▼ -6.08 after sell → book $10,511.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $2,642.75 | ▼ -92.92 after sell → book $10,509.76; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 654 | $2.01 | $8.56 | $-36.61 | $3,948.73 | ▼ -36.61 after sell → book $10,501.20; vs 09:30 mark -8.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 281 | $4.82 | $3.68 | $+12.36 | $5,299.47 | ▲ +12.36 after sell → book $10,497.52; vs 09:30 mark -3.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 629 | $2.05 | $8.23 | $-60.37 | $6,580.69 | ▼ -60.37 after sell → book $10,489.29; vs 09:30 mark -8.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 56 | $23.20 | $2.18 | $-28.42 | $7,877.71 | ▼ -28.42 after sell → book $10,487.11; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 115 | $11.56 | $2.36 | $-3.55 | $9,204.75 | ▼ -3.55 after sell → book $10,484.75; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 8 | $160.00 | $2.03 | $+15.55 | $10,482.71 | ▲ +15.55 after sell → book $10,482.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,482.71 | ▲ close $10,482.71 vs 09:30 $10,513.86 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,482.71 | ▲ 09:30 equity $10,482.71 vs yday $10,482.71 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,482.71 | ▲ close $10,482.71 vs 09:30 $10,482.71 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,482.71 | ▲ 09:30 equity $10,482.71 vs yday $10,482.71 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $9,397.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.0; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $8,161.19 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 35 | $36.46 | $2.10 | — | $6,883.00 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+2.9; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 70 | $18.61 | $2.20 | — | $5,578.10 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 71 | $18.21 | $2.20 | — | $4,282.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-19.1; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 19 | $68.79 | $2.05 | — | $2,973.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 223 | $5.87 | $2.88 | — | $1,662.04 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1310.34 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 481 | $2.72 | $6.20 | — | $347.52 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.4; leftover $1310.34 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $347.52 | ▲ close $10,766.00 vs 09:30 $10,482.71 (session +304.95) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $347.52 | ▲ 09:30 equity $10,949.48 vs yday $10,766.00 (+183.48) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,438.09 | ▲ +5.02 after sell → book $10,947.45; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,659.08 | ▼ -14.98 after sell → book $10,945.40; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 35 | $36.67 | $2.12 | $+3.14 | $3,940.41 | ▲ +3.14 after sell → book $10,943.28; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 70 | $22.46 | $2.22 | $+265.08 | $5,510.39 | ▲ +265.08 after sell → book $10,941.06; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 71 | $19.59 | $2.23 | $+93.55 | $6,899.05 | ▲ +93.55 after sell → book $10,938.83; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 19 | $72.70 | $2.07 | $+70.17 | $8,278.28 | ▲ +70.17 after sell → book $10,936.76; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 223 | $5.58 | $2.92 | $-70.47 | $9,519.70 | ▼ -70.47 after sell → book $10,933.84; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 481 | $2.94 | $6.30 | $+93.32 | $10,927.54 | ▲ +93.32 after sell → book $10,927.54; vs 09:30 mark -6.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $9,756.29 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 9 | $151.43 | $2.02 | — | $8,391.40 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 9 | $147.61 | $2.02 | — | $7,060.89 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 133 | $10.25 | $2.39 | — | $5,695.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 179 | $7.59 | $2.53 | — | $4,334.12 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 39 | $34.93 | $2.11 | — | $2,969.74 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.6; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 8034 | $0.17 | $37.76 | — | $1,566.20 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1365.94 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 86 | $15.87 | $2.25 | — | $199.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1365.94 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.13 | ▲ close $10,954.40 vs 09:30 $10,949.48 (session +79.93) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $199.13 | ▲ 09:30 equity $11,171.92 vs yday $10,954.40 (+217.52) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,442.76 | ▲ +72.37 after sell → book $11,169.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 9 | $158.04 | $2.04 | $+55.43 | $2,863.08 | ▲ +55.43 after sell → book $11,167.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 9 | $146.50 | $2.04 | $-14.04 | $4,179.54 | ▼ -14.04 after sell → book $11,165.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 133 | $10.12 | $2.42 | $-22.10 | $5,523.08 | ▼ -22.10 after sell → book $11,163.40; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 179 | $7.98 | $2.57 | $+64.71 | $6,948.93 | ▲ +64.71 after sell → book $11,160.83; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 39 | $34.52 | $2.13 | $-20.22 | $8,293.08 | ▼ -20.22 after sell → book $11,158.70; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 8034 | $0.17 | $39.10 | $-76.86 | $9,619.76 | ▼ -76.86 after sell → book $11,119.60; vs 09:30 mark -39.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 86 | $17.44 | $2.27 | $+130.50 | $11,117.32 | ▲ +130.50 after sell → book $11,117.32; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 12 | $108.55 | $2.03 | — | $9,812.70 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+21.3; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $8,492.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 16 | $85.00 | $2.04 | — | $7,130.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1432 | $0.97 | $18.19 | — | $5,723.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 351 | $3.95 | $4.53 | — | $4,332.73 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 98 | $14.07 | $2.28 | — | $2,951.58 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 238 | $5.83 | $3.07 | — | $1,560.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1389.67 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 388 | $3.58 | $5.01 | — | $166.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1389.67 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.93 | ▼ close $10,963.88 vs 09:30 $11,171.92 (session -114.30) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.93 | ▲ 09:30 equity $11,197.45 vs yday $10,963.88 (+233.57) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 12 | $107.57 | $2.05 | $-15.83 | $1,455.72 | ▼ -15.83 after sell → book $11,195.40; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $2,835.19 | ▲ +59.74 after sell → book $11,193.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 16 | $82.83 | $2.06 | $-38.82 | $4,158.41 | ▼ -38.82 after sell → book $11,191.31; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1432 | $0.94 | $18.01 | $-79.15 | $5,486.49 | ▼ -79.15 after sell → book $11,173.31; vs 09:30 mark -18.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 351 | $3.87 | $4.60 | $-37.20 | $6,840.26 | ▼ -37.20 after sell → book $11,168.71; vs 09:30 mark -4.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 98 | $13.90 | $2.31 | $-21.26 | $8,200.15 | ▼ -21.26 after sell → book $11,166.40; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 238 | $6.42 | $3.12 | $+133.04 | $9,723.80 | ▲ +133.04 after sell → book $11,163.28; vs 09:30 mark -3.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 388 | $3.71 | $5.08 | $+40.35 | $11,158.20 | ▲ +40.35 after sell → book $11,158.20; vs 09:30 mark -5.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,893.22 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+6.5; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $8,732.63 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-5.8; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 15 | $88.83 | $2.04 | — | $7,398.14 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+7.6; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 149 | $9.31 | $2.44 | — | $6,008.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 103 | $13.47 | $2.30 | — | $4,618.29 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1256 | $1.11 | $16.20 | — | $3,207.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 139 | $9.99 | $2.41 | — | $1,816.91 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1394.77 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 764 | $1.82 | $9.86 | — | $412.75 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1394.77 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $412.75 | ▼ close $10,993.34 vs 09:30 $11,197.45 (session -125.60) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $412.75 | ▼ 09:30 equity $10,964.51 vs yday $10,993.34 (-28.83) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1256 | $1.05 | $16.42 | $-107.98 | $1,715.13 | ▼ -107.98 after sell → book $10,948.09; vs 09:30 mark -16.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 139 | $9.91 | $2.44 | $-15.97 | $3,090.18 | ▼ -15.97 after sell → book $10,945.65; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 764 | $1.79 | $9.99 | $-42.77 | $4,451.57 | ▼ -42.77 after sell → book $10,935.66; vs 09:30 mark -9.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 5 | $93.97 | $2.00 | — | $3,979.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-0.6; leftover $556.45 | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 59 | $9.40 | $2.17 | — | $3,422.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=+9.5; leftover $556.45 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,422.95 | ▼ close $10,909.46 vs 09:30 $10,964.51 (session -22.03) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,422.95 | ▲ 09:30 equity $10,940.74 vs yday $10,909.46 (+31.28) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $4,530.93 | ▼ -52.62 after sell → book $10,938.72; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 149 | $9.50 | $2.47 | $+23.40 | $5,943.95 | ▲ +23.40 after sell → book $10,936.24; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 103 | $12.84 | $2.33 | $-70.03 | $7,264.15 | ▼ -70.03 after sell → book $10,933.92; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 5 | $93.97 | $2.02 | $-4.03 | $7,731.97 | ▼ -4.03 after sell → book $10,931.89; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 59 | $8.90 | $2.19 | $-33.85 | $8,254.89 | ▼ -33.85 after sell → book $10,929.71; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 49 | $27.79 | $2.14 | — | $6,891.04 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1375.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 140 | $9.81 | $2.41 | — | $5,515.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1375.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 66 | $20.65 | $2.19 | — | $4,150.14 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1375.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 350 | $3.93 | $4.51 | — | $2,770.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1375.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 87 | $15.72 | $2.25 | — | $1,400.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1375.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 54 | $25.40 | $2.15 | — | $26.48 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1375.81 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.48 | ▼ close $10,590.97 vs 09:30 $10,940.74 (session -323.08) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.48 | ▼ 09:30 equity $10,512.13 vs yday $10,590.97 (-78.84) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,336.05 | ▲ +44.59 after sell → book $10,510.09; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 15 | $87.67 | $2.06 | $-21.42 | $2,649.12 | ▼ -21.42 after sell → book $10,508.04; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 49 | $26.22 | $2.16 | $-81.22 | $3,931.74 | ▼ -81.22 after sell → book $10,505.88; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 140 | $9.67 | $2.44 | $-24.45 | $5,283.10 | ▼ -24.45 after sell → book $10,503.44; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 66 | $20.52 | $2.21 | $-12.98 | $6,635.21 | ▼ -12.98 after sell → book $10,501.23; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 350 | $3.77 | $4.58 | $-65.10 | $7,950.12 | ▼ -65.10 after sell → book $10,496.64; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 87 | $14.38 | $2.28 | $-121.11 | $9,198.91 | ▼ -121.11 after sell → book $10,494.37; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 54 | $23.99 | $2.17 | $-80.46 | $10,492.19 | ▼ -80.46 after sell → book $10,492.19; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,492.19 | ▲ close $10,492.19 vs 09:30 $10,512.13 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,669.17 | ▲ 09:30 equity $9,669.17 vs yday $9,669.17 (+0.00) | 09:30 open · cash $9,669.17 · no holdings · equity $9,669.17 vs prior close $9,669.17 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 10 | $115.36 | $2.02 | — | $8,513.55 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1208.65 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 31 | $38.51 | $2.08 | — | $7,317.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.7; leftover $1208.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 157 | $7.65 | $2.46 | — | $6,114.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1208.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 46 | $26.27 | $2.13 | — | $4,903.60 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1208.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 14 | $83.76 | $2.03 | — | $3,728.93 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1208.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 133 | $9.05 | $2.39 | — | $2,522.89 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-27.1; leftover $1208.65 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 51 | $23.58 | $2.14 | — | $1,318.16 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1208.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 549 | $2.20 | $7.08 | — | $103.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1208.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.28 | ▼ close $9,567.06 vs 09:30 $9,669.17 (session -79.77) | 16:00 close · cash $103.28 · equity $9,567.06 vs 09:30 $9,669.17 (-102.11; session marks -79.77) · 8 name(s) marked open→close (per-name table). HALO×10 09:30 $115.36 → close $113.90 -14.60; BLFS×31 09:30 $38.51 → close $38.49 -0.62; MRVI×157 09:30 $7.65 → close $7.60 -7.85; WRBY×46 09:30 $26.27 → close $26.71 +20.24; TXG×14 09:30 $83.76 → close $85.71 +27.30; AEHL×133 09:30 $9.05 → close $9.36 +41.23; BRVE×51 09:30 $23.58 → close $20.62 -150.96; HLP×549 09:30 $2.20 → close $2.21 +5.49 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
