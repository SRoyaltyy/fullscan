# Factor mine action — `union_last_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green, no 🚨

Cash book **-15.24%** ($8,476) · signal-only (no cash/fees) was +37.42%. Starts YES **3/30**. Fills 171 · skips 312 · realized $-335.16.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,366.41.

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
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $49.60 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $43.53 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $39.18 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $34.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $7.03 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.95 | ▲ close $10,677.53 vs 09:30 $10,321.25 (session +356.53) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,645.20 vs yday $10,677.53 (-32.33) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.95 | ▲ close $10,729.57 vs 09:30 $10,645.20 (session +84.37) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,626.31 vs yday $10,729.57 (-103.26) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 33 | $60.00 | $2.11 | $+2.40 | $2,012.84 | ▲ +2.40 after sell → book $10,624.20; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 43 | $43.56 | $2.14 | $-108.32 | $3,883.77 | ▼ -108.32 after sell → book $10,622.05; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 39 | $51.77 | $2.13 | $+40.49 | $5,900.67 | ▲ +40.49 after sell → book $10,619.92; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 2469 | $1.14 | $32.28 | $+755.08 | $8,683.05 | ▲ +755.08 after sell → book $10,587.64; vs 09:30 mark -32.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 85 | $22.16 | $2.27 | $-103.97 | $10,564.37 | ▼ -103.97 after sell → book $10,585.36; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,564.37 | ▼ close $10,584.89 vs 09:30 $10,626.31 (session -0.47) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,564.37 | ▼ 09:30 equity $10,584.87 vs yday $10,584.89 (-0.02) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 7 | $0.88 | $0.10 | $-0.59 | $10,570.43 | ▼ -0.59 after sell → book $10,584.77; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 4 | $1.42 | $0.09 | $-0.48 | $10,576.02 | ▼ -0.48 after sell → book $10,584.68; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,580.74 | ▲ +0.36 after sell → book $10,584.61; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,584.55 | ▼ -0.42 after sell → book $10,584.55; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,584.55 | ▲ close $10,584.55 vs 09:30 $10,584.87 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,584.55 | ▲ 09:30 equity $10,584.55 vs yday $10,584.55 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 64 | $20.55 | $2.18 | — | $9,267.17 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 64 | $20.65 | $2.18 | — | $7,943.38 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 229 | $5.77 | $2.95 | — | $6,619.10 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 67 | $19.63 | $2.19 | — | $5,301.70 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $3,995.86 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 756 | $1.75 | $9.75 | — | $2,663.10 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $1,360.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 268 | $4.92 | $3.46 | — | $38.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1323.07 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.21 | ▲ close $10,730.97 vs 09:30 $10,584.55 (session +173.28) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.21 | ▲ 09:30 equity $11,103.25 vs yday $10,730.97 (+372.28) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $34.20 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.78 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $30.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.78 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.84 | ▼ close $11,084.43 vs 09:30 $11,103.25 (session -18.73) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.84 | ▲ 09:30 equity $11,181.96 vs yday $11,084.43 (+97.53) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.84 | ▼ close $11,155.85 vs 09:30 $11,181.96 (session -26.11) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.84 | ▼ 09:30 equity $11,010.61 vs yday $11,155.85 (-145.24) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 64 | $20.32 | $2.20 | $-19.11 | $1,329.12 | ▼ -19.11 after sell → book $11,008.41; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 64 | $20.47 | $2.20 | $-15.91 | $2,637.00 | ▼ -15.91 after sell → book $11,006.21; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 229 | $5.53 | $3.00 | $-60.92 | $3,900.36 | ▼ -60.92 after sell → book $11,003.20; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 67 | $21.21 | $2.21 | $+101.46 | $5,319.22 | ▲ +101.46 after sell → book $11,000.99; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 44 | $32.32 | $2.14 | $+114.09 | $6,739.16 | ▲ +114.09 after sell → book $10,998.85; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 756 | $1.90 | $9.89 | $+93.76 | $8,165.67 | ▲ +93.76 after sell → book $10,988.96; vs 09:30 mark -9.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 9 | $156.51 | $2.04 | $+103.67 | $9,572.22 | ▲ +103.67 after sell → book $10,986.92; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 268 | $5.25 | $3.51 | $+81.47 | $10,975.71 | ▲ +81.47 after sell → book $10,983.41; vs 09:30 mark -3.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3832 | $0.36 | $25.21 | — | $9,578.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-15.6; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 123 | $11.12 | $2.36 | — | $8,208.52 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 100 | $13.59 | $2.29 | — | $6,847.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 144 | $9.49 | $2.42 | — | $5,478.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $4,108.62 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 301 | $4.55 | $3.88 | — | $2,735.19 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 62 | $21.79 | $2.18 | — | $1,382.04 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable; 🔵; ret5=+3.1; leftover $1371.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 841 | $1.63 | $10.85 | — | $0.36 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1371.96 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.36 | ▲ close $11,084.95 vs 09:30 $11,010.61 (session +152.84) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.36 | ▼ 09:30 equity $11,043.27 vs yday $11,084.95 (-41.68) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $5.08 | ▲ +0.71 after sell → book $11,043.19; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $8.08 | ▼ -0.36 after sell → book $11,043.13; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.08 | ▲ close $11,107.12 vs 09:30 $11,043.27 (session +64.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.08 | ▲ 09:30 equity $11,142.65 vs yday $11,107.12 (+35.53) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.08 | ▼ close $10,936.70 vs 09:30 $11,142.65 (session -205.95) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.08 | ▼ 09:30 equity $10,920.27 vs yday $10,936.70 (-16.43) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3832 | $0.36 | $26.13 | $-24.52 | $1,380.63 | ▼ -24.52 after sell → book $10,894.14; vs 09:30 mark -26.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 123 | $10.47 | $2.39 | $-84.70 | $2,666.05 | ▼ -84.70 after sell → book $10,891.75; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 100 | $13.05 | $2.32 | $-58.61 | $3,968.74 | ▼ -58.61 after sell → book $10,889.44; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 144 | $9.70 | $2.46 | $+25.36 | $5,363.08 | ▲ +25.36 after sell → book $10,886.98; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 37 | $39.60 | $2.12 | $+93.46 | $6,826.16 | ▲ +93.46 after sell → book $10,884.86; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 301 | $4.21 | $3.94 | $-110.17 | $8,089.42 | ▼ -110.17 after sell → book $10,880.91; vs 09:30 mark -3.95 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 62 | $22.10 | $2.20 | $+14.85 | $9,457.43 | ▲ +14.85 after sell → book $10,878.72; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 841 | $1.69 | $11.00 | $+28.61 | $10,867.72 | ▲ +28.61 after sell → book $10,867.72; vs 09:30 mark -11.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 32 | $41.74 | $2.09 | — | $9,529.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+2.4; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 92 | $14.63 | $2.27 | — | $8,181.72 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+5.8; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 506 | $2.68 | $6.53 | — | $6,819.12 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+16.3; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 157 | $8.61 | $2.46 | — | $5,464.89 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $4,148.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 139 | $9.73 | $2.41 | — | $2,793.36 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 148 | $9.13 | $2.43 | — | $1,439.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1358.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 552 | $2.46 | $7.12 | — | $74.65 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; ret5=+7.9; leftover $1358.46 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.65 | ▼ close $10,631.50 vs 09:30 $10,920.27 (session -208.90) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.65 | ▲ 09:30 equity $10,653.18 vs yday $10,631.50 (+21.68) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.65 | ▼ close $10,519.79 vs 09:30 $10,653.18 (session -133.39) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.65 | ▲ 09:30 equity $10,715.20 vs yday $10,519.79 (+195.41) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.65 | ▼ close $10,479.14 vs 09:30 $10,715.20 (session -236.06) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.65 | ▼ 09:30 equity $10,427.46 vs yday $10,479.14 (-51.68) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 32 | $42.10 | $2.11 | $+7.33 | $1,419.74 | ▲ +7.33 after sell → book $10,425.35; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 92 | $15.70 | $2.29 | $+93.88 | $2,861.85 | ▲ +93.88 after sell → book $10,423.06; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SLI` | 506 | $2.49 | $6.62 | $-109.29 | $4,115.17 | ▼ -109.29 after sell → book $10,416.44; vs 09:30 mark -6.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 157 | $7.25 | $2.50 | $-218.48 | $5,250.92 | ▼ -218.48 after sell → book $10,413.94; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $139.65 | $2.04 | $-61.83 | $6,505.73 | ▼ -61.83 after sell → book $10,411.90; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 139 | $10.07 | $2.44 | $+42.41 | $7,903.02 | ▲ +42.41 after sell → book $10,409.46; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 148 | $8.73 | $2.47 | $-64.10 | $9,192.59 | ▼ -64.10 after sell → book $10,406.99; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 552 | $2.20 | $7.22 | $-157.86 | $10,399.77 | ▼ -157.86 after sell → book $10,399.77; vs 09:30 mark -7.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,399.77 | ▲ close $10,399.77 vs 09:30 $10,427.46 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,399.77 | ▲ 09:30 equity $10,399.77 vs yday $10,399.77 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $9,128.59 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,838.61 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 358 | $3.63 | $4.62 | — | $6,534.45 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 161 | $8.03 | $2.47 | — | $5,239.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,045.08 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $2,751.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 87 | $14.85 | $2.25 | — | $1,457.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1299.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 596 | $2.18 | $7.69 | — | $150.40 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1299.97 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.40 | ▼ close $10,143.74 vs 09:30 $10,399.77 (session -230.62) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.40 | ▼ 09:30 equity $10,088.72 vs yday $10,143.74 (-55.02) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 5 | $3.15 | $0.17 | — | $134.48 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $18.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 7 | $2.51 | $0.20 | — | $116.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $18.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `FCEL` | 1 | $14.52 | $0.15 | — | $102.04 | — | union ∩ last_green, no 🚨; gate last_green=True; list yday_mover; ret5=-24.1; leftover $18.80 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.04 | ▲ close $10,187.10 vs 09:30 $10,088.72 (session +98.90) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.04 | ▼ 09:30 equity $10,165.59 vs yday $10,187.10 (-21.51) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.04 | ▼ close $10,036.55 vs 09:30 $10,165.59 (session -129.04) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.04 | ▼ 09:30 equity $9,980.95 vs yday $10,036.55 (-55.60) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $1,375.80 | ▲ +2.58 after sell → book $9,978.87; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 30 | $42.01 | $2.10 | $-31.78 | $2,634.00 | ▼ -31.78 after sell → book $9,976.77; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 358 | $3.28 | $4.69 | $-134.61 | $3,803.55 | ▼ -134.61 after sell → book $9,972.08; vs 09:30 mark -4.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 161 | $8.01 | $2.51 | $-8.20 | $5,090.65 | ▼ -8.20 after sell → book $9,969.57; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $6,220.54 | ▼ -64.17 after sell → book $9,967.53; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 77 | $15.46 | $2.24 | $-105.33 | $7,408.72 | ▼ -105.33 after sell → book $9,965.29; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 87 | $13.60 | $2.28 | $-113.28 | $8,589.64 | ▼ -113.28 after sell → book $9,963.01; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 596 | $2.22 | $7.80 | $+8.35 | $9,904.97 | ▲ +8.35 after sell → book $9,955.22; vs 09:30 mark -7.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,904.97 | ▼ close $9,954.61 vs 09:30 $9,980.95 (session -0.61) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,904.97 | ▼ 09:30 equity $9,954.03 vs yday $9,954.61 (-0.58) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `SLBT` | 5 | $2.58 | $0.16 | $-3.19 | $9,917.70 | ▼ -3.19 after sell → book $9,953.86; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 7 | $2.87 | $0.24 | $+2.08 | $9,937.55 | ▲ +2.08 after sell → book $9,953.62; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `FCEL` | 1 | $16.07 | $0.18 | $+1.22 | $9,953.44 | ▲ +1.22 after sell → book $9,953.44; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,953.44 | ▲ close $9,953.44 vs 09:30 $9,954.03 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,953.44 | ▲ 09:30 equity $9,953.44 vs yday $9,953.44 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $8,710.39 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+8.3; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $7,474.35 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+19.6; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 609 | $2.04 | $7.86 | — | $6,224.14 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 261 | $4.75 | $3.37 | — | $4,981.02 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 586 | $2.12 | $7.56 | — | $3,731.14 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 52 | $23.63 | $2.15 | — | $2,500.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 107 | $11.55 | $2.31 | — | $1,262.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1244.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $157.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1244.18 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.21 | ▼ close $9,893.62 vs 09:30 $9,953.44 (session -30.50) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.21 | ▼ 09:30 equity $9,792.00 vs yday $9,893.62 (-101.62) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.21 | ▲ close $9,839.69 vs 09:30 $9,792.00 (session +47.69) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.21 | ▼ 09:30 equity $9,792.54 vs yday $9,839.69 (-47.15) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.21 | ▼ close $9,623.40 vs 09:30 $9,792.54 (session -169.14) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.21 | ▼ 09:30 equity $9,523.16 vs yday $9,623.40 (-100.24) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 6 | $194.84 | $2.03 | $-76.04 | $1,324.22 | ▼ -76.04 after sell → book $9,521.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 22 | $51.29 | $2.08 | $-109.73 | $2,450.53 | ▼ -109.73 after sell → book $9,519.06; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 609 | $1.89 | $7.97 | $-107.17 | $3,593.57 | ▼ -107.17 after sell → book $9,511.09; vs 09:30 mark -7.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 261 | $4.73 | $3.42 | $-12.01 | $4,824.68 | ▼ -12.01 after sell → book $9,507.67; vs 09:30 mark -3.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 586 | $1.84 | $7.67 | $-179.31 | $5,895.25 | ▼ -179.31 after sell → book $9,500.00; vs 09:30 mark -7.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 52 | $25.58 | $2.17 | $+97.09 | $7,223.25 | ▲ +97.09 after sell → book $9,497.84; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 107 | $10.75 | $2.34 | $-90.25 | $8,371.16 | ▼ -90.25 after sell → book $9,495.50; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $9,493.47 | ▲ +17.45 after sell → book $9,493.47; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,407.91 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.0; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,249.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 32 | $36.46 | $2.09 | — | $6,080.27 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+2.9; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 63 | $18.61 | $2.18 | — | $4,905.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 65 | $18.21 | $2.19 | — | $3,719.82 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-19.1; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $2,548.35 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 202 | $5.87 | $2.61 | — | $1,360.00 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1186.68 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 436 | $2.72 | $5.62 | — | $168.46 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.4; leftover $1186.68 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.46 | ▲ close $9,746.28 vs 09:30 $9,523.16 (session +273.57) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.46 | ▲ 09:30 equity $9,914.33 vs yday $9,746.28 (+168.05) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $147.75 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $21.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $132.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $21.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 123 | $0.17 | $0.58 | — | $110.92 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $21.06 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $94.89 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $21.06 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.89 | ▲ close $10,079.34 vs 09:30 $9,914.33 (session +166.12) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.89 | ▲ 09:30 equity $10,111.34 vs yday $10,079.34 (+32.00) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 12 | $0.97 | $0.15 | — | $83.10 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $11.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 3 | $3.95 | $0.13 | — | $71.12 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $11.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $59.34 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $11.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 3 | $3.58 | $0.12 | — | $48.48 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $11.86 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.48 | ▼ close $10,033.94 vs 09:30 $10,111.34 (session -76.88) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.48 | ▲ 09:30 equity $10,127.60 vs yday $10,033.94 (+93.66) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,113.50 | ▼ -20.54 after sell → book $10,125.58; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $2,255.50 | ▼ -16.84 after sell → book $10,123.53; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 32 | $36.70 | $2.11 | $+3.49 | $3,427.79 | ▲ +3.49 after sell → book $10,121.42; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 63 | $22.11 | $2.20 | $+216.12 | $4,818.52 | ▲ +216.12 after sell → book $10,119.22; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 65 | $20.55 | $2.21 | $+147.71 | $6,152.06 | ▲ +147.71 after sell → book $10,117.01; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+170.83 | $7,494.36 | ▲ +170.83 after sell → book $10,114.95; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 202 | $5.62 | $2.65 | $-55.76 | $8,626.95 | ▼ -55.76 after sell → book $10,112.30; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 436 | $3.13 | $5.71 | $+167.43 | $9,985.92 | ▲ +167.43 after sell → book $10,106.59; vs 09:30 mark -5.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,878.82 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+6.5; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,718.22 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-5.8; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,472.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+7.6; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 134 | $9.31 | $2.39 | — | $5,222.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 92 | $13.47 | $2.27 | — | $3,980.67 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1124 | $1.11 | $14.50 | — | $2,718.53 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 124 | $9.99 | $2.36 | — | $1,477.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1248.24 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 683 | $1.82 | $8.81 | — | $222.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1248.24 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.13 | ▼ close $9,953.32 vs 09:30 $10,127.60 (session -116.90) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.13 | ▼ 09:30 equity $9,927.00 vs yday $9,953.32 (-26.32) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $242.26 | ▼ -0.58 after sell → book $9,926.77; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 123 | $0.16 | $0.60 | $-2.40 | $261.34 | ▼ -2.40 after sell → book $9,926.18; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 3 | $9.40 | $0.29 | — | $232.85 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=+9.5; leftover $32.67 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.85 | ▲ close $10,143.81 vs 09:30 $9,927.00 (session +217.93) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.85 | ▼ 09:30 equity $10,132.03 vs yday $10,143.81 (-11.78) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 2 | $7.95 | $0.18 | $+0.38 | $248.56 | ▲ +0.38 after sell → book $10,131.85; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $265.47 | ▲ +0.87 after sell → book $10,131.66; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 12 | $0.89 | $0.16 | $-1.28 | $275.99 | ▼ -1.28 after sell → book $10,131.49; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 3 | $4.10 | $0.15 | $+0.17 | $288.14 | ▲ +0.17 after sell → book $10,131.34; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $300.56 | ▲ +0.65 after sell → book $10,131.19; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 3 | $3.59 | $0.14 | $-0.22 | $311.20 | ▼ -0.22 after sell → book $10,131.05; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $283.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $51.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 5 | $9.81 | $0.51 | — | $233.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $51.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 2 | $20.65 | $0.42 | — | $191.85 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $51.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 13 | $3.93 | $0.55 | — | $140.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $51.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 3 | $15.72 | $0.48 | — | $92.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $51.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 2 | $25.40 | $0.51 | — | $41.26 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $51.87 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.26 | ▼ close $9,764.87 vs 09:30 $10,132.03 (session -363.43) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.26 | ▼ 09:30 equity $9,684.13 vs yday $9,764.87 (-80.74) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,186.88 | ▲ +38.52 after sell → book $9,682.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $2,308.48 | ▼ -39.00 after sell → book $9,680.08; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $3,533.88 | ▼ -20.25 after sell → book $9,678.03; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 134 | $8.67 | $2.42 | $-90.58 | $4,693.23 | ▼ -90.58 after sell → book $9,675.61; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 92 | $12.26 | $2.29 | $-116.34 | $5,818.86 | ▼ -116.34 after sell → book $9,673.31; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1124 | $1.05 | $14.70 | $-96.64 | $6,984.36 | ▼ -96.64 after sell → book $9,658.62; vs 09:30 mark -14.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 124 | $9.80 | $2.39 | $-28.31 | $8,197.17 | ▼ -28.31 after sell → book $9,656.23; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 683 | $1.73 | $8.93 | $-86.04 | $9,366.41 | ▼ -86.04 after sell → book $9,647.29; vs 09:30 mark -8.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,366.41 | ▲ close $9,649.96 vs 09:30 $9,684.13 (session +2.67) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,351.39 | ▲ 09:30 equity $8,567.87 vs yday $8,567.39 (+0.48) | 09:30 open · cash $8,351.39 (unchanged overnight, no fees) · equity $8,567.87 vs prior close $8,567.39 (+0.48) · 9 name(s) re-marked at the open (per-name table). ADMA×2 yday $9.52 → 09:30 $9.52 +0.00; APPS×2 yday $10.88 → 09:30 $10.88 +0.00; ARHS×3 yday $9.47 → 09:30 $9.47 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; HELP×2 yday $12.59 → 09:30 $12.59 +0.00; NN×2 yday $14.45 → 09:30 $14.45 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PGEN×3 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,311.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1043.92 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $6,269.29 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.7; leftover $1043.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 136 | $7.65 | $2.40 | — | $5,226.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1043.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $4,199.86 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1043.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $3,192.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1043.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 115 | $9.05 | $2.33 | — | $2,149.63 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-27.1; leftover $1043.92 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 44 | $23.58 | $2.12 | — | $1,109.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1043.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 474 | $2.20 | $6.11 | — | $61.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1043.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.07 | ▼ close $8,476.38 vs 09:30 $8,567.87 (session -70.30) | 16:00 close · cash $61.07 · equity $8,476.38 vs 09:30 $8,567.87 (-91.49; session marks -70.30) · 17 name(s) marked open→close (per-name table). ADMA×2 09:30 $9.52 → close $9.52 +0.00; APPS×2 09:30 $10.88 → close $10.88 +0.00; ARHS×3 09:30 $9.47 → close $9.47 +0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; HELP×2 09:30 $12.59 → close $12.59 +0.00; NN×2 09:30 $14.45 → close $14.45 -0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PGEN×3 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×136 09:30 $7.65 → close $7.60 -6.80; WRBY×39 09:30 $26.27 → close $26.71 +17.16; TXG×12 09:30 $83.76 → close $85.71 +23.40; AEHL×115 09:30 $9.05 → close $9.36 +35.65; BRVE×44 09:30 $23.58 → close $20.62 -130.24; HLP×474 09:30 $2.20 → close $2.21 +4.74 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VST` | cash | leftover split 7.03 < 1 share @ 146.90 |
| 2026-08-14 | `DAVE` | cash | leftover split 7.03 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 7.03 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 7.03 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.37 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.37 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 4.37 < 1 share @ 202.70 |
| 2026-08-17 | `NB` | cash | leftover split 4.37 < 1 share @ 5.07 |
| 2026-08-17 | `CDNL` | cash | leftover split 4.37 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 4.37 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 4.37 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 4.37 < 1 share @ 92.99 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 4.78 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 4.78 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 4.78 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 4.78 < 1 share @ 11.13 |
| 2026-08-21 | `DE` | cash | leftover split 4.78 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 4.78 < 1 share @ 14.96 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 1.01 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 1.01 < 1 share @ 24.84 |
| 2026-08-26 | `CRMD` | cash | leftover split 1.01 < 1 share @ 8.60 |
| 2026-08-26 | `RZLT` | cash | leftover split 1.01 < 1 share @ 5.01 |
| 2026-08-26 | `AVBP` | cash | leftover split 1.01 < 1 share @ 31.21 |
| 2026-08-26 | `ABX` | cash | leftover split 1.01 < 1 share @ 9.83 |
| 2026-08-26 | `ITG` | cash | leftover split 1.01 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 1.01 < 1 share @ 9.48 |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 1.01 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 1.01 < 1 share @ 14.42 |
| 2026-08-27 | `SLI` | cash | leftover split 1.01 < 1 share @ 2.60 |
| 2026-08-27 | `ITG` | cash | leftover split 1.01 < 1 share @ 12.36 |
| 2026-08-27 | `BE` | cash | leftover split 1.01 < 1 share @ 227.10 |
| 2026-08-27 | `INDP` | cash | leftover split 1.01 < 1 share @ 1.13 |
| 2026-08-27 | `CAPR` | cash | leftover split 1.01 < 1 share @ 9.19 |
| 2026-08-27 | `BZ` | cash | leftover split 1.01 < 1 share @ 18.50 |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 18.80 < 1 share @ 263.36 |
| 2026-09-04 | `DELL` | cash | leftover split 18.80 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 18.80 < 1 share @ 82.70 |
| 2026-09-04 | `MDB` | cash | leftover split 18.80 < 1 share @ 378.34 |
| 2026-09-04 | `ASST` | cash | leftover split 18.80 < 1 share @ 25.18 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 21.06 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 21.06 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 21.06 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 21.06 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 11.86 < 1 share @ 108.55 |
| 2026-09-18 | `VICR` | cash | leftover split 11.86 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 11.86 < 1 share @ 85.00 |
| 2026-09-18 | `BHVN` | cash | leftover split 11.86 < 1 share @ 14.07 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 32.67 < 1 share @ 93.97 |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALOY` | 3 | 2026-09-22 @ $9.40 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=+9.5; leftover $32.67 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $51.87 |
| `ADMA` | 5 | 2026-09-23 @ $9.81 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $51.87 |
| `OMER` | 2 | 2026-09-23 @ $20.65 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $51.87 |
| `INDP` | 13 | 2026-09-23 @ $3.93 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $51.87 |
| `SGRY` | 3 | 2026-09-23 @ $15.72 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $51.87 |
| `TNGX` | 2 | 2026-09-23 @ $25.40 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $51.87 |
