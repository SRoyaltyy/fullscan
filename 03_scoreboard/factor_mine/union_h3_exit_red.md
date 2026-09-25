# Factor mine action — `union_h3_exit_red`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · buy last-green, sell next 09:30 if last bar flipped red

Cash book **-15.07%** ($8,493) · signal-only (no cash/fees) was +22.17%. Starts YES **3/30**. Fills 189 · skips 297 · realized $-713.89.

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
- Early exit: sell at the next 09:30 if the last bar flipped red, even inside the floor.
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,121.18.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.25 | ▲ close $10,286.85 vs 09:30 $10,000.00 (session +322.82) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $49.60 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $43.53 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $39.18 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $34.95 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $7.03 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 64 | $20.55 | $2.18 | — | $9,267.17 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 64 | $20.65 | $2.18 | — | $7,943.38 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 229 | $5.77 | $2.95 | — | $6,619.10 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 67 | $19.63 | $2.19 | — | $5,301.70 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $3,995.86 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 756 | $1.75 | $9.75 | — | $2,663.10 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $1,360.23 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1323.07 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 268 | $4.92 | $3.46 | — | $38.21 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1323.07 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.21 | ▲ close $10,730.97 vs 09:30 $10,584.55 (session +173.28) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.21 | ▲ 09:30 equity $11,103.25 vs yday $10,730.97 (+372.28) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $34.20 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.78 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $30.84 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.78 | — |
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
| 2026-08-25 09:30 ET | **SELL** | `CYPH` | 3 | $1.56 | $0.08 | $+0.60 | $10,980.31 | ▲ +0.60 after sell → book $10,983.33; vs 09:30 mark -0.08 | exit last-red after 2 sess | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3833 | $0.36 | $25.22 | — | $9,582.88 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-15.6; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 123 | $11.12 | $2.36 | — | $8,212.76 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 100 | $13.59 | $2.29 | — | $6,851.47 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 144 | $9.49 | $2.42 | — | $5,482.48 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $4,112.86 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 301 | $4.55 | $3.88 | — | $2,739.43 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 62 | $21.79 | $2.18 | — | $1,386.28 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable; 🔵; ret5=+3.1; leftover $1372.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 842 | $1.63 | $10.86 | — | $2.95 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1372.54 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.95 | ▲ close $11,084.72 vs 09:30 $11,010.61 (session +152.70) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.95 | ▼ 09:30 equity $11,043.17 vs yday $11,084.72 (-41.55) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $5.96 | ▼ -0.36 after sell → book $11,043.12; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.96 | ▲ close $11,107.09 vs 09:30 $11,043.17 (session +63.98) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.96 | ▲ 09:30 equity $11,142.65 vs yday $11,107.09 (+35.56) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 100 | $12.98 | $2.32 | $-65.61 | $1,301.64 | ▼ -65.61 after sell → book $11,140.34; vs 09:30 mark -2.31 | exit last-red after 2 sess | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 3 | $41.44 | $1.25 | — | $1,176.07 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+3.1; leftover $162.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 11 | $14.42 | $1.62 | — | $1,015.83 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+7.1; leftover $162.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 62 | $2.60 | $1.80 | — | $852.83 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,ohlc_hot; ret5=+13.0; leftover $162.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `ITG` | 13 | $12.36 | $1.65 | — | $690.50 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $162.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `INDP` | 143 | $1.13 | $2.04 | — | $526.87 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,yday_mover; ret5=+21.3; leftover $162.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 17 | $9.19 | $1.61 | — | $369.03 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $162.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 8 | $18.50 | $1.50 | — | $219.52 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $162.70 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.52 | ▼ close $10,929.82 vs 09:30 $11,142.65 (session -199.04) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.52 | ▼ 09:30 equity $10,923.84 vs yday $10,929.82 (-5.98) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3833 | $0.36 | $26.14 | $-24.53 | $1,592.43 | ▼ -24.53 after sell → book $10,897.70; vs 09:30 mark -26.14 | exit last-red after 3 sess | — |
| 2026-08-28 09:30 ET | **SELL** | `VITL` | 123 | $10.47 | $2.39 | $-84.70 | $2,877.85 | ▼ -84.70 after sell → book $10,895.31; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 144 | $9.70 | $2.46 | $+25.36 | $4,272.19 | ▲ +25.36 after sell → book $10,892.85; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 37 | $39.60 | $2.12 | $+93.46 | $5,735.27 | ▲ +93.46 after sell → book $10,890.73; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 301 | $4.21 | $3.94 | $-110.17 | $6,998.54 | ▼ -110.17 after sell → book $10,886.79; vs 09:30 mark -3.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 62 | $22.10 | $2.20 | $+14.85 | $8,366.54 | ▲ +14.85 after sell → book $10,884.59; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 842 | $1.69 | $11.01 | $+28.65 | $9,778.51 | ▲ +28.65 after sell → book $10,873.58; vs 09:30 mark -11.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 8 | $18.15 | $1.50 | $-5.80 | $9,922.21 | ▼ -5.80 after sell → book $10,872.08; vs 09:30 mark -1.50 | exit last-red after 1 sess | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 288 | $8.61 | $3.72 | — | $7,438.82 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $2480.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 16 | $146.07 | $2.04 | — | $5,099.66 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2480.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 271 | $9.13 | $3.50 | — | $2,621.93 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer; 🔵; ret5=+20.0; leftover $2480.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 1008 | $2.46 | $13.00 | — | $129.25 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer; ret5=+7.9; leftover $2480.55 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.25 | ▼ close $10,671.85 vs 09:30 $10,923.84 (session -177.98) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.25 | ▼ 09:30 equity $10,637.79 vs yday $10,671.85 (-34.06) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.25 | ▼ close $10,275.03 vs 09:30 $10,637.79 (session -362.75) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.25 | ▼ 09:30 equity $10,168.60 vs yday $10,275.03 (-106.43) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 3 | $41.94 | $1.29 | $-1.04 | $253.78 | ▼ -1.04 after sell → book $10,167.31; vs 09:30 mark -1.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 11 | $15.82 | $1.79 | $+11.99 | $426.01 | ▲ +11.99 after sell → book $10,165.52; vs 09:30 mark -1.79 | exit last-red after 3 sess | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 62 | $2.67 | $1.86 | $+0.68 | $589.69 | ▲ +0.68 after sell → book $10,163.66; vs 09:30 mark -1.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ITG` | 13 | $12.12 | $1.63 | $-6.40 | $745.61 | ▼ -6.40 after sell → book $10,162.02; vs 09:30 mark -1.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `INDP` | 143 | $1.10 | $2.04 | $-8.37 | $900.88 | ▼ -8.37 after sell → book $10,159.99; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 17 | $10.77 | $1.90 | $+23.34 | $1,082.06 | ▲ +23.34 after sell → book $10,158.08; vs 09:30 mark -1.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,082.06 | ▼ close $10,025.80 vs 09:30 $10,168.60 (session -132.28) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,082.06 | ▼ 09:30 equity $9,987.89 vs yday $10,025.80 (-37.91) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 288 | $7.25 | $3.78 | $-399.17 | $3,166.29 | ▼ -399.17 after sell → book $9,984.12; vs 09:30 mark -3.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 16 | $139.65 | $2.07 | $-106.82 | $5,398.62 | ▼ -106.82 after sell → book $9,982.05; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 271 | $8.73 | $3.56 | $-115.46 | $7,760.89 | ▼ -115.46 after sell → book $9,978.49; vs 09:30 mark -3.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 1008 | $2.20 | $13.19 | $-288.27 | $9,965.30 | ▼ -288.27 after sell → book $9,965.30; vs 09:30 mark -13.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,965.30 | ▲ close $9,965.30 vs 09:30 $9,987.89 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,965.30 | ▲ 09:30 equity $9,965.30 vs yday $9,965.30 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,747.00 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,499.96 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 343 | $3.63 | $4.42 | — | $6,250.44 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 155 | $8.03 | $2.46 | — | $5,003.34 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,809.27 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $2,566.08 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 83 | $14.85 | $2.24 | — | $1,331.29 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1245.66 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 571 | $2.18 | $7.37 | — | $79.14 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1245.66 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.14 | ▼ close $9,718.24 vs 09:30 $9,965.30 (session -222.21) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.14 | ▼ 09:30 equity $9,665.22 vs yday $9,718.24 (-53.02) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 23 | $52.03 | $2.08 | $-23.69 | $1,273.75 | ▼ -23.69 after sell → book $9,663.14; vs 09:30 mark -2.08 | exit last-red after 1 sess | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $41.50 | $2.10 | $-45.64 | $2,475.16 | ▼ -45.64 after sell → book $9,661.05; vs 09:30 mark -2.09 | exit last-red after 1 sess | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 343 | $3.46 | $4.49 | $-67.23 | $3,657.44 | ▼ -67.23 after sell → book $9,656.55; vs 09:30 mark -4.50 | exit last-red after 1 sess | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $4,825.68 | ▼ -25.83 after sell → book $9,654.52; vs 09:30 mark -2.03 | exit last-red after 1 sess | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 2 | $263.36 | $2.00 | — | $4,296.96 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $3,781.19 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 191 | $3.15 | $2.56 | — | $3,176.98 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 7 | $82.70 | $2.01 | — | $2,596.06 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 240 | $2.51 | $3.10 | — | $1,990.57 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `FCEL` | 41 | $14.52 | $2.11 | — | $1,393.14 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_mover; ret5=-24.1; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $1,012.80 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list yday_mover; ret5=-12.7; leftover $603.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 23 | $25.18 | $2.06 | — | $431.60 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $603.21 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $431.60 | ▲ close $9,812.90 vs 09:30 $9,665.22 (session +176.21) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $431.60 | ▼ 09:30 equity $9,721.51 vs yday $9,812.90 (-91.39) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $431.60 | ▲ close $9,751.63 vs 09:30 $9,721.51 (session +30.12) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $431.60 | ▲ 09:30 equity $9,754.10 vs yday $9,751.63 (+2.47) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 155 | $8.01 | $2.49 | $-8.05 | $1,670.66 | ▼ -8.05 after sell → book $9,751.61; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 74 | $15.46 | $2.23 | $-101.39 | $2,812.47 | ▼ -101.39 after sell → book $9,749.38; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 83 | $13.60 | $2.26 | $-108.25 | $3,939.01 | ▼ -108.25 after sell → book $9,747.12; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 571 | $2.22 | $7.47 | $+8.00 | $5,199.15 | ▲ +8.00 after sell → book $9,739.64; vs 09:30 mark -7.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,199.15 | ▼ close $9,638.64 vs 09:30 $9,754.10 (session -101.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,199.15 | ▼ 09:30 equity $9,580.18 vs yday $9,638.64 (-58.46) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 2 | $245.35 | $2.02 | $-40.03 | $5,687.84 | ▼ -40.03 after sell → book $9,578.16; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DELL` | 1 | $523.83 | $2.01 | $+6.04 | $6,209.66 | ▲ +6.04 after sell → book $9,576.15; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `SLBT` | 191 | $2.58 | $2.60 | $-114.04 | $6,699.83 | ▼ -114.04 after sell → book $9,573.54; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `TARS` | 7 | $80.89 | $2.03 | $-16.70 | $7,264.04 | ▼ -16.70 after sell → book $9,571.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 240 | $2.87 | $3.15 | $+80.16 | $7,949.70 | ▲ +80.16 after sell → book $9,568.37; vs 09:30 mark -3.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `FCEL` | 41 | $16.07 | $2.13 | $+59.30 | $8,606.43 | ▲ +59.30 after sell → book $9,566.23; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MDB` | 1 | $360.42 | $2.01 | $-21.93 | $8,964.84 | ▼ -21.93 after sell → book $9,564.22; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASST` | 23 | $26.06 | $2.08 | $+16.10 | $9,562.14 | ▲ +16.10 after sell → book $9,562.14; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,562.14 | ▲ close $9,562.14 vs 09:30 $9,580.18 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,562.14 | ▲ 09:30 equity $9,562.14 vs yday $9,562.14 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $8,525.94 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+8.3; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 21 | $56.09 | $2.05 | — | $7,345.99 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ret5=+19.6; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 585 | $2.04 | $7.55 | — | $6,145.05 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 251 | $4.75 | $3.24 | — | $4,949.56 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 563 | $2.12 | $7.26 | — | $3,748.74 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 50 | $23.63 | $2.14 | — | $2,565.10 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $1,373.15 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1195.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $268.29 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1195.27 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.29 | ▼ close $9,497.16 vs 09:30 $9,562.14 (session -36.43) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.29 | ▼ 09:30 equity $9,408.12 vs yday $9,497.16 (-89.04) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.29 | ▲ close $9,463.11 vs 09:30 $9,408.12 (session +54.99) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.29 | ▼ 09:30 equity $9,414.63 vs yday $9,463.11 (-48.48) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.29 | ▼ close $9,257.66 vs 09:30 $9,414.63 (session -156.97) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.29 | ▼ 09:30 equity $9,158.97 vs yday $9,257.66 (-98.69) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $1,240.46 | ▼ -64.03 after sell → book $9,156.94; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 21 | $51.29 | $2.07 | $-104.93 | $2,315.48 | ▼ -104.93 after sell → book $9,154.87; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 585 | $1.89 | $7.65 | $-102.95 | $3,413.48 | ▼ -102.95 after sell → book $9,147.22; vs 09:30 mark -7.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 251 | $4.73 | $3.29 | $-11.55 | $4,597.42 | ▼ -11.55 after sell → book $9,143.93; vs 09:30 mark -3.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 563 | $1.84 | $7.37 | $-172.27 | $5,625.97 | ▼ -172.27 after sell → book $9,136.56; vs 09:30 mark -7.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 50 | $25.58 | $2.16 | $+93.20 | $6,902.81 | ▲ +93.20 after sell → book $9,134.40; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 103 | $10.75 | $2.33 | $-87.03 | $8,007.73 | ▼ -87.03 after sell → book $9,132.07; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $9,130.04 | ▲ +17.45 after sell → book $9,130.04; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,044.48 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+4.0; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $6,962.77 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 31 | $36.46 | $2.08 | — | $5,830.43 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ret5=+2.9; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 61 | $18.61 | $2.17 | — | $4,693.04 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 62 | $18.21 | $2.18 | — | $3,561.85 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-19.1; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $2,459.17 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 194 | $5.87 | $2.57 | — | $1,317.82 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1141.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 419 | $2.72 | $5.41 | — | $172.73 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-0.4; leftover $1141.26 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.73 | ▲ close $9,373.51 vs 09:30 $9,158.97 (session +263.95) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.73 | ▲ 09:30 equity $9,534.48 vs yday $9,373.51 (+160.97) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $152.02 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $21.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $136.68 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $21.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 127 | $0.17 | $0.60 | — | $114.50 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $21.59 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $98.46 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $21.59 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.46 | ▲ close $9,688.82 vs 09:30 $9,534.48 (session +155.47) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.46 | ▲ 09:30 equity $9,719.20 vs yday $9,688.82 (+30.38) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 12 | $0.97 | $0.15 | — | $86.67 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $12.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 3 | $3.95 | $0.13 | — | $74.69 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $12.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $62.91 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $12.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 3 | $3.58 | $0.12 | — | $52.06 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $12.31 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.06 | ▼ close $9,646.11 vs 09:30 $9,719.20 (session -72.58) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.06 | ▲ 09:30 equity $9,735.73 vs yday $9,646.11 (+89.62) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,117.07 | ▼ -20.54 after sell → book $9,733.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 14 | $76.27 | $2.05 | $-15.98 | $2,182.80 | ▼ -15.98 after sell → book $9,731.65; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 31 | $36.70 | $2.10 | $+3.25 | $3,318.40 | ▲ +3.25 after sell → book $9,729.55; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 61 | $22.11 | $2.19 | $+209.13 | $4,664.91 | ▲ +209.13 after sell → book $9,727.35; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 62 | $20.55 | $2.20 | $+140.71 | $5,936.82 | ▲ +140.71 after sell → book $9,725.16; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $7,200.04 | ▲ +160.54 after sell → book $9,723.10; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 194 | $5.62 | $2.61 | $-53.69 | $8,287.71 | ▼ -53.69 after sell → book $9,720.49; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 419 | $3.13 | $5.49 | $+160.90 | $9,593.69 | ▲ +160.90 after sell → book $9,715.00; vs 09:30 mark -5.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 2 | $7.84 | $0.18 | $+0.16 | $9,609.19 | ▲ +0.16 after sell → book $9,714.82; vs 09:30 mark -0.18 | exit last-red after 2 sess | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,502.09 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+6.5; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,341.49 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=-5.8; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $6,184.67 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+7.6; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 129 | $9.31 | $2.38 | — | $4,981.30 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 89 | $13.47 | $2.26 | — | $3,779.77 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1082 | $1.11 | $13.96 | — | $2,564.79 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 120 | $9.99 | $2.35 | — | $1,363.64 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1201.15 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 658 | $1.82 | $8.49 | — | $154.30 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1201.15 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.30 | ▼ close $9,566.92 vs 09:30 $9,735.73 (session -112.42) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.30 | ▼ 09:30 equity $9,541.51 vs yday $9,566.92 (-25.41) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $174.43 | ▼ -0.58 after sell → book $9,541.28; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 127 | $0.16 | $0.62 | $-2.48 | $194.14 | ▼ -2.48 after sell → book $9,540.67; vs 09:30 mark -0.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 2 | $9.40 | $0.19 | — | $175.15 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=+9.5; leftover $24.27 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.15 | ▲ close $9,750.83 vs 09:30 $9,541.51 (session +210.35) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.15 | ▼ 09:30 equity $9,739.03 vs yday $9,750.83 (-11.80) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $192.05 | ▲ +0.87 after sell → book $9,738.83; vs 09:30 mark -0.20 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 12 | $0.89 | $0.16 | $-1.28 | $202.57 | ▼ -1.28 after sell → book $9,738.67; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 3 | $4.10 | $0.15 | $+0.17 | $214.72 | ▲ +0.17 after sell → book $9,738.52; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $227.14 | ▲ +0.65 after sell → book $9,738.36; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 3 | $3.59 | $0.14 | $-0.22 | $237.78 | ▼ -0.22 after sell → book $9,738.23; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $209.71 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $39.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 4 | $9.81 | $0.40 | — | $170.06 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $39.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $149.20 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $39.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 10 | $3.93 | $0.42 | — | $109.48 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $39.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 2 | $15.72 | $0.32 | — | $77.72 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $39.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 1 | $25.40 | $0.26 | — | $52.06 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $39.63 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.06 | ▼ close $9,389.52 vs 09:30 $9,739.03 (session -346.81) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.06 | ▼ 09:30 equity $9,312.46 vs yday $9,389.52 (-77.06) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,197.68 | ▲ +38.52 after sell → book $9,310.43; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $2,319.28 | ▼ -39.00 after sell → book $9,308.41; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $3,457.01 | ▼ -19.09 after sell → book $9,306.36; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 129 | $8.67 | $2.41 | $-87.35 | $4,573.03 | ▼ -87.35 after sell → book $9,303.95; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 89 | $12.26 | $2.28 | $-112.67 | $5,661.89 | ▼ -112.67 after sell → book $9,301.67; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1082 | $1.05 | $14.15 | $-93.03 | $6,783.84 | ▼ -93.03 after sell → book $9,287.52; vs 09:30 mark -14.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 120 | $9.80 | $2.38 | $-27.53 | $7,957.46 | ▼ -27.53 after sell → book $9,285.14; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 658 | $1.73 | $8.61 | $-82.90 | $9,083.90 | ▼ -82.90 after sell → book $9,276.53; vs 09:30 mark -8.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 10 | $3.77 | $0.43 | $-2.45 | $9,121.18 | ▼ -2.45 after sell → book $9,276.11; vs 09:30 mark -0.42 | exit last-red after 1 sess | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,121.18 | ▼ close $9,275.73 vs 09:30 $9,312.46 (session -0.38) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,341.81 | ▲ 09:30 equity $8,584.56 vs yday $8,584.08 (+0.48) | 09:30 open · cash $8,341.81 (unchanged overnight, no fees) · equity $8,584.56 vs prior close $8,584.08 (+0.48) · 10 name(s) re-marked at the open (per-name table). ADMA×2 yday $9.52 → 09:30 $9.52 +0.00; APPS×2 yday $10.88 → 09:30 $10.88 +0.00; ARHS×3 yday $9.47 → 09:30 $9.47 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; HELP×2 yday $12.59 → 09:30 $12.59 +0.00; NN×2 yday $14.45 → 09:30 $14.45 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PGEN×3 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `OMER` | 1 | $20.61 | $0.23 | $-0.48 | $8,362.19 | ▼ -0.48 after sell → book $8,584.33; vs 09:30 mark -0.23 | exit last-red after 2 sess | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,321.93 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1045.27 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $6,280.09 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+4.7; leftover $1045.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 136 | $7.65 | $2.40 | — | $5,237.29 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1045.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $4,210.66 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1045.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $3,203.51 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1045.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 115 | $9.05 | $2.33 | — | $2,160.43 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=-27.1; leftover $1045.27 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 44 | $23.58 | $2.12 | — | $1,120.78 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1045.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 475 | $2.20 | $6.13 | — | $69.66 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1045.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.66 | ▼ close $8,493.37 vs 09:30 $8,584.56 (session -69.76) | 16:00 close · cash $69.66 · equity $8,493.37 vs 09:30 $8,584.56 (-91.19; session marks -69.76) · 17 name(s) marked open→close (per-name table). ADMA×2 09:30 $9.52 → close $9.52 +0.00; APPS×2 09:30 $10.88 → close $10.88 +0.00; ARHS×3 09:30 $9.47 → close $9.47 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; HELP×2 09:30 $12.59 → close $12.59 +0.00; NN×2 09:30 $14.45 → close $14.45 -0.00; PGEN×3 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×136 09:30 $7.65 → close $7.60 -6.80; WRBY×39 09:30 $26.27 → close $26.71 +17.16; TXG×12 09:30 $83.76 → close $85.71 +23.40; AEHL×115 09:30 $9.05 → close $9.36 +35.65; BRVE×44 09:30 $23.58 → close $20.62 -130.24; HLP×475 09:30 $2.20 → close $2.21 +4.75 | — |

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
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 0.74 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 0.74 < 1 share @ 24.84 |
| 2026-08-26 | `CRMD` | cash | leftover split 0.74 < 1 share @ 8.60 |
| 2026-08-26 | `RZLT` | cash | leftover split 0.74 < 1 share @ 5.01 |
| 2026-08-26 | `AVBP` | cash | leftover split 0.74 < 1 share @ 31.21 |
| 2026-08-26 | `ABX` | cash | leftover split 0.74 < 1 share @ 9.83 |
| 2026-08-26 | `ITG` | cash | leftover split 0.74 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 0.74 < 1 share @ 9.48 |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | cash | leftover split 162.70 < 1 share @ 227.10 |
| 2026-08-28 | `ITG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ITG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TARS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MDB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `TARS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MDB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `ILMN` | cash | leftover split 21.59 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 21.59 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 21.59 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 21.59 < 1 share @ 34.93 |
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
| 2026-09-18 | `RBRK` | cash | leftover split 12.31 < 1 share @ 108.55 |
| 2026-09-18 | `VICR` | cash | leftover split 12.31 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 12.31 < 1 share @ 85.00 |
| 2026-09-18 | `BHVN` | cash | leftover split 12.31 < 1 share @ 14.07 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-22 | `USFD` | cash | leftover split 24.27 < 1 share @ 93.97 |
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
| `ALOY` | 2 | 2026-09-22 @ $9.40 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; ret5=+9.5; leftover $24.27 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $39.63 |
| `ADMA` | 4 | 2026-09-23 @ $9.81 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $39.63 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $39.63 |
| `SGRY` | 2 | 2026-09-23 @ $15.72 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $39.63 |
| `TNGX` | 1 | 2026-09-23 @ $25.40 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $39.63 |
