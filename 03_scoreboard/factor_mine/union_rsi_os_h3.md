# Factor mine action — `union_rsi_os_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-14.71%** ($8,529) · signal-only (no cash/fees) was -42.97%. Starts YES **6/30**. Fills 72 · skips 88 · realized $-2389.72.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is oversold (≤30) — Finviz prior export, else computed on prior bars.
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
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,554.91.

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
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 929 | $2.69 | $11.98 | — | $7,489.01 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 342 | $7.29 | $4.41 | — | $4,991.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 230 | $10.83 | $2.97 | — | $2,497.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 247 | $10.06 | $3.19 | — | $9.54 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $2500.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▲ close $10,431.83 vs 09:30 $10,000.00 (session +454.38) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $10,219.44 vs yday $10,431.83 (-212.39) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1 | $1.62 | $0.02 | — | $7.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $2.39 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $10,021.56 vs 09:30 $10,219.44 (session -197.87) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,800.00 vs yday $10,021.56 (-221.56) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $9,759.13 vs 09:30 $9,800.00 (session -40.87) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,737.15 vs yday $9,759.13 (-21.98) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 929 | $2.56 | $12.16 | $-144.91 | $2,373.98 | ▼ -144.91 after sell → book $9,724.99; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 342 | $6.74 | $4.49 | $-197.00 | $4,674.58 | ▼ -197.00 after sell → book $9,720.51; vs 09:30 mark -4.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 230 | $10.85 | $3.03 | $-1.39 | $7,167.05 | ▼ -1.39 after sell → book $9,717.48; vs 09:30 mark -3.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `YSS` | 247 | $10.32 | $3.25 | $+57.79 | $9,712.84 | ▲ +57.79 after sell → book $9,714.23; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,712.84 | ▲ close $9,714.38 vs 09:30 $9,737.15 (session +0.15) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,712.84 | ▲ 09:30 equity $9,714.39 vs yday $9,714.38 (+0.01) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 1 | $1.55 | $0.04 | $-0.13 | $9,714.36 | ▼ -0.13 after sell → book $9,714.36; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 288 | $33.61 | $3.72 | — | $30.96 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.4; leftover $9714.36 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▲ close $9,722.16 vs 09:30 $9,714.39 (session +11.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▼ 09:30 equity $9,716.40 vs yday $9,722.16 (-5.76) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▲ close $9,739.44 vs 09:30 $9,716.40 (session +23.04) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▼ 09:30 equity $9,693.36 vs yday $9,739.44 (-46.08) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▼ close $9,281.52 vs 09:30 $9,693.36 (session -411.84) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▲ 09:30 equity $9,342.00 vs yday $9,281.52 (+60.48) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `LZB` | 288 | $32.33 | $3.84 | $-376.19 | $9,338.16 | ▼ -376.19 after sell → book $9,338.16; vs 09:30 mark -3.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 421 | $11.09 | $5.43 | — | $4,663.84 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-8.0; leftover $4669.08 | — |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 784 | $5.93 | $10.11 | — | $4.61 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-17.5; leftover $4669.08 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.61 | ▲ close $9,774.42 vs 09:30 $9,342.00 (session +451.80) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.61 | ▼ 09:30 equity $9,186.05 vs yday $9,774.42 (-588.37) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.61 | ▼ close $8,723.36 vs 09:30 $9,186.05 (session -462.69) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.61 | ▲ 09:30 equity $8,933.15 vs yday $8,723.36 (+209.79) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.61 | ▲ close $8,937.66 vs 09:30 $8,933.15 (session +4.51) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.61 | ▼ 09:30 equity $8,772.44 vs yday $8,937.66 (-165.22) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 784 | $6.27 | $10.28 | $+246.16 | $4,910.01 | ▲ +246.16 after sell → book $8,762.16; vs 09:30 mark -10.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 8 | $306.34 | $2.01 | — | $2,457.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $2455.00 | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 2094 | $1.16 | $27.01 | — | $1.22 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-13.8; leftover $2455.00 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.22 | ▼ close $8,531.66 vs 09:30 $8,772.44 (session -201.47) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.22 | ▼ 09:30 equity $8,162.94 vs yday $8,531.66 (-368.72) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 421 | $8.70 | $5.53 | $-1017.15 | $3,658.39 | ▼ -1,017.15 after sell → book $8,157.41; vs 09:30 mark -5.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,658.39 | ▲ close $8,228.65 vs 09:30 $8,162.94 (session +71.24) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,658.39 | ▼ 09:30 equity $8,086.61 vs yday $8,228.65 (-142.04) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,658.39 | ▼ close $7,803.70 vs 09:30 $8,086.61 (session -282.91) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,658.39 | ▲ 09:30 equity $7,859.47 vs yday $7,803.70 (+55.77) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `DY` | 8 | $287.99 | $2.04 | $-150.86 | $5,960.27 | ▼ -150.86 after sell → book $7,857.43; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LX` | 2094 | $0.91 | $25.62 | $-584.51 | $7,831.81 | ▼ -584.51 after sell → book $7,831.81; vs 09:30 mark -25.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,831.81 | ▲ close $7,831.81 vs 09:30 $7,859.47 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,831.81 | ▲ 09:30 equity $7,831.81 vs yday $7,831.81 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 133 | $7.31 | $2.39 | — | $6,857.20 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 94 | $10.38 | $2.27 | — | $5,879.67 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1183 | $0.83 | $13.33 | — | $4,888.00 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1529 | $0.64 | $14.37 | — | $3,895.07 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 344 | $2.84 | $4.44 | — | $2,913.67 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 44 | $22.00 | $2.12 | — | $1,943.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 49 | $19.86 | $2.14 | — | $968.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-5.5; leftover $978.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 75 | $12.78 | $2.21 | — | $7.56 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-4.4; leftover $978.98 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.56 | ▼ close $7,654.06 vs 09:30 $7,831.81 (session -134.47) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.56 | ▲ 09:30 equity $7,848.47 vs yday $7,654.06 (+194.41) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 4 | $1.75 | $0.08 | — | $0.47 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $7.56 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.47 | ▼ close $7,767.35 vs 09:30 $7,848.47 (session -81.04) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.47 | ▼ 09:30 equity $7,730.61 vs yday $7,767.35 (-36.74) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.47 | ▼ close $7,627.84 vs 09:30 $7,730.61 (session -102.76) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.47 | ▼ 09:30 equity $7,589.75 vs yday $7,627.84 (-38.09) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 133 | $7.27 | $2.42 | $-10.13 | $964.96 | ▼ -10.13 after sell → book $7,587.33; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 94 | $10.49 | $2.30 | $+6.24 | $1,948.72 | ▲ +6.24 after sell → book $7,585.04; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LX` | 1183 | $0.83 | $13.63 | $-17.50 | $2,922.90 | ▼ -17.50 after sell → book $7,571.40; vs 09:30 mark -13.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 1529 | $0.59 | $13.93 | $-98.64 | $3,817.19 | ▼ -98.64 after sell → book $7,557.47; vs 09:30 mark -13.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FJET` | 344 | $2.63 | $4.50 | $-81.18 | $4,717.40 | ▼ -81.18 after sell → book $7,552.96; vs 09:30 mark -4.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OSW` | 44 | $21.86 | $2.14 | $-10.42 | $5,677.10 | ▼ -10.42 after sell → book $7,550.82; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PL` | 49 | $18.00 | $2.16 | $-95.43 | $6,556.95 | ▼ -95.43 after sell → book $7,548.67; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SWBI` | 75 | $13.12 | $2.24 | $+21.05 | $7,538.71 | ▲ +21.05 after sell → book $7,546.43; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,538.71 | ▼ close $7,546.15 vs 09:30 $7,589.75 (session -0.28) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,538.71 | ▼ 09:30 equity $7,545.95 vs yday $7,546.15 (-0.20) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AIIO` | 4 | $1.81 | $0.10 | $+0.05 | $7,545.84 | ▲ +0.05 after sell → book $7,545.84; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,545.84 | ▲ close $7,545.84 vs 09:30 $7,545.95 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,545.84 | ▲ 09:30 equity $7,545.84 vs yday $7,545.84 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 122 | $20.61 | $2.36 | — | $5,029.07 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $2515.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 714 | $3.52 | $9.21 | — | $2,506.58 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2515.28 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 45 | $54.66 | $2.12 | — | $44.75 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $2515.28 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.75 | ▲ close $7,569.84 vs 09:30 $7,545.84 (session +37.69) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.75 | ▲ 09:30 equity $7,604.47 vs yday $7,569.84 (+34.63) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.75 | ▲ close $7,826.41 vs 09:30 $7,604.47 (session +221.94) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.75 | ▼ 09:30 equity $7,806.99 vs yday $7,826.41 (-19.42) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.75 | ▲ close $7,968.88 vs 09:30 $7,806.99 (session +161.89) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.75 | ▲ 09:30 equity $8,078.12 vs yday $7,968.88 (+109.24) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `NAVN` | 122 | $22.50 | $2.40 | $+225.83 | $2,787.35 | ▲ +225.83 after sell → book $8,075.72; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RWT` | 714 | $3.98 | $9.35 | $+309.88 | $5,619.72 | ▲ +309.88 after sell → book $8,066.37; vs 09:30 mark -9.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COO` | 45 | $54.37 | $2.15 | $-17.33 | $8,064.22 | ▼ -17.33 after sell → book $8,064.22; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 97 | $10.30 | $2.28 | — | $7,062.84 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 146 | $6.86 | $2.43 | — | $6,058.85 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.4; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6300 | $0.16 | $28.98 | — | $5,021.87 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1194 | $0.84 | $13.66 | — | $4,000.47 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 430 | $2.34 | $5.55 | — | $2,988.73 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 370 | $2.72 | $4.77 | — | $1,977.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 146 | $6.86 | $2.43 | — | $973.57 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1008.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 265 | $3.66 | $3.42 | — | $0.25 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1008.03 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▼ close $7,885.60 vs 09:30 $8,078.12 (session -115.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $7,914.60 vs yday $7,885.60 (+29.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▲ close $7,955.40 vs 09:30 $7,914.60 (session +40.80) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $7,990.37 vs yday $7,955.40 (+34.97) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▼ close $7,895.33 vs 09:30 $7,990.37 (session -95.04) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $7,981.92 vs yday $7,895.33 (+86.59) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ALHC` | 97 | $8.33 | $2.31 | $-195.68 | $805.95 | ▼ -195.68 after sell → book $7,979.62; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `PLAY` | 146 | $6.68 | $2.46 | $-31.17 | $1,778.77 | ▼ -31.17 after sell → book $7,977.16; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `DVLT` | 6300 | $0.16 | $30.04 | $-59.02 | $2,756.73 | ▼ -59.02 after sell → book $7,947.12; vs 09:30 mark -30.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `NMRA` | 1194 | $0.74 | $12.59 | $-154.01 | $3,624.12 | ▼ -154.01 after sell → book $7,934.53; vs 09:30 mark -12.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ZSQR` | 430 | $2.68 | $5.63 | $+135.02 | $4,770.89 | ▲ +135.02 after sell → book $7,928.90; vs 09:30 mark -5.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CTMX` | 370 | $2.80 | $4.84 | $+19.98 | $5,802.05 | ▲ +19.98 after sell → book $7,924.06; vs 09:30 mark -4.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRBP` | 146 | $7.51 | $2.46 | $+90.01 | $6,896.04 | ▲ +90.01 after sell → book $7,921.59; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 265 | $3.87 | $3.47 | $+48.76 | $7,918.12 | ▲ +48.76 after sell → book $7,918.12; vs 09:30 mark -3.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 65 | $40.00 | $2.19 | — | $5,315.94 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-32.2; leftover $2639.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 439 | $6.00 | $5.66 | — | $2,676.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-24.1; leftover $2639.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 38 | $68.39 | $2.10 | — | $75.35 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-7.0; leftover $2639.37 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.35 | ▼ close $7,901.42 vs 09:30 $7,981.92 (session -6.75) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.35 | ▼ 09:30 equity $7,897.03 vs yday $7,901.42 (-4.39) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.35 | ▲ close $7,910.20 vs 09:30 $7,897.03 (session +13.17) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.35 | ▲ 09:30 equity $8,004.25 vs yday $7,910.20 (+94.05) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 24 | $0.77 | $0.26 | — | $56.66 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $18.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 15 | $1.22 | $0.23 | — | $38.13 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-33.0; leftover $18.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `XNDU` | 3 | $5.99 | $0.19 | — | $19.97 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.9; leftover $18.84 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.97 | ▼ close $7,614.13 vs 09:30 $8,004.25 (session -389.45) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.97 | ▲ 09:30 equity $7,615.83 vs yday $7,614.13 (+1.70) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `XENE` | 65 | $36.50 | $2.21 | $-231.90 | $2,390.26 | ▼ -231.90 after sell → book $7,613.61; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SION` | 439 | $5.50 | $5.76 | $-230.92 | $4,799.00 | ▼ -230.92 after sell → book $7,607.86; vs 09:30 mark -5.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `THO` | 38 | $72.58 | $2.14 | $+154.98 | $7,554.91 | ▲ +154.98 after sell → book $7,605.72; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,554.91 | ▼ close $7,604.11 vs 09:30 $7,615.83 (session -1.61) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,942.88 | ▲ 09:30 equity $8,983.30 vs yday $8,983.30 (+0.00) | 09:30 open · cash $8,942.88 (unchanged overnight, no fees) · equity $8,983.30 vs prior close $8,983.30 (+0.00) · 3 name(s) re-marked at the open (per-name table). CMPX×13 yday $1.13 → 09:30 $1.13 +0.00; NMRA×22 yday $0.70 → 09:30 $0.70 +0.00; XNDU×2 yday $5.10 → 09:30 $5.10 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 1354 | $2.20 | $17.47 | — | $5,946.61 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.1; leftover $2980.96 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 134 | $22.21 | $2.39 | — | $2,968.08 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2980.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GEN` | 129 | $22.91 | $2.38 | — | $10.31 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.6; leftover $2980.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.31 | ▼ close $8,528.71 vs 09:30 $8,983.30 (session -432.36) | 16:00 close · cash $10.31 · equity $8,528.71 vs 09:30 $8,983.30 (-454.59; session marks -432.36) · 6 name(s) marked open→close (per-name table). CMPX×13 09:30 $1.14 → close $1.14 -0.00; NMRA×22 09:30 $0.70 → close $0.70 +0.00; XNDU×2 09:30 $5.10 → close $5.10 -0.00; SFIX×1354 09:30 $2.20 → close $2.15 -60.93; ACAD×134 09:30 $22.21 → close $20.68 -205.02; GEN×129 09:30 $22.91 → close $21.62 -166.41 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 2.39 < 1 share @ 39.85 |
| 2026-08-17 | `KLC` | cash | leftover split 2.39 < 1 share @ 2.62 |
| 2026-08-17 | `CSAN` | cash | leftover split 2.39 < 1 share @ 2.50 |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | cash | leftover split 15.48 < 1 share @ 42.41 |
| 2026-08-21 | `WMT` | cash | leftover split 15.48 < 1 share @ 103.69 |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 4.61 < 1 share @ 121.87 |
| 2026-08-27 | `DKS` | cash | leftover split 4.61 < 1 share @ 128.73 |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FJET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FJET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OSW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SWBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AIIO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AIIO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRLN` | cash | leftover split 0.25 < 1 share @ 2.27 |
| 2026-09-18 | `ALHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RARE` | cash | leftover split 0.12 < 1 share @ 14.79 |
| 2026-09-18 | `FLNC` | cash | leftover split 0.12 < 1 share @ 7.54 |
| 2026-09-22 | `XENE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `XENE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `EVER` | cash | leftover split 18.84 < 1 share @ 19.46 |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `XNDU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ALKT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LXEO` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NMRA` | 24 | 2026-09-23 @ $0.77 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $18.84 |
| `CMPX` | 15 | 2026-09-23 @ $1.22 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-33.0; leftover $18.84 |
| `XNDU` | 3 | 2026-09-23 @ $5.99 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.9; leftover $18.84 |
