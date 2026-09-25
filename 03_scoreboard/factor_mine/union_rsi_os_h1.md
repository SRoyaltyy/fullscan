# Factor mine action — `union_rsi_os_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-29.98%** ($7,002) · signal-only (no cash/fees) was -7.54%. Starts YES **0/30**. Fills 93 · skips 20 · realized $-3180.18.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,819.85.

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
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 929 | $2.80 | $12.16 | $+78.05 | $2,598.58 | ▲ +78.05 after sell → book $10,207.28; vs 09:30 mark -12.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 342 | $7.24 | $4.49 | $-26.00 | $5,070.17 | ▼ -26.00 after sell → book $10,202.79; vs 09:30 mark -4.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 230 | $11.19 | $3.03 | $+76.81 | $7,640.85 | ▲ +76.81 after sell → book $10,199.77; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 247 | $10.36 | $3.25 | $+67.67 | $10,196.52 | ▲ +67.67 after sell → book $10,196.52; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 63 | $39.85 | $2.18 | — | $7,683.79 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $2549.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1573 | $1.62 | $20.29 | — | $5,115.24 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $2549.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 972 | $2.62 | $12.54 | — | $2,556.06 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $2549.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 1017 | $2.50 | $13.12 | — | $0.44 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $2549.13 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.44 | ▼ close $9,701.70 vs 09:30 $10,219.44 (session -446.70) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.44 | ▲ 09:30 equity $9,705.69 vs yday $9,701.70 (+3.99) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 63 | $41.57 | $2.21 | $+103.97 | $2,617.14 | ▲ +103.97 after sell → book $9,703.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 1573 | $1.32 | $20.57 | $-504.90 | $4,680.80 | ▼ -504.90 after sell → book $9,682.91; vs 09:30 mark -20.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 972 | $2.52 | $12.72 | $-122.46 | $7,117.52 | ▼ -122.46 after sell → book $9,670.19; vs 09:30 mark -12.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CSAN` | 1017 | $2.51 | $13.31 | $-16.26 | $9,656.88 | ▼ -16.26 after sell → book $9,656.88; vs 09:30 mark -13.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,656.88 | ▲ close $9,656.88 vs 09:30 $9,705.69 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,656.88 | ▲ 09:30 equity $9,656.88 vs yday $9,656.88 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,656.88 | ▲ close $9,656.88 vs 09:30 $9,656.88 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,656.88 | ▲ 09:30 equity $9,656.88 vs yday $9,656.88 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 287 | $33.61 | $3.70 | — | $7.11 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.4; leftover $9656.88 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.11 | ▲ close $9,664.66 vs 09:30 $9,656.88 (session +11.48) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.11 | ▼ 09:30 equity $9,658.92 vs yday $9,664.66 (-5.74) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 287 | $33.63 | $3.83 | $-1.79 | $9,655.09 | ▼ -1.79 after sell → book $9,655.09; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 113 | $42.41 | $2.33 | — | $4,860.43 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.1; leftover $4827.54 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 46 | $103.69 | $2.13 | — | $88.56 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-10.3; leftover $4827.54 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.56 | ▲ close $9,670.30 vs 09:30 $9,658.92 (session +19.67) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.56 | ▲ 09:30 equity $9,743.65 vs yday $9,670.30 (+73.35) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 113 | $43.05 | $2.39 | $+67.60 | $4,950.82 | ▲ +67.60 after sell → book $9,741.26; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 46 | $104.14 | $2.18 | $+16.40 | $9,739.09 | ▲ +16.40 after sell → book $9,739.09; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.09 | ▲ close $9,739.09 vs 09:30 $9,743.65 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.09 | ▲ 09:30 equity $9,739.09 vs yday $9,739.09 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 439 | $11.09 | $5.66 | — | $4,864.92 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-8.0; leftover $4869.54 | — |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 818 | $5.93 | $10.55 | — | $3.62 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-17.5; leftover $4869.54 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.62 | ▲ close $10,194.15 vs 09:30 $9,739.09 (session +471.28) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.62 | ▼ 09:30 equity $9,580.72 vs yday $10,194.15 (-613.43) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.62 | ▼ close $9,098.07 vs 09:30 $9,580.72 (session -482.65) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.62 | ▲ 09:30 equity $9,316.94 vs yday $9,098.07 (+218.87) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.62 | ▲ close $9,321.71 vs 09:30 $9,316.94 (session +4.77) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.62 | ▼ 09:30 equity $9,149.33 vs yday $9,321.71 (-172.38) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 818 | $6.27 | $10.73 | $+256.84 | $5,121.75 | ▲ +256.84 after sell → book $9,138.60; vs 09:30 mark -10.73 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 8 | $306.34 | $2.01 | — | $2,669.02 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $2560.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 2207 | $1.16 | $28.47 | — | $80.43 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-13.8; leftover $2560.88 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.43 | ▼ close $8,902.61 vs 09:30 $9,149.33 (session -205.51) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.43 | ▼ 09:30 equity $8,512.88 vs yday $8,902.61 (-389.73) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 439 | $8.70 | $5.77 | $-1060.64 | $3,893.96 | ▼ -1,060.64 after sell → book $8,507.11; vs 09:30 mark -5.77 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 8 | $298.01 | $2.04 | $-70.70 | $6,276.00 | ▼ -70.70 after sell → book $8,505.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,276.00 | ▲ close $8,637.49 vs 09:30 $8,512.88 (session +132.42) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,276.00 | ▼ 09:30 equity $8,505.07 vs yday $8,637.49 (-132.42) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `LX` | 2207 | $1.01 | $28.85 | $-388.37 | $8,476.22 | ▼ -388.37 after sell → book $8,476.22; vs 09:30 mark -28.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,476.22 | ▲ close $8,476.22 vs 09:30 $8,505.07 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,476.22 | ▲ 09:30 equity $8,476.22 vs yday $8,476.22 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,476.22 | ▲ close $8,476.22 vs 09:30 $8,476.22 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,476.22 | ▲ 09:30 equity $8,476.22 vs yday $8,476.22 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 144 | $7.31 | $2.42 | — | $7,421.15 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 102 | $10.38 | $2.30 | — | $6,360.61 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1281 | $0.83 | $14.44 | — | $5,286.78 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1655 | $0.64 | $15.56 | — | $4,212.03 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 373 | $2.84 | $4.81 | — | $3,147.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 48 | $22.00 | $2.13 | — | $2,089.76 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 53 | $19.86 | $2.15 | — | $1,035.03 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-5.5; leftover $1059.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 80 | $12.78 | $2.23 | — | $10.40 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-4.4; leftover $1059.53 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.40 | ▼ close $8,285.58 vs 09:30 $8,476.22 (session -144.59) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.40 | ▲ 09:30 equity $8,493.76 vs yday $8,285.58 (+208.18) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 102 | $11.23 | $2.32 | $+82.59 | $1,153.54 | ▲ +82.59 after sell → book $8,491.44; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 1281 | $0.86 | $15.06 | $+10.22 | $2,237.58 | ▲ +10.22 after sell → book $8,476.38; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 1655 | $0.60 | $15.18 | $-96.94 | $3,215.40 | ▼ -96.94 after sell → book $8,461.20; vs 09:30 mark -15.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FJET` | 373 | $2.80 | $4.88 | $-24.62 | $4,254.92 | ▼ -24.62 after sell → book $8,456.32; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 48 | $22.27 | $2.15 | $+8.67 | $5,321.72 | ▲ +8.67 after sell → book $8,454.16; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PL` | 53 | $19.64 | $2.17 | $-15.98 | $6,360.47 | ▼ -15.98 after sell → book $8,451.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 3607 | $1.75 | $46.53 | — | $1.69 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $6360.47 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.69 | ▲ close $8,703.69 vs 09:30 $8,493.76 (session +298.23) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.69 | ▼ 09:30 equity $8,557.88 vs yday $8,703.69 (-145.81) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 144 | $7.13 | $2.46 | $-30.80 | $1,025.96 | ▼ -30.80 after sell → book $8,555.43; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SWBI` | 80 | $12.51 | $2.25 | $-26.08 | $2,024.50 | ▼ -26.08 after sell → book $8,553.17; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AIIO` | 3607 | $1.81 | $47.18 | $+122.71 | $8,505.99 | ▲ +122.71 after sell → book $8,505.99; vs 09:30 mark -47.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,505.99 | ▲ close $8,505.99 vs 09:30 $8,557.88 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,505.99 | ▲ 09:30 equity $8,505.99 vs yday $8,505.99 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,505.99 | ▲ close $8,505.99 vs 09:30 $8,505.99 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,505.99 | ▲ 09:30 equity $8,505.99 vs yday $8,505.99 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,505.99 | ▲ close $8,505.99 vs 09:30 $8,505.99 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,505.99 | ▲ 09:30 equity $8,505.99 vs yday $8,505.99 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 137 | $20.61 | $2.40 | — | $5,680.02 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $2835.33 | — |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 805 | $3.52 | $10.38 | — | $2,836.04 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2835.33 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 51 | $54.66 | $2.14 | — | $46.23 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $2835.33 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.23 | ▲ close $8,533.13 vs 09:30 $8,505.99 (session +42.07) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.23 | ▲ 09:30 equity $8,572.36 vs yday $8,533.13 (+39.23) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 137 | $21.10 | $2.45 | $+62.28 | $2,934.49 | ▲ +62.28 after sell → book $8,569.92; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RWT` | 805 | $3.53 | $10.54 | $-12.88 | $5,765.60 | ▼ -12.88 after sell → book $8,559.38; vs 09:30 mark -10.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 51 | $54.78 | $2.18 | $+1.80 | $8,557.20 | ▲ +1.80 after sell → book $8,557.20; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,557.20 | ▲ close $8,557.20 vs 09:30 $8,572.36 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,557.20 | ▲ 09:30 equity $8,557.20 vs yday $8,557.20 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,557.20 | ▲ close $8,557.20 vs 09:30 $8,557.20 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,557.20 | ▲ 09:30 equity $8,557.20 vs yday $8,557.20 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 103 | $10.30 | $2.30 | — | $7,494.00 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 155 | $6.86 | $2.46 | — | $6,428.25 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.4; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6685 | $0.16 | $30.75 | — | $5,327.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1267 | $0.84 | $14.49 | — | $4,244.05 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 457 | $2.34 | $5.90 | — | $3,168.78 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 393 | $2.72 | $5.07 | — | $2,094.75 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 155 | $6.86 | $2.46 | — | $1,028.99 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1069.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 280 | $3.66 | $3.61 | — | $0.58 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1069.65 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.58 | ▼ close $8,368.18 vs 09:30 $8,557.20 (session -122.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.58 | ▲ 09:30 equity $8,398.87 vs yday $8,368.18 (+30.69) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 155 | $6.96 | $2.49 | $+10.55 | $1,076.89 | ▲ +10.55 after sell → book $8,396.38; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DVLT` | 6685 | $0.17 | $32.54 | $+3.56 | $2,180.80 | ▲ +3.56 after sell → book $8,363.84; vs 09:30 mark -32.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `NMRA` | 1267 | $0.78 | $13.90 | $-109.49 | $3,155.16 | ▼ -109.49 after sell → book $8,349.94; vs 09:30 mark -13.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 457 | $2.35 | $5.98 | $-7.31 | $4,223.13 | ▼ -7.31 after sell → book $8,343.96; vs 09:30 mark -5.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CTMX` | 393 | $2.83 | $5.14 | $+33.02 | $5,330.17 | ▲ +33.02 after sell → book $8,338.81; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRBP` | 155 | $7.26 | $2.49 | $+57.05 | $6,452.98 | ▲ +57.05 after sell → book $8,336.32; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 280 | $3.57 | $3.67 | $-32.48 | $7,448.91 | ▼ -32.48 after sell → book $8,332.65; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `MRLN` | 3262 | $2.27 | $42.08 | — | $2.09 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-29.6; leftover $7448.91 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.09 | ▼ close $7,617.91 vs 09:30 $8,398.87 (session -672.66) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.09 | ▲ 09:30 equity $7,648.47 vs yday $7,617.91 (+30.56) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 103 | $8.68 | $2.33 | $-171.49 | $893.81 | ▼ -171.49 after sell → book $7,646.15; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MRLN` | 3262 | $2.07 | $42.68 | $-737.16 | $7,603.47 | ▼ -737.16 after sell → book $7,603.47; vs 09:30 mark -42.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 257 | $14.79 | $3.32 | — | $3,799.13 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $3801.74 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 503 | $7.54 | $6.49 | — | $2.53 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-20.9; leftover $3801.74 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.53 | ▼ close $7,413.56 vs 09:30 $7,648.47 (session -180.10) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.53 | ▲ 09:30 equity $7,451.67 vs yday $7,413.56 (+38.11) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 257 | $14.58 | $3.39 | $-60.67 | $3,746.20 | ▼ -60.67 after sell → book $7,448.28; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 503 | $7.36 | $6.60 | $-101.12 | $7,441.68 | ▼ -101.12 after sell → book $7,441.68; vs 09:30 mark -6.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 62 | $40.00 | $2.18 | — | $4,959.51 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-32.2; leftover $2480.56 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 413 | $6.00 | $5.33 | — | $2,476.18 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-24.1; leftover $2480.56 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 36 | $68.39 | $2.10 | — | $12.04 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-7.0; leftover $2480.56 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.04 | ▼ close $7,425.26 vs 09:30 $7,451.67 (session -6.82) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.04 | ▼ 09:30 equity $7,421.13 vs yday $7,425.26 (-4.13) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 413 | $5.99 | $5.42 | $-14.87 | $2,480.50 | ▼ -14.87 after sell → book $7,415.72; vs 09:30 mark -5.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,480.50 | ▲ close $7,415.72 vs 09:30 $7,421.13 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,480.50 | ▲ 09:30 equity $7,500.88 vs yday $7,415.72 (+85.16) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `XENE` | 62 | $39.51 | $2.21 | $-34.76 | $4,927.91 | ▼ -34.76 after sell → book $7,498.67; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `THO` | 36 | $71.41 | $2.13 | $+104.49 | $7,496.54 | ▲ +104.49 after sell → book $7,496.54; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 2440 | $0.77 | $26.06 | — | $5,596.56 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1874.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1536 | $1.22 | $19.81 | — | $3,702.83 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-33.0; leftover $1874.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `XNDU` | 312 | $5.99 | $4.02 | — | $1,829.92 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.9; leftover $1874.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVER` | 93 | $19.46 | $2.27 | — | $17.87 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-5.0; leftover $1874.14 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.87 | ▼ close $6,901.18 vs 09:30 $7,500.88 (session -543.19) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.87 | ▼ 09:30 equity $6,872.26 vs yday $6,901.18 (-28.92) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 2440 | $0.75 | $25.94 | $-105.68 | $1,812.17 | ▼ -105.68 after sell → book $6,846.32; vs 09:30 mark -25.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1536 | $1.17 | $20.08 | $-116.70 | $3,589.21 | ▼ -116.70 after sell → book $6,826.24; vs 09:30 mark -20.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `XNDU` | 312 | $5.12 | $4.09 | $-279.55 | $5,182.56 | ▼ -279.55 after sell → book $6,822.15; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVER` | 93 | $17.63 | $2.30 | $-174.76 | $6,819.85 | ▼ -174.76 after sell → book $6,819.85; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,819.85 | ▲ close $6,819.85 vs 09:30 $6,872.26 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,377.64 | ▲ 09:30 equity $7,377.64 vs yday $7,377.64 (+0.00) | 09:30 open · cash $7,377.64 · no holdings · equity $7,377.64 vs prior close $7,377.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 1117 | $2.20 | $14.41 | — | $4,905.83 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.1; leftover $2459.21 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 110 | $22.21 | $2.32 | — | $2,460.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2459.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GEN` | 107 | $22.91 | $2.31 | — | $6.73 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.6; leftover $2459.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.73 | ▼ close $7,002.00 vs 09:30 $7,377.64 (session -356.60) | 16:00 close · cash $6.73 · equity $7,002.00 vs 09:30 $7,377.64 (-375.64; session marks -356.60) · 3 name(s) marked open→close (per-name table). SFIX×1117 09:30 $2.20 → close $2.15 -50.27; ACAD×110 09:30 $22.21 → close $20.68 -168.30; GEN×107 09:30 $22.91 → close $21.62 -138.03 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-26 | `DKS` | cash | leftover split 3.62 < 1 share @ 121.87 |
| 2026-08-27 | `DKS` | cash | leftover split 3.62 < 1 share @ 128.73 |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `XENE` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ALKT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LXEO` | hard_red | hard-red S=-7.66 sit; no new buys |
