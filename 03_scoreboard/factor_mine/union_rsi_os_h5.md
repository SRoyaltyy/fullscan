# Factor mine action — `union_rsi_os_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os hold 5, no 🚨

Cash book **-19.02%** ($8,098) · signal-only (no cash/fees) was -62.78%. Starts YES **4/30**. Fills 68 · skips 145 · realized $-2687.08.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $776.41.

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
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 929 | $2.69 | $11.98 | — | $7,489.01 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 342 | $7.29 | $4.41 | — | $4,991.41 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 230 | $10.83 | $2.97 | — | $2,497.55 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 247 | $10.06 | $3.19 | — | $9.54 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $2500.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▲ close $10,431.83 vs 09:30 $10,000.00 (session +454.38) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $10,219.44 vs yday $10,431.83 (-212.39) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1 | $1.62 | $0.02 | — | $7.90 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $2.39 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $10,021.56 vs 09:30 $10,219.44 (session -197.87) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,800.00 vs yday $10,021.56 (-221.56) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $9,759.13 vs 09:30 $9,800.00 (session -40.87) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,737.15 vs yday $9,759.13 (-21.98) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $9,582.88 vs 09:30 $9,737.15 (session -154.27) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,562.39 vs yday $9,582.88 (-20.49) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $9,085.17 vs 09:30 $9,562.39 (session -477.22) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▲ 09:30 equity $9,125.15 vs yday $9,085.17 (+39.98) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `NCMI` | 929 | $2.55 | $12.16 | $-154.20 | $2,364.69 | ▼ -154.20 after sell → book $9,112.99; vs 09:30 mark -12.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `QMLS` | 342 | $5.44 | $4.48 | $-641.60 | $4,220.69 | ▼ -641.60 after sell → book $9,108.51; vs 09:30 mark -4.48 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `CLBT` | 230 | $11.30 | $3.03 | $+102.11 | $6,816.66 | ▲ +102.11 after sell → book $9,105.48; vs 09:30 mark -3.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `YSS` | 247 | $9.26 | $3.25 | $-204.03 | $9,100.64 | ▼ -204.03 after sell → book $9,102.24; vs 09:30 mark -3.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 107 | $42.41 | $2.31 | — | $4,560.46 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.1; leftover $4550.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 43 | $103.69 | $2.12 | — | $99.67 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-10.3; leftover $4550.32 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.67 | ▲ close $9,116.34 vs 09:30 $9,125.15 (session +18.53) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.67 | ▲ 09:30 equity $9,185.58 vs yday $9,116.34 (+69.24) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `INV` | 1 | $1.54 | $0.04 | $-0.14 | $101.17 | ▼ -0.14 after sell → book $9,185.54; vs 09:30 mark -0.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.17 | ▲ close $9,348.65 vs 09:30 $9,185.58 (session +163.11) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.17 | ▼ 09:30 equity $9,309.52 vs yday $9,348.65 (-39.13) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 4 | $11.09 | $0.46 | — | $56.36 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-8.0; leftover $50.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 8 | $5.93 | $0.50 | — | $8.42 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-17.5; leftover $50.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.42 | ▼ close $9,305.52 vs 09:30 $9,309.52 (session -3.05) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.42 | ▲ 09:30 equity $9,330.24 vs yday $9,305.52 (+24.72) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.42 | ▼ close $9,294.59 vs 09:30 $9,330.24 (session -35.65) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.42 | ▼ 09:30 equity $9,171.16 vs yday $9,294.59 (-123.43) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.42 | ▲ close $9,182.64 vs 09:30 $9,171.16 (session +11.48) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.42 | ▲ 09:30 equity $9,206.09 vs yday $9,182.64 (+23.45) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AAP` | 107 | $43.72 | $2.37 | $+135.49 | $4,684.09 | ▲ +135.49 after sell → book $9,203.72; vs 09:30 mark -2.37 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `WMT` | 43 | $103.09 | $2.16 | $-30.08 | $9,114.80 | ▼ -30.08 after sell → book $9,201.56; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 14 | $306.34 | $2.03 | — | $4,824.00 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $4557.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 3928 | $1.16 | $50.67 | — | $216.85 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list overnight; ret5=-13.8; leftover $4557.40 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.85 | ▼ close $9,056.73 vs 09:30 $9,206.09 (session -92.12) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.85 | ▼ 09:30 equity $8,438.67 vs yday $9,056.73 (-618.06) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.85 | ▲ close $8,578.35 vs 09:30 $8,438.67 (session +139.68) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.85 | ▼ 09:30 equity $8,314.33 vs yday $8,578.35 (-264.02) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `QFIN` | 4 | $8.65 | $0.38 | $-10.59 | $251.07 | ▼ -10.59 after sell → book $8,313.95; vs 09:30 mark -0.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `QMLS` | 8 | $5.92 | $0.52 | $-1.10 | $297.92 | ▼ -1.10 after sell → book $8,313.44; vs 09:30 mark -0.51 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $297.92 | ▼ close $7,784.61 vs 09:30 $8,314.33 (session -528.82) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $297.92 | ▲ 09:30 equity $7,888.55 vs yday $7,784.61 (+103.94) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $297.92 | ▼ close $7,680.53 vs 09:30 $7,888.55 (session -208.02) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $297.92 | ▲ 09:30 equity $7,704.37 vs yday $7,680.53 (+23.84) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 5 | $7.31 | $0.38 | — | $260.99 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $42.56 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 4 | $10.38 | $0.43 | — | $219.06 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $42.56 | — |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 66 | $0.64 | $0.62 | — | $176.20 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $42.56 | — |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 14 | $2.84 | $0.44 | — | $136.00 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $42.56 | — |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 1 | $22.00 | $0.22 | — | $113.78 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $42.56 | — |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 2 | $19.86 | $0.40 | — | $73.65 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list overnight; ret5=-5.5; leftover $42.56 | — |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 3 | $12.78 | $0.39 | — | $34.92 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-4.4; leftover $42.56 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.92 | ▲ close $7,772.91 vs 09:30 $7,704.37 (session +71.42) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.92 | ▲ 09:30 equity $7,815.78 vs yday $7,772.91 (+42.87) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `DY` | 14 | $296.40 | $2.08 | $-143.27 | $4,182.45 | ▼ -143.27 after sell → book $7,813.70; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 3928 | $0.86 | $46.17 | $-1283.09 | $7,506.50 | ▼ -1,283.09 after sell → book $7,767.53; vs 09:30 mark -46.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 4257 | $1.75 | $54.92 | — | $1.84 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $7506.50 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.84 | ▲ close $8,091.00 vs 09:30 $7,815.78 (session +378.38) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.84 | ▼ 09:30 equity $7,961.05 vs yday $8,091.00 (-129.95) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.84 | ▲ close $8,555.00 vs 09:30 $7,961.05 (session +593.95) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.84 | ▼ 09:30 equity $8,469.40 vs yday $8,555.00 (-85.60) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.84 | ▼ close $8,160.40 vs 09:30 $8,469.40 (session -309.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.84 | ▼ 09:30 equity $7,947.81 vs yday $8,160.40 (-212.59) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.84 | ▼ close $7,859.44 vs 09:30 $7,947.81 (session -88.37) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.84 | ▼ 09:30 equity $7,734.87 vs yday $7,859.44 (-124.57) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `SION` | 5 | $7.79 | $0.42 | $+1.60 | $40.37 | ▲ +1.60 after sell → book $7,734.45; vs 09:30 mark -0.42 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ALMS` | 4 | $9.99 | $0.43 | $-2.40 | $79.89 | ▼ -2.40 after sell → book $7,734.02; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `EVTL` | 66 | $0.57 | $0.59 | $-6.03 | $116.72 | ▼ -6.03 after sell → book $7,733.42; vs 09:30 mark -0.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `FJET` | 14 | $2.04 | $0.35 | $-11.99 | $144.93 | ▼ -11.99 after sell → book $7,733.07; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `OSW` | 1 | $22.28 | $0.25 | $-0.19 | $166.97 | ▼ -0.19 after sell → book $7,732.83; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `PL` | 2 | $16.67 | $0.36 | $-7.14 | $199.95 | ▼ -7.14 after sell → book $7,732.47; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SWBI` | 3 | $13.40 | $0.43 | $+1.04 | $239.72 | ▲ +1.04 after sell → book $7,732.04; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 3 | $20.61 | $0.63 | — | $177.26 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $79.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 22 | $3.52 | $0.84 | — | $98.98 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $79.91 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 1 | $54.66 | $0.55 | — | $43.77 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $79.91 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.77 | ▼ close $7,390.60 vs 09:30 $7,734.87 (session -339.42) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.77 | ▼ 09:30 equity $7,306.13 vs yday $7,390.60 (-84.47) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AIIO` | 4257 | $1.66 | $55.68 | $-493.72 | $7,054.71 | ▼ -493.72 after sell → book $7,250.45; vs 09:30 mark -55.68 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,054.71 | ▲ close $7,257.30 vs 09:30 $7,306.13 (session +6.85) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,054.71 | ▼ 09:30 equity $7,256.67 vs yday $7,257.30 (-0.63) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,054.71 | ▲ close $7,261.26 vs 09:30 $7,256.67 (session +4.59) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,054.71 | ▲ 09:30 equity $7,264.14 vs yday $7,261.26 (+2.88) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 85 | $10.30 | $2.25 | — | $6,176.97 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 128 | $6.86 | $2.37 | — | $5,296.51 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.4; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 5511 | $0.16 | $25.35 | — | $4,389.40 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1044 | $0.84 | $11.94 | — | $3,496.32 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 376 | $2.34 | $4.85 | — | $2,611.63 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 324 | $2.72 | $4.18 | — | $1,726.17 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 128 | $6.86 | $2.37 | — | $845.72 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $881.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 230 | $3.66 | $2.97 | — | $0.95 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $881.84 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.95 | ▼ close $7,105.83 vs 09:30 $7,264.14 (session -102.02) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.95 | ▲ 09:30 equity $7,132.95 vs yday $7,105.83 (+27.12) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.95 | ▲ close $7,164.88 vs 09:30 $7,132.95 (session +31.93) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.95 | ▲ 09:30 equity $7,197.08 vs yday $7,164.88 (+32.20) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `NAVN` | 3 | $22.17 | $0.69 | $+3.36 | $66.77 | ▲ +3.36 after sell → book $7,196.38; vs 09:30 mark -0.70 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `RWT` | 22 | $3.97 | $0.96 | $+8.10 | $153.15 | ▲ +8.10 after sell → book $7,195.42; vs 09:30 mark -0.96 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COO` | 1 | $54.50 | $0.57 | $-1.28 | $207.08 | ▼ -1.28 after sell → book $7,194.86; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 7 | $14.79 | $1.06 | — | $102.49 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $103.54 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 13 | $7.54 | $1.02 | — | $3.52 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-20.9; leftover $103.54 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.52 | ▼ close $7,105.07 vs 09:30 $7,197.08 (session -87.70) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.52 | ▲ 09:30 equity $7,181.80 vs yday $7,105.07 (+76.73) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.52 | ▼ close $7,174.20 vs 09:30 $7,181.80 (session -7.60) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.52 | ▲ 09:30 equity $7,222.34 vs yday $7,174.20 (+48.14) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.52 | ▲ close $7,354.75 vs 09:30 $7,222.34 (session +132.42) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.52 | ▲ 09:30 equity $7,394.51 vs yday $7,354.75 (+39.76) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALHC` | 85 | $7.92 | $2.27 | $-206.81 | $674.45 | ▼ -206.81 after sell → book $7,392.24; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `PLAY` | 128 | $7.02 | $2.41 | $+15.70 | $1,570.61 | ▲ +15.70 after sell → book $7,389.84; vs 09:30 mark -2.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `DVLT` | 5511 | $0.16 | $26.28 | $-51.63 | $2,426.09 | ▼ -51.63 after sell → book $7,363.56; vs 09:30 mark -26.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ZSQR` | 376 | $2.78 | $4.92 | $+155.67 | $3,466.45 | ▲ +155.67 after sell → book $7,358.64; vs 09:30 mark -4.92 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `CTMX` | 324 | $2.90 | $4.24 | $+49.90 | $4,401.81 | ▲ +49.90 after sell → book $7,354.40; vs 09:30 mark -4.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRBP` | 128 | $7.83 | $2.41 | $+119.38 | $5,401.64 | ▲ +119.38 after sell → book $7,351.99; vs 09:30 mark -2.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 230 | $4.10 | $3.02 | $+95.22 | $6,341.62 | ▲ +95.22 after sell → book $7,348.98; vs 09:30 mark -3.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1732 | $1.22 | $22.34 | — | $4,206.24 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-33.0; leftover $2113.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `XNDU` | 352 | $5.99 | $4.54 | — | $2,093.22 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.9; leftover $2113.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVER` | 107 | $19.46 | $2.31 | — | $8.69 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-5.0; leftover $2113.87 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.69 | ▼ close $6,763.56 vs 09:30 $7,394.51 (session -556.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.69 | ▼ 09:30 equity $6,702.17 vs yday $6,763.56 (-61.39) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1044 | $0.75 | $11.10 | $-125.36 | $776.41 | ▼ -125.36 after sell → book $6,691.07; vs 09:30 mark -11.10 | dropped from list after 6 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $776.41 | ▼ close $6,670.22 vs 09:30 $6,702.17 (session -20.85) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $971.50 | ▲ 09:30 equity $8,150.57 vs yday $8,150.57 (-0.00) | 09:30 open · cash $971.50 (unchanged overnight, no fees) · equity $8,150.57 vs prior close $8,150.57 (-0.00) · 3 name(s) re-marked at the open (per-name table). CMPX×2182 yday $1.13 → 09:30 $1.13 +0.00; EVER×135 yday $18.06 → 09:30 $18.06 +0.00; XNDU×444 yday $5.10 → 09:30 $5.10 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 147 | $2.20 | $2.43 | — | $645.67 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.1; leftover $323.83 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 14 | $22.21 | $2.03 | — | $332.70 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $323.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GEN` | 14 | $22.91 | $2.03 | — | $9.93 | — | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.6; leftover $323.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.93 | ▼ close $8,097.98 vs 09:30 $8,150.57 (session -46.10) | 16:00 close · cash $9.93 · equity $8,097.98 vs 09:30 $8,150.57 (-52.59; session marks -46.10) · 6 name(s) marked open→close (per-name table). CMPX×2182 09:30 $1.14 → close $1.14 -0.00; EVER×135 09:30 $18.06 → close $18.06 -0.00; XNDU×444 09:30 $5.10 → close $5.10 -0.00; SFIX×147 09:30 $2.20 → close $2.15 -6.62; ACAD×14 09:30 $22.21 → close $20.68 -21.42; GEN×14 09:30 $22.91 → close $21.62 -18.06 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `YSS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 2.39 < 1 share @ 39.85 |
| 2026-08-17 | `KLC` | cash | leftover split 2.39 < 1 share @ 2.62 |
| 2026-08-17 | `CSAN` | cash | leftover split 2.39 < 1 share @ 2.50 |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `YSS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NCMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `QMLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `CLBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `YSS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `NCMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `QMLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `CLBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `YSS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `INV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `LZB` | cash | leftover split 7.90 < 1 share @ 33.61 |
| 2026-08-21 | `INV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `AAP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `WMT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `AAP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `WMT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 8.42 < 1 share @ 121.87 |
| 2026-08-27 | `AAP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `WMT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 8.42 < 1 share @ 128.73 |
| 2026-08-28 | `QMLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `QFIN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `QMLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `DY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-03 | `DY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `FJET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `PL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `FJET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `OSW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `PL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SWBI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `AIIO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `SION` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `EVTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `FJET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `OSW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `PL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SWBI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `AIIO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SION` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `EVTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `OSW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `PL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SWBI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `AIIO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `AIIO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `COO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `NAVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `RWT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `NAVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RWT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `CTMX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `CRBP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `MRLN` | cash | leftover split 0.95 < 1 share @ 2.27 |
| 2026-09-18 | `ALHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `CTMX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `CRBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `ALHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `PLAY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `NMRA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ZSQR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `CTMX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `CRBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `XENE` | cash | leftover split 1.17 < 1 share @ 40.00 |
| 2026-09-21 | `SION` | cash | leftover split 1.17 < 1 share @ 6.00 |
| 2026-09-21 | `THO` | cash | leftover split 1.17 < 1 share @ 68.39 |
| 2026-09-22 | `ALHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `PLAY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `NMRA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ZSQR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `CTMX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `CRBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `XNDU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `EVER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ALKT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LXEO` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RARE` | 7 | 2026-09-18 @ $14.79 | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $103.54 |
| `FLNC` | 13 | 2026-09-18 @ $7.54 | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-20.9; leftover $103.54 |
| `CMPX` | 1732 | 2026-09-23 @ $1.22 | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-33.0; leftover $2113.87 |
| `XNDU` | 352 | 2026-09-23 @ $5.99 | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.9; leftover $2113.87 |
| `EVER` | 107 | 2026-09-23 @ $19.46 | union ∩ rsi_os hold 5, no 🚨; gate rsi_os=True; list yday_mover; ret5=-5.0; leftover $2113.87 |
