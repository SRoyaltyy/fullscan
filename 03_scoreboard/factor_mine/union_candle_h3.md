# Factor mine action — `union_candle_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ candle, no 🚨

Cash book **-17.07%** ($8,293) · signal-only (no cash/fees) was +24.03%. Starts YES **8/30**. Fills 188 · skips 304 · realized $-1035.82.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior-candle capture flag is on.
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
- **Gate** `candle_capture=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,510.27.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 72 | $45.98 | $2.21 | — | $6,687.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+12.3; leftover $3333.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 65 | $50.62 | $2.19 | — | $3,394.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+6.2; leftover $3333.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 142 | $23.33 | $2.42 | — | $79.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+19.7; leftover $3333.33 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.27 | ▲ close $10,136.75 vs 09:30 $10,000.00 (session +143.55) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.27 | ▼ 09:30 equity $10,102.24 vs yday $10,136.75 (-34.51) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $70.55 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $9.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 1 | $7.29 | $0.08 | — | $63.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $9.91 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.19 | ▼ close $9,924.76 vs 09:30 $10,102.24 (session -177.31) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.19 | ▲ 09:30 equity $9,954.74 vs yday $9,924.76 (+29.98) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $55.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $7.90 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.42 | ▼ close $9,836.96 vs 09:30 $9,954.74 (session -117.69) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.42 | ▼ 09:30 equity $9,726.74 vs yday $9,836.96 (-110.22) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 72 | $43.56 | $2.24 | $-178.69 | $3,189.50 | ▼ -178.69 after sell → book $9,724.50; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 65 | $51.77 | $2.22 | $+70.13 | $6,552.32 | ▲ +70.13 after sell → book $9,722.27; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 142 | $22.16 | $2.46 | $-171.02 | $9,696.58 | ▼ -171.02 after sell → book $9,719.81; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,696.58 | ▼ close $9,719.62 vs 09:30 $9,726.74 (session -0.19) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,696.58 | ▲ 09:30 equity $9,719.70 vs yday $9,719.62 (+0.08) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $9,706.04 | ▲ +0.75 after sell → book $9,719.58; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 1 | $6.74 | $0.09 | $-0.72 | $9,712.69 | ▼ -0.72 after sell → book $9,719.49; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,712.69 | ▼ close $9,719.37 vs 09:30 $9,719.70 (session -0.12) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,712.69 | ▼ 09:30 equity $9,719.25 vs yday $9,719.37 (-0.12) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $9,719.15 | ▼ -1.31 after sell → book $9,719.15; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,504.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,304.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,105.06 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $4,888.12 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 694 | $1.75 | $8.95 | — | $3,664.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,506.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 246 | $4.92 | $3.17 | — | $1,292.84 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1214.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $268.59 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1214.89 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.59 | ▲ close $9,932.43 vs 09:30 $9,719.25 (session +238.04) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.59 | ▲ 09:30 equity $10,273.09 vs yday $9,932.43 (+340.66) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $233.84 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $38.37 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $200.10 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $38.37 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 29 | $1.32 | $0.47 | — | $161.35 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $38.37 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 1 | $33.36 | $0.34 | — | $127.66 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $38.37 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 23 | $1.66 | $0.45 | — | $89.03 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $38.37 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.03 | ▼ close $10,266.93 vs 09:30 $10,273.09 (session -4.21) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.03 | ▲ 09:30 equity $10,358.16 vs yday $10,266.93 (+91.23) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.03 | ▲ close $10,372.42 vs 09:30 $10,358.16 (session +14.26) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.03 | ▼ 09:30 equity $10,205.68 vs yday $10,372.42 (-166.74) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 59 | $20.32 | $2.19 | $-17.92 | $1,285.72 | ▼ -17.92 after sell → book $10,203.49; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 58 | $20.47 | $2.18 | $-14.79 | $2,470.79 | ▼ -14.79 after sell → book $10,201.30; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 61 | $21.21 | $2.19 | $+92.01 | $3,762.41 | ▲ +92.01 after sell → book $10,199.11; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 41 | $32.32 | $2.13 | $+106.04 | $5,085.40 | ▲ +106.04 after sell → book $10,196.98; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 694 | $1.90 | $9.08 | $+86.07 | $6,394.92 | ▲ +86.07 after sell → book $10,187.90; vs 09:30 mark -9.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $7,644.97 | ▲ +91.71 after sell → book $10,185.87; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 246 | $5.25 | $3.22 | $+74.78 | $8,933.24 | ▲ +74.78 after sell → book $10,182.64; vs 09:30 mark -3.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 5 | $212.00 | $2.02 | $+33.72 | $9,991.22 | ▲ +33.72 after sell → book $10,180.62; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $9,135.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.0; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 91 | $13.59 | $2.26 | — | $7,896.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 131 | $9.49 | $2.38 | — | $6,650.75 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $5,428.98 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 624 | $2.00 | $8.05 | — | $4,172.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 238 | $5.24 | $3.07 | — | $2,922.74 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 142 | $8.79 | $2.42 | — | $1,672.15 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1248.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 218 | $5.71 | $2.81 | — | $424.56 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1248.90 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $424.56 | ▲ close $10,346.07 vs 09:30 $10,205.68 (session +190.53) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $424.56 | ▼ 09:30 equity $10,296.42 vs yday $10,346.07 (-49.65) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $457.40 | ▼ -1.91 after sell → book $10,296.06; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 3 | $15.35 | $0.49 | $+11.83 | $502.96 | ▲ +11.83 after sell → book $10,295.57; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 29 | $1.60 | $0.57 | $+7.08 | $548.79 | ▲ +7.08 after sell → book $10,295.00; vs 09:30 mark -0.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GMAB` | 1 | $33.78 | $0.36 | $-0.28 | $582.21 | ▼ -0.28 after sell → book $10,294.64; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 23 | $1.53 | $0.44 | $-3.88 | $616.96 | ▼ -3.88 after sell → book $10,294.20; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 10 | $8.60 | $0.89 | — | $530.07 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+4.8; leftover $88.14 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 2 | $31.21 | $0.63 | — | $467.02 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $88.14 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 5 | $16.77 | $0.85 | — | $382.31 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $88.14 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 13 | $6.53 | $0.89 | — | $296.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+3.6; leftover $88.14 | — |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 18 | $4.78 | $0.91 | — | $209.58 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+8.1; leftover $88.14 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 40 | $2.20 | $1.00 | — | $120.58 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+17.8; leftover $88.14 | — |
| 2026-08-26 09:30 ET | **BUY** | `GRRR` | 6 | $14.03 | $0.86 | — | $35.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_mover; ret5=-7.6; leftover $88.14 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.54 | ▼ close $10,202.05 vs 09:30 $10,296.42 (session -86.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.54 | ▲ 09:30 equity $10,273.60 vs yday $10,202.05 (+71.55) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 2 | $2.60 | $0.06 | — | $30.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; ret5=+13.0; leftover $5.92 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.28 | ▲ close $10,290.93 vs 09:30 $10,273.60 (session +17.39) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.28 | ▲ 09:30 equity $10,313.96 vs yday $10,290.93 (+23.03) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $875.79 | ▼ -10.43 after sell → book $10,311.95; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 91 | $13.05 | $2.29 | $-53.69 | $2,061.05 | ▼ -53.69 after sell → book $10,309.66; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 131 | $9.70 | $2.41 | $+22.71 | $3,329.33 | ▲ +22.71 after sell → book $10,307.24; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 33 | $39.60 | $2.11 | $+82.92 | $4,634.02 | ▲ +82.92 after sell → book $10,305.13; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 624 | $1.89 | $8.16 | $-84.85 | $5,805.22 | ▼ -84.85 after sell → book $10,296.97; vs 09:30 mark -8.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 238 | $4.84 | $3.12 | $-101.39 | $6,954.02 | ▼ -101.39 after sell → book $10,293.85; vs 09:30 mark -3.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 142 | $9.08 | $2.45 | $+36.31 | $8,240.93 | ▲ +36.31 after sell → book $10,291.40; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 218 | $6.73 | $2.86 | $+216.69 | $9,705.21 | ▲ +216.69 after sell → book $10,288.54; vs 09:30 mark -2.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 38 | $41.74 | $2.10 | — | $8,116.99 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+2.4; leftover $1617.54 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 110 | $14.63 | $2.32 | — | $6,505.37 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+5.8; leftover $1617.54 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 49 | $32.90 | $2.14 | — | $4,891.13 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1617.54 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 166 | $9.73 | $2.49 | — | $3,273.46 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1617.54 | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1163 | $1.39 | $15.00 | — | $1,641.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1617.54 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 177 | $9.13 | $2.52 | — | $23.36 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1617.54 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.36 | ▼ close $10,000.41 vs 09:30 $10,313.96 (session -261.56) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.36 | ▼ 09:30 equity $9,939.22 vs yday $10,000.41 (-61.19) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CRMD` | 10 | $8.26 | $0.88 | $-5.17 | $105.08 | ▼ -5.17 after sell → book $9,938.34; vs 09:30 mark -0.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 2 | $29.94 | $0.62 | $-3.80 | $164.34 | ▼ -3.80 after sell → book $9,937.72; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 5 | $17.70 | $0.92 | $+2.88 | $251.92 | ▲ +2.88 after sell → book $9,936.80; vs 09:30 mark -0.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ACRS` | 13 | $5.97 | $0.84 | $-9.00 | $328.69 | ▼ -9.00 after sell → book $9,935.96; vs 09:30 mark -0.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TMCI` | 18 | $4.60 | $0.90 | $-5.06 | $410.59 | ▼ -5.06 after sell → book $9,935.06; vs 09:30 mark -0.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BRR` | 40 | $2.23 | $1.03 | $-0.83 | $498.76 | ▼ -0.83 after sell → book $9,934.03; vs 09:30 mark -1.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 6 | $14.44 | $0.90 | $+0.70 | $584.49 | ▲ +0.70 after sell → book $9,933.12; vs 09:30 mark -0.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $584.49 | ▼ close $9,872.27 vs 09:30 $9,939.22 (session -60.85) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $584.49 | ▲ 09:30 equity $10,126.27 vs yday $9,872.27 (+254.00) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 2 | $2.67 | $0.08 | $+0.00 | $589.75 | ▼ +0.00 after sell → book $10,126.19; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $589.75 | ▼ close $10,089.62 vs 09:30 $10,126.27 (session -36.57) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $589.75 | ▼ 09:30 equity $10,082.67 vs yday $10,089.62 (-6.95) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 38 | $42.10 | $2.13 | $+9.45 | $2,187.43 | ▲ +9.45 after sell → book $10,080.55; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 110 | $15.70 | $2.35 | $+113.03 | $3,912.08 | ▲ +113.03 after sell → book $10,078.20; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 49 | $32.42 | $2.16 | $-27.82 | $5,498.50 | ▼ -27.82 after sell → book $10,076.04; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 166 | $10.07 | $2.53 | $+51.42 | $7,167.59 | ▲ +51.42 after sell → book $10,073.51; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1163 | $1.17 | $15.21 | $-286.07 | $8,513.09 | ▼ -286.07 after sell → book $10,058.30; vs 09:30 mark -15.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 177 | $8.73 | $2.56 | $-75.88 | $10,055.74 | ▼ -75.88 after sell → book $10,055.74; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,055.74 | ▲ close $10,055.74 vs 09:30 $10,082.67 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,055.74 | ▲ 09:30 equity $10,055.74 vs yday $10,055.74 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,837.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,590.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 346 | $3.63 | $4.46 | — | $6,329.95 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 156 | $8.03 | $2.46 | — | $5,074.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,880.74 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $2,711.17 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $1,467.98 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1256.97 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.85 | $2.24 | — | $218.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1256.97 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.34 | ▼ close $9,848.25 vs 09:30 $10,055.74 (session -187.95) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.34 | ▼ 09:30 equity $9,832.94 vs yday $9,848.25 (-15.31) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 16 | $1.90 | $0.35 | — | $187.58 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $31.19 | — |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 3 | $10.02 | $0.31 | — | $157.21 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $31.19 | — |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 5 | $5.25 | $0.28 | — | $130.69 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $31.19 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 9 | $3.15 | $0.31 | — | $102.03 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $31.19 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 6 | $4.53 | $0.29 | — | $74.56 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $31.19 | — |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 2 | $12.95 | $0.27 | — | $48.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+21.8; leftover $31.19 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.39 | ▲ close $9,839.55 vs 09:30 $9,832.94 (session +8.42) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.39 | ▼ 09:30 equity $9,820.24 vs yday $9,839.55 (-19.31) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.39 | ▼ close $9,631.29 vs 09:30 $9,820.24 (session -188.95) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.39 | ▼ 09:30 equity $9,582.74 vs yday $9,631.29 (-48.55) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,268.99 | ▲ +2.30 after sell → book $9,580.66; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,485.19 | ▼ -30.85 after sell → book $9,578.57; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 346 | $3.28 | $4.53 | $-130.09 | $3,615.53 | ▼ -130.09 after sell → book $9,574.03; vs 09:30 mark -4.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 156 | $8.01 | $2.49 | $-8.07 | $4,862.60 | ▼ -8.07 after sell → book $9,571.54; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $5,992.49 | ▼ -64.17 after sell → book $9,569.50; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $7,112.82 | ▼ -49.25 after sell → book $9,567.47; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 74 | $15.46 | $2.23 | $-101.39 | $8,254.63 | ▼ -101.39 after sell → book $9,565.24; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 84 | $13.60 | $2.27 | $-109.51 | $9,394.76 | ▼ -109.51 after sell → book $9,562.97; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,394.76 | ▼ close $9,561.27 vs 09:30 $9,582.74 (session -1.70) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,394.76 | ▼ 09:30 equity $9,559.72 vs yday $9,561.27 (-1.55) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 16 | $1.83 | $0.36 | $-1.83 | $9,423.68 | ▼ -1.83 after sell → book $9,559.36; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `CCOI` | 3 | $9.20 | $0.30 | $-3.07 | $9,450.97 | ▼ -3.07 after sell → book $9,559.05; vs 09:30 mark -0.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `UAMY` | 5 | $5.10 | $0.29 | $-1.32 | $9,476.18 | ▼ -1.32 after sell → book $9,558.76; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `SLBT` | 9 | $2.58 | $0.28 | $-5.72 | $9,499.12 | ▼ -5.72 after sell → book $9,558.48; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IRD` | 6 | $5.87 | $0.39 | $+7.36 | $9,533.95 | ▲ +7.36 after sell → book $9,558.09; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `FMC` | 2 | $12.07 | $0.27 | $-2.29 | $9,557.83 | ▼ -2.29 after sell → book $9,557.83; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,557.83 | ▲ close $9,557.83 vs 09:30 $9,559.72 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,557.83 | ▲ 09:30 equity $9,557.83 vs yday $9,557.83 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $8,521.62 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+8.3; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 21 | $56.09 | $2.05 | — | $7,341.68 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+19.6; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 585 | $2.04 | $7.55 | — | $6,140.73 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 251 | $4.75 | $3.24 | — | $4,945.24 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 563 | $2.12 | $7.26 | — | $3,744.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 50 | $23.63 | $2.14 | — | $2,560.78 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-6.3; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $1,368.83 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1194.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $263.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1194.73 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.97 | ▼ close $9,492.84 vs 09:30 $9,557.83 (session -36.43) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.97 | ▼ 09:30 equity $9,403.80 vs yday $9,492.84 (-89.04) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.97 | ▲ close $9,458.79 vs 09:30 $9,403.80 (session +54.99) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.97 | ▼ 09:30 equity $9,410.31 vs yday $9,458.79 (-48.48) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.97 | ▼ close $9,253.34 vs 09:30 $9,410.31 (session -156.97) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.97 | ▼ 09:30 equity $9,154.65 vs yday $9,253.34 (-98.69) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $1,236.15 | ▼ -64.03 after sell → book $9,152.63; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 21 | $51.29 | $2.07 | $-104.93 | $2,311.16 | ▼ -104.93 after sell → book $9,150.55; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 585 | $1.89 | $7.65 | $-102.95 | $3,409.16 | ▼ -102.95 after sell → book $9,142.90; vs 09:30 mark -7.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 251 | $4.73 | $3.29 | $-11.55 | $4,593.10 | ▼ -11.55 after sell → book $9,139.61; vs 09:30 mark -3.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 563 | $1.84 | $7.37 | $-172.27 | $5,621.65 | ▼ -172.27 after sell → book $9,132.24; vs 09:30 mark -7.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 50 | $25.58 | $2.16 | $+93.20 | $6,898.49 | ▲ +93.20 after sell → book $9,130.08; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 103 | $10.75 | $2.33 | $-87.03 | $8,003.42 | ▼ -87.03 after sell → book $9,127.76; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RDDT` | 7 | $160.62 | $2.03 | $+17.45 | $9,125.73 | ▲ +17.45 after sell → book $9,125.73; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,040.17 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+4.0; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $6,958.45 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 31 | $36.46 | $2.08 | — | $5,826.11 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+2.9; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $4,723.43 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 194 | $5.87 | $2.57 | — | $3,582.08 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $2,443.85 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $1,303.96 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1140.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 88 | $12.89 | $2.25 | — | $167.38 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-18.2; leftover $1140.72 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.38 | ▼ close $9,045.45 vs 09:30 $9,154.65 (session -63.15) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.38 | ▲ 09:30 equity $9,173.17 vs yday $9,045.45 (+127.72) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $146.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $20.92 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $131.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $20.92 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.33 | ▲ close $9,340.57 vs 09:30 $9,173.17 (session +167.77) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.33 | ▲ 09:30 equity $9,357.68 vs yday $9,340.57 (+17.11) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 4 | $3.95 | $0.17 | — | $115.36 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $16.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $101.15 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $16.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $89.37 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $16.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 4 | $3.58 | $0.16 | — | $74.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $16.42 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 19 | $0.85 | $0.22 | — | $58.52 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=+3.6; leftover $16.42 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.52 | ▼ close $9,255.15 vs 09:30 $9,357.68 (session -101.72) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.52 | ▲ 09:30 equity $9,266.65 vs yday $9,255.15 (+11.50) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,123.54 | ▼ -20.54 after sell → book $9,264.63; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 14 | $76.27 | $2.05 | $-15.98 | $2,189.27 | ▼ -15.98 after sell → book $9,262.57; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 31 | $36.70 | $2.10 | $+3.25 | $3,324.87 | ▲ +3.25 after sell → book $9,260.47; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $4,588.09 | ▲ +160.54 after sell → book $9,258.41; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 194 | $5.62 | $2.61 | $-53.69 | $5,675.75 | ▼ -53.69 after sell → book $9,255.80; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $6,758.68 | ▼ -55.30 after sell → book $9,253.75; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 42 | $28.69 | $2.14 | $+62.95 | $7,961.53 | ▲ +62.95 after sell → book $9,251.61; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HQ` | 88 | $13.41 | $2.28 | $+41.23 | $9,139.33 | ▲ +41.23 after sell → book $9,249.34; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,032.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.5; leftover $1142.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 12 | $88.83 | $2.03 | — | $6,964.24 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+7.6; leftover $1142.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 122 | $9.31 | $2.36 | — | $5,826.07 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1142.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 84 | $13.47 | $2.24 | — | $4,691.92 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1142.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1029 | $1.11 | $13.27 | — | $3,536.46 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1142.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 114 | $9.99 | $2.33 | — | $2,395.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1142.42 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 67 | $16.91 | $2.19 | — | $1,260.11 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1142.42 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,260.11 | ▼ close $9,186.73 vs 09:30 $9,266.65 (session -36.17) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,260.11 | ▼ 09:30 equity $9,178.19 vs yday $9,186.73 (-8.54) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $1,280.24 | ▼ -0.58 after sell → book $9,177.96; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 17 | $9.40 | $1.65 | — | $1,118.79 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=+9.5; leftover $160.03 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 17 | $9.11 | $1.60 | — | $962.32 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $160.03 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 158 | $1.01 | $2.07 | — | $800.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $160.03 | — |
| 2026-09-22 09:30 ET | **BUY** | `SECZ` | 12 | $12.96 | $1.59 | — | $643.56 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+64.4; leftover $160.03 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $643.56 | ▲ close $9,308.32 vs 09:30 $9,178.19 (session +137.27) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $643.56 | ▼ 09:30 equity $9,280.95 vs yday $9,308.32 (-27.37) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 2 | $7.95 | $0.18 | $+0.38 | $659.27 | ▲ +0.38 after sell → book $9,280.76; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 4 | $4.10 | $0.20 | $+0.23 | $675.48 | ▲ +0.23 after sell → book $9,280.57; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $690.15 | ▲ +0.45 after sell → book $9,280.40; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $702.57 | ▲ +0.65 after sell → book $9,280.24; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 4 | $3.59 | $0.18 | $-0.29 | $716.76 | ▼ -0.29 after sell → book $9,280.07; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 19 | $0.81 | $0.23 | $-1.21 | $731.92 | ▼ -1.21 after sell → book $9,279.84; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 31 | $3.93 | $1.31 | — | $608.78 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $121.99 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 7 | $15.55 | $1.11 | — | $498.82 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $121.99 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 2 | $41.76 | $0.84 | — | $414.46 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $121.99 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 12 | $9.90 | $1.22 | — | $294.43 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $121.99 | — |
| 2026-09-23 09:30 ET | **BUY** | `FEAM` | 41 | $2.92 | $1.32 | — | $173.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+86.5; leftover $121.99 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 166 | $0.73 | $1.72 | — | $49.83 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $121.99 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.83 | ▲ close $9,371.29 vs 09:30 $9,280.95 (session +98.97) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.83 | ▼ 09:30 equity $9,367.24 vs yday $9,371.29 (-4.05) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,195.45 | ▲ +38.52 after sell → book $9,365.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-17.93 | $2,245.50 | ▼ -17.93 after sell → book $9,363.16; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 122 | $8.67 | $2.39 | $-82.82 | $3,300.86 | ▼ -82.82 after sell → book $9,360.77; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 84 | $12.26 | $2.27 | $-106.57 | $4,328.43 | ▼ -106.57 after sell → book $9,358.51; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1029 | $1.05 | $13.45 | $-88.47 | $5,395.43 | ▼ -88.47 after sell → book $9,345.05; vs 09:30 mark -13.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 114 | $9.80 | $2.36 | $-26.35 | $6,510.27 | ▼ -26.35 after sell → book $9,342.69; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,510.27 | ▲ close $9,664.90 vs 09:30 $9,367.24 (session +322.21) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,251.31 | ▼ 09:30 equity $8,365.56 vs yday $8,366.30 (-0.74) | 09:30 open · cash $7,251.31 (unchanged overnight, no fees) · equity $8,365.56 vs prior close $8,366.30 (-0.74) · 13 name(s) re-marked at the open (per-name table). AIBZ×29 yday $4.31 → 09:30 $4.31 +0.00; APPS×12 yday $10.88 → 09:30 $10.88 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; CNXC×1 yday $29.39 → 09:30 $29.39 +0.00; EL×1 yday $95.37 → 09:30 $95.37 +0.00; FSLY×5 yday $26.68 → 09:30 $26.68 +0.00; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; INDP×12 yday $4.00 → 09:30 $4.00 +0.00; IVVD×149 yday $0.91 → 09:30 $0.91 +0.00; NN×9 yday $14.45 → 09:30 $14.45 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; PGEN×6 yday $7.70 → 09:30 $7.70 +0.00; TNGX×2 yday $24.63 → 09:30 $24.63 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 1 | $123.50 | $1.26 | $+14.42 | $7,373.55 | ▲ +14.42 after sell → book $8,364.30; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 7 | $115.36 | $2.01 | — | $6,564.02 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $921.69 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 23 | $38.51 | $2.06 | — | $5,676.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+4.7; leftover $921.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 120 | $7.65 | $2.35 | — | $4,755.88 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $921.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 11 | $83.76 | $2.02 | — | $3,832.50 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $921.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 101 | $9.05 | $2.29 | — | $2,916.16 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-27.1; leftover $921.69 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 39 | $23.58 | $2.11 | — | $1,994.43 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $921.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 418 | $2.20 | $5.39 | — | $1,069.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $921.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 153 | $6.00 | $2.45 | — | $148.99 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $921.69 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.99 | ▼ close $8,293.38 vs 09:30 $8,365.56 (session -50.23) | 16:00 close · cash $148.99 · equity $8,293.38 vs 09:30 $8,365.56 (-72.18; session marks -50.23) · 20 name(s) marked open→close (per-name table). AIBZ×29 09:30 $4.31 → close $4.31 -0.00; APPS×12 09:30 $10.88 → close $10.88 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; CNXC×1 09:30 $29.39 → close $29.39 -0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FSLY×5 09:30 $26.68 → close $26.68 +0.00; INDP×12 09:30 $4.00 → close $4.00 +0.00; IVVD×149 09:30 $0.91 → close $0.91 -0.00; NN×9 09:30 $14.45 → close $14.45 -0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; PGEN×6 09:30 $7.70 → close $7.70 -0.00; TNGX×2 09:30 $24.63 → close $24.63 -0.00; HALO×7 09:30 $115.36 → close $113.90 -10.22; BLFS×23 09:30 $38.51 → close $38.49 -0.46; MRVI×120 09:30 $7.65 → close $7.60 -6.00; TXG×11 09:30 $83.76 → close $85.71 +21.45; AEHL×101 09:30 $9.05 → close $9.36 +31.31; BRVE×39 09:30 $23.58 → close $20.62 -115.44; HLP×418 09:30 $2.20 → close $2.21 +4.18; SATL×153 09:30 $6.00 → close $6.17 +26.01 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 9.91 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 9.91 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 9.91 < 1 share @ 16.50 |
| 2026-08-14 | `ARX` | cash | leftover split 9.91 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 9.91 < 1 share @ 11.12 |
| 2026-08-14 | `TBBB` | cash | leftover split 9.91 < 1 share @ 48.82 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.90 < 1 share @ 46.18 |
| 2026-08-17 | `FANG` | cash | leftover split 7.90 < 1 share @ 202.70 |
| 2026-08-17 | `CDNL` | cash | leftover split 7.90 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 7.90 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 7.90 < 1 share @ 31.30 |
| 2026-08-17 | `HTFL` | cash | leftover split 7.90 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 7.90 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 38.37 < 1 share @ 119.43 |
| 2026-08-21 | `CRSP` | cash | leftover split 38.37 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GMAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GMAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TMCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 5.92 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 5.92 < 1 share @ 14.42 |
| 2026-08-27 | `ABX` | cash | leftover split 5.92 < 1 share @ 9.68 |
| 2026-08-27 | `ITG` | cash | leftover split 5.92 < 1 share @ 12.36 |
| 2026-08-27 | `IRDM` | cash | leftover split 5.92 < 1 share @ 47.46 |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TMCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 31.19 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UAMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UAMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `EYPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
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
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 20.92 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 20.92 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 20.92 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 20.92 < 1 share @ 34.93 |
| 2026-09-17 | `AXTI` | cash | leftover split 20.92 < 1 share @ 67.91 |
| 2026-09-17 | `ARQT` | cash | leftover split 20.92 < 1 share @ 25.95 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 16.42 < 1 share @ 108.55 |
| 2026-09-18 | `VICR` | cash | leftover split 16.42 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 16.42 < 1 share @ 85.00 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1142.42 < 1 share @ 1826.00 |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 67 | 2026-09-21 @ $16.91 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1142.42 |
| `ALOY` | 17 | 2026-09-22 @ $9.40 | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=+9.5; leftover $160.03 |
| `CRML` | 17 | 2026-09-22 @ $9.11 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $160.03 |
| `IVVD` | 158 | 2026-09-22 @ $1.01 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $160.03 |
| `SECZ` | 12 | 2026-09-22 @ $12.96 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+64.4; leftover $160.03 |
| `INDP` | 31 | 2026-09-23 @ $3.93 | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $121.99 |
| `CLPT` | 7 | 2026-09-23 @ $15.55 | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $121.99 |
| `VKTX` | 2 | 2026-09-23 @ $41.76 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $121.99 |
| `BFLY` | 12 | 2026-09-23 @ $9.90 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $121.99 |
| `FEAM` | 41 | 2026-09-23 @ $2.92 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+86.5; leftover $121.99 |
| `EVTL` | 166 | 2026-09-23 @ $0.73 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $121.99 |
