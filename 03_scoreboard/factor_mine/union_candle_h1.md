# Factor mine action — `union_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ candle, no 🚨

Cash book **-12.93%** ($8,707) · signal-only (no cash/fees) was -3.26%. Starts YES **0/30**. Fills 254 · skips 106 · realized $-563.17.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `candle_capture=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,436.84.

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
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 72 | $44.09 | $2.24 | $-140.53 | $3,251.50 | ▼ -140.53 after sell → book $10,099.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 65 | $55.29 | $2.22 | $+298.93 | $6,843.13 | ▲ +298.93 after sell → book $10,097.77; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 142 | $22.92 | $2.47 | $-63.10 | $10,095.30 | ▼ -63.10 after sell → book $10,095.30; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $8,883.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 292 | $4.31 | $3.77 | — | $7,621.15 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $6,612.16 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $5,355.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $4,101.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 113 | $11.12 | $2.33 | — | $2,842.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $1,578.71 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1261.91 | — |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $356.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1261.91 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $356.14 | ▼ close $9,852.23 vs 09:30 $10,102.24 (session -223.95) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.14 | ▲ 09:30 equity $9,879.81 vs yday $9,852.23 (+27.58) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $1,516.84 | ▼ -51.17 after sell → book $9,877.74; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 292 | $4.60 | $3.83 | $+77.09 | $2,856.21 | ▲ +77.09 after sell → book $9,873.91; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $3,905.26 | ▲ +40.05 after sell → book $9,871.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $5,098.50 | ▼ -62.98 after sell → book $9,869.66; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $6,348.77 | ▼ -4.38 after sell → book $9,867.45; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 113 | $9.57 | $2.36 | $-179.84 | $7,427.83 | ▼ -179.84 after sell → book $9,865.10; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $8,677.80 | ▼ -13.71 after sell → book $9,862.55; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $9,860.46 | ▼ -39.90 after sell → book $9,860.46; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $8,657.72 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+6.7; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $7,439.51 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+8.3; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $6,241.93 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 135 | $9.12 | $2.40 | — | $5,008.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $3,785.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-3.8; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,587.78 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,381.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1232.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 641 | $1.92 | $8.27 | — | $142.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1232.56 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.34 | ▼ close $9,699.31 vs 09:30 $9,879.81 (session -138.05) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.34 | ▼ 09:30 equity $9,682.19 vs yday $9,699.31 (-17.12) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $1,388.25 | ▲ +43.16 after sell → book $9,680.10; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $2,639.80 | ▲ +33.34 after sell → book $9,678.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $3,884.80 | ▲ +47.42 after sell → book $9,675.97; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 135 | $9.03 | $2.43 | $-16.97 | $5,101.43 | ▼ -16.97 after sell → book $9,673.55; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $6,320.39 | ▼ -3.84 after sell → book $9,671.42; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,521.79 | ▲ +3.66 after sell → book $9,669.32; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,577.50 | ▼ -150.74 after sell → book $9,667.20; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 641 | $1.70 | $8.39 | $-157.67 | $9,658.82 | ▼ -157.67 after sell → book $9,658.82; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,658.82 | ▲ close $9,658.82 vs 09:30 $9,682.19 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,658.82 | ▲ 09:30 equity $9,658.82 vs yday $9,658.82 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,658.82 | ▲ close $9,658.82 vs 09:30 $9,658.82 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,658.82 | ▲ 09:30 equity $9,658.82 vs yday $9,658.82 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,464.75 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,264.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,065.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $4,877.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 689 | $1.75 | $8.89 | — | $3,663.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,505.00 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 245 | $4.92 | $3.16 | — | $1,296.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1207.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $272.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1207.35 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $272.19 | ▲ close $9,869.89 vs 09:30 $9,658.82 (session +235.75) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.19 | ▲ 09:30 equity $10,208.47 vs yday $9,869.89 (+338.58) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,540.20 | ▲ +73.95 after sell → book $10,206.28; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $2,799.52 | ▲ +59.45 after sell → book $10,204.10; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $4,088.70 | ▲ +89.57 after sell → book $10,201.91; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $5,373.36 | ▲ +97.36 after sell → book $10,199.77; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 689 | $1.79 | $9.01 | $+9.66 | $6,597.66 | ▲ +9.66 after sell → book $10,190.76; vs 09:30 mark -9.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $7,833.23 | ▲ +77.23 after sell → book $10,188.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 245 | $5.20 | $3.21 | $+62.23 | $9,104.02 | ▲ +62.23 after sell → book $10,185.52; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,907.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1300.57 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $6,615.48 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1300.57 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,322.06 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1300.57 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $4,065.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1300.57 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 985 | $1.32 | $12.71 | — | $2,752.98 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1300.57 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $1,483.20 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1300.57 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 783 | $1.66 | $10.10 | — | $173.32 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1300.57 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.32 | ▲ close $10,392.06 vs 09:30 $10,208.47 (session +240.08) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.32 | ▲ 09:30 equity $10,749.71 vs yday $10,392.06 (+357.65) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,256.44 | ▲ +58.87 after sell → book $10,747.68; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,459.50 | ▲ +6.74 after sell → book $10,745.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $3,700.02 | ▼ -51.70 after sell → book $10,743.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.33 | $2.37 | $+250.49 | $5,243.93 | ▲ +250.49 after sell → book $10,741.04; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $6,475.60 | ▼ -24.50 after sell → book $10,738.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 985 | $1.83 | $12.88 | $+476.76 | $8,265.27 | ▲ +476.76 after sell → book $10,726.08; vs 09:30 mark -12.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $9,510.31 | ▼ -24.75 after sell → book $10,723.96; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 783 | $1.55 | $10.24 | $-106.47 | $10,713.72 | ▼ -106.47 after sell → book $10,713.72; vs 09:30 mark -10.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.72 | ▲ close $10,713.72 vs 09:30 $10,749.71 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.72 | ▲ 09:30 equity $10,713.72 vs yday $10,713.72 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $9,430.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.0; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.59 | $2.28 | — | $8,096.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 141 | $9.49 | $2.41 | — | $6,756.20 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 36 | $36.96 | $2.10 | — | $5,423.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 669 | $2.00 | $8.63 | — | $4,076.91 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 255 | $5.24 | $3.29 | — | $2,737.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 152 | $8.79 | $2.45 | — | $1,398.90 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1339.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 234 | $5.71 | $3.02 | — | $59.74 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1339.21 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.74 | ▲ close $10,885.24 vs 09:30 $10,713.72 (session +197.70) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.74 | ▼ 09:30 equity $10,834.44 vs yday $10,885.24 (-50.80) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $1,340.22 | ▼ -2.43 after sell → book $10,832.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 98 | $13.63 | $2.31 | $-0.67 | $2,673.65 | ▼ -0.67 after sell → book $10,830.11; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 141 | $9.89 | $2.45 | $+51.54 | $4,065.69 | ▲ +51.54 after sell → book $10,827.66; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 36 | $38.24 | $2.12 | $+41.86 | $5,440.21 | ▲ +41.86 after sell → book $10,825.54; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 669 | $1.93 | $8.75 | $-64.21 | $6,722.63 | ▼ -64.21 after sell → book $10,816.79; vs 09:30 mark -8.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 255 | $4.98 | $3.34 | $-72.93 | $7,989.19 | ▼ -72.93 after sell → book $10,813.45; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 234 | $5.97 | $3.07 | $+54.75 | $9,383.10 | ▲ +54.75 after sell → book $10,810.38; vs 09:30 mark -3.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 155 | $8.60 | $2.46 | — | $8,047.64 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+4.8; leftover $1340.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 42 | $31.21 | $2.12 | — | $6,734.71 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1340.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 79 | $16.77 | $2.23 | — | $5,407.65 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1340.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 205 | $6.53 | $2.64 | — | $4,066.36 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+3.6; leftover $1340.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 280 | $4.78 | $3.61 | — | $2,724.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+8.1; leftover $1340.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 609 | $2.20 | $7.86 | — | $1,376.69 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+17.8; leftover $1340.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `GRRR` | 95 | $14.03 | $2.27 | — | $41.56 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_mover; ret5=-7.6; leftover $1340.44 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.56 | ▲ close $10,952.96 vs 09:30 $10,834.44 (session +165.77) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.56 | ▲ 09:30 equity $10,972.87 vs yday $10,952.96 (+19.91) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 152 | $9.41 | $2.48 | $+89.31 | $1,469.40 | ▲ +89.31 after sell → book $10,970.39; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 155 | $8.49 | $2.49 | $-22.00 | $2,782.86 | ▼ -22.00 after sell → book $10,967.90; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 205 | $6.15 | $2.69 | $-83.23 | $4,040.92 | ▼ -83.23 after sell → book $10,965.21; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TMCI` | 280 | $4.72 | $3.67 | $-24.08 | $5,358.85 | ▼ -24.08 after sell → book $10,961.54; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 609 | $2.19 | $7.97 | $-21.91 | $6,684.59 | ▼ -21.91 after sell → book $10,953.57; vs 09:30 mark -7.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 95 | $15.94 | $2.30 | $+176.87 | $8,196.59 | ▲ +176.87 after sell → book $10,951.27; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $6,868.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+3.1; leftover $1366.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 94 | $14.42 | $2.27 | — | $5,510.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+7.1; leftover $1366.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 525 | $2.60 | $6.77 | — | $4,138.90 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; ret5=+13.0; leftover $1366.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 141 | $9.68 | $2.41 | — | $2,771.61 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1366.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `ITG` | 110 | $12.36 | $2.32 | — | $1,409.69 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $1366.10 | — |
| 2026-08-27 09:30 ET | **BUY** | `IRDM` | 28 | $47.46 | $2.07 | — | $78.73 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ret5=-3.1; leftover $1366.10 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.73 | ▲ close $11,031.14 vs 09:30 $10,972.87 (session +97.81) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.73 | ▲ 09:30 equity $11,045.80 vs yday $11,031.14 (+14.66) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 42 | $30.53 | $2.14 | $-32.81 | $1,358.86 | ▼ -32.81 after sell → book $11,043.67; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 141 | $9.88 | $2.45 | $+23.34 | $2,749.49 | ▲ +23.34 after sell → book $11,041.22; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 110 | $12.79 | $2.35 | $+42.63 | $4,154.04 | ▲ +42.63 after sell → book $11,038.87; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `IRDM` | 28 | $47.61 | $2.09 | $+0.03 | $5,485.02 | ▲ +0.03 after sell → book $11,036.77; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $4,134.01 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1371.26 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 140 | $9.73 | $2.41 | — | $2,769.40 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1371.26 | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 986 | $1.39 | $12.72 | — | $1,386.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1371.26 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 150 | $9.13 | $2.44 | — | $14.20 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1371.26 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.20 | ▼ close $10,707.64 vs 09:30 $11,045.80 (session -309.45) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.20 | ▼ 09:30 equity $10,665.71 vs yday $10,707.64 (-41.93) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 79 | $17.70 | $2.25 | $+68.99 | $1,410.25 | ▲ +68.99 after sell → book $10,663.46; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,752.14 | ▲ +13.73 after sell → book $10,661.35; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 94 | $14.54 | $2.30 | $+6.71 | $4,116.61 | ▲ +6.71 after sell → book $10,659.06; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 525 | $2.58 | $6.87 | $-24.14 | $5,464.24 | ▼ -24.14 after sell → book $10,652.19; vs 09:30 mark -6.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $6,739.25 | ▼ -76.00 after sell → book $10,650.05; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 140 | $9.50 | $2.44 | $-37.05 | $8,066.81 | ▼ -37.05 after sell → book $10,647.61; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 986 | $1.30 | $12.89 | $-114.35 | $9,335.71 | ▼ -114.35 after sell → book $10,634.71; vs 09:30 mark -12.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 150 | $8.66 | $2.48 | $-75.42 | $10,632.24 | ▼ -75.42 after sell → book $10,632.24; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,632.24 | ▲ close $10,632.24 vs 09:30 $10,665.71 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,632.24 | ▲ 09:30 equity $10,632.24 vs yday $10,632.24 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,632.24 | ▲ close $10,632.24 vs 09:30 $10,632.24 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,632.24 | ▲ 09:30 equity $10,632.24 vs yday $10,632.24 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,632.24 | ▲ close $10,632.24 vs 09:30 $10,632.24 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,632.24 | ▲ 09:30 equity $10,632.24 vs yday $10,632.24 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,308.17 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $8,018.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 366 | $3.63 | $4.72 | — | $6,684.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $8.03 | $2.48 | — | $5,357.46 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,030.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $2,715.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.77 | $2.23 | — | $1,388.36 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1329.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 89 | $14.85 | $2.26 | — | $64.45 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1329.03 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.45 | ▼ close $10,413.81 vs 09:30 $10,632.24 (session -198.55) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.45 | ▼ 09:30 equity $10,399.85 vs yday $10,413.81 (-13.96) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 25 | $52.03 | $2.09 | $-25.40 | $1,363.12 | ▼ -25.40 after sell → book $10,397.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 366 | $3.46 | $4.79 | $-71.73 | $2,624.68 | ▼ -71.73 after sell → book $10,392.97; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 165 | $7.91 | $2.52 | $-24.81 | $3,927.31 | ▼ -24.81 after sell → book $10,390.45; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $5,225.57 | ▼ -28.26 after sell → book $10,388.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,606.11 | ▲ +65.02 after sell → book $10,386.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 79 | $15.61 | $2.25 | $-96.12 | $7,837.05 | ▼ -96.12 after sell → book $10,384.12; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 89 | $14.63 | $2.28 | $-24.12 | $9,136.84 | ▼ -24.12 after sell → book $10,381.84; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 686 | $1.90 | $8.85 | — | $7,824.59 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1305.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $6,769.15 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1305.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 130 | $10.02 | $2.38 | — | $5,464.17 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1305.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 248 | $5.25 | $3.20 | — | $4,158.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $1305.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 414 | $3.15 | $5.34 | — | $2,849.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1305.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 288 | $4.53 | $3.72 | — | $1,541.17 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1305.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 100 | $12.95 | $2.29 | — | $243.88 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+21.8; leftover $1305.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $243.88 | ▲ close $10,371.26 vs 09:30 $10,399.85 (session +17.20) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $243.88 | ▼ 09:30 equity $10,311.56 vs yday $10,371.26 (-59.70) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 30 | $42.20 | $2.10 | $-26.08 | $1,507.78 | ▼ -26.08 after sell → book $10,309.46; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 686 | $2.00 | $8.97 | $+50.78 | $2,870.81 | ▲ +50.78 after sell → book $10,300.49; vs 09:30 mark -8.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $3,883.67 | ▼ -42.58 after sell → book $10,298.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 130 | $9.98 | $2.41 | $-9.99 | $5,178.66 | ▼ -9.99 after sell → book $10,296.06; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UAMY` | 248 | $5.28 | $3.25 | $+0.99 | $6,484.85 | ▲ +0.99 after sell → book $10,292.81; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SLBT` | 414 | $2.88 | $5.42 | $-122.54 | $7,671.75 | ▼ -122.54 after sell → book $10,287.39; vs 09:30 mark -5.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 288 | $4.53 | $3.77 | $-7.49 | $8,972.61 | ▼ -7.49 after sell → book $10,283.61; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FMC` | 100 | $13.11 | $2.32 | $+11.39 | $10,281.30 | ▲ +11.39 after sell → book $10,281.30; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.30 | ▲ close $10,281.30 vs 09:30 $10,311.56 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.30 | ▲ 09:30 equity $10,281.30 vs yday $10,281.30 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.30 | ▲ close $10,281.30 vs 09:30 $10,281.30 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.30 | ▲ 09:30 equity $10,281.30 vs yday $10,281.30 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.30 | ▲ close $10,281.30 vs 09:30 $10,281.30 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.30 | ▲ 09:30 equity $10,281.30 vs yday $10,281.30 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $9,038.25 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+8.3; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $7,802.21 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+19.6; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 629 | $2.04 | $8.11 | — | $6,510.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 270 | $4.75 | $3.48 | — | $5,224.95 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 606 | $2.12 | $7.82 | — | $3,932.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 54 | $23.63 | $2.15 | — | $2,654.24 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-6.3; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 111 | $11.55 | $2.32 | — | $1,369.87 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1285.16 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 8 | $157.55 | $2.01 | — | $107.46 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1285.16 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.46 | ▼ close $10,217.00 vs 09:30 $10,281.30 (session -34.33) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.46 | ▼ 09:30 equity $10,119.47 vs yday $10,217.00 (-97.53) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $1,344.43 | ▼ -6.08 after sell → book $10,117.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 22 | $52.23 | $2.08 | $-89.05 | $2,491.41 | ▼ -89.05 after sell → book $10,115.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 629 | $2.01 | $8.23 | $-35.21 | $3,747.48 | ▼ -35.21 after sell → book $10,107.14; vs 09:30 mark -8.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 270 | $4.82 | $3.54 | $+11.88 | $5,045.34 | ▲ +11.88 after sell → book $10,103.60; vs 09:30 mark -3.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 606 | $2.05 | $7.93 | $-58.17 | $6,279.71 | ▼ -58.17 after sell → book $10,095.67; vs 09:30 mark -7.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 54 | $23.20 | $2.17 | $-27.54 | $7,530.34 | ▼ -27.54 after sell → book $10,093.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 111 | $11.56 | $2.35 | $-3.56 | $8,811.15 | ▼ -3.56 after sell → book $10,091.15; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 8 | $160.00 | $2.03 | $+15.55 | $10,089.11 | ▲ +15.55 after sell → book $10,089.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,089.11 | ▲ close $10,089.11 vs 09:30 $10,119.47 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,089.11 | ▲ 09:30 equity $10,089.11 vs yday $10,089.11 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,089.11 | ▲ close $10,089.11 vs 09:30 $10,089.11 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,089.11 | ▲ 09:30 equity $10,089.11 vs yday $10,089.11 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $9,003.55 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+4.0; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,767.59 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $6,525.86 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+2.9; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $5,285.60 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 214 | $5.87 | $2.76 | — | $4,026.65 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $2,801.02 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 46 | $27.09 | $2.13 | — | $1,552.75 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1261.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 97 | $12.89 | $2.28 | — | $300.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-18.2; leftover $1261.14 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $300.14 | ▼ close $10,004.08 vs 09:30 $10,089.11 (session -67.65) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $300.14 | ▲ 09:30 equity $10,143.98 vs yday $10,004.08 (+139.90) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,390.72 | ▲ +5.02 after sell → book $10,141.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,611.70 | ▼ -14.98 after sell → book $10,139.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $3,856.37 | ▲ +2.94 after sell → book $10,137.79; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $5,162.91 | ▲ +66.27 after sell → book $10,135.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 214 | $5.58 | $2.81 | $-67.63 | $6,354.22 | ▼ -67.63 after sell → book $10,132.92; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $7,516.97 | ▼ -62.88 after sell → book $10,130.87; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 46 | $28.23 | $2.15 | $+48.16 | $8,813.40 | ▲ +48.16 after sell → book $10,128.72; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 97 | $13.56 | $2.31 | $+60.40 | $10,126.41 | ▲ +60.40 after sell → book $10,126.41; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $8,955.16 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $7,741.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,558.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 123 | $10.25 | $2.36 | — | $5,295.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 166 | $7.59 | $2.49 | — | $4,033.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 36 | $34.93 | $2.10 | — | $2,773.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.6; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $1,549.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1265.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $301.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1265.80 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $301.54 | ▲ close $10,218.02 vs 09:30 $10,143.98 (session +108.76) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $301.54 | ▲ 09:30 equity $10,305.35 vs yday $10,218.02 (+87.33) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,545.16 | ▲ +72.37 after sell → book $10,303.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,807.45 | ▲ +48.83 after sell → book $10,301.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $3,977.41 | ▼ -12.93 after sell → book $10,299.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 123 | $10.12 | $2.39 | $-20.74 | $5,219.78 | ▼ -20.74 after sell → book $10,296.86; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 166 | $7.98 | $2.53 | $+59.73 | $6,541.94 | ▲ +59.73 after sell → book $10,294.34; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 36 | $34.52 | $2.12 | $-18.98 | $7,782.54 | ▼ -18.98 after sell → book $10,292.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $9,035.44 | ▲ +28.47 after sell → book $10,290.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 48 | $26.14 | $2.15 | $+4.83 | $10,288.00 | ▲ +4.83 after sell → book $10,288.00; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $9,091.93 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+21.3; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $7,991.82 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $6,714.79 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 325 | $3.95 | $4.19 | — | $5,426.85 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 91 | $14.07 | $2.26 | — | $4,144.21 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 220 | $5.83 | $2.84 | — | $2,858.78 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 359 | $3.58 | $4.63 | — | $1,568.92 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1286.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1512 | $0.85 | $17.39 | — | $266.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=+3.6; leftover $1286.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.34 | ▼ close $10,240.78 vs 09:30 $10,305.35 (session -9.85) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.34 | ▲ 09:30 equity $10,415.51 vs yday $10,240.78 (+174.73) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $1,447.56 | ▼ -14.85 after sell → book $10,413.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $2,596.79 | ▲ +49.12 after sell → book $10,411.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $3,837.18 | ▼ -36.64 after sell → book $10,409.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 325 | $3.87 | $4.26 | $-34.45 | $5,090.68 | ▼ -34.45 after sell → book $10,405.14; vs 09:30 mark -4.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 91 | $13.90 | $2.29 | $-20.02 | $6,353.29 | ▼ -20.02 after sell → book $10,402.85; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 220 | $6.42 | $2.89 | $+122.98 | $7,761.70 | ▲ +122.98 after sell → book $10,399.96; vs 09:30 mark -2.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 359 | $3.71 | $4.70 | $+37.34 | $9,088.89 | ▲ +37.34 after sell → book $10,395.26; vs 09:30 mark -4.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1512 | $0.86 | $17.86 | $-14.08 | $10,377.40 | ▼ -14.08 after sell → book $10,377.40; vs 09:30 mark -17.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,112.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.5; leftover $1297.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $7,866.77 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+7.6; leftover $1297.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 139 | $9.31 | $2.41 | — | $6,570.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1297.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 96 | $13.47 | $2.28 | — | $5,274.40 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1297.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1168 | $1.11 | $15.07 | — | $3,962.85 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1297.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 129 | $9.99 | $2.38 | — | $2,671.76 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1297.17 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 76 | $16.91 | $2.22 | — | $1,384.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1297.17 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,384.39 | ▼ close $10,310.09 vs 09:30 $10,415.51 (session -38.92) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,384.39 | ▼ 09:30 equity $10,301.06 vs yday $10,310.09 (-9.03) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1168 | $1.05 | $15.27 | $-100.42 | $2,595.51 | ▼ -100.42 after sell → book $10,285.78; vs 09:30 mark -15.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 129 | $9.91 | $2.41 | $-15.11 | $3,871.50 | ▼ -15.11 after sell → book $10,283.38; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 51 | $9.40 | $2.14 | — | $3,389.95 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=+9.5; leftover $483.94 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 53 | $9.11 | $2.15 | — | $2,904.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $483.94 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 479 | $1.01 | $6.18 | — | $2,415.00 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $483.94 | — |
| 2026-09-22 09:30 ET | **BUY** | `SECZ` | 37 | $12.96 | $2.10 | — | $1,933.38 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+64.4; leftover $483.94 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,933.38 | ▼ close $10,201.17 vs 09:30 $10,301.06 (session -69.63) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,933.38 | ▼ 09:30 equity $10,184.98 vs yday $10,201.17 (-16.19) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 139 | $9.50 | $2.44 | $+21.56 | $3,251.44 | ▲ +21.56 after sell → book $10,182.54; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 96 | $12.84 | $2.30 | $-65.54 | $4,481.78 | ▼ -65.54 after sell → book $10,180.24; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 76 | $16.92 | $2.24 | $-3.70 | $5,765.46 | ▼ -3.70 after sell → book $10,178.00; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 51 | $8.90 | $2.16 | $-29.81 | $6,217.20 | ▼ -29.81 after sell → book $10,175.84; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 53 | $8.39 | $2.17 | $-42.48 | $6,659.70 | ▼ -42.48 after sell → book $10,173.67; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 479 | $0.95 | $6.08 | $-41.00 | $7,108.67 | ▼ -41.00 after sell → book $10,167.59; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 37 | $12.80 | $2.12 | $-10.14 | $7,580.15 | ▼ -10.14 after sell → book $10,165.47; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 321 | $3.93 | $4.14 | — | $6,314.48 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1263.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 81 | $15.55 | $2.23 | — | $5,052.69 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1263.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 30 | $41.76 | $2.08 | — | $3,797.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1263.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 127 | $9.90 | $2.37 | — | $2,538.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1263.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `FEAM` | 432 | $2.92 | $5.57 | — | $1,271.13 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+86.5; leftover $1263.36 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 1707 | $0.73 | $17.65 | — | $0.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $1263.36 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▼ close $9,698.32 vs 09:30 $10,184.98 (session -433.10) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▼ 09:30 equity $9,474.24 vs yday $9,698.32 (-224.08) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,310.11 | ▲ +44.59 after sell → book $9,472.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,535.51 | ▼ -20.25 after sell → book $9,470.15; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 321 | $3.77 | $4.20 | $-59.71 | $3,741.47 | ▼ -59.71 after sell → book $9,465.94; vs 09:30 mark -4.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 81 | $14.82 | $2.26 | $-63.62 | $4,939.64 | ▼ -63.62 after sell → book $9,463.69; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 30 | $36.02 | $2.10 | $-176.23 | $6,018.29 | ▼ -176.23 after sell → book $9,461.59; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 127 | $9.12 | $2.40 | $-103.83 | $7,174.12 | ▼ -103.83 after sell → book $9,459.19; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 432 | $2.68 | $5.65 | $-114.91 | $8,326.23 | ▼ -114.91 after sell → book $9,453.53; vs 09:30 mark -5.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVTL` | 1707 | $0.66 | $16.69 | $-159.97 | $9,436.84 | ▼ -159.97 after sell → book $9,436.84; vs 09:30 mark -16.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,436.84 | ▲ close $9,436.84 vs 09:30 $9,474.24 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,787.47 | ▲ 09:30 equity $8,787.47 vs yday $8,787.47 (+0.00) | 09:30 open · cash $8,787.47 · no holdings · equity $8,787.47 vs prior close $8,787.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,747.21 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1098.43 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $6,666.86 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+4.7; leftover $1098.43 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 143 | $7.65 | $2.42 | — | $5,570.49 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1098.43 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $4,479.58 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1098.43 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 121 | $9.05 | $2.35 | — | $3,382.18 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-27.1; leftover $1098.43 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 46 | $23.58 | $2.13 | — | $2,295.37 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1098.43 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 499 | $2.20 | $6.44 | — | $1,191.13 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1098.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 183 | $6.00 | $2.54 | — | $90.59 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1098.43 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.59 | ▼ close $8,707.42 vs 09:30 $8,787.47 (session -58.05) | 16:00 close · cash $90.59 · equity $8,707.42 vs 09:30 $8,787.47 (-80.05; session marks -58.05) · 8 name(s) marked open→close (per-name table). HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×143 09:30 $7.65 → close $7.60 -7.15; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×121 09:30 $9.05 → close $9.36 +37.51; BRVE×46 09:30 $23.58 → close $20.62 -136.16; HLP×499 09:30 $2.20 → close $2.21 +4.99; SATL×183 09:30 $6.00 → close $6.17 +31.11 | — |

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
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-21 | `SNDK` | cash | leftover split 1297.17 < 1 share @ 1826.00 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |
