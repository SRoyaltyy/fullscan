# Factor mine action — `union_last_red_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **-28.76%** ($7,124) · signal-only (no cash/fees) was +0.15%. Starts YES **10/30**. Fills 178 · skips 295 · realized $+93.52.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).
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
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,048.08.

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
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,106.28 vs 09:30 $10,000.00 (session +115.74) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▲ 09:30 equity $10,117.74 vs yday $10,106.28 (+11.46) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,154.28 vs 09:30 $10,117.74 (session +36.54) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▼ 09:30 equity $10,139.88 vs yday $10,154.28 (-14.40) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $24.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $3.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 2 | $1.62 | $0.04 | — | $21.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $3.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 1 | $2.62 | $0.03 | — | $18.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $3.52 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.95 | ▲ close $10,263.84 vs 09:30 $10,139.88 (session +124.06) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.95 | ▼ 09:30 equity $10,105.87 vs yday $10,263.84 (-157.97) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 50 | $49.28 | $2.17 | $-25.31 | $2,480.78 | ▼ -25.31 after sell → book $10,103.70; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 213 | $12.66 | $2.80 | $+198.93 | $5,174.55 | ▲ +198.93 after sell → book $10,100.89; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $7,511.68 | ▼ -163.28 after sell → book $10,098.62; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $10,087.97 | ▲ +86.83 after sell → book $10,096.25; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,087.97 | ▲ close $10,096.49 vs 09:30 $10,105.87 (session +0.24) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.97 | ▲ 09:30 equity $10,096.61 vs yday $10,096.49 (+0.12) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,087.97 | ▲ close $10,097.18 vs 09:30 $10,096.61 (session +0.57) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.97 | ▼ 09:30 equity $10,097.15 vs yday $10,097.18 (-0.03) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,091.12 | ▼ -0.13 after sell → book $10,097.10; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 2 | $1.55 | $0.06 | $-0.24 | $10,094.16 | ▼ -0.24 after sell → book $10,097.04; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KLC` | 1 | $2.88 | $0.05 | $+0.18 | $10,096.99 | ▲ +0.18 after sell → book $10,096.99; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,911.83 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 169 | $7.44 | $2.50 | — | $7,651.97 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 15 | $82.99 | $2.04 | — | $6,405.09 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 58 | $21.40 | $2.16 | — | $5,161.72 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 284 | $4.43 | $3.66 | — | $3,899.94 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4207 | $0.30 | $25.24 | — | $2,612.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3565 | $0.35 | $23.32 | — | $1,327.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1262.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $107.10 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1262.12 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.10 | ▲ close $10,160.92 vs 09:30 $10,097.15 (session +126.95) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.10 | ▲ 09:30 equity $10,303.50 vs yday $10,160.92 (+142.58) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $94.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $13.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 6 | $1.93 | $0.13 | — | $82.90 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $13.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 7 | $1.71 | $0.14 | — | $70.79 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $13.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 45 | $0.29 | $0.27 | — | $57.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $13.39 | — |
| 2026-08-21 09:30 ET | **BUY** | `PRQR` | 5 | $2.28 | $0.13 | — | $45.76 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $13.39 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.76 | ▲ close $10,332.52 vs 09:30 $10,303.50 (session +29.84) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.76 | ▼ 09:30 equity $10,301.29 vs yday $10,332.52 (-31.23) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.76 | ▲ close $10,307.68 vs 09:30 $10,301.29 (session +6.39) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.76 | ▼ 09:30 equity $10,249.78 vs yday $10,307.68 (-57.90) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,289.90 | ▲ +58.97 after sell → book $10,247.74; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 169 | $8.53 | $2.54 | $+179.18 | $2,728.93 | ▲ +179.18 after sell → book $10,245.20; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRCL` | 15 | $84.73 | $2.06 | $+22.01 | $3,997.82 | ▲ +22.01 after sell → book $10,243.14; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WYFI` | 58 | $20.90 | $2.18 | $-33.35 | $5,207.84 | ▼ -33.35 after sell → book $10,240.96; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 284 | $4.42 | $3.72 | $-10.22 | $6,459.40 | ▼ -10.22 after sell → book $10,237.24; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 4207 | $0.31 | $26.37 | $-9.54 | $7,737.20 | ▼ -9.54 after sell → book $10,210.87; vs 09:30 mark -26.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SAFX` | 3565 | $0.36 | $24.06 | $-33.11 | $8,989.41 | ▼ -33.11 after sell → book $10,186.81; vs 09:30 mark -24.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 26 | $43.63 | $2.09 | $-87.88 | $10,121.70 | ▼ -87.88 after sell → book $10,184.72; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 53 | $23.77 | $2.15 | — | $8,859.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 115 | $10.98 | $2.33 | — | $7,594.71 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,368.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 256 | $4.94 | $3.30 | — | $5,100.91 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+7.1; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $4,244.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+6.0; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 174 | $7.25 | $2.51 | — | $2,980.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 332 | $3.80 | $4.28 | — | $1,715.08 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1265.21 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 811 | $1.56 | $10.46 | — | $439.46 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1265.21 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $439.46 | ▲ close $10,432.19 vs 09:30 $10,249.78 (session +276.55) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $439.46 | ▼ 09:30 equity $10,414.48 vs yday $10,432.19 (-17.71) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 5 | $2.41 | $0.16 | $-0.59 | $451.35 | ▼ -0.59 after sell → book $10,414.32; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 6 | $2.03 | $0.16 | $+0.31 | $463.37 | ▲ +0.31 after sell → book $10,414.16; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ENHA` | 7 | $1.63 | $0.16 | $-0.86 | $474.63 | ▼ -0.86 after sell → book $10,414.00; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAN` | 45 | $0.40 | $0.33 | $+4.03 | $492.16 | ▲ +4.03 after sell → book $10,413.67; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PRQR` | 5 | $2.38 | $0.15 | $+0.22 | $503.91 | ▲ +0.22 after sell → book $10,413.52; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 7 | $11.12 | $0.80 | — | $425.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $83.98 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 4 | $17.51 | $0.71 | — | $354.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $83.98 | — |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 1 | $65.34 | $0.66 | — | $288.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $83.98 | — |
| 2026-08-26 09:30 ET | **BUY** | `INDP` | 77 | $1.09 | $1.07 | — | $203.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $83.98 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVTS` | 6 | $12.60 | $0.77 | — | $127.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $83.98 | — |
| 2026-08-26 09:30 ET | **BUY** | `IRDM` | 1 | $46.96 | $0.47 | — | $79.71 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $83.98 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.71 | ▲ close $10,635.17 vs 09:30 $10,414.48 (session +226.14) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.71 | ▲ 09:30 equity $10,701.34 vs yday $10,635.17 (+66.17) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 1 | $12.98 | $0.13 | — | $66.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $15.94 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 1 | $9.68 | $0.10 | — | $56.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $15.94 | — |
| 2026-08-27 09:30 ET | **BUY** | `SENS` | 1 | $9.33 | $0.10 | — | $47.39 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+2.5; leftover $15.94 | — |
| 2026-08-27 09:30 ET | **BUY** | `ACRS` | 2 | $6.15 | $0.13 | — | $34.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.9; leftover $15.94 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.96 | ▲ close $10,888.73 vs 09:30 $10,701.34 (session +187.85) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.96 | ▼ 09:30 equity $10,786.93 vs yday $10,888.73 (-101.80) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 115 | $10.97 | $2.36 | $-5.85 | $1,294.15 | ▼ -5.85 after sell → book $10,784.57; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $60.52 | $2.07 | $-17.52 | $2,502.48 | ▼ -17.52 after sell → book $10,782.50; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 256 | $4.95 | $3.35 | $-4.10 | $3,766.32 | ▼ -4.10 after sell → book $10,779.14; vs 09:30 mark -3.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $4,611.83 | ▼ -10.43 after sell → book $10,777.13; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 174 | $9.73 | $2.55 | $+426.45 | $6,302.29 | ▲ +426.45 after sell → book $10,774.57; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 332 | $3.77 | $4.35 | $-18.59 | $7,549.59 | ▼ -18.59 after sell → book $10,770.23; vs 09:30 mark -4.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 811 | $1.82 | $10.61 | $+189.79 | $9,015.00 | ▲ +189.79 after sell → book $10,759.62; vs 09:30 mark -10.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $7,729.79 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1287.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 82 | $15.66 | $2.24 | — | $6,443.44 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1287.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $5,170.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1287.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $3,907.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1287.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 81 | $15.88 | $2.23 | — | $2,618.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $1287.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 70 | $18.15 | $2.20 | — | $1,346.26 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $1287.86 | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 926 | $1.39 | $11.95 | — | $47.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1287.86 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.17 | ▼ close $10,428.24 vs 09:30 $10,786.93 (session -306.61) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.17 | ▼ 09:30 equity $10,371.60 vs yday $10,428.24 (-56.64) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 53 | $23.68 | $2.17 | $-9.09 | $1,300.04 | ▼ -9.09 after sell → book $10,369.43; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 7 | $10.82 | $0.80 | $-3.70 | $1,374.99 | ▼ -3.70 after sell → book $10,368.64; vs 09:30 mark -0.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVEX` | 4 | $17.63 | $0.74 | $-0.97 | $1,444.77 | ▼ -0.97 after sell → book $10,367.90; vs 09:30 mark -0.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AXTI` | 1 | $60.05 | $0.62 | $-6.57 | $1,504.20 | ▼ -6.57 after sell → book $10,367.28; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `INDP` | 77 | $1.14 | $1.13 | $+1.65 | $1,590.84 | ▲ +1.65 after sell → book $10,366.14; vs 09:30 mark -1.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVTS` | 6 | $11.45 | $0.72 | $-8.40 | $1,658.82 | ▼ -8.40 after sell → book $10,365.42; vs 09:30 mark -0.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `IRDM` | 1 | $46.64 | $0.49 | $-1.28 | $1,704.97 | ▼ -1.28 after sell → book $10,364.93; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,704.97 | ▼ close $10,344.66 vs 09:30 $10,371.60 (session -20.27) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,704.97 | ▼ 09:30 equity $10,225.92 vs yday $10,344.66 (-118.74) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `KURA` | 1 | $12.54 | $0.15 | $-0.72 | $1,717.36 | ▼ -0.72 after sell → book $10,225.77; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ABX` | 1 | $9.43 | $0.12 | $-0.47 | $1,726.67 | ▼ -0.47 after sell → book $10,225.65; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SENS` | 1 | $9.17 | $0.11 | $-0.37 | $1,735.73 | ▼ -0.37 after sell → book $10,225.54; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ACRS` | 2 | $6.09 | $0.15 | $-0.40 | $1,747.76 | ▼ -0.40 after sell → book $10,225.39; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,747.76 | ▼ close $10,184.72 vs 09:30 $10,225.92 (session -40.67) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,747.76 | ▲ 09:30 equity $10,206.06 vs yday $10,184.72 (+21.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 39 | $32.42 | $2.13 | $-22.95 | $3,010.01 | ▼ -22.95 after sell → book $10,203.93; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 82 | $13.92 | $2.26 | $-147.18 | $4,149.19 | ▼ -147.18 after sell → book $10,201.67; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 16 | $78.84 | $2.06 | $-13.38 | $5,408.58 | ▼ -13.38 after sell → book $10,199.62; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $6,585.10 | ▼ -86.68 after sell → book $10,197.59; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 81 | $15.97 | $2.26 | $+2.80 | $7,876.42 | ▲ +2.80 after sell → book $10,195.34; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 70 | $17.65 | $2.22 | $-39.42 | $9,109.69 | ▼ -39.42 after sell → book $10,193.11; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 926 | $1.17 | $12.11 | $-227.77 | $10,181.00 | ▼ -227.77 after sell → book $10,181.00; vs 09:30 mark -12.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,181.00 | ▲ close $10,181.00 vs 09:30 $10,206.06 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,181.00 | ▲ 09:30 equity $10,181.00 vs yday $10,181.00 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.45 | $2.24 | — | $8,911.87 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,742.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $6,521.00 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3375 | $0.38 | $22.85 | — | $5,225.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 69 | $18.28 | $2.20 | — | $3,962.26 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1957 | $0.65 | $18.59 | — | $2,671.62 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `GMRS` | 99 | $12.83 | $2.29 | — | $1,399.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $1272.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `KLRA` | 79 | $15.95 | $2.23 | — | $136.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $1272.63 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.88 | ▲ close $10,155.36 vs 09:30 $10,181.00 (session +28.81) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.88 | ▲ 09:30 equity $10,189.04 vs yday $10,155.36 (+33.68) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 4 | $3.46 | $0.15 | — | $122.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $17.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 6 | $2.52 | $0.17 | — | $107.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $17.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $94.04 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $17.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 9 | $1.90 | $0.20 | — | $76.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $17.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $4.78 | $0.15 | — | $62.25 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $17.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 10 | $1.59 | $0.19 | — | $46.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $17.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $34.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $17.11 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.74 | ▲ close $10,330.96 vs 09:30 $10,189.04 (session +143.03) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.74 | ▼ 09:30 equity $10,293.91 vs yday $10,330.96 (-37.05) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.74 | ▲ close $10,371.00 vs 09:30 $10,293.91 (session +77.10) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.74 | ▼ 09:30 equity $10,316.64 vs yday $10,371.00 (-54.36) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 82 | $15.16 | $2.26 | $-28.28 | $1,275.60 | ▼ -28.28 after sell → book $10,314.38; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $2,395.92 | ▼ -49.25 after sell → book $10,312.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EIX` | 22 | $59.49 | $2.08 | $+85.41 | $3,702.63 | ▲ +85.41 after sell → book $10,310.27; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SAFX` | 3375 | $0.40 | $24.20 | $+30.58 | $5,028.43 | ▲ +30.58 after sell → book $10,286.07; vs 09:30 mark -24.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRVO` | 69 | $18.60 | $2.22 | $+17.66 | $6,309.61 | ▲ +17.66 after sell → book $10,283.85; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DEFT` | 1957 | $0.63 | $18.46 | $-84.02 | $7,516.24 | ▼ -84.02 after sell → book $10,265.40; vs 09:30 mark -18.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GMRS` | 99 | $13.80 | $2.31 | $+91.43 | $8,880.12 | ▲ +91.43 after sell → book $10,263.08; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `KLRA` | 79 | $16.27 | $2.25 | $+20.80 | $10,163.20 | ▲ +20.80 after sell → book $10,260.83; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,163.20 | ▼ close $10,255.64 vs 09:30 $10,316.64 (session -5.19) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,163.20 | ▼ 09:30 equity $10,254.25 vs yday $10,255.64 (-1.39) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 4 | $2.85 | $0.15 | $-2.74 | $10,174.46 | ▼ -2.74 after sell → book $10,254.10; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 6 | $2.22 | $0.17 | $-2.14 | $10,187.61 | ▼ -2.14 after sell → book $10,253.93; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $10,199.68 | ▼ -1.49 after sell → book $10,253.78; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 9 | $1.83 | $0.21 | $-1.04 | $10,215.94 | ▼ -1.04 after sell → book $10,253.57; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 3 | $3.92 | $0.15 | $-2.87 | $10,227.55 | ▼ -2.87 after sell → book $10,253.42; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 10 | $1.53 | $0.20 | $-0.99 | $10,242.65 | ▼ -0.99 after sell → book $10,253.22; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $10,253.09 | ▼ -0.98 after sell → book $10,253.09; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,253.09 | ▲ close $10,253.09 vs 09:30 $10,254.25 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,253.09 | ▲ 09:30 equity $10,253.09 vs yday $10,253.09 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 78 | $16.28 | $2.22 | — | $8,981.03 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=-1.1; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 469 | $2.73 | $6.05 | — | $7,694.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=-3.0; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $6,541.59 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $5,277.33 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+4.7; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 62 | $20.61 | $2.18 | — | $3,997.34 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-24.7; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `SLBT` | 613 | $2.09 | $7.91 | — | $2,708.26 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-36.6; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `BHVN` | 98 | $13.03 | $2.28 | — | $1,429.04 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-18.5; leftover $1281.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 87 | $14.71 | $2.25 | — | $147.02 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-12.8; leftover $1281.64 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.02 | ▼ close $10,133.77 vs 09:30 $10,253.09 (session -92.41) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.02 | ▼ 09:30 equity $9,944.76 vs yday $10,133.77 (-189.01) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.02 | ▲ close $10,073.69 vs 09:30 $9,944.76 (session +128.93) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.02 | ▼ 09:30 equity $10,054.67 vs yday $10,073.69 (-19.02) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.02 | ▼ close $9,891.66 vs 09:30 $10,054.67 (session -163.01) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.02 | ▲ 09:30 equity $9,910.20 vs yday $9,891.66 (+18.54) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 78 | $16.16 | $2.25 | $-13.83 | $1,405.25 | ▼ -13.83 after sell → book $9,907.95; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 469 | $2.72 | $6.14 | $-16.88 | $2,674.79 | ▼ -16.88 after sell → book $9,901.81; vs 09:30 mark -6.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $3,652.97 | ▼ -174.84 after sell → book $9,899.78; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 8 | $147.79 | $2.03 | $-83.97 | $4,833.25 | ▼ -83.97 after sell → book $9,897.74; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NAVN` | 62 | $22.50 | $2.20 | $+112.81 | $6,226.06 | ▲ +112.81 after sell → book $9,895.55; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SLBT` | 613 | $1.91 | $8.02 | $-126.27 | $7,388.87 | ▼ -126.27 after sell → book $9,887.53; vs 09:30 mark -8.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BHVN` | 98 | $12.34 | $2.31 | $-72.21 | $8,595.88 | ▼ -72.21 after sell → book $9,885.22; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AEO` | 87 | $14.82 | $2.28 | $+5.04 | $9,882.94 | ▲ +5.04 after sell → book $9,882.94; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 86 | $14.31 | $2.25 | — | $8,650.03 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+4.8; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 47 | $26.27 | $2.13 | — | $7,413.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWRD` | 617 | $2.00 | $7.96 | — | $6,171.25 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+0.0; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 119 | $10.30 | $2.35 | — | $4,943.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-23.0; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 180 | $6.86 | $2.53 | — | $3,705.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-22.4; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 7721 | $0.16 | $35.52 | — | $2,435.00 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.8; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `USDE` | 203 | $6.06 | $2.62 | — | $1,202.20 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-19.6; leftover $1235.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1404 | $0.84 | $16.06 | — | $1.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-33.5; leftover $1235.37 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.16 | ▼ close $9,598.99 vs 09:30 $9,910.20 (session -212.54) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.16 | ▼ 09:30 equity $9,564.09 vs yday $9,598.99 (-34.90) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.16 | ▲ close $9,828.75 vs 09:30 $9,564.09 (session +264.66) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.16 | ▲ 09:30 equity $10,264.25 vs yday $9,828.75 (+435.50) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.16 | ▼ close $10,166.04 vs 09:30 $10,264.25 (session -98.20) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.16 | ▲ 09:30 equity $10,833.72 vs yday $10,166.04 (+667.68) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 86 | $13.65 | $2.27 | $-61.28 | $1,172.79 | ▼ -61.28 after sell → book $10,831.45; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 47 | $25.94 | $2.15 | $-19.79 | $2,389.82 | ▼ -19.79 after sell → book $10,829.30; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 617 | $2.15 | $8.07 | $+76.52 | $3,708.30 | ▲ +76.52 after sell → book $10,821.23; vs 09:30 mark -8.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ALHC` | 119 | $8.33 | $2.38 | $-239.15 | $4,697.19 | ▼ -239.15 after sell → book $10,818.85; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `PLAY` | 180 | $6.68 | $2.57 | $-37.50 | $5,897.02 | ▼ -37.50 after sell → book $10,816.28; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `DVLT` | 7721 | $0.16 | $36.81 | $-72.32 | $7,095.57 | ▼ -72.32 after sell → book $10,779.47; vs 09:30 mark -36.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 203 | $13.05 | $2.67 | $+1413.68 | $9,742.05 | ▲ +1,413.68 after sell → book $10,776.80; vs 09:30 mark -2.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `NMRA` | 1404 | $0.74 | $14.80 | $-181.09 | $10,761.99 | ▼ -181.09 after sell → book $10,761.99; vs 09:30 mark -14.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `AEHL` | 186 | $8.26 | $2.55 | — | $9,223.09 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+7.7; leftover $1537.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 38 | $40.00 | $2.10 | — | $7,700.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-32.2; leftover $1537.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 256 | $6.00 | $3.30 | — | $6,161.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-24.1; leftover $1537.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `KDK` | 449 | $3.42 | $5.79 | — | $4,620.31 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-12.3; leftover $1537.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 14 | $105.72 | $2.03 | — | $3,138.20 | — | union ∩ last_red, no 🚨; gate last_red=True; list overnight; ret5=-11.5; leftover $1537.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 73 | $20.85 | $2.21 | — | $1,613.94 | — | union ∩ last_red, no 🚨; gate last_red=True; list overnight; ret5=-2.5; leftover $1537.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 22 | $68.39 | $2.06 | — | $107.30 | — | union ∩ last_red, no 🚨; gate last_red=True; list overnight; ret5=-7.0; leftover $1537.43 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.30 | ▼ close $10,350.61 vs 09:30 $10,833.72 (session -391.34) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.30 | ▼ 09:30 equity $10,348.05 vs yday $10,350.61 (-2.56) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 26 | $0.58 | $0.23 | — | $91.99 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $15.33 | — |
| 2026-09-22 09:30 ET | **BUY** | `MX` | 4 | $3.18 | $0.14 | — | $79.13 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $15.33 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.13 | ▲ close $10,354.14 vs 09:30 $10,348.05 (session +6.46) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.13 | ▲ 09:30 equity $10,391.23 vs yday $10,354.14 (+37.09) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 1 | $7.95 | $0.08 | — | $71.10 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $11.30 | — |
| 2026-09-23 09:30 ET | **BUY** | `DNA` | 1 | $9.13 | $0.09 | — | $61.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $11.30 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.88 | ▼ close $10,140.37 vs 09:30 $10,391.23 (session -250.69) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.88 | ▼ 09:30 equity $10,110.49 vs yday $10,140.37 (-29.88) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `AEHL` | 186 | $8.21 | $2.59 | $-14.44 | $1,586.35 | ▼ -14.44 after sell → book $10,107.90; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `XENE` | 38 | $36.50 | $2.13 | $-137.23 | $2,971.22 | ▼ -137.23 after sell → book $10,105.77; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SION` | 256 | $5.50 | $3.36 | $-134.66 | $4,375.86 | ▼ -134.66 after sell → book $10,102.41; vs 09:30 mark -3.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `KDK` | 449 | $2.96 | $5.88 | $-218.21 | $5,699.03 | ▼ -218.21 after sell → book $10,096.54; vs 09:30 mark -5.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABVX` | 14 | $92.97 | $2.05 | $-182.58 | $6,998.55 | ▼ -182.58 after sell → book $10,094.48; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MLKN` | 73 | $19.96 | $2.23 | $-69.41 | $8,453.40 | ▼ -69.41 after sell → book $10,092.25; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `THO` | 22 | $72.58 | $2.08 | $+88.05 | $10,048.08 | ▲ +88.05 after sell → book $10,090.17; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,048.08 | ▲ close $10,092.58 vs 09:30 $10,110.49 (session +2.41) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,264.85 | ▲ 09:30 equity $7,343.81 vs yday $7,342.28 (+1.53) | 09:30 open · cash $6,264.85 (unchanged overnight, no fees) · equity $7,343.81 vs prior close $7,342.28 (+1.53) · 8 name(s) re-marked at the open (per-name table). AEHL×17 yday $8.96 → 09:30 $9.05 +1.53; DEFT×223 yday $0.53 → 09:30 $0.53 +0.00; DLO×9 yday $13.88 → 09:30 $13.88 +0.00; FWDI×16 yday $8.35 → 09:30 $8.35 +0.00; INDP×42 yday $4.00 → 09:30 $4.00 +0.00; MX×41 yday $3.18 → 09:30 $3.18 +0.00; PACS×3 yday $41.46 → 09:30 $41.46 +0.00; UPXI×107 yday $1.17 → 09:30 $1.17 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `AEHL` | 17 | $9.05 | $1.61 | $+21.18 | $6,417.09 | ▲ +21.18 after sell → book $7,342.20; vs 09:30 mark -1.61 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 38 | $20.61 | $2.10 | — | $5,631.81 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+9.1; leftover $802.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NEOV` | 335 | $2.39 | $4.32 | — | $4,826.84 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-31.4; leftover $802.14 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 364 | $2.20 | $4.70 | — | $4,021.34 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-24.1; leftover $802.14 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `LRMR` | 241 | $3.32 | $3.11 | — | $3,218.11 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-12.6; leftover $802.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 36 | $22.21 | $2.10 | — | $2,416.45 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-19.2; leftover $802.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SGMT` | 88 | $9.11 | $2.25 | — | $1,612.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $802.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SMWB` | 106 | $7.50 | $2.31 | — | $815.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-9.7; leftover $802.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $815.21 | ▼ close $7,123.58 vs 09:30 $7,343.81 (session -197.72) | 16:00 close · cash $815.21 · equity $7,123.58 vs 09:30 $7,343.81 (-220.23; session marks -197.72) · 14 name(s) marked open→close (per-name table). DEFT×223 09:30 $0.53 → close $0.53 +0.00; DLO×9 09:30 $13.88 → close $13.88 +0.00; FWDI×16 09:30 $8.35 → close $8.35 +0.00; INDP×42 09:30 $4.00 → close $4.00 +0.00; MX×41 09:30 $3.18 → close $3.18 +0.00; PACS×3 09:30 $41.46 → close $41.46 -0.00; UPXI×107 09:30 $1.17 → close $1.17 -0.00; OMER×38 09:30 $20.61 → close $20.08 -20.14; NEOV×335 09:30 $2.39 → close $2.19 -67.00; SFIX×364 09:30 $2.20 → close $2.15 -16.38; LRMR×241 09:30 $3.32 → close $3.08 -59.04; ACAD×36 09:30 $22.21 → close $20.68 -55.08; SGMT×88 09:30 $9.11 → close $9.24 +11.44; SMWB×106 09:30 $7.50 → close $7.58 +8.48 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 3.52 < 1 share @ 359.83 |
| 2026-08-14 | `NRG` | cash | leftover split 3.52 < 1 share @ 120.00 |
| 2026-08-14 | `MARA` | cash | leftover split 3.52 < 1 share @ 9.01 |
| 2026-08-14 | `ARX` | cash | leftover split 3.52 < 1 share @ 19.57 |
| 2026-08-14 | `HLIT` | cash | leftover split 3.52 < 1 share @ 13.18 |
| 2026-08-14 | `SECZ` | cash | leftover split 3.52 < 1 share @ 5.84 |
| 2026-08-14 | `LFTO` | cash | leftover split 3.52 < 1 share @ 20.57 |
| 2026-08-14 | `REZI` | cash | leftover split 3.52 < 1 share @ 20.56 |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 3.52 < 1 share @ 4.05 |
| 2026-08-17 | `TGB` | cash | leftover split 3.52 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 3.52 < 1 share @ 90.54 |
| 2026-08-17 | `CAPR` | cash | leftover split 3.52 < 1 share @ 6.87 |
| 2026-08-17 | `NU` | cash | leftover split 3.52 < 1 share @ 15.40 |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KLC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KLC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 13.39 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 13.39 < 1 share @ 115.18 |
| 2026-08-21 | `GMAB` | cash | leftover split 13.39 < 1 share @ 33.36 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PRQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLQT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `PRQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVEX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NVTS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `IRDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVBP` | cash | leftover split 15.94 < 1 share @ 30.79 |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AXTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NVTS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `IRDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GMRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `KLRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 17.11 < 1 share @ 52.03 |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GMRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `KLRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ORBS` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ON` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SYNA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CDW` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ADBT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `XHLD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARQQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CYPH` | cash | leftover split 0.17 < 1 share @ 2.67 |
| 2026-09-17 | `MRLN` | cash | leftover split 0.17 < 1 share @ 2.27 |
| 2026-09-17 | `PALI` | cash | leftover split 0.17 < 1 share @ 1.75 |
| 2026-09-17 | `BAK` | cash | leftover split 0.17 < 1 share @ 1.77 |
| 2026-09-17 | `JBHT` | cash | leftover split 0.17 < 1 share @ 238.60 |
| 2026-09-17 | `INDP` | cash | leftover split 0.17 < 1 share @ 3.30 |
| 2026-09-17 | `BTGO` | cash | leftover split 0.17 < 1 share @ 6.56 |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ALHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 0.58 < 1 share @ 209.52 |
| 2026-09-18 | `FIVN` | cash | leftover split 0.58 < 1 share @ 34.44 |
| 2026-09-22 | `XENE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `KDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABVX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MLKN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open |
| 2026-09-22 | `BRVE` | no_price | no 09:30 open |
| 2026-09-23 | `XENE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `KDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABVX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MLKN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `MX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 11.30 < 1 share @ 116.85 |
| 2026-09-23 | `FTRE` | cash | leftover split 11.30 < 1 share @ 20.25 |
| 2026-09-23 | `MAZE` | cash | leftover split 11.30 < 1 share @ 28.30 |
| 2026-09-23 | `BLLN` | cash | leftover split 11.30 < 1 share @ 116.00 |
| 2026-09-23 | `VICR` | cash | leftover split 11.30 < 1 share @ 266.50 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `MX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 26 | 2026-09-22 @ $0.58 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $15.33 |
| `MX` | 4 | 2026-09-22 @ $3.18 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $15.33 |
| `PGEN` | 1 | 2026-09-23 @ $7.95 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $11.30 |
| `DNA` | 1 | 2026-09-23 @ $9.13 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $11.30 |
