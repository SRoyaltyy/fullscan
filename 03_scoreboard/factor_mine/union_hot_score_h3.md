# Factor mine action — `union_hot_score_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · rank by hot_score

Cash book **+13.76%** ($11,376) · signal-only (no cash/fees) was +392.57%. Starts YES **29/30**. Fills 190 · skips 265 · realized $+1176.18.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,100.70.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 17 | $0.77 | $0.18 | — | $69.59 | — | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $13.42 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.59 | ▲ close $10,511.32 vs 09:30 $10,312.70 (session +199.06) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.59 | ▼ 09:30 equity $10,483.18 vs yday $10,511.32 (-28.14) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $61.12 | — | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $8.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $54.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $46.41 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.70 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.41 | ▲ close $10,583.70 vs 09:30 $10,483.18 (session +100.76) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.41 | ▼ 09:30 equity $10,439.30 vs yday $10,583.70 (-144.40) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $1,220.44 | ▼ -69.50 after sell → book $10,437.21; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $2,392.75 | ▼ -66.33 after sell → book $10,435.04; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,633.15 | ▲ +23.38 after sell → book $10,432.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $5,372.00 | ▲ +471.89 after sell → book $10,412.78; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $6,539.56 | ▼ -83.63 after sell → book $10,410.65; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $7,879.19 | ▲ +97.12 after sell → book $10,408.31; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $9,154.93 | ▲ +41.02 after sell → book $10,406.13; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,352.86 | ▼ -0.12 after sell → book $10,404.06; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,352.86 | ▲ close $10,405.80 vs 09:30 $10,439.30 (session +1.74) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,352.86 | ▲ 09:30 equity $10,406.34 vs yday $10,405.80 (+0.54) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,364.76 | ▼ -1.45 after sell → book $10,406.18; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,373.74 | ▼ -2.25 after sell → book $10,406.06; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 17 | $0.57 | $0.17 | $-3.68 | $10,383.27 | ▼ -3.68 after sell → book $10,405.90; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,383.27 | ▲ close $10,406.59 vs 09:30 $10,406.34 (session +0.69) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,383.27 | ▼ 09:30 equity $10,405.69 vs yday $10,406.59 (-0.90) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,391.36 | ▼ -0.38 after sell → book $10,405.58; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,398.92 | ▲ +0.62 after sell → book $10,405.48; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $10,405.38 | ▼ -1.31 after sell → book $10,405.38; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,202.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,887.01 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,585.04 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 949 | $1.37 | $12.24 | — | $5,272.67 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 145 | $8.91 | $2.42 | — | $3,978.29 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 541 | $2.40 | $6.98 | — | $2,672.91 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 196 | $6.61 | $2.58 | — | $1,375.75 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 526 | $2.47 | $6.79 | — | $69.75 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1300.67 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.75 | ▼ close $10,210.58 vs 09:30 $10,405.69 (session -144.87) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.75 | ▲ 09:30 equity $10,520.49 vs yday $10,210.58 (+309.91) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 2 | $4.49 | $0.10 | — | $60.67 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $11.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 1 | $6.81 | $0.07 | — | $53.79 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $11.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $42.55 | — | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $11.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $33.37 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $11.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 1 | $8.28 | $0.09 | — | $25.01 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $11.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 39 | $0.29 | $0.23 | — | $13.31 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $11.62 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.31 | ▲ close $10,753.96 vs 09:30 $10,520.49 (session +234.17) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.31 | ▲ 09:30 equity $11,147.90 vs yday $10,753.96 (+393.94) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.31 | ▼ close $10,672.20 vs 09:30 $11,147.90 (session -475.70) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.31 | ▼ 09:30 equity $10,626.30 vs yday $10,672.20 (-45.90) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $1,159.28 | ▼ -57.17 after sell → book $10,624.27; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $11.00 | $2.35 | $-94.32 | $2,366.93 | ▼ -94.32 after sell → book $10,621.92; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AZI` | 949 | $1.31 | $12.41 | $-81.59 | $3,597.71 | ▼ -81.59 after sell → book $10,609.51; vs 09:30 mark -12.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 145 | $9.36 | $2.46 | $+60.37 | $4,952.45 | ▲ +60.37 after sell → book $10,607.05; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALEC` | 541 | $2.32 | $7.08 | $-57.34 | $6,200.49 | ▼ -57.34 after sell → book $10,599.97; vs 09:30 mark -7.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 196 | $6.75 | $2.62 | $+23.22 | $7,520.87 | ▲ +23.22 after sell → book $10,597.35; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 526 | $2.38 | $6.88 | $-61.01 | $8,765.87 | ▼ -61.01 after sell → book $10,590.47; vs 09:30 mark -6.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 60 | $24.11 | $2.17 | — | $7,317.10 | — | rank by hot_score; rank hot_score; list yday_mover; ret5=+891.7; leftover $1460.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 76 | $19.04 | $2.22 | — | $5,867.84 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $1460.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 278 | $5.24 | $3.59 | — | $4,407.53 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1460.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 255 | $5.71 | $3.29 | — | $2,948.19 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1460.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 166 | $8.79 | $2.49 | — | $1,486.56 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1460.98 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 411 | $3.55 | $5.30 | — | $22.21 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+27.9; leftover $1460.98 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.21 | ▲ close $11,359.96 vs 09:30 $10,626.30 (session +788.55) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.21 | ▼ 09:30 equity $11,082.06 vs yday $11,359.96 (-277.90) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,817.02 | ▲ +479.57 after sell → book $11,067.27; vs 09:30 mark -14.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,832.19 | ▲ +3.93 after sell → book $11,067.10; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $1,840.43 | ▼ -0.94 after sell → book $11,066.99; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MRVI` | 1 | $8.85 | $0.11 | $+0.37 | $1,849.16 | ▲ +0.37 after sell → book $11,066.88; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAN` | 39 | $0.40 | $0.29 | $+3.49 | $1,864.36 | ▲ +3.49 after sell → book $11,066.59; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 26 | $14.11 | $2.07 | — | $1,495.43 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $372.87 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 64 | $5.81 | $2.18 | — | $1,121.41 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $372.87 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 32 | $11.59 | $2.09 | — | $748.60 | — | rank by hot_score; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $372.87 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 9 | $40.50 | $2.02 | — | $382.08 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.8; leftover $372.87 | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 27 | $13.63 | $2.07 | — | $12.00 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $372.87 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.00 | ▼ close $10,975.69 vs 09:30 $11,082.06 (session -80.47) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.00 | ▲ 09:30 equity $11,175.95 vs yday $10,975.69 (+200.26) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.00 | ▲ close $11,300.29 vs 09:30 $11,175.95 (session +124.34) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.00 | ▼ 09:30 equity $11,196.58 vs yday $11,300.29 (-103.71) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 2 | $3.69 | $0.10 | $-1.80 | $19.28 | ▼ -1.80 after sell → book $11,196.48; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 60 | $23.40 | $2.19 | $-46.96 | $1,421.09 | ▼ -46.96 after sell → book $11,194.29; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 76 | $22.50 | $2.24 | $+258.50 | $3,128.85 | ▲ +258.50 after sell → book $11,192.05; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 278 | $4.84 | $3.64 | $-118.43 | $4,470.72 | ▼ -118.43 after sell → book $11,188.40; vs 09:30 mark -3.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 255 | $6.73 | $3.35 | $+253.46 | $6,183.53 | ▲ +253.46 after sell → book $11,185.06; vs 09:30 mark -3.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 166 | $9.08 | $2.53 | $+43.12 | $7,688.28 | ▲ +43.12 after sell → book $11,182.53; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 411 | $3.80 | $5.38 | $+92.07 | $9,244.70 | ▲ +92.07 after sell → book $11,177.15; vs 09:30 mark -5.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 11 | $137.19 | $2.02 | — | $7,733.58 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+7.1; leftover $1540.78 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $6,270.86 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1540.78 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 3 | $461.85 | $2.00 | — | $4,883.32 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.8; leftover $1540.78 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 71 | $21.49 | $2.20 | — | $3,355.32 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.3; leftover $1540.78 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 83 | $18.36 | $2.24 | — | $1,829.20 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.8; leftover $1540.78 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 168 | $9.13 | $2.49 | — | $292.87 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+20.0; leftover $1540.78 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $292.87 | ▼ close $10,917.42 vs 09:30 $11,196.58 (session -246.75) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $292.87 | ▼ 09:30 equity $10,807.89 vs yday $10,917.42 (-109.53) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 1 | $9.50 | $0.12 | $+2.50 | $302.25 | ▲ +2.50 after sell → book $10,807.77; vs 09:30 mark -0.12 | dropped from list after 6 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 9 | $35.77 | $2.04 | $-46.62 | $622.14 | ▼ -46.62 after sell → book $10,805.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `KURA` | 27 | $12.71 | $2.09 | $-29.00 | $963.22 | ▼ -29.00 after sell → book $10,803.64; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $963.22 | ▲ close $10,949.51 vs 09:30 $10,807.89 (session +145.87) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $963.22 | ▼ 09:30 equity $10,790.09 vs yday $10,949.51 (-159.42) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 26 | $13.04 | $2.09 | $-31.98 | $1,300.18 | ▼ -31.98 after sell → book $10,788.01; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `USDE` | 64 | $8.15 | $2.20 | $+145.38 | $1,819.57 | ▲ +145.38 after sell → book $10,785.80; vs 09:30 mark -2.21 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PURR` | 32 | $11.73 | $2.11 | $+0.45 | $2,192.83 | ▲ +0.45 after sell → book $10,783.70; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,192.83 | ▲ close $10,972.16 vs 09:30 $10,790.09 (session +188.46) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,192.83 | ▼ 09:30 equity $10,921.34 vs yday $10,972.16 (-50.82) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $139.65 | $2.04 | $-68.26 | $3,587.29 | ▼ -68.26 after sell → book $10,919.30; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 3 | $413.78 | $2.02 | $-148.23 | $4,826.61 | ▼ -148.23 after sell → book $10,917.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SRPT` | 71 | $21.33 | $2.23 | $-15.79 | $6,338.81 | ▼ -15.79 after sell → book $10,915.05; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 83 | $17.40 | $2.26 | $-84.18 | $7,780.75 | ▼ -84.18 after sell → book $10,912.79; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 168 | $8.73 | $2.53 | $-72.23 | $9,244.85 | ▼ -72.23 after sell → book $10,910.25; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,244.85 | ▼ close $10,903.76 vs 09:30 $10,921.34 (session -6.49) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,244.85 | ▼ 09:30 equity $10,850.25 vs yday $10,903.76 (-53.51) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 11 | $145.94 | $2.05 | $+92.24 | $10,848.20 | ▲ +92.24 after sell → book $10,848.20; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 761 | $1.78 | $9.82 | — | $9,483.80 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 73 | $18.40 | $2.21 | — | $8,138.40 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 98 | $13.71 | $2.28 | — | $6,792.53 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $23.88 | $2.16 | — | $5,453.09 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 997 | $1.36 | $12.86 | — | $4,084.31 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 702 | $1.93 | $9.06 | — | $2,720.40 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $1,439.28 | — | rank by hot_score; rank hot_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1356.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 52 | $25.62 | $2.15 | — | $104.63 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.1; leftover $1356.03 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.63 | ▼ close $10,446.86 vs 09:30 $10,850.25 (session -358.79) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.63 | ▼ 09:30 equity $10,373.79 vs yday $10,446.86 (-73.07) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 2 | $7.87 | $0.16 | — | $88.73 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $17.44 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 3 | $5.79 | $0.18 | — | $71.17 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $17.44 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 6 | $2.51 | $0.17 | — | $55.95 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $17.44 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 1 | $16.40 | $0.17 | — | $39.38 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $17.44 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 3 | $4.53 | $0.14 | — | $25.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $17.44 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.64 | ▲ close $10,864.12 vs 09:30 $10,373.79 (session +491.16) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.64 | ▼ 09:30 equity $10,768.63 vs yday $10,864.12 (-95.49) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.64 | ▼ close $10,637.49 vs 09:30 $10,768.63 (session -131.13) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.64 | ▲ 09:30 equity $10,730.64 vs yday $10,637.49 (+93.15) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 761 | $1.45 | $9.95 | $-270.90 | $1,119.14 | ▼ -270.90 after sell → book $10,720.69; vs 09:30 mark -9.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `REAX` | 73 | $20.70 | $2.23 | $+163.46 | $2,628.01 | ▲ +163.46 after sell → book $10,718.46; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNH` | 98 | $13.64 | $2.31 | $-11.46 | $3,962.42 | ▼ -11.46 after sell → book $10,716.15; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 56 | $23.22 | $2.18 | $-41.30 | $5,260.56 | ▼ -41.30 after sell → book $10,713.97; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SID` | 997 | $1.28 | $13.04 | $-105.66 | $6,523.68 | ▼ -105.66 after sell → book $10,700.93; vs 09:30 mark -13.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 702 | $1.94 | $9.18 | $-11.22 | $7,876.38 | ▼ -11.22 after sell → book $10,691.75; vs 09:30 mark -9.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AGCO` | 10 | $127.69 | $2.04 | $-6.26 | $9,151.24 | ▼ -6.26 after sell → book $10,689.71; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ASST` | 52 | $28.00 | $2.17 | $+119.19 | $10,605.07 | ▲ +119.19 after sell → book $10,687.54; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,605.07 | ▼ close $10,685.97 vs 09:30 $10,730.64 (session -1.57) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,605.07 | ▼ 09:30 equity $10,684.66 vs yday $10,685.97 (-1.31) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `USDE` | 2 | $6.73 | $0.16 | $-2.60 | $10,618.37 | ▼ -2.60 after sell → book $10,684.50; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 3 | $5.22 | $0.19 | $-2.08 | $10,633.84 | ▼ -2.08 after sell → book $10,684.31; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 6 | $2.87 | $0.21 | $+1.78 | $10,650.85 | ▲ +1.78 after sell → book $10,684.10; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `FRNM` | 1 | $15.64 | $0.18 | $-1.11 | $10,666.31 | ▼ -1.11 after sell → book $10,683.92; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,666.31 | ▲ close $10,684.52 vs 09:30 $10,684.66 (session +0.60) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,666.31 | ▲ 09:30 equity $10,684.79 vs yday $10,684.52 (+0.27) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 564 | $2.70 | $7.28 | — | $9,136.24 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1523.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 310 | $4.91 | $4.00 | — | $7,610.14 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1523.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 486 | $3.13 | $6.27 | — | $6,082.69 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $1523.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 1088 | $1.40 | $14.04 | — | $4,545.46 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=-17.2; leftover $1523.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 139 | $10.95 | $2.41 | — | $3,021.00 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1523.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 27 | $54.91 | $2.07 | — | $1,536.36 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+24.3; leftover $1523.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 18 | $84.27 | $2.04 | — | $17.45 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1523.76 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.45 | ▲ close $10,818.56 vs 09:30 $10,684.79 (session +171.87) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.45 | ▲ 09:30 equity $10,828.07 vs yday $10,818.56 (+9.51) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 3 | $6.02 | $0.21 | $+4.12 | $35.30 | ▲ +4.12 after sell → book $10,827.86; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 3) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.30 | ▲ close $11,049.58 vs 09:30 $10,828.07 (session +221.72) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.30 | ▲ 09:30 equity $11,132.77 vs yday $11,049.58 (+83.19) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.30 | ▲ close $11,279.21 vs 09:30 $11,132.77 (session +146.44) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.30 | ▼ 09:30 equity $11,176.25 vs yday $11,279.21 (-102.96) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 310 | $4.77 | $4.06 | $-51.46 | $1,509.94 | ▼ -51.46 after sell → book $11,172.19; vs 09:30 mark -4.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 486 | $3.48 | $6.36 | $+157.47 | $3,194.86 | ▲ +157.47 after sell → book $11,165.83; vs 09:30 mark -6.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `GPRO` | 1088 | $1.31 | $14.23 | $-126.18 | $4,605.91 | ▼ -126.18 after sell → book $11,151.60; vs 09:30 mark -14.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 139 | $10.82 | $2.44 | $-22.92 | $6,107.45 | ▼ -22.92 after sell → book $11,149.16; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 27 | $50.69 | $2.09 | $-118.10 | $7,473.99 | ▼ -118.10 after sell → book $11,147.07; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 692 | $1.80 | $8.93 | — | $6,219.46 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1245.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 53 | $23.29 | $2.15 | — | $4,982.94 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $1245.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 85 | $14.62 | $2.25 | — | $3,738.00 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $1245.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 79 | $15.75 | $2.23 | — | $2,491.52 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1245.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `FRO` | 23 | $52.52 | $2.06 | — | $1,281.50 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+10.7; leftover $1245.66 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 44 | $28.16 | $2.12 | — | $40.34 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1245.66 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.34 | ▼ close $11,025.10 vs 09:30 $11,176.25 (session -102.24) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.34 | ▲ 09:30 equity $11,123.10 vs yday $11,025.10 (+98.00) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 18 | $86.76 | $2.07 | $+40.71 | $1,599.95 | ▲ +40.71 after sell → book $11,121.03; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 11 | $22.46 | $2.02 | — | $1,350.87 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $266.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 7 | $36.76 | $2.01 | — | $1,091.54 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $266.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 249 | $1.07 | $3.21 | — | $821.90 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $266.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $672.81 | — | rank by hot_score; rank hot_score; list flatten,ohlc_hot; ret5=+17.7; leftover $266.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 26 | $10.25 | $2.07 | — | $404.24 | — | rank by hot_score; rank hot_score; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $266.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 9 | $28.23 | $2.02 | — | $148.15 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.3; leftover $266.66 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.15 | ▲ close $11,820.04 vs 09:30 $11,123.10 (session +711.82) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.15 | ▼ 09:30 equity $11,581.85 vs yday $11,820.04 (-238.19) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 8 | $3.04 | $0.27 | — | $123.60 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $24.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 16 | $1.49 | $0.29 | — | $99.48 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $24.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 6 | $3.94 | $0.25 | — | $75.58 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $24.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 2 | $9.32 | $0.19 | — | $56.75 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $24.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `CHPT` | 2 | $10.00 | $0.21 | — | $36.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $24.69 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.54 | ▼ close $11,354.23 vs 09:30 $11,581.85 (session -226.41) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.54 | ▲ 09:30 equity $11,436.07 vs yday $11,354.23 (+81.84) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 564 | $3.55 | $7.39 | $+464.74 | $2,031.36 | ▲ +464.74 after sell → book $11,428.69; vs 09:30 mark -7.38 | dropped from list after 6 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HLP` | 692 | $2.08 | $9.05 | $+175.78 | $3,461.67 | ▲ +175.78 after sell → book $11,419.64; vs 09:30 mark -9.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 53 | $29.43 | $2.17 | $+321.10 | $5,019.29 | ▲ +321.10 after sell → book $11,417.47; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SSL` | 85 | $13.87 | $2.27 | $-68.26 | $6,195.97 | ▼ -68.26 after sell → book $11,415.20; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `REF` | 79 | $14.79 | $2.25 | $-80.32 | $7,362.13 | ▼ -80.32 after sell → book $11,412.95; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FRO` | 23 | $49.83 | $2.08 | $-66.01 | $8,506.14 | ▼ -66.01 after sell → book $11,410.87; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 44 | $30.23 | $2.14 | $+86.82 | $9,834.11 | ▲ +86.82 after sell → book $11,408.72; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 663 | $2.47 | $8.55 | — | $8,187.95 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $1639.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 96 | $16.91 | $2.28 | — | $6,562.31 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $1639.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 284 | $5.75 | $3.66 | — | $4,924.23 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1639.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 125 | $13.05 | $2.37 | — | $3,290.61 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1639.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 199 | $8.22 | $2.59 | — | $1,652.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1639.02 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 153 | $10.71 | $2.45 | — | $11.17 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1639.02 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.17 | ▼ close $11,360.17 vs 09:30 $11,436.07 (session -26.66) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.17 | ▲ 09:30 equity $11,424.54 vs yday $11,360.17 (+64.37) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `FPS` | 7 | $37.41 | $2.03 | $+0.51 | $271.01 | ▲ +0.51 after sell → book $11,422.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 26 | $10.18 | $2.09 | $-5.98 | $533.60 | ▼ -5.98 after sell → book $11,420.42; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 8 | $9.11 | $0.75 | — | $459.97 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; leftover $76.23 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 10 | $7.23 | $0.75 | — | $386.91 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $76.23 | — |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 24 | $3.10 | $0.82 | — | $311.70 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $76.23 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 75 | $1.01 | $0.98 | — | $234.97 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+14.3; leftover $76.23 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.97 | ▲ close $11,468.69 vs 09:30 $11,424.54 (session +51.57) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.97 | ▲ 09:30 equity $11,630.00 vs yday $11,468.69 (+161.31) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 11 | $23.00 | $2.04 | $+1.87 | $485.92 | ▲ +1.87 after sell → book $11,627.95; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `IQ` | 249 | $1.03 | $3.26 | $-16.44 | $739.13 | ▼ -16.44 after sell → book $11,624.69; vs 09:30 mark -3.26 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $880.08 | ▼ -8.14 after sell → book $11,623.24; vs 09:30 mark -1.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 9 | $27.74 | $2.04 | $-8.46 | $1,127.71 | ▼ -8.46 after sell → book $11,621.21; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CYPH` | 8 | $3.82 | $0.35 | $+5.66 | $1,157.92 | ▲ +5.66 after sell → book $11,620.86; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 16 | $1.41 | $0.29 | $-1.86 | $1,180.18 | ▼ -1.86 after sell → book $11,620.56; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RXT` | 6 | $4.07 | $0.28 | $+0.24 | $1,204.32 | ▲ +0.24 after sell → book $11,620.28; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 2 | $12.80 | $0.28 | $+6.49 | $1,229.64 | ▲ +6.49 after sell → book $11,620.00; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CHPT` | 2 | $9.76 | $0.22 | $-0.91 | $1,248.94 | ▼ -0.91 after sell → book $11,619.78; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 77 | $2.70 | $2.22 | — | $1,038.82 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $208.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 4 | $41.76 | $1.68 | — | $870.09 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $208.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 46 | $4.49 | $2.13 | — | $661.43 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $208.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 21 | $9.90 | $2.05 | — | $451.47 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $208.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 2 | $70.84 | $1.42 | — | $308.37 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $208.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 283 | $0.73 | $2.93 | — | $97.72 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $208.16 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.72 | ▲ close $11,808.65 vs 09:30 $11,630.00 (session +201.31) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.72 | ▲ 09:30 equity $11,809.49 vs yday $11,808.65 (+0.84) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 663 | $2.68 | $8.68 | $+122.00 | $1,865.88 | ▲ +122.00 after sell → book $11,800.81; vs 09:30 mark -8.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 284 | $5.62 | $3.72 | $-45.73 | $3,458.24 | ▼ -45.73 after sell → book $11,797.09; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 125 | $12.76 | $2.40 | $-41.01 | $5,050.84 | ▼ -41.01 after sell → book $11,794.69; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FWDI` | 199 | $7.94 | $2.63 | $-60.94 | $6,628.27 | ▼ -60.94 after sell → book $11,792.06; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABTC` | 153 | $9.64 | $2.49 | $-168.65 | $8,100.70 | ▼ -168.65 after sell → book $11,789.57; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,100.70 | ▲ close $12,381.21 vs 09:30 $11,809.49 (session +591.64) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,431.54 | ▲ 09:30 equity $11,541.93 vs yday $11,380.73 (+161.20) | 09:30 open · cash $8,431.54 (unchanged overnight, no fees) · equity $11,541.93 vs prior close $11,380.73 (+161.20) · 9 name(s) re-marked at the open (per-name table). BFLY×1 yday $9.41 → 09:30 $9.41 +0.00; DNA×1 yday $10.25 → 09:30 $10.20 -0.05; GLND×3 yday $5.35 → 09:30 $6.06 +2.13; INDP×2 yday $4.00 → 09:30 $4.00 +0.00; IVVD×8 yday $0.91 → 09:30 $0.91 +0.00; NUAI×1 yday $6.94 → 09:30 $6.94 +0.00; SVIA×2 yday $3.96 → 09:30 $3.96 +0.00; TJGC×102 yday $28.20 → 09:30 $29.76 +159.12; VNET×1 yday $6.92 → 09:30 $6.92 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 104 | $16.21 | $2.30 | — | $6,743.40 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1686.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 108 | $15.58 | $2.31 | — | $5,058.33 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $1686.31 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 13 | $123.50 | $2.03 | — | $3,450.80 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1686.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 421 | $4.00 | $5.43 | — | $1,759.26 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1686.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 436 | $3.86 | $5.62 | — | $70.68 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1686.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.68 | ▼ close $11,375.94 vs 09:30 $11,541.93 (session -148.29) | 16:00 close · cash $70.68 · equity $11,375.94 vs 09:30 $11,541.93 (-165.99; session marks -148.29) · 14 name(s) marked open→close (per-name table). BFLY×1 09:30 $9.41 → close $9.41 -0.00; DNA×1 09:30 $10.20 → close $10.66 +0.46; GLND×3 09:30 $6.06 → close $5.54 -1.56; INDP×2 09:30 $4.00 → close $4.00 +0.00; IVVD×8 09:30 $0.91 → close $0.91 -0.00; NUAI×1 09:30 $6.94 → close $6.94 +0.00; SVIA×2 09:30 $3.96 → close $3.96 +0.00; TJGC×102 09:30 $29.76 → close $26.24 -359.04; VNET×1 09:30 $6.92 → close $6.92 +0.00; SECZ×104 09:30 $16.21 → close $15.96 -26.00; USDE×108 09:30 $15.58 → close $17.25 +180.24; GRAL×13 09:30 $123.50 → close $126.89 +44.07; CYPH×421 09:30 $4.00 → close $4.12 +48.42; ZSQR×436 09:30 $3.86 → close $3.78 -34.88 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 13.42 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 13.42 < 1 share @ 19.57 |
| 2026-08-14 | `LIFE` | cash | leftover split 13.42 < 1 share @ 35.04 |
| 2026-08-14 | `VOYG` | cash | leftover split 13.42 < 1 share @ 44.49 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BZAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 8.70 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 8.70 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 8.70 < 1 share @ 32.55 |
| 2026-08-17 | `SMJF` | cash | leftover split 8.70 < 1 share @ 10.10 |
| 2026-08-17 | `ALOY` | cash | leftover split 8.70 < 1 share @ 14.66 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BZAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PURR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MRNA` | cash | leftover split 2.00 < 1 share @ 144.18 |
| 2026-08-27 | `BZ` | cash | leftover split 2.00 < 1 share @ 18.50 |
| 2026-08-27 | `OABI` | cash | leftover split 2.00 < 1 share @ 4.81 |
| 2026-08-27 | `AQST` | cash | leftover split 2.00 < 1 share @ 5.39 |
| 2026-08-27 | `VERA` | cash | leftover split 2.00 < 1 share @ 36.70 |
| 2026-08-27 | `VYX` | cash | leftover split 2.00 < 1 share @ 8.95 |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PURR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SRPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SRPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HOOD` | cash | leftover split 17.44 < 1 share @ 120.47 |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SEDG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GME` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TEM` | cash | leftover split 24.69 < 1 share @ 81.40 |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BBNX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `IQ` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ADPT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARM` | cash | leftover split 76.23 < 1 share @ 319.41 |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 96 | 2026-09-21 @ $16.91 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $1639.02 |
| `CRML` | 8 | 2026-09-22 @ $9.11 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; leftover $76.23 |
| `NUAI` | 10 | 2026-09-22 @ $7.23 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $76.23 |
| `INDP` | 24 | 2026-09-22 @ $3.10 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $76.23 |
| `IVVD` | 75 | 2026-09-22 @ $1.01 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+14.3; leftover $76.23 |
| `GLND` | 77 | 2026-09-23 @ $2.70 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $208.16 |
| `VKTX` | 4 | 2026-09-23 @ $41.76 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $208.16 |
| `SVIA` | 46 | 2026-09-23 @ $4.49 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $208.16 |
| `BFLY` | 21 | 2026-09-23 @ $9.90 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $208.16 |
| `INOD` | 2 | 2026-09-23 @ $70.84 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $208.16 |
| `EVTL` | 283 | 2026-09-23 @ $0.73 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $208.16 |
