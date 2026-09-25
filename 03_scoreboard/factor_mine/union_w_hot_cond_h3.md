# Factor mine action — `union_w_hot_cond_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **+14.08%** ($11,408) · signal-only (no cash/fees) was +337.30%. Starts YES **29/30**. Fills 195 · skips 286 · realized $+834.44.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: a mix of tape-heat and green cameras.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by a mix of tape-heat and green cameras and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,742.96.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 17 | $0.77 | $0.18 | — | $69.59 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $13.42 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.59 | ▲ close $10,511.32 vs 09:30 $10,312.70 (session +199.06) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.59 | ▼ 09:30 equity $10,483.18 vs yday $10,511.32 (-28.14) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $61.12 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $8.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $54.18 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $46.41 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.70 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,202.25 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,887.01 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,585.04 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 145 | $8.91 | $2.42 | — | $5,290.66 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 526 | $2.47 | $6.79 | — | $3,984.66 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 21 | $61.83 | $2.05 | — | $2,684.17 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,525.84 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1300.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $228.07 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1300.67 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $228.07 | ▲ close $10,440.73 vs 09:30 $10,405.69 (session +69.74) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $228.07 | ▲ 09:30 equity $10,710.01 vs yday $10,440.73 (+269.28) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 8 | $4.49 | $0.38 | — | $191.77 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $38.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $158.04 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $38.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 4 | $9.08 | $0.38 | — | $121.34 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $38.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 4 | $8.28 | $0.34 | — | $87.88 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $38.01 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 5 | $6.81 | $0.36 | — | $53.47 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $38.01 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.47 | ▲ close $11,100.11 vs 09:30 $10,710.01 (session +391.90) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.47 | ▲ 09:30 equity $11,461.59 vs yday $11,100.11 (+361.48) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.47 | ▼ close $11,108.38 vs 09:30 $11,461.59 (session -353.21) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.47 | ▼ 09:30 equity $11,012.96 vs yday $11,108.38 (-95.42) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $1,199.44 | ▼ -57.17 after sell → book $11,010.93; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $11.00 | $2.35 | $-94.32 | $2,407.09 | ▼ -94.32 after sell → book $11,008.58; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 145 | $9.36 | $2.46 | $+60.37 | $3,761.83 | ▲ +60.37 after sell → book $11,006.12; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 526 | $2.38 | $6.88 | $-61.01 | $5,006.83 | ▼ -61.01 after sell → book $10,999.24; vs 09:30 mark -6.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEM` | 21 | $66.58 | $2.07 | $+95.62 | $6,402.93 | ▲ +95.62 after sell → book $10,997.16; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $7,652.98 | ▲ +91.71 after sell → book $10,995.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 66 | $21.21 | $2.21 | $+99.88 | $9,050.63 | ▲ +99.88 after sell → book $10,992.92; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 62 | $24.11 | $2.18 | — | $7,553.63 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+891.7; leftover $1508.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 287 | $5.24 | $3.70 | — | $6,046.05 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1508.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 79 | $19.04 | $2.23 | — | $4,539.66 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+49.5; leftover $1508.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 424 | $3.55 | $5.47 | — | $3,028.99 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+27.9; leftover $1508.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 43 | $35.05 | $2.12 | — | $1,519.72 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1508.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 925 | $1.63 | $11.93 | — | $0.04 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1508.44 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.04 | ▲ close $11,702.41 vs 09:30 $11,012.96 (session +737.12) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.04 | ▼ 09:30 equity $11,469.30 vs yday $11,702.41 (-233.11) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,794.85 | ▲ +479.57 after sell → book $11,454.51; vs 09:30 mark -14.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 3 | $15.35 | $0.49 | $+11.83 | $1,840.41 | ▲ +11.83 after sell → book $11,454.02; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `IOVA` | 4 | $8.34 | $0.37 | $-3.70 | $1,873.40 | ▼ -3.70 after sell → book $11,453.65; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MRVI` | 4 | $8.85 | $0.39 | $+1.55 | $1,908.42 | ▲ +1.55 after sell → book $11,453.26; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 5 | $8.29 | $0.45 | $+6.59 | $1,949.42 | ▲ +6.59 after sell → book $11,452.81; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 19 | $14.11 | $2.05 | — | $1,679.28 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+11.4; leftover $278.49 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 47 | $5.81 | $2.13 | — | $1,404.08 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $278.49 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 24 | $11.59 | $2.06 | — | $1,123.98 | — | rank by w_hot_cond; rank w_hot_cond; list overnight; 🔵; ret5=+64.9; leftover $278.49 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 6 | $40.50 | $2.01 | — | $878.97 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $278.49 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 2 | $124.67 | $2.00 | — | $627.64 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $278.49 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 53 | $5.21 | $2.15 | — | $349.36 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $278.49 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 1 | $267.02 | $1.99 | — | $80.34 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $278.49 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.34 | ▼ close $11,281.12 vs 09:30 $11,469.30 (session -157.30) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.34 | ▲ 09:30 equity $11,371.85 vs yday $11,281.12 (+90.73) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 1 | $9.19 | $0.09 | — | $71.06 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $11.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 4 | $2.60 | $0.12 | — | $60.54 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot; ret5=+13.0; leftover $11.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `DJT` | 1 | $9.59 | $0.10 | — | $50.86 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+13.8; leftover $11.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 2 | $4.81 | $0.10 | — | $41.14 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+14.8; leftover $11.48 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.14 | ▲ close $11,413.80 vs 09:30 $11,371.85 (session +42.35) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.14 | ▼ 09:30 equity $11,309.69 vs yday $11,413.80 (-104.11) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 8 | $3.69 | $0.34 | $-7.12 | $70.32 | ▼ -7.12 after sell → book $11,309.35; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 62 | $23.40 | $2.20 | $-48.39 | $1,518.92 | ▼ -48.39 after sell → book $11,307.15; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 287 | $4.84 | $3.76 | $-122.26 | $2,904.24 | ▼ -122.26 after sell → book $11,303.39; vs 09:30 mark -3.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 79 | $22.50 | $2.25 | $+268.86 | $4,679.48 | ▲ +268.86 after sell → book $11,301.13; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 424 | $3.80 | $5.55 | $+94.98 | $6,285.13 | ▲ +94.98 after sell → book $11,295.58; vs 09:30 mark -5.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 43 | $34.50 | $2.14 | $-27.91 | $7,766.49 | ▼ -27.91 after sell → book $11,293.44; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 925 | $1.69 | $12.10 | $+31.47 | $9,317.64 | ▲ +31.47 after sell → book $11,281.34; vs 09:30 mark -12.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $7,854.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1552.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 66 | $23.30 | $2.19 | — | $6,314.93 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+14.5; leftover $1552.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 87 | $17.78 | $2.25 | — | $4,765.82 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1552.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 170 | $9.13 | $2.50 | — | $3,211.22 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+20.0; leftover $1552.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $1,791.60 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1552.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 3 | $461.85 | $2.00 | — | $404.05 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.8; leftover $1552.94 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $404.05 | ▼ close $11,018.42 vs 09:30 $11,309.69 (session -249.94) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $404.05 | ▼ 09:30 equity $10,971.55 vs yday $11,018.42 (-46.87) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 47 | $6.76 | $2.15 | $+40.37 | $719.62 | ▲ +40.37 after sell → book $10,969.40; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 6 | $35.77 | $2.03 | $-32.42 | $932.21 | ▼ -32.42 after sell → book $10,967.37; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FUTU` | 2 | $123.67 | $2.02 | $-6.01 | $1,177.54 | ▼ -6.01 after sell → book $10,965.36; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 53 | $5.00 | $2.17 | $-15.45 | $1,440.37 | ▼ -15.45 after sell → book $10,963.19; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FNV` | 1 | $265.78 | $2.01 | $-5.25 | $1,704.14 | ▼ -5.25 after sell → book $10,961.18; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,704.14 | ▼ close $10,871.34 vs 09:30 $10,971.55 (session -89.84) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,704.14 | ▼ 09:30 equity $10,678.18 vs yday $10,871.34 (-193.16) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 19 | $13.04 | $2.07 | $-24.44 | $1,949.83 | ▼ -24.44 after sell → book $10,676.11; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PURR` | 24 | $11.73 | $2.08 | $-0.66 | $2,229.27 | ▼ -0.66 after sell → book $10,674.03; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 1 | $10.77 | $0.13 | $+1.35 | $2,239.91 | ▲ +1.35 after sell → book $10,673.90; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 4 | $2.67 | $0.14 | $+0.03 | $2,250.45 | ▲ +0.03 after sell → book $10,673.76; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DJT` | 1 | $9.57 | $0.12 | $-0.23 | $2,259.90 | ▼ -0.23 after sell → book $10,673.64; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `OABI` | 2 | $4.35 | $0.11 | $-1.14 | $2,268.49 | ▼ -1.14 after sell → book $10,673.53; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,268.49 | ▲ close $10,793.89 vs 09:30 $10,678.18 (session +120.36) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,268.49 | ▼ 09:30 equity $10,770.77 vs yday $10,793.89 (-23.12) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $139.65 | $2.04 | $-68.26 | $3,662.95 | ▼ -68.26 after sell → book $10,768.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 66 | $22.20 | $2.21 | $-77.00 | $5,125.93 | ▼ -77.00 after sell → book $10,766.51; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MEI` | 87 | $18.22 | $2.28 | $+33.75 | $6,708.80 | ▲ +33.75 after sell → book $10,764.24; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 170 | $8.73 | $2.54 | $-73.04 | $8,190.36 | ▼ -73.04 after sell → book $10,761.70; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 10 | $133.00 | $2.04 | $-91.66 | $9,518.32 | ▼ -91.66 after sell → book $10,759.66; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 3 | $413.78 | $2.02 | $-148.23 | $10,757.64 | ▼ -148.23 after sell → book $10,757.64; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,757.64 | ▲ close $10,757.64 vs 09:30 $10,770.77 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,757.64 | ▲ 09:30 equity $10,757.64 vs yday $10,757.64 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 755 | $1.78 | $9.74 | — | $9,404.00 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+183.1; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 73 | $18.40 | $2.21 | — | $8,058.59 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-32.2; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 696 | $1.93 | $8.98 | — | $6,706.33 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $23.88 | $2.16 | — | $5,366.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $4,085.77 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $2,741.94 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 129 | $10.42 | $2.38 | — | $1,395.38 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1344.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 167 | $8.03 | $2.49 | — | $51.88 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1344.70 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.88 | ▼ close $10,278.45 vs 09:30 $10,757.64 (session -446.98) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.88 | ▲ 09:30 equity $10,328.14 vs yday $10,278.45 (+49.69) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 1 | $5.79 | $0.06 | — | $46.03 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $7.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 2 | $2.51 | $0.06 | — | $40.96 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $7.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 1 | $4.53 | $0.05 | — | $36.38 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $7.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 1 | $5.75 | $0.06 | — | $30.57 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $7.41 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.57 | ▲ close $10,703.30 vs 09:30 $10,328.14 (session +375.38) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.57 | ▼ 09:30 equity $10,551.01 vs yday $10,703.30 (-152.29) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.57 | ▼ close $10,497.52 vs 09:30 $10,551.01 (session -53.49) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.57 | ▼ 09:30 equity $10,453.07 vs yday $10,497.52 (-44.45) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 755 | $1.45 | $9.87 | $-268.76 | $1,115.44 | ▼ -268.76 after sell → book $10,443.19; vs 09:30 mark -9.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `REAX` | 73 | $20.70 | $2.23 | $+163.46 | $2,624.31 | ▲ +163.46 after sell → book $10,440.96; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 696 | $1.94 | $9.10 | $-11.12 | $3,965.45 | ▼ -11.12 after sell → book $10,431.86; vs 09:30 mark -9.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 56 | $23.22 | $2.18 | $-41.30 | $5,263.59 | ▼ -41.30 after sell → book $10,429.68; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AGCO` | 10 | $127.69 | $2.04 | $-6.26 | $6,538.45 | ▼ -6.26 after sell → book $10,427.64; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 80 | $15.46 | $2.25 | $-109.28 | $7,772.99 | ▼ -109.28 after sell → book $10,425.38; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 129 | $10.02 | $2.41 | $-56.39 | $9,063.16 | ▼ -56.39 after sell → book $10,422.97; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 167 | $8.01 | $2.53 | $-8.36 | $10,398.31 | ▼ -8.36 after sell → book $10,420.45; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,398.31 | ▼ close $10,420.11 vs 09:30 $10,453.07 (session -0.34) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,398.31 | ▼ 09:30 equity $10,419.99 vs yday $10,420.11 (-0.12) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 1 | $5.22 | $0.08 | $-0.71 | $10,403.45 | ▼ -0.71 after sell → book $10,419.91; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 2 | $2.87 | $0.08 | $+0.58 | $10,409.11 | ▲ +0.58 after sell → book $10,419.83; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 1 | $4.85 | $0.07 | $-1.03 | $10,413.88 | ▼ -1.03 after sell → book $10,419.75; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,413.88 | ▲ close $10,419.95 vs 09:30 $10,419.99 (session +0.20) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,413.88 | ▲ 09:30 equity $10,420.04 vs yday $10,419.95 (+0.09) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `IRD` | 1 | $6.16 | $0.08 | $+1.50 | $10,419.96 | ▲ +1.50 after sell → book $10,419.96; vs 09:30 mark -0.08 | dropped from list after 4 sess (min 3) | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 482 | $2.70 | $6.22 | — | $9,112.34 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 265 | $4.91 | $3.42 | — | $7,807.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 416 | $3.13 | $5.37 | — | $6,500.33 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+24.2; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 930 | $1.40 | $12.00 | — | $5,186.33 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-17.2; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 15 | $84.27 | $2.04 | — | $3,920.25 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 118 | $10.95 | $2.34 | — | $2,625.80 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 23 | $54.91 | $2.06 | — | $1,360.81 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+24.3; leftover $1302.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 71 | $18.30 | $2.20 | — | $59.31 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1302.50 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.31 | ▲ close $10,541.25 vs 09:30 $10,420.04 (session +156.93) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.31 | ▼ 09:30 equity $10,538.37 vs yday $10,541.25 (-2.88) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.31 | ▲ close $10,758.71 vs 09:30 $10,538.37 (session +220.34) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.31 | ▲ 09:30 equity $10,802.45 vs yday $10,758.71 (+43.74) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.31 | ▲ close $10,889.82 vs 09:30 $10,802.45 (session +87.37) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.31 | ▼ 09:30 equity $10,795.62 vs yday $10,889.82 (-94.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 265 | $4.77 | $3.47 | $-43.99 | $1,319.89 | ▼ -43.99 after sell → book $10,792.15; vs 09:30 mark -3.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 416 | $3.48 | $5.45 | $+134.79 | $2,762.12 | ▲ +134.79 after sell → book $10,786.70; vs 09:30 mark -5.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `GPRO` | 930 | $1.31 | $12.16 | $-107.86 | $3,968.26 | ▼ -107.86 after sell → book $10,774.54; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 118 | $10.82 | $2.37 | $-20.06 | $5,242.64 | ▼ -20.06 after sell → book $10,772.16; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 23 | $50.69 | $2.08 | $-101.20 | $6,406.44 | ▼ -101.20 after sell → book $10,770.09; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 71 | $17.73 | $2.22 | $-44.90 | $7,663.04 | ▼ -44.90 after sell → book $10,767.86; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 54 | $23.29 | $2.15 | — | $6,403.23 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+16.1; leftover $1277.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 10 | $118.18 | $2.02 | — | $5,219.41 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1277.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 22 | $55.66 | $2.06 | — | $3,992.83 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $1277.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 5 | $236.92 | $2.00 | — | $2,806.23 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.5; leftover $1277.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 9 | $140.88 | $2.02 | — | $1,536.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1277.17 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 47 | $27.09 | $2.13 | — | $260.93 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1277.17 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.93 | ▼ close $10,599.86 vs 09:30 $10,795.62 (session -155.62) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.93 | ▲ 09:30 equity $10,713.41 vs yday $10,599.86 (+113.55) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 15 | $86.76 | $2.06 | $+33.26 | $1,560.27 | ▲ +33.26 after sell → book $10,711.35; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `HLP` | 148 | $2.10 | $2.43 | — | $1,247.04 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+60.5; leftover $312.05 | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 13 | $22.46 | $2.03 | — | $953.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $312.05 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 130 | $2.40 | $2.38 | — | $638.65 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $312.05 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 12 | $25.95 | $2.03 | — | $325.23 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $312.05 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $89.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot; ret5=+11.7; leftover $312.05 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.38 | ▲ close $11,445.11 vs 09:30 $10,713.41 (session +744.62) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.38 | ▼ 09:30 equity $11,345.92 vs yday $11,445.11 (-99.19) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 5 | $3.04 | $0.17 | — | $74.04 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $17.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 4 | $3.94 | $0.17 | — | $58.11 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $17.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 2 | $7.98 | $0.17 | — | $41.99 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $17.88 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.99 | ▼ close $11,064.41 vs 09:30 $11,345.92 (session -281.02) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.99 | ▲ 09:30 equity $11,103.77 vs yday $11,064.41 (+39.36) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 482 | $3.55 | $6.31 | $+397.17 | $1,746.77 | ▲ +397.17 after sell → book $11,097.45; vs 09:30 mark -6.32 | dropped from list after 6 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 54 | $29.43 | $2.17 | $+327.23 | $3,333.82 | ▲ +327.23 after sell → book $11,095.28; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 10 | $118.44 | $2.04 | $-1.46 | $4,516.18 | ▼ -1.46 after sell → book $11,093.24; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 22 | $58.23 | $2.08 | $+52.41 | $5,795.16 | ▲ +52.41 after sell → book $11,091.16; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-30.53 | $6,951.24 | ▼ -30.53 after sell → book $11,089.14; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RVTY` | 9 | $144.53 | $2.04 | $+28.80 | $8,249.97 | ▲ +28.80 after sell → book $11,087.10; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 47 | $28.69 | $2.15 | $+70.92 | $9,596.25 | ▲ +70.92 after sell → book $11,084.95; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 485 | $2.47 | $6.26 | — | $8,392.04 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+73.6; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 70 | $16.91 | $2.20 | — | $7,206.14 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+50.5; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 102 | $11.67 | $2.30 | — | $6,013.51 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+31.3; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $4,860.25 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.5; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 91 | $13.05 | $2.26 | — | $3,670.44 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $2,526.63 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+10.6; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 48 | $24.93 | $2.13 | — | $1,327.86 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+8.7; leftover $1199.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 112 | $10.71 | $2.33 | — | $126.01 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1199.53 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.01 | ▲ close $11,129.73 vs 09:30 $11,103.77 (session +66.27) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.01 | ▼ 09:30 equity $11,102.53 vs yday $11,129.73 (-27.20) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 1 | $9.11 | $0.09 | — | $116.81 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+44.4; leftover $18.00 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 2 | $7.23 | $0.15 | — | $102.20 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+36.6; leftover $18.00 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.20 | ▲ close $11,180.92 vs 09:30 $11,102.53 (session +78.63) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.20 | ▲ 09:30 equity $11,479.96 vs yday $11,180.92 (+299.04) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HLP` | 148 | $2.01 | $2.47 | $-18.22 | $397.21 | ▼ -18.22 after sell → book $11,477.49; vs 09:30 mark -2.47 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 13 | $23.00 | $2.05 | $+2.94 | $694.16 | ▲ +2.94 after sell → book $11,475.44; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 130 | $2.24 | $2.41 | $-25.59 | $982.95 | ▼ -25.59 after sell → book $11,473.03; vs 09:30 mark -2.41 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 12 | $27.79 | $2.05 | $+18.01 | $1,314.38 | ▲ +18.01 after sell → book $11,470.98; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ILMN` | 1 | $248.79 | $2.01 | $+10.93 | $1,561.16 | ▲ +10.93 after sell → book $11,468.97; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CYPH` | 5 | $3.82 | $0.23 | $+3.53 | $1,580.03 | ▲ +3.53 after sell → book $11,468.74; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RXT` | 4 | $4.07 | $0.19 | $+0.16 | $1,596.12 | ▲ +0.16 after sell → book $11,468.55; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 2 | $7.95 | $0.18 | $-0.41 | $1,611.83 | ▼ -0.41 after sell → book $11,468.36; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 99 | $2.70 | $2.29 | — | $1,342.24 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+109.2; leftover $268.64 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 27 | $9.90 | $2.07 | — | $1,072.87 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $268.64 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 6 | $41.76 | $2.01 | — | $820.31 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $268.64 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 3 | $70.84 | $2.00 | — | $605.79 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $268.64 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 13 | $19.70 | $2.03 | — | $347.66 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $268.64 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 365 | $0.73 | $3.77 | — | $75.97 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $268.64 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.97 | ▲ close $11,872.85 vs 09:30 $11,479.96 (session +418.66) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.97 | ▼ 09:30 equity $11,815.02 vs yday $11,872.85 (-57.83) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 485 | $2.68 | $6.35 | $+89.25 | $1,369.43 | ▲ +89.25 after sell → book $11,808.67; vs 09:30 mark -6.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 91 | $12.76 | $2.29 | $-30.94 | $2,528.30 | ▼ -30.94 after sell → book $11,806.39; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 6 | $164.04 | $2.03 | $-161.60 | $3,510.51 | ▼ -161.60 after sell → book $11,804.36; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `UMC` | 48 | $24.11 | $2.15 | $-43.65 | $4,665.64 | ▼ -43.65 after sell → book $11,802.20; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABTC` | 112 | $9.64 | $2.35 | $-124.52 | $5,742.96 | ▼ -124.52 after sell → book $11,799.85; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,742.96 | ▲ close $12,529.77 vs 09:30 $11,815.02 (session +729.93) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,039.63 | ▲ 09:30 equity $11,594.35 vs yday $11,452.04 (+142.31) | 09:30 open · cash $7,039.63 (unchanged overnight, no fees) · equity $11,594.35 vs prior close $11,452.04 (+142.31) · 11 name(s) re-marked at the open (per-name table). ARQQ×2 yday $23.12 → 09:30 $23.12 +0.00; BFLY×3 yday $9.41 → 09:30 $9.41 +0.00; FIVN×1 yday $36.66 → 09:30 $36.66 +0.00; FSLY×1 yday $26.68 → 09:30 $26.68 +0.00; GLND×13 yday $5.35 → 09:30 $6.06 +9.23; IBRX×3 yday $8.64 → 09:30 $8.64 +0.00; INDP×16 yday $4.00 → 09:30 $4.00 +0.00; NUAI×6 yday $6.94 → 09:30 $6.94 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; TJGC×85 yday $28.20 → 09:30 $29.76 +132.60; VICR×6 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 72 | $16.21 | $2.21 | — | $5,870.30 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1173.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 9 | $123.50 | $2.02 | — | $4,756.79 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1173.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 75 | $15.58 | $2.21 | — | $3,585.99 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+84.4; leftover $1173.27 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 292 | $4.00 | $3.77 | — | $2,412.76 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1173.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 303 | $3.86 | $3.91 | — | $1,239.27 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1173.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 149 | $7.85 | $2.44 | — | $67.19 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $1173.27 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.19 | ▼ close $11,407.90 vs 09:30 $11,594.35 (session -169.90) | 16:00 close · cash $67.19 · equity $11,407.90 vs 09:30 $11,594.35 (-186.45; session marks -169.90) · 17 name(s) marked open→close (per-name table). ARQQ×2 09:30 $23.12 → close $23.12 +0.00; BFLY×3 09:30 $9.41 → close $9.41 -0.00; FIVN×1 09:30 $36.66 → close $36.66 -0.00; FSLY×1 09:30 $26.68 → close $26.68 +0.00; GLND×13 09:30 $6.06 → close $5.54 -6.76; IBRX×3 09:30 $8.64 → close $8.64 +0.00; INDP×16 09:30 $4.00 → close $4.00 +0.00; NUAI×6 09:30 $6.94 → close $6.94 +0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; TJGC×85 09:30 $29.76 → close $26.24 -299.20; VICR×6 09:30 $276.06 → close $276.06 -0.00; SECZ×72 09:30 $16.21 → close $15.96 -18.00; GRAL×9 09:30 $123.50 → close $126.89 +30.51; USDE×75 09:30 $15.58 → close $17.25 +125.17; CYPH×292 09:30 $4.00 → close $4.12 +33.58; ZSQR×303 09:30 $3.86 → close $3.78 -24.24; RSKD×149 09:30 $7.85 → close $7.78 -10.43 | — |

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
| 2026-08-14 | `BRUN` | cash | leftover split 13.42 < 1 share @ 26.25 |
| 2026-08-14 | `LIFE` | cash | leftover split 13.42 < 1 share @ 35.04 |
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
| 2026-08-17 | `UMAC` | cash | leftover split 8.70 < 1 share @ 32.55 |
| 2026-08-17 | `HTFL` | cash | leftover split 8.70 < 1 share @ 41.23 |
| 2026-08-17 | `ALOY` | cash | leftover split 8.70 < 1 share @ 14.66 |
| 2026-08-17 | `LPTH` | cash | leftover split 8.70 < 1 share @ 14.94 |
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
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
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
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 38.01 < 1 share @ 119.43 |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PURR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FNV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BZ` | cash | leftover split 11.48 < 1 share @ 18.50 |
| 2026-08-27 | `PGY` | cash | leftover split 11.48 < 1 share @ 22.93 |
| 2026-08-27 | `MRNA` | cash | leftover split 11.48 < 1 share @ 144.18 |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PURR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FNV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DJT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DJT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `WFRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EBS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AGCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRNM` | cash | leftover split 7.41 < 1 share @ 16.40 |
| 2026-09-04 | `TARS` | cash | leftover split 7.41 < 1 share @ 82.70 |
| 2026-09-04 | `ASST` | cash | leftover split 7.41 < 1 share @ 25.18 |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIMO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `GPRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRWD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TEM` | cash | leftover split 17.88 < 1 share @ 81.40 |
| 2026-09-18 | `RBRK` | cash | leftover split 17.88 < 1 share @ 108.55 |
| 2026-09-21 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HLP` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BBNX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ILMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ARM` | cash | leftover split 18.00 < 1 share @ 319.41 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `META` | cash | leftover split 18.00 < 1 share @ 731.40 |
| 2026-09-22 | `FSLY` | cash | leftover split 18.00 < 1 share @ 28.02 |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `AMRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 70 | 2026-09-21 @ $16.91 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+50.5; leftover $1199.53 |
| `SECZ` | 102 | 2026-09-21 @ $11.67 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+31.3; leftover $1199.53 |
| `VICR` | 5 | 2026-09-21 @ $230.25 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.5; leftover $1199.53 |
| `CRML` | 1 | 2026-09-22 @ $9.11 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+44.4; leftover $18.00 |
| `NUAI` | 2 | 2026-09-22 @ $7.23 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+36.6; leftover $18.00 |
| `GLND` | 99 | 2026-09-23 @ $2.70 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+109.2; leftover $268.64 |
| `BFLY` | 27 | 2026-09-23 @ $9.90 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $268.64 |
| `VKTX` | 6 | 2026-09-23 @ $41.76 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $268.64 |
| `INOD` | 3 | 2026-09-23 @ $70.84 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $268.64 |
| `AMRX` | 13 | 2026-09-23 @ $19.70 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $268.64 |
| `EVTL` | 365 | 2026-09-23 @ $0.73 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $268.64 |
