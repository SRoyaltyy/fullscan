# Factor mine action — `union_white_both_n4_h5_s12`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · both+top4 hold5, stop −12% at 09:30 even inside hold

Cash book **+3.53%** ($10,353) · signal-only (no cash/fees) was +22.84%. Starts YES **12/30**. Fills 67 · skips 137 · realized $+2194.89.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the morning-board Score (100 minus list rank) — only after the pool is chosen.
- Must-have: at most 0 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-have: yesterday's session was up AND a major good catalyst.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by the morning-board Score (100 minus list rank) — only after the pool is chosen and keep the top 4.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- Stop-loss: sell at the next 09:30 if that open is 12% worse than our fill, even inside the minimum hold.
- The hold timer still applies if take-profit and stop-loss do not fire.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $126.10.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 83 | $59.80 | $2.24 | — | $5,034.36 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $71.00 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.00 | ▲ close $10,422.85 vs 09:30 $10,000.00 (session +427.38) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.00 | ▲ 09:30 equity $10,440.37 vs yday $10,422.85 (+17.52) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $56.05 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $17.75 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 4 | $4.31 | $0.18 | — | $38.63 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $17.75 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.63 | ▼ close $10,388.71 vs 09:30 $10,440.37 (session -51.33) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.63 | ▼ 09:30 equity $10,352.63 vs yday $10,388.71 (-36.08) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 1 | $9.12 | $0.09 | — | $29.41 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $9.66 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▼ close $10,155.91 vs 09:30 $10,352.63 (session -196.62) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▼ 09:30 equity $10,124.27 vs yday $10,155.91 (-31.64) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▼ close $10,107.33 vs 09:30 $10,124.27 (session -16.94) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▲ 09:30 equity $10,184.61 vs yday $10,107.33 (+77.28) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▲ close $10,206.02 vs 09:30 $10,184.61 (session +21.41) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▼ 09:30 equity $10,136.77 vs yday $10,206.02 (-69.25) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 83 | $58.64 | $2.29 | $-100.81 | $4,894.24 | ▼ -100.81 after sell → book $10,134.48; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 98 | $53.06 | $2.34 | $+234.18 | $10,091.78 | ▲ +234.18 after sell → book $10,132.14; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `BETR` | 1 | $12.95 | $0.15 | $-2.15 | $10,104.58 | ▼ -2.15 after sell → book $10,131.99; vs 09:30 mark -0.15 | stop-loss after 4 sess | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,645.24 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2526.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,124.44 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2526.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,665.22 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $2526.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2196 | $1.15 | $28.33 | — | $111.49 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2526.14 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.49 | ▲ close $10,505.18 vs 09:30 $10,136.77 (session +407.88) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.49 | ▲ 09:30 equity $10,985.85 vs yday $10,505.18 (+480.67) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 4 | $4.43 | $0.21 | $+0.09 | $129.00 | ▲ +0.09 after sell → book $10,985.64; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $94.25 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $43.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $60.52 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $43.00 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.52 | ▲ close $11,348.82 vs 09:30 $10,985.85 (session +363.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.52 | ▲ 09:30 equity $12,308.51 vs yday $11,348.82 (+959.69) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `ABX` | 1 | $9.76 | $0.12 | $+0.43 | $70.16 | ▲ +0.43 after sell → book $12,308.39; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.16 | ▼ close $11,984.64 vs 09:30 $12,308.51 (session -323.75) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.16 | ▼ 09:30 equity $11,567.63 vs yday $11,984.64 (-417.01) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $53.29 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $23.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 14 | $1.63 | $0.27 | — | $30.20 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $23.39 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.20 | ▲ close $12,046.47 vs 09:30 $11,567.63 (session +479.28) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.20 | ▼ 09:30 equity $11,815.86 vs yday $12,046.47 (-230.61) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.20 | ▼ close $11,731.42 vs 09:30 $11,815.86 (session -84.44) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.20 | ▲ 09:30 equity $11,971.07 vs yday $11,731.42 (+239.65) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 27 | $95.52 | $2.10 | $+117.60 | $2,607.13 | ▲ +117.60 after sell → book $11,968.96; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 85 | $32.32 | $2.28 | $+224.12 | $5,352.05 | ▲ +224.12 after sell → book $11,966.68; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 17 | $155.89 | $2.07 | $+188.84 | $8,000.11 | ▲ +188.84 after sell → book $11,964.61; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 2196 | $1.75 | $28.72 | $+1260.55 | $11,814.39 | ▲ +1,260.55 after sell → book $11,935.89; vs 09:30 mark -28.72 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,814.39 | ▲ close $11,935.99 vs 09:30 $11,971.07 (session +0.10) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,814.39 | ▼ 09:30 equity $11,933.78 vs yday $11,935.99 (-2.21) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 2 | $16.44 | $0.35 | $-2.22 | $11,846.91 | ▼ -2.22 after sell → book $11,933.42; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 3 | $15.43 | $0.49 | $+12.07 | $11,892.71 | ▲ +12.07 after sell → book $11,932.93; vs 09:30 mark -0.49 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 20 | $141.76 | $2.05 | — | $9,055.46 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2973.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 24 | $122.81 | $2.06 | — | $6,105.96 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $2973.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,184.25 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2973.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 32 | $91.49 | $2.09 | — | $254.49 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $2973.18 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.49 | ▼ close $11,482.20 vs 09:30 $11,933.78 (session -442.52) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $254.49 | ▲ 09:30 equity $11,555.90 vs yday $11,482.20 (+73.70) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.49 | ▲ close $11,578.03 vs 09:30 $11,555.90 (session +22.13) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $254.49 | ▼ 09:30 equity $11,375.22 vs yday $11,578.03 (-202.81) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $270.80 | ▼ -0.56 after sell → book $11,375.03; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 14 | $1.68 | $0.30 | $+0.13 | $294.02 | ▲ +0.13 after sell → book $11,374.73; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $294.02 | ▲ close $11,441.25 vs 09:30 $11,375.22 (session +66.52) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $294.02 | ▼ 09:30 equity $11,418.14 vs yday $11,441.25 (-23.11) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $294.02 | ▲ close $11,519.32 vs 09:30 $11,418.14 (session +101.18) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $294.02 | ▼ 09:30 equity $11,441.51 vs yday $11,519.32 (-77.81) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 1 | $52.88 | $0.53 | — | $240.61 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $73.50 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 1 | $42.93 | $0.43 | — | $197.24 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $73.50 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 20 | $3.63 | $0.79 | — | $123.86 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $73.50 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 9 | $8.03 | $0.75 | — | $50.84 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $73.50 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.84 | ▲ close $11,573.76 vs 09:30 $11,441.51 (session +134.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.84 | ▲ 09:30 equity $11,752.42 vs yday $11,573.76 (+178.66) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 20 | $138.71 | $2.08 | $-65.13 | $2,822.96 | ▼ -65.13 after sell → book $11,750.34; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 24 | $118.58 | $2.09 | $-105.68 | $5,666.78 | ▼ -105.68 after sell → book $11,748.24; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 9 | $326.10 | $2.05 | $+11.14 | $8,599.63 | ▲ +11.14 after sell → book $11,746.19; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 32 | $91.02 | $2.12 | $-19.25 | $11,510.15 | ▼ -19.25 after sell → book $11,744.07; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 10 | $263.36 | $2.02 | — | $8,874.53 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2877.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 5 | $513.78 | $2.00 | — | $6,303.63 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $2877.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 635 | $4.53 | $8.19 | — | $3,418.88 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $2877.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 500 | $5.75 | $6.45 | — | $537.43 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $2877.54 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $537.43 | ▲ close $11,932.85 vs 09:30 $11,752.42 (session +207.45) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $537.43 | ▼ 09:30 equity $11,770.84 vs yday $11,932.85 (-162.01) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $537.43 | ▼ close $11,352.85 vs 09:30 $11,770.84 (session -417.99) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $537.43 | ▲ 09:30 equity $11,987.29 vs yday $11,352.85 (+634.44) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $537.43 | ▼ close $11,973.14 vs 09:30 $11,987.29 (session -14.15) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $537.43 | ▲ 09:30 equity $11,984.29 vs yday $11,973.14 (+11.15) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 20 | $2.85 | $0.65 | $-17.04 | $593.78 | ▼ -17.04 after sell → book $11,983.64; vs 09:30 mark -0.65 | stop-loss after 4 sess | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 500 | $4.85 | $6.55 | $-463.00 | $3,012.23 | ▼ -463.00 after sell → book $11,977.09; vs 09:30 mark -6.55 | stop-loss after 3 sess | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,012.23 | ▲ close $11,992.42 vs 09:30 $11,984.29 (session +15.33) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,012.23 | ▲ 09:30 equity $12,098.51 vs yday $11,992.42 (+106.09) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 1 | $53.53 | $0.56 | $-0.44 | $3,065.20 | ▼ -0.44 after sell → book $12,097.95; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 1 | $41.30 | $0.44 | $-2.50 | $3,106.07 | ▼ -2.50 after sell → book $12,097.52; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 9 | $7.70 | $0.74 | $-4.46 | $3,174.63 | ▼ -4.46 after sell → book $12,096.78; vs 09:30 mark -0.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 30 | $52.55 | $2.08 | — | $1,596.05 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1587.31 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 86 | $18.30 | $2.25 | — | $20.00 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1587.31 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.00 | ▲ close $12,461.85 vs 09:30 $12,098.51 (session +369.40) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.00 | ▼ 09:30 equity $12,372.13 vs yday $12,461.85 (-89.72) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CRM` | 10 | $255.75 | $2.05 | $-80.17 | $2,575.45 | ▼ -80.17 after sell → book $12,370.08; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `DELL` | 5 | $538.57 | $2.04 | $+119.91 | $5,266.26 | ▲ +119.91 after sell → book $12,368.04; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 635 | $6.02 | $8.33 | $+929.63 | $9,080.64 | ▲ +929.63 after sell → book $12,359.72; vs 09:30 mark -8.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,080.64 | ▼ close $12,156.22 vs 09:30 $12,372.13 (session -203.50) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,080.64 | ▼ 09:30 equity $12,139.74 vs yday $12,156.22 (-16.48) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,080.64 | ▼ close $12,115.56 vs 09:30 $12,139.74 (session -24.18) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,080.64 | ▼ 09:30 equity $12,063.42 vs yday $12,115.56 (-52.14) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 50 | $89.38 | $2.14 | — | $4,609.50 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4540.32 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 38 | $118.18 | $2.10 | — | $116.55 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4540.32 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.55 | ▼ close $11,712.71 vs 09:30 $12,063.42 (session -346.46) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.55 | ▲ 09:30 equity $11,851.35 vs yday $11,712.71 (+138.64) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.55 | ▲ close $12,279.49 vs 09:30 $11,851.35 (session +428.14) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.55 | ▲ 09:30 equity $12,383.74 vs yday $12,279.49 (+104.25) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 30 | $51.19 | $2.10 | $-45.13 | $1,650.00 | ▼ -45.13 after sell → book $12,381.64; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAYP` | 86 | $17.91 | $2.27 | $-38.06 | $3,187.98 | ▼ -38.06 after sell → book $12,379.36; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 7 | $108.55 | $2.01 | — | $2,426.12 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $797.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 9 | $85.00 | $2.02 | — | $1,659.11 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $797.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 23 | $34.44 | $2.06 | — | $864.93 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $797.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 53 | $14.79 | $2.15 | — | $78.91 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $797.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.91 | ▼ close $11,997.11 vs 09:30 $12,383.74 (session -374.02) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.91 | ▲ 09:30 equity $12,092.83 vs yday $11,997.11 (+95.72) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.91 | ▲ close $12,131.96 vs 09:30 $12,092.83 (session +39.13) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.91 | ▲ 09:30 equity $12,138.85 vs yday $12,131.96 (+6.89) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.91 | ▲ close $12,183.10 vs 09:30 $12,138.85 (session +44.26) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.91 | ▲ 09:30 equity $12,286.31 vs yday $12,183.10 (+103.21) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SWKS` | 50 | $90.21 | $2.19 | $+37.17 | $4,587.22 | ▲ +37.17 after sell → book $12,284.13; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QRVO` | 38 | $118.44 | $2.15 | $+5.63 | $9,085.79 | ▲ +5.63 after sell → book $12,281.98; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 13 | $166.54 | $2.03 | — | $6,918.74 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2271.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 81 | $27.79 | $2.23 | — | $4,665.52 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2271.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 231 | $9.81 | $2.98 | — | $2,396.43 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2271.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 112 | $20.25 | $2.33 | — | $126.10 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2271.45 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.10 | ▼ close $12,005.52 vs 09:30 $12,286.31 (session -266.89) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.10 | ▼ 09:30 equity $11,924.89 vs yday $12,005.52 (-80.63) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.10 | ▲ close $12,068.82 vs 09:30 $11,924.89 (session +143.94) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.48 | ▼ 09:30 equity $10,368.60 vs yday $10,376.54 (-7.94) | 09:30 open · cash $99.48 (unchanged overnight, no fees) · equity $10,368.60 vs prior close $10,376.54 (-7.94) · 9 name(s) re-marked at the open (per-name table). A×12 yday $172.84 → 09:30 $171.98 -10.32; ARQT×71 yday $26.27 → 09:30 $26.27 +0.00; DXCM×22 yday $87.47 → 09:30 $87.47 +0.00; ECO×7 yday $78.22 → 09:30 $78.22 +0.00; FIVN×18 yday $36.66 → 09:30 $36.66 +0.00; HALO×17 yday $115.22 → 09:30 $115.36 +2.38; IOVA×4 yday $10.80 → 09:30 $10.80 +0.00; RARE×43 yday $14.77 → 09:30 $14.77 +0.00; RBRK×5 yday $113.80 → 09:30 $113.80 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 4 | $7.65 | $0.32 | — | $68.56 | — | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $33.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.56 | ▼ close $10,352.98 vs 09:30 $10,368.60 (session -15.30) | 16:00 close · cash $68.56 · equity $10,352.98 vs 09:30 $10,368.60 (-15.62; session marks -15.30) · 10 name(s) marked open→close (per-name table). A×12 09:30 $171.98 → close $172.79 +9.72; ARQT×71 09:30 $26.27 → close $26.27 +0.00; DXCM×22 09:30 $87.47 → close $87.47 +0.00; ECO×7 09:30 $78.22 → close $78.22 +0.00; FIVN×18 09:30 $36.66 → close $36.66 -0.00; HALO×17 09:30 $115.36 → close $113.90 -24.82; IOVA×4 09:30 $10.80 → close $10.80 +0.00; RARE×43 09:30 $14.77 → close $14.77 +0.00; RBRK×5 09:30 $113.80 → close $113.80 +0.00; MRVI×4 09:30 $7.65 → close $7.60 -0.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 17.75 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 17.75 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ALM` | cash | leftover split 9.66 < 1 share @ 16.20 |
| 2026-08-17 | `NMAX` | cash | leftover split 9.66 < 1 share @ 10.97 |
| 2026-08-17 | `AAOI` | cash | leftover split 9.66 < 1 share @ 152.64 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 43.00 < 1 share @ 216.30 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `EZPW` | cash | leftover split 23.39 < 1 share @ 35.05 |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `TTMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `KEYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `TTMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `KEYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `IRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `CRM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `IRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAYP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `BAND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAYP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SWKS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QRVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SWKS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QRVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 7 | 2026-09-18 @ $108.55 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $797.00 |
| `ECO` | 9 | 2026-09-18 @ $85.00 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $797.00 |
| `FIVN` | 23 | 2026-09-18 @ $34.44 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $797.00 |
| `RARE` | 53 | 2026-09-18 @ $14.79 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $797.00 |
| `A` | 13 | 2026-09-23 @ $166.54 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2271.45 |
| `ARQT` | 81 | 2026-09-23 @ $27.79 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2271.45 |
| `ADMA` | 231 | 2026-09-23 @ $9.81 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2271.45 |
| `FTRE` | 112 | 2026-09-23 @ $20.25 | both+top4 hold5, stop −12% at 09:30 even inside hold; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2271.45 |
