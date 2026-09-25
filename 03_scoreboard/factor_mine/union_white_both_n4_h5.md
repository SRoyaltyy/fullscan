# Factor mine action — `union_white_both_n4_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **+1.92%** ($10,192) · signal-only (no cash/fees) was +22.84%. Starts YES **9/30**. Fills 67 · skips 141 · realized $+2101.03.

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
- No extra panic button — only the hold timer and the sell rule below.
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $45.78.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 83 | $59.80 | $2.24 | — | $5,034.36 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $71.00 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.00 | ▲ close $10,422.85 vs 09:30 $10,000.00 (session +427.38) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.00 | ▲ 09:30 equity $10,440.37 vs yday $10,422.85 (+17.52) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $56.05 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $17.75 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 4 | $4.31 | $0.18 | — | $38.63 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $17.75 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.63 | ▼ close $10,388.71 vs 09:30 $10,440.37 (session -51.33) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.63 | ▼ 09:30 equity $10,352.63 vs yday $10,388.71 (-36.08) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 1 | $9.12 | $0.09 | — | $29.41 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $9.66 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▼ close $10,155.91 vs 09:30 $10,352.63 (session -196.62) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▼ 09:30 equity $10,124.27 vs yday $10,155.91 (-31.64) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▼ close $10,107.33 vs 09:30 $10,124.27 (session -16.94) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▲ 09:30 equity $10,184.61 vs yday $10,107.33 (+77.28) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▲ close $10,206.02 vs 09:30 $10,184.61 (session +21.41) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▼ 09:30 equity $10,136.77 vs yday $10,206.02 (-69.25) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 83 | $58.64 | $2.29 | $-100.81 | $4,894.24 | ▼ -100.81 after sell → book $10,134.48; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 98 | $53.06 | $2.34 | $+234.18 | $10,091.78 | ▲ +234.18 after sell → book $10,132.14; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,632.44 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2522.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,111.64 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2522.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,652.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $2522.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2193 | $1.15 | $28.29 | — | $102.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2522.95 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.18 | ▲ close $10,503.90 vs 09:30 $10,136.77 (session +406.41) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.18 | ▲ 09:30 equity $10,984.31 vs yday $10,503.90 (+480.41) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 1 | $11.73 | $0.14 | $-3.36 | $113.77 | ▼ -3.36 after sell → book $10,984.17; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 4 | $4.43 | $0.21 | $+0.09 | $131.28 | ▲ +0.09 after sell → book $10,983.96; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $96.53 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $43.76 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $62.80 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $43.76 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.80 | ▲ close $11,346.84 vs 09:30 $10,984.31 (session +363.57) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.80 | ▲ 09:30 equity $12,305.30 vs yday $11,346.84 (+958.46) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `ABX` | 1 | $9.76 | $0.12 | $+0.43 | $72.44 | ▲ +0.43 after sell → book $12,305.18; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.44 | ▼ close $11,981.88 vs 09:30 $12,305.30 (session -323.30) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.44 | ▼ 09:30 equity $11,565.23 vs yday $11,981.88 (-416.65) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $55.57 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $24.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 14 | $1.63 | $0.27 | — | $32.48 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $24.15 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.48 | ▲ close $12,043.83 vs 09:30 $11,565.23 (session +479.04) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.48 | ▼ 09:30 equity $11,813.34 vs yday $12,043.83 (-230.49) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.48 | ▼ close $11,728.81 vs 09:30 $11,813.34 (session -84.53) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.48 | ▲ 09:30 equity $11,968.10 vs yday $11,728.81 (+239.29) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 27 | $95.52 | $2.10 | $+117.60 | $2,609.42 | ▲ +117.60 after sell → book $11,966.00; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 85 | $32.32 | $2.28 | $+224.12 | $5,354.33 | ▲ +224.12 after sell → book $11,963.71; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 17 | $155.89 | $2.07 | $+188.84 | $8,002.39 | ▲ +188.84 after sell → book $11,961.64; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 2193 | $1.75 | $28.68 | $+1258.83 | $11,811.46 | ▲ +1,258.83 after sell → book $11,932.96; vs 09:30 mark -28.68 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,811.46 | ▲ close $11,933.06 vs 09:30 $11,968.10 (session +0.10) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,811.46 | ▼ 09:30 equity $11,930.85 vs yday $11,933.06 (-2.21) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 2 | $16.44 | $0.35 | $-2.22 | $11,843.98 | ▼ -2.22 after sell → book $11,930.49; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 3 | $15.43 | $0.49 | $+12.07 | $11,889.78 | ▲ +12.07 after sell → book $11,930.00; vs 09:30 mark -0.49 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 20 | $141.76 | $2.05 | — | $9,052.53 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2972.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 24 | $122.81 | $2.06 | — | $6,103.03 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $2972.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,181.32 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2972.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 32 | $91.49 | $2.09 | — | $251.56 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $2972.45 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.56 | ▼ close $11,479.27 vs 09:30 $11,930.85 (session -442.52) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.56 | ▲ 09:30 equity $11,552.97 vs yday $11,479.27 (+73.70) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.56 | ▲ close $11,575.10 vs 09:30 $11,552.97 (session +22.13) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.56 | ▼ 09:30 equity $11,372.29 vs yday $11,575.10 (-202.81) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $267.87 | ▼ -0.56 after sell → book $11,372.10; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 14 | $1.68 | $0.30 | $+0.13 | $291.09 | ▲ +0.13 after sell → book $11,371.80; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.09 | ▲ close $11,438.32 vs 09:30 $11,372.29 (session +66.52) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.09 | ▼ 09:30 equity $11,415.21 vs yday $11,438.32 (-23.11) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.09 | ▲ close $11,516.39 vs 09:30 $11,415.21 (session +101.18) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.09 | ▼ 09:30 equity $11,438.58 vs yday $11,516.39 (-77.81) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 1 | $52.88 | $0.53 | — | $237.68 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $72.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 1 | $42.93 | $0.43 | — | $194.31 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $72.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 20 | $3.63 | $0.79 | — | $120.93 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $72.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 9 | $8.03 | $0.75 | — | $47.91 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $72.77 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.91 | ▲ close $11,570.83 vs 09:30 $11,438.58 (session +134.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.91 | ▲ 09:30 equity $11,749.49 vs yday $11,570.83 (+178.66) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 20 | $138.71 | $2.08 | $-65.13 | $2,820.03 | ▼ -65.13 after sell → book $11,747.41; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 24 | $118.58 | $2.09 | $-105.68 | $5,663.85 | ▼ -105.68 after sell → book $11,745.31; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 9 | $326.10 | $2.05 | $+11.14 | $8,596.70 | ▲ +11.14 after sell → book $11,743.26; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 32 | $91.02 | $2.12 | $-19.25 | $11,507.22 | ▼ -19.25 after sell → book $11,741.14; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 10 | $263.36 | $2.02 | — | $8,871.60 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2876.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 5 | $513.78 | $2.00 | — | $6,300.70 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $2876.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 635 | $4.53 | $8.19 | — | $3,415.95 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $2876.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 500 | $5.75 | $6.45 | — | $534.50 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $2876.81 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▲ close $11,929.92 vs 09:30 $11,749.49 (session +207.45) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▼ 09:30 equity $11,767.91 vs yday $11,929.92 (-162.01) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▼ close $11,349.92 vs 09:30 $11,767.91 (session -417.99) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▲ 09:30 equity $11,984.36 vs yday $11,349.92 (+634.44) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▼ close $11,970.21 vs 09:30 $11,984.36 (session -14.15) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▲ 09:30 equity $11,981.36 vs yday $11,970.21 (+11.16) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▼ close $11,929.49 vs 09:30 $11,981.36 (session -51.87) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▲ 09:30 equity $12,031.18 vs yday $11,929.49 (+101.69) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 1 | $53.53 | $0.56 | $-0.44 | $587.48 | ▼ -0.44 after sell → book $12,030.63; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 1 | $41.30 | $0.44 | $-2.50 | $628.34 | ▼ -2.50 after sell → book $12,030.19; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 20 | $2.77 | $0.63 | $-18.62 | $683.11 | ▼ -18.62 after sell → book $12,029.56; vs 09:30 mark -0.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 9 | $7.70 | $0.74 | $-4.46 | $751.67 | ▼ -4.46 after sell → book $12,028.82; vs 09:30 mark -0.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 7 | $52.55 | $2.01 | — | $381.81 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $375.83 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 20 | $18.30 | $2.05 | — | $13.76 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $375.83 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.76 | ▲ close $12,189.90 vs 09:30 $12,031.18 (session +165.14) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.76 | ▼ 09:30 equity $12,115.71 vs yday $12,189.90 (-74.19) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CRM` | 10 | $255.75 | $2.05 | $-80.17 | $2,569.21 | ▼ -80.17 after sell → book $12,113.66; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `DELL` | 5 | $538.57 | $2.04 | $+119.91 | $5,260.02 | ▲ +119.91 after sell → book $12,111.62; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 635 | $6.02 | $8.33 | $+929.63 | $9,074.39 | ▲ +929.63 after sell → book $12,103.29; vs 09:30 mark -8.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `LENZ` | 500 | $4.53 | $6.55 | $-623.00 | $11,332.84 | ▼ -623.00 after sell → book $12,096.74; vs 09:30 mark -6.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,332.84 | ▼ close $12,049.23 vs 09:30 $12,115.71 (session -47.51) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,332.84 | ▼ 09:30 equity $12,045.41 vs yday $12,049.23 (-3.82) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,332.84 | ▼ close $12,039.80 vs 09:30 $12,045.41 (session -5.61) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,332.84 | ▼ 09:30 equity $12,027.64 vs yday $12,039.80 (-12.16) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 63 | $89.38 | $2.18 | — | $5,699.72 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5666.42 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 47 | $118.18 | $2.13 | — | $143.13 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5666.42 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.13 | ▼ close $11,587.40 vs 09:30 $12,027.64 (session -435.93) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.13 | ▲ 09:30 equity $11,715.26 vs yday $11,587.40 (+127.86) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.13 | ▲ close $12,225.03 vs 09:30 $11,715.26 (session +509.77) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.13 | ▲ 09:30 equity $12,334.50 vs yday $12,225.03 (+109.47) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 7 | $51.19 | $2.03 | $-13.60 | $499.39 | ▼ -13.60 after sell → book $12,332.46; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAYP` | 20 | $17.91 | $2.07 | $-11.92 | $855.52 | ▼ -11.92 after sell → book $12,330.39; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 1 | $108.55 | $1.09 | — | $745.89 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $213.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 2 | $85.00 | $1.71 | — | $574.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $213.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 6 | $34.44 | $2.01 | — | $365.53 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $213.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 14 | $14.79 | $2.03 | — | $156.44 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $213.88 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.44 | ▼ close $11,930.35 vs 09:30 $12,334.50 (session -393.21) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.44 | ▲ 09:30 equity $12,047.05 vs yday $11,930.35 (+116.70) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.44 | ▼ close $11,961.31 vs 09:30 $12,047.05 (session -85.74) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.44 | ▲ 09:30 equity $11,963.13 vs yday $11,961.31 (+1.82) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.44 | ▲ close $11,974.82 vs 09:30 $11,963.13 (session +11.69) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.44 | ▲ 09:30 equity $12,122.94 vs yday $11,974.82 (+148.12) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SWKS` | 63 | $90.21 | $2.23 | $+47.88 | $5,837.43 | ▲ +47.88 after sell → book $12,120.70; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QRVO` | 47 | $118.44 | $2.19 | $+7.90 | $11,401.93 | ▲ +7.90 after sell → book $12,118.52; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 17 | $166.54 | $2.04 | — | $8,568.71 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2850.48 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 102 | $27.79 | $2.30 | — | $5,731.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2850.48 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 290 | $9.81 | $3.74 | — | $2,883.19 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2850.48 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 140 | $20.25 | $2.41 | — | $45.78 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2850.48 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.78 | ▼ close $11,798.81 vs 09:30 $12,122.94 (session -309.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.78 | ▼ 09:30 equity $11,727.25 vs yday $11,798.81 (-71.56) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.78 | ▲ close $11,924.18 vs 09:30 $11,727.25 (session +196.93) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.63 | ▼ 09:30 equity $10,211.21 vs yday $10,220.45 (-9.24) | 09:30 open · cash $174.63 (unchanged overnight, no fees) · equity $10,211.21 vs prior close $10,220.45 (-9.24) · 10 name(s) re-marked at the open (per-name table). A×14 yday $172.84 → 09:30 $171.98 -12.04; ARQT×87 yday $26.27 → 09:30 $26.27 +0.00; DXCM×27 yday $87.47 → 09:30 $87.47 +0.00; ECO×2 yday $78.22 → 09:30 $78.22 +0.00; FIVN×5 yday $36.66 → 09:30 $36.66 +0.00; HALO×20 yday $115.22 → 09:30 $115.36 +2.80; IOVA×2 yday $10.80 → 09:30 $10.80 +0.00; MGTX×2 yday $11.05 → 09:30 $11.05 +0.00; RARE×12 yday $14.77 → 09:30 $14.77 +0.00; RBRK×1 yday $113.80 → 09:30 $113.80 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 7 | $7.65 | $0.56 | — | $120.52 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $58.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.52 | ▼ close $10,192.44 vs 09:30 $10,211.21 (session -18.21) | 16:00 close · cash $120.52 · equity $10,192.44 vs 09:30 $10,211.21 (-18.77; session marks -18.21) · 11 name(s) marked open→close (per-name table). A×14 09:30 $171.98 → close $172.79 +11.34; ARQT×87 09:30 $26.27 → close $26.27 +0.00; DXCM×27 09:30 $87.47 → close $87.47 +0.00; ECO×2 09:30 $78.22 → close $78.22 +0.00; FIVN×5 09:30 $36.66 → close $36.66 -0.00; HALO×20 09:30 $115.36 → close $113.90 -29.20; IOVA×2 09:30 $10.80 → close $10.80 +0.00; MGTX×2 09:30 $11.05 → close $11.05 +0.00; RARE×12 09:30 $14.77 → close $14.77 +0.00; RBRK×1 09:30 $113.80 → close $113.80 +0.00; MRVI×7 09:30 $7.65 → close $7.60 -0.35 | — |

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
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 43.76 < 1 share @ 216.30 |
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
| 2026-08-25 | `EZPW` | cash | leftover split 24.15 < 1 share @ 35.05 |
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
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `IRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `LENZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `CRM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `IRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `LENZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| `RBRK` | 1 | 2026-09-18 @ $108.55 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $213.88 |
| `ECO` | 2 | 2026-09-18 @ $85.00 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $213.88 |
| `FIVN` | 6 | 2026-09-18 @ $34.44 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $213.88 |
| `RARE` | 14 | 2026-09-18 @ $14.79 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $213.88 |
| `A` | 17 | 2026-09-23 @ $166.54 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2850.48 |
| `ARQT` | 102 | 2026-09-23 @ $27.79 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2850.48 |
| `ADMA` | 290 | 2026-09-23 @ $9.81 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2850.48 |
| `FTRE` | 140 | 2026-09-23 @ $20.25 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2850.48 |
