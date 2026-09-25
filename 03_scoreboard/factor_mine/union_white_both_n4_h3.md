# Factor mine action — `union_white_both_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **-3.79%** ($9,621) · signal-only (no cash/fees) was +32.51%. Starts YES **3/30**. Fills 67 · skips 73 · realized $+170.12.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $61.09.

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
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 83 | $60.00 | $2.29 | $+12.07 | $5,007.12 | ▲ +12.07 after sell → book $10,121.98; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $10,078.24 | ▲ +107.76 after sell → book $10,119.64; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,078.24 | ▲ close $10,119.70 vs 09:30 $10,124.27 (session +0.06) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,078.24 | ▼ 09:30 equity $10,119.51 vs yday $10,119.70 (-0.19) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 1 | $13.03 | $0.15 | $-2.07 | $10,091.12 | ▼ -2.07 after sell → book $10,119.36; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 4 | $4.79 | $0.22 | $+1.51 | $10,110.05 | ▲ +1.51 after sell → book $10,119.13; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,110.05 | ▲ close $10,119.20 vs 09:30 $10,119.51 (session +0.07) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,110.05 | ▼ 09:30 equity $10,119.18 vs yday $10,119.20 (-0.02) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 1 | $9.13 | $0.11 | $-0.20 | $10,119.07 | ▼ -0.20 after sell → book $10,119.07; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,659.73 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2529.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,138.93 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2529.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,679.71 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $2529.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2199 | $1.15 | $28.37 | — | $122.50 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2529.77 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.50 | ▲ close $10,493.12 vs 09:30 $10,119.18 (session +408.77) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.50 | ▲ 09:30 equity $10,973.97 vs yday $10,493.12 (+480.85) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $87.75 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $40.83 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $54.01 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $40.83 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.01 | ▲ close $11,336.91 vs 09:30 $10,973.97 (session +363.64) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.01 | ▲ 09:30 equity $12,297.73 vs yday $11,336.91 (+960.82) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.01 | ▼ close $11,973.53 vs 09:30 $12,297.73 (session -324.20) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.01 | ▼ 09:30 equity $11,556.16 vs yday $11,973.53 (-417.37) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 27 | $95.86 | $2.10 | $+126.78 | $2,640.13 | ▲ +126.78 after sell → book $11,554.06; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 85 | $32.32 | $2.28 | $+224.12 | $5,385.05 | ▲ +224.12 after sell → book $11,551.78; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 17 | $156.51 | $2.07 | $+199.38 | $8,043.65 | ▲ +199.38 after sell → book $11,549.71; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 321 | $8.35 | $4.14 | — | $5,359.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $2681.22 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 1644 | $1.63 | $21.21 | — | $2,658.23 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $2681.22 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 75 | $35.05 | $2.21 | — | $27.26 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $2681.22 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.26 | ▲ close $11,947.57 vs 09:30 $11,556.16 (session +425.43) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.26 | ▲ 09:30 equity $11,948.23 vs yday $11,947.57 (+0.66) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 2199 | $1.60 | $28.76 | $+932.42 | $3,516.90 | ▲ +932.42 after sell → book $11,919.47; vs 09:30 mark -28.76 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $3,549.75 | ▼ -1.91 after sell → book $11,919.12; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 3 | $15.35 | $0.49 | $+11.83 | $3,595.31 | ▲ +11.83 after sell → book $11,918.63; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,595.31 | ▼ close $11,642.24 vs 09:30 $11,948.23 (session -276.39) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,595.31 | ▲ 09:30 equity $11,693.66 vs yday $11,642.24 (+51.42) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,595.31 | ▼ close $11,605.49 vs 09:30 $11,693.66 (session -88.17) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,595.31 | ▲ 09:30 equity $11,619.05 vs yday $11,605.49 (+13.56) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 321 | $8.28 | $4.22 | $-30.83 | $6,248.97 | ▼ -30.83 after sell → book $11,614.83; vs 09:30 mark -4.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 1644 | $1.69 | $21.50 | $+55.93 | $9,005.83 | ▲ +55.93 after sell → book $11,593.33; vs 09:30 mark -21.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 75 | $34.50 | $2.25 | $-45.71 | $11,591.08 | ▼ -45.71 after sell → book $11,591.08; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 20 | $141.76 | $2.05 | — | $8,753.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2897.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 23 | $122.81 | $2.06 | — | $5,927.14 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $2897.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $3,329.85 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2897.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 31 | $91.49 | $2.08 | — | $491.57 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $2897.77 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.57 | ▼ close $11,151.21 vs 09:30 $11,619.05 (session -431.66) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.57 | ▲ 09:30 equity $11,221.67 vs yday $11,151.21 (+70.46) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.57 | ▲ close $11,243.89 vs 09:30 $11,221.67 (session +22.22) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.57 | ▼ 09:30 equity $11,045.55 vs yday $11,243.89 (-198.34) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.57 | ▲ close $11,114.81 vs 09:30 $11,045.55 (session +69.26) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.57 | ▼ 09:30 equity $11,094.04 vs yday $11,114.81 (-20.77) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 20 | $133.00 | $2.08 | $-179.33 | $3,149.49 | ▼ -179.33 after sell → book $11,091.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 23 | $114.22 | $2.09 | $-201.72 | $5,774.46 | ▼ -201.72 after sell → book $11,089.87; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $318.04 | $2.04 | $-55.02 | $8,316.74 | ▼ -55.02 after sell → book $11,087.83; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 31 | $89.39 | $2.12 | $-69.30 | $11,085.71 | ▼ -69.30 after sell → book $11,085.71; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,085.71 | ▲ close $11,085.71 vs 09:30 $11,094.04 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,085.71 | ▲ 09:30 equity $11,085.71 vs yday $11,085.71 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 52 | $52.88 | $2.15 | — | $8,333.81 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2771.43 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 64 | $42.93 | $2.18 | — | $5,584.10 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2771.43 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 763 | $3.63 | $9.84 | — | $2,804.57 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2771.43 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 345 | $8.03 | $4.45 | — | $29.77 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $2771.43 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.77 | ▼ close $10,845.07 vs 09:30 $11,085.71 (session -222.02) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.77 | ▼ 09:30 equity $10,760.26 vs yday $10,845.07 (-84.81) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 1 | $4.53 | $0.05 | — | $25.19 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $7.44 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 1 | $5.75 | $0.06 | — | $19.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $7.44 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.38 | ▲ close $10,889.66 vs 09:30 $10,760.26 (session +129.51) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.38 | ▲ 09:30 equity $11,000.87 vs yday $10,889.66 (+111.21) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.38 | ▼ close $10,798.10 vs 09:30 $11,000.87 (session -202.77) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.38 | ▼ 09:30 equity $10,749.05 vs yday $10,798.10 (-49.05) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 52 | $53.16 | $2.18 | $+10.24 | $2,781.52 | ▲ +10.24 after sell → book $10,746.87; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 64 | $42.01 | $2.21 | $-63.28 | $5,467.95 | ▼ -63.28 after sell → book $10,744.66; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 763 | $3.28 | $9.99 | $-286.88 | $7,960.60 | ▼ -286.88 after sell → book $10,734.67; vs 09:30 mark -9.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 345 | $8.01 | $4.53 | $-15.88 | $10,719.52 | ▼ -15.88 after sell → book $10,730.14; vs 09:30 mark -4.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,719.52 | ▲ close $10,730.16 vs 09:30 $10,749.05 (session +0.02) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,719.52 | ▲ 09:30 equity $10,730.24 vs yday $10,730.16 (+0.08) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `IRD` | 1 | $5.87 | $0.08 | $+1.21 | $10,725.31 | ▲ +1.21 after sell → book $10,730.16; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 1 | $4.85 | $0.07 | $-1.03 | $10,730.09 | ▼ -1.03 after sell → book $10,730.09; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,730.09 | ▲ close $10,730.09 vs 09:30 $10,730.24 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,730.09 | ▲ 09:30 equity $10,730.09 vs yday $10,730.09 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 102 | $52.55 | $2.30 | — | $5,367.69 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $5365.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 293 | $18.30 | $3.78 | — | $2.01 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $5365.04 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.01 | ▲ close $11,208.60 vs 09:30 $10,730.09 (session +484.59) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.01 | ▼ 09:30 equity $11,161.85 vs yday $11,208.60 (-46.75) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.01 | ▼ close $10,470.19 vs 09:30 $11,161.85 (session -691.66) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.01 | ▼ 09:30 equity $10,413.93 vs yday $10,470.19 (-56.26) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.01 | ▼ close $10,331.43 vs 09:30 $10,413.93 (session -82.50) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.01 | ▼ 09:30 equity $10,154.10 vs yday $10,331.43 (-177.33) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 102 | $48.60 | $2.35 | $-407.55 | $4,956.86 | ▼ -407.55 after sell → book $10,151.75; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 293 | $17.73 | $3.87 | $-174.66 | $10,147.88 | ▼ -174.66 after sell → book $10,147.88; vs 09:30 mark -3.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 56 | $89.38 | $2.16 | — | $5,140.44 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5073.94 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 42 | $118.18 | $2.12 | — | $174.77 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5073.94 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.77 | ▼ close $9,754.55 vs 09:30 $10,154.10 (session -389.06) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.77 | ▲ 09:30 equity $9,859.13 vs yday $9,754.55 (+104.58) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.77 | ▲ close $10,308.11 vs 09:30 $9,859.13 (session +448.98) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.77 | ▲ 09:30 equity $10,401.49 vs yday $10,308.11 (+93.38) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 1 | $34.44 | $0.35 | — | $139.98 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $43.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $110.10 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $43.69 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.10 | ▼ close $10,063.71 vs 09:30 $10,401.49 (session -337.13) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.10 | ▲ 09:30 equity $10,167.70 vs yday $10,063.71 (+103.99) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 56 | $89.66 | $2.21 | $+11.31 | $5,128.85 | ▲ +11.31 after sell → book $10,165.49; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 42 | $118.44 | $2.17 | $+6.64 | $10,101.16 | ▲ +6.64 after sell → book $10,163.32; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.16 | ▲ close $10,167.47 vs 09:30 $10,167.70 (session +4.15) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.16 | ▲ 09:30 equity $10,167.73 vs yday $10,167.47 (+0.26) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.16 | ▲ close $10,169.40 vs 09:30 $10,167.73 (session +1.67) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.16 | ▲ 09:30 equity $10,170.87 vs yday $10,169.40 (+1.47) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `FIVN` | 1 | $38.91 | $0.41 | $+3.71 | $10,139.66 | ▲ +3.71 after sell → book $10,170.46; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 2 | $15.40 | $0.33 | $+0.58 | $10,170.12 | ▲ +0.58 after sell → book $10,170.12; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 15 | $166.54 | $2.04 | — | $7,669.99 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2542.53 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 91 | $27.79 | $2.26 | — | $5,138.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2542.53 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 259 | $9.81 | $3.34 | — | $2,594.70 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2542.53 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 125 | $20.25 | $2.37 | — | $61.09 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2542.53 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.09 | ▼ close $9,893.77 vs 09:30 $10,170.87 (session -266.35) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.09 | ▼ 09:30 equity $9,835.89 vs yday $9,893.77 (-57.88) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.09 | ▲ close $10,012.44 vs 09:30 $9,835.89 (session +176.55) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,310.89 | ▲ 09:30 equity $9,531.33 vs yday $9,531.19 (+0.14) | 09:30 open · cash $9,310.89 (unchanged overnight, no fees) · equity $9,531.33 vs prior close $9,531.19 (+0.14) · 2 name(s) re-marked at the open (per-name table). ARQT×4 yday $26.27 → 09:30 $26.27 +0.00; HALO×1 yday $115.22 → 09:30 $115.36 +0.14 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 405 | $7.65 | $5.22 | — | $6,207.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $3103.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 37 | $83.76 | $2.10 | — | $3,106.19 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $3103.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 37 | $83.69 | $2.10 | — | $7.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $3103.63 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.38 | ▲ close $9,621.00 vs 09:30 $9,531.33 (session +99.10) | 16:00 close · cash $7.38 · equity $9,621.00 vs 09:30 $9,531.33 (+89.67; session marks +99.10) · 5 name(s) marked open→close (per-name table). ARQT×4 09:30 $26.27 → close $26.27 +0.00; HALO×1 09:30 $115.36 → close $113.90 -1.46; MRVI×405 09:30 $7.65 → close $7.60 -20.25; TXG×37 09:30 $83.76 → close $85.71 +72.15; TEM×37 09:30 $83.69 → close $85.01 +48.66 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 17.75 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 17.75 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALM` | cash | leftover split 9.66 < 1 share @ 16.20 |
| 2026-08-17 | `NMAX` | cash | leftover split 9.66 < 1 share @ 10.97 |
| 2026-08-17 | `AAOI` | cash | leftover split 9.66 < 1 share @ 152.64 |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 40.83 < 1 share @ 216.30 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 7.44 < 1 share @ 263.36 |
| 2026-09-04 | `DELL` | cash | leftover split 7.44 < 1 share @ 513.78 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 43.69 < 1 share @ 108.55 |
| 2026-09-18 | `ECO` | cash | leftover split 43.69 < 1 share @ 85.00 |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `A` | 15 | 2026-09-23 @ $166.54 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2542.53 |
| `ARQT` | 91 | 2026-09-23 @ $27.79 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2542.53 |
| `ADMA` | 259 | 2026-09-23 @ $9.81 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2542.53 |
| `FTRE` | 125 | 2026-09-23 @ $20.25 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2542.53 |
