# Factor mine action — `union_white_both_n4_h2`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **2** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **-3.12%** ($9,689) · signal-only (no cash/fees) was +20.82%. Starts YES **13/30**. Fills 76 · skips 47 · realized $+2113.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 2 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 2 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 2 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **2**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $64.78.

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
| 2026-08-17 09:30 ET | **SELL** | `BTSG` | 83 | $61.69 | $2.29 | $+152.34 | $5,156.60 | ▲ +152.34 after sell → book $10,350.33; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TPG` | 98 | $52.67 | $2.34 | $+195.96 | $10,315.92 | ▲ +195.96 after sell → book $10,347.99; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 282 | $9.12 | $3.64 | — | $7,740.44 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $2578.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 159 | $16.20 | $2.47 | — | $5,162.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2578.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 235 | $10.97 | $3.03 | — | $2,581.20 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $2578.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 16 | $152.64 | $2.04 | — | $136.92 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $2578.98 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.92 | ▼ close $10,255.22 vs 09:30 $10,352.63 (session -81.60) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.92 | ▼ 09:30 equity $9,986.82 vs yday $10,255.22 (-268.40) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BETR` | 1 | $13.21 | $0.16 | $-1.90 | $149.97 | ▼ -1.90 after sell → book $9,986.66; vs 09:30 mark -0.16 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 09:30 ET | **SELL** | `ANGX` | 4 | $4.79 | $0.22 | $+1.51 | $168.91 | ▲ +1.51 after sell → book $9,986.44; vs 09:30 mark -0.22 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.91 | ▼ close $9,978.74 vs 09:30 $9,986.82 (session -7.70) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.91 | ▲ 09:30 equity $10,157.52 vs yday $9,978.74 (+178.78) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ABX` | 282 | $9.08 | $3.71 | $-18.62 | $2,725.76 | ▼ -18.62 after sell → book $10,153.81; vs 09:30 mark -3.71 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALM` | 159 | $16.05 | $2.51 | $-28.83 | $5,275.20 | ▼ -28.83 after sell → book $10,151.30; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 235 | $11.50 | $3.09 | $+118.43 | $7,974.61 | ▲ +118.43 after sell → book $10,148.21; vs 09:30 mark -3.09 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `AAOI` | 16 | $135.85 | $2.07 | $-272.74 | $10,146.14 | ▼ -272.74 after sell → book $10,146.14; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.14 | ▲ close $10,146.14 vs 09:30 $10,157.52 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.14 | ▲ 09:30 equity $10,146.14 vs yday $10,146.14 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,686.80 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2536.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,166.01 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2536.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,706.79 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $2536.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2205 | $1.15 | $28.44 | — | $142.59 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2536.54 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.59 | ▲ close $10,520.35 vs 09:30 $10,146.14 (session +409.01) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.59 | ▲ 09:30 equity $11,001.98 vs yday $10,520.35 (+481.63) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $107.84 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $47.53 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 4 | $11.13 | $0.46 | — | $62.86 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $47.53 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.86 | ▲ close $11,367.73 vs 09:30 $11,001.98 (session +366.56) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.86 | ▲ 09:30 equity $12,330.89 vs yday $11,367.73 (+963.16) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BHP` | 27 | $97.31 | $2.10 | $+165.93 | $2,688.13 | ▲ +165.93 after sell → book $12,328.79; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `KGC` | 85 | $33.03 | $2.28 | $+284.47 | $5,493.40 | ▲ +284.47 after sell → book $12,326.51; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 17 | $159.50 | $2.07 | $+250.21 | $8,202.83 | ▲ +250.21 after sell → book $12,324.44; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2205 | $1.83 | $28.84 | $+1442.11 | $12,209.14 | ▲ +1,442.11 after sell → book $12,295.60; vs 09:30 mark -28.84 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,209.14 | ▲ close $12,299.64 vs 09:30 $12,330.89 (session +4.04) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,209.14 | ▼ 09:30 equity $12,298.88 vs yday $12,299.64 (-0.76) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AUPH` | 2 | $16.63 | $0.36 | $-1.85 | $12,242.04 | ▼ -1.85 after sell → book $12,298.52; vs 09:30 mark -0.36 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **SELL** | `ARCT` | 4 | $14.12 | $0.60 | $+10.91 | $12,297.92 | ▲ +10.91 after sell → book $12,297.92; vs 09:30 mark -0.60 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 368 | $8.35 | $4.75 | — | $9,220.37 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $3074.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 1886 | $1.63 | $24.33 | — | $6,121.86 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $3074.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1970 | $1.56 | $25.41 | — | $3,023.25 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3074.48 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 86 | $35.05 | $2.25 | — | $6.70 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3074.48 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.70 | ▲ close $12,680.14 vs 09:30 $12,298.88 (session +438.96) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.70 | ▲ 09:30 equity $12,703.63 vs yday $12,680.14 (+23.49) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.70 | ▼ close $12,445.78 vs 09:30 $12,703.63 (session -257.85) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.70 | ▲ 09:30 equity $12,741.16 vs yday $12,445.78 (+295.38) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 368 | $8.49 | $4.83 | $+41.94 | $3,126.19 | ▲ +41.94 after sell → book $12,736.33; vs 09:30 mark -4.83 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 1886 | $1.74 | $24.67 | $+158.46 | $6,383.16 | ▲ +158.46 after sell → book $12,711.66; vs 09:30 mark -24.67 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1970 | $1.75 | $25.77 | $+323.12 | $9,804.89 | ▲ +323.12 after sell → book $12,685.89; vs 09:30 mark -25.77 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 86 | $33.50 | $2.29 | $-137.83 | $12,683.61 | ▼ -137.83 after sell → book $12,683.61; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,683.61 | ▲ close $12,683.61 vs 09:30 $12,741.16 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,683.61 | ▲ 09:30 equity $12,683.61 vs yday $12,683.61 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 22 | $141.76 | $2.06 | — | $9,562.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $3170.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 25 | $122.81 | $2.06 | — | $6,490.52 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $3170.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,568.81 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $3170.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 34 | $91.49 | $2.09 | — | $456.06 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $3170.90 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.06 | ▼ close $12,201.20 vs 09:30 $12,683.61 (session -474.18) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $456.06 | ▲ 09:30 equity $12,279.08 vs yday $12,201.20 (+77.88) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.06 | ▲ close $12,303.52 vs 09:30 $12,279.08 (session +24.44) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $456.06 | ▼ 09:30 equity $12,085.87 vs yday $12,303.52 (-217.65) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SMTC` | 22 | $127.63 | $2.09 | $-315.00 | $3,261.83 | ▼ -315.00 after sell → book $12,083.78; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `TTMI` | 25 | $116.68 | $2.10 | $-157.41 | $6,176.73 | ▼ -157.41 after sell → book $12,081.68; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `KEYS` | 9 | $321.47 | $2.05 | $-30.53 | $9,067.91 | ▼ -30.53 after sell → book $12,079.63; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `AVT` | 34 | $88.58 | $2.13 | $-103.16 | $12,077.50 | ▼ -103.16 after sell → book $12,077.50; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,077.50 | ▲ close $12,077.50 vs 09:30 $12,085.87 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,077.50 | ▲ 09:30 equity $12,077.50 vs yday $12,077.50 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,077.50 | ▲ close $12,077.50 vs 09:30 $12,077.50 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,077.50 | ▲ 09:30 equity $12,077.50 vs yday $12,077.50 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 57 | $52.88 | $2.16 | — | $9,061.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3019.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 70 | $42.93 | $2.20 | — | $6,053.88 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $3019.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 831 | $3.63 | $10.72 | — | $3,026.63 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $3019.38 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 376 | $8.03 | $4.85 | — | $2.50 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $3019.38 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.50 | ▼ close $11,815.28 vs 09:30 $12,077.50 (session -242.29) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.50 | ▼ 09:30 equity $11,722.63 vs yday $11,815.28 (-92.65) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.50 | ▲ close $11,863.41 vs 09:30 $11,722.63 (session +140.78) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.50 | ▲ 09:30 equity $11,985.70 vs yday $11,863.41 (+122.29) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 57 | $54.31 | $2.20 | $+77.15 | $3,095.98 | ▲ +77.15 after sell → book $11,983.51; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 70 | $42.20 | $2.24 | $-55.54 | $6,047.74 | ▼ -55.54 after sell → book $11,981.27; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 831 | $3.43 | $10.88 | $-187.80 | $8,887.19 | ▼ -187.80 after sell → book $11,970.39; vs 09:30 mark -10.88 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `VSTM` | 376 | $8.20 | $4.94 | $+54.13 | $11,965.45 | ▲ +54.13 after sell → book $11,965.45; vs 09:30 mark -4.94 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.45 | ▲ close $11,965.45 vs 09:30 $11,985.70 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.45 | ▲ 09:30 equity $11,965.45 vs yday $11,965.45 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.45 | ▲ close $11,965.45 vs 09:30 $11,965.45 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.45 | ▲ 09:30 equity $11,965.45 vs yday $11,965.45 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.45 | ▲ close $11,965.45 vs 09:30 $11,965.45 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.45 | ▲ 09:30 equity $11,965.45 vs yday $11,965.45 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 113 | $52.55 | $2.33 | — | $6,024.98 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $5982.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 326 | $18.30 | $4.21 | — | $54.97 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $5982.73 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.97 | ▲ close $12,495.98 vs 09:30 $11,965.45 (session +537.06) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.97 | ▼ 09:30 equity $12,443.95 vs yday $12,495.98 (-52.03) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.97 | ▼ close $11,678.26 vs 09:30 $12,443.95 (session -765.69) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.97 | ▼ 09:30 equity $11,615.40 vs yday $11,678.26 (-62.86) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BAND` | 113 | $49.51 | $2.39 | $-348.24 | $5,647.21 | ▼ -348.24 after sell → book $11,613.01; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `PAYP` | 326 | $18.30 | $4.31 | $-8.51 | $11,608.70 | ▼ -8.51 after sell → book $11,608.70; vs 09:30 mark -4.31 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,608.70 | ▲ close $11,608.70 vs 09:30 $11,615.40 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,608.70 | ▲ 09:30 equity $11,608.70 vs yday $11,608.70 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 64 | $89.38 | $2.18 | — | $5,886.20 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5804.35 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 49 | $118.18 | $2.14 | — | $93.24 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5804.35 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.24 | ▼ close $11,155.53 vs 09:30 $11,608.70 (session -448.85) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.24 | ▲ 09:30 equity $11,275.98 vs yday $11,155.53 (+120.45) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.24 | ▲ close $11,793.71 vs 09:30 $11,275.98 (session +517.73) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.24 | ▲ 09:30 equity $11,901.68 vs yday $11,793.71 (+107.97) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SWKS` | 64 | $92.05 | $2.24 | $+166.46 | $5,982.20 | ▲ +166.46 after sell → book $11,899.44; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **SELL** | `QRVO` | 49 | $120.76 | $2.19 | $+122.09 | $11,897.25 | ▲ +122.09 after sell → book $11,897.25; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 27 | $108.55 | $2.07 | — | $8,964.33 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $2974.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 34 | $85.00 | $2.09 | — | $6,072.23 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $2974.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 86 | $34.44 | $2.25 | — | $3,108.15 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2974.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 201 | $14.79 | $2.60 | — | $132.76 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2974.31 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.76 | ▼ close $11,611.16 vs 09:30 $11,901.68 (session -277.08) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.76 | ▲ 09:30 equity $11,621.95 vs yday $11,611.16 (+10.79) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.76 | ▲ close $12,119.61 vs 09:30 $11,621.95 (session +497.66) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.76 | ▲ 09:30 equity $12,145.74 vs yday $12,119.61 (+26.13) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `RARE` | 201 | $14.78 | $2.66 | $-7.26 | $3,100.88 | ▼ -7.26 after sell → book $12,143.08; vs 09:30 mark -2.66 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,100.88 | ▲ close $12,143.08 vs 09:30 $12,145.74 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,100.88 | ▼ 09:30 equity $12,119.83 vs yday $12,143.08 (-23.25) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RBRK` | 27 | $112.46 | $2.11 | $+101.39 | $6,135.20 | ▲ +101.39 after sell → book $12,117.73; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `ECO` | 34 | $77.55 | $2.12 | $-257.52 | $8,769.77 | ▼ -257.52 after sell → book $12,115.60; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `FIVN` | 86 | $38.91 | $2.29 | $+379.45 | $12,113.32 | ▲ +379.45 after sell → book $12,113.32; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 18 | $166.54 | $2.04 | — | $9,113.55 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $3028.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 108 | $27.79 | $2.31 | — | $6,109.92 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $3028.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 308 | $9.81 | $3.97 | — | $3,084.46 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $3028.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 149 | $20.25 | $2.44 | — | $64.78 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $3028.33 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.78 | ▼ close $11,785.66 vs 09:30 $12,119.83 (session -316.89) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.78 | ▼ 09:30 equity $11,716.60 vs yday $11,785.66 (-69.06) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.78 | ▲ close $11,928.20 vs 09:30 $11,716.60 (session +211.60) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.88 | ▼ 09:30 equity $9,700.61 vs yday $9,709.71 (-9.10) | 09:30 open · cash $196.88 (unchanged overnight, no fees) · equity $9,700.61 vs prior close $9,709.71 (-9.10) · 4 name(s) re-marked at the open (per-name table). A×14 yday $172.84 → 09:30 $171.98 -12.04; ARQT×88 yday $26.27 → 09:30 $26.27 +0.00; DXCM×27 yday $87.47 → 09:30 $87.47 +0.00; HALO×21 yday $115.22 → 09:30 $115.36 +2.94 | — |
| 2026-09-25 09:30 ET | **SELL** | `A` | 14 | $171.98 | $2.06 | $+72.07 | $2,602.54 | ▲ +72.07 after sell → book $9,698.55; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 2) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 113 | $7.65 | $2.33 | — | $1,735.76 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $867.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 10 | $83.76 | $2.02 | — | $896.14 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $867.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 10 | $83.69 | $2.02 | — | $57.17 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $867.51 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.17 | ▼ close $9,688.52 vs 09:30 $9,700.61 (session -3.66) | 16:00 close · cash $57.17 · equity $9,688.52 vs 09:30 $9,700.61 (-12.09; session marks -3.66) · 6 name(s) marked open→close (per-name table). ARQT×88 09:30 $26.27 → close $26.27 +0.00; DXCM×27 09:30 $87.47 → close $87.47 +0.00; HALO×21 09:30 $115.36 → close $113.90 -30.66; MRVI×113 09:30 $7.65 → close $7.60 -5.65; TXG×10 09:30 $83.76 → close $85.71 +19.50; TEM×10 09:30 $83.69 → close $85.01 +13.15 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 17.75 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 17.75 < 1 share @ 503.50 |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `AAOI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 47.53 < 1 share @ 216.30 |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 0.63 < 1 share @ 263.36 |
| 2026-09-04 | `DELL` | cash | leftover split 0.63 < 1 share @ 513.78 |
| 2026-09-04 | `IRD` | cash | leftover split 0.63 < 1 share @ 4.53 |
| 2026-09-04 | `LENZ` | cash | leftover split 0.63 < 1 share @ 5.75 |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `RBRK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ECO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open — carry |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/2 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `A` | 18 | 2026-09-23 @ $166.54 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $3028.33 |
| `ARQT` | 108 | 2026-09-23 @ $27.79 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $3028.33 |
| `ADMA` | 308 | 2026-09-23 @ $9.81 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $3028.33 |
| `FTRE` | 149 | 2026-09-23 @ $20.25 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $3028.33 |
