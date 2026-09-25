# Factor mine action — `union_white_both_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **-4.77%** ($9,523) · signal-only (no cash/fees) was +6.63%. Starts YES **7/30**. Fills 94 · skips 0 · realized $+1337.35.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,337.32.

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
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 83 | $59.65 | $2.29 | $-16.98 | $5,019.66 | ▼ -16.98 after sell → book $10,438.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 98 | $55.29 | $2.34 | $+452.72 | $10,435.74 | ▲ +452.72 after sell → book $10,435.74; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 45 | $57.61 | $2.12 | — | $7,841.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.7; leftover $2608.93 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 176 | $14.80 | $2.52 | — | $5,233.84 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $2608.93 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 605 | $4.31 | $7.80 | — | $2,618.49 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2608.93 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 5 | $503.50 | $2.00 | — | $98.98 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable; 🔵; ⚪; ret5=+7.9; leftover $2608.93 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.98 | ▼ close $10,227.36 vs 09:30 $10,440.37 (session -193.92) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.98 | ▲ 09:30 equity $10,407.20 vs yday $10,227.36 (+179.84) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 45 | $55.37 | $2.15 | $-105.08 | $2,588.48 | ▼ -105.08 after sell → book $10,405.05; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 176 | $13.67 | $2.57 | $-203.96 | $4,991.83 | ▼ -203.96 after sell → book $10,402.48; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 605 | $4.60 | $7.93 | $+159.72 | $7,766.91 | ▲ +159.72 after sell → book $10,394.56; vs 09:30 mark -7.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 5 | $525.53 | $2.04 | $+106.11 | $10,392.52 | ▲ +106.11 after sell → book $10,392.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 284 | $9.12 | $3.66 | — | $7,798.78 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $2598.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 160 | $16.20 | $2.47 | — | $5,204.31 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2598.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 236 | $10.97 | $3.04 | — | $2,612.34 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $2598.13 | — |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 17 | $152.64 | $2.04 | — | $15.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $2598.13 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.42 | ▼ close $10,301.19 vs 09:30 $10,407.20 (session -80.11) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.42 | ▼ 09:30 equity $10,023.30 vs yday $10,301.19 (-277.89) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 284 | $9.03 | $3.73 | $-32.95 | $2,576.21 | ▼ -32.95 after sell → book $10,019.57; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 160 | $15.78 | $2.52 | $-72.19 | $5,098.49 | ▼ -72.19 after sell → book $10,017.05; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 236 | $10.31 | $3.10 | $-161.91 | $7,528.55 | ▼ -161.91 after sell → book $10,013.95; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 17 | $146.20 | $2.07 | $-113.59 | $10,011.88 | ▼ -113.59 after sell → book $10,011.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,011.88 | ▲ close $10,011.88 vs 09:30 $10,023.30 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,011.88 | ▲ 09:30 equity $10,011.88 vs yday $10,011.88 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,011.88 | ▲ close $10,011.88 vs 09:30 $10,011.88 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,011.88 | ▲ 09:30 equity $10,011.88 vs yday $10,011.88 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,552.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2502.97 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 84 | $29.63 | $2.24 | — | $5,061.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2502.97 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,602.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $2502.97 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2176 | $1.15 | $28.07 | — | $71.68 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2502.97 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.68 | ▲ close $10,383.50 vs 09:30 $10,011.88 (session +406.05) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.68 | ▲ 09:30 equity $10,860.62 vs yday $10,383.50 (+477.12) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 27 | $95.72 | $2.10 | $+123.00 | $2,654.02 | ▲ +123.00 after sell → book $10,858.52; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 84 | $32.17 | $2.28 | $+208.84 | $5,354.03 | ▲ +208.84 after sell → book $10,856.25; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 17 | $154.70 | $2.07 | $+168.61 | $7,981.85 | ▲ +168.61 after sell → book $10,854.17; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 154 | $17.20 | $2.45 | — | $5,330.60 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $2660.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 12 | $216.30 | $2.03 | — | $2,732.98 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $2660.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 239 | $11.13 | $3.08 | — | $69.82 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $2660.62 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.82 | ▲ close $11,531.11 vs 09:30 $10,860.62 (session +684.50) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.82 | ▲ 09:30 equity $12,393.91 vs yday $11,531.11 (+862.80) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2176 | $1.83 | $28.46 | $+1423.15 | $4,023.44 | ▲ +1,423.15 after sell → book $12,365.45; vs 09:30 mark -28.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 154 | $16.57 | $2.50 | $-101.97 | $6,572.72 | ▼ -101.97 after sell → book $12,362.95; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 12 | $217.03 | $2.06 | $+4.68 | $9,175.02 | ▲ +4.68 after sell → book $12,360.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 239 | $13.33 | $3.15 | $+519.57 | $12,357.75 | ▲ +519.57 after sell → book $12,357.75; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,357.75 | ▲ close $12,357.75 vs 09:30 $12,393.91 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,357.75 | ▲ 09:30 equity $12,357.75 vs yday $12,357.75 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 369 | $8.35 | $4.76 | — | $9,271.84 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $3089.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 1895 | $1.63 | $24.45 | — | $6,158.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $3089.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1980 | $1.56 | $25.54 | — | $3,044.20 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3089.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 86 | $35.05 | $2.25 | — | $27.65 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3089.44 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.65 | ▲ close $12,741.62 vs 09:30 $12,357.75 (session +440.87) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.65 | ▲ 09:30 equity $12,764.98 vs yday $12,741.62 (+23.36) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 369 | $8.60 | $4.85 | $+82.64 | $3,196.20 | ▲ +82.64 after sell → book $12,760.13; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 1895 | $1.75 | $24.79 | $+187.64 | $6,497.14 | ▲ +187.64 after sell → book $12,735.34; vs 09:30 mark -24.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1980 | $1.60 | $25.90 | $+27.76 | $9,639.25 | ▲ +27.76 after sell → book $12,709.45; vs 09:30 mark -25.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 86 | $35.70 | $2.29 | $+51.37 | $12,707.16 | ▲ +51.37 after sell → book $12,707.16; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,707.16 | ▲ close $12,707.16 vs 09:30 $12,764.98 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,707.16 | ▲ 09:30 equity $12,707.16 vs yday $12,707.16 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,707.16 | ▲ close $12,707.16 vs 09:30 $12,707.16 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,707.16 | ▲ 09:30 equity $12,707.16 vs yday $12,707.16 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 22 | $141.76 | $2.06 | — | $9,586.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $3176.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 25 | $122.81 | $2.06 | — | $6,514.07 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $3176.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,592.36 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $3176.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 34 | $91.49 | $2.09 | — | $479.61 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $3176.79 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.61 | ▼ close $12,224.75 vs 09:30 $12,707.16 (session -474.18) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.61 | ▲ 09:30 equity $12,302.63 vs yday $12,224.75 (+77.88) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 22 | $132.30 | $2.09 | $-212.27 | $3,388.12 | ▼ -212.27 after sell → book $12,300.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 25 | $118.83 | $2.10 | $-103.66 | $6,356.77 | ▼ -103.66 after sell → book $12,298.44; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 9 | $322.49 | $2.05 | $-21.35 | $9,257.13 | ▼ -21.35 after sell → book $12,296.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 34 | $89.39 | $2.13 | $-75.62 | $12,294.26 | ▼ -75.62 after sell → book $12,294.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,294.26 | ▲ close $12,294.26 vs 09:30 $12,302.63 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,294.26 | ▲ 09:30 equity $12,294.26 vs yday $12,294.26 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,294.26 | ▲ close $12,294.26 vs 09:30 $12,294.26 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,294.26 | ▲ 09:30 equity $12,294.26 vs yday $12,294.26 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,294.26 | ▲ close $12,294.26 vs 09:30 $12,294.26 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,294.26 | ▲ 09:30 equity $12,294.26 vs yday $12,294.26 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 58 | $52.88 | $2.16 | — | $9,225.06 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3073.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 71 | $42.93 | $2.20 | — | $6,174.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $3073.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 846 | $3.63 | $10.91 | — | $3,092.93 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $3073.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 382 | $8.03 | $4.93 | — | $20.55 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $3073.57 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.55 | ▼ close $12,027.73 vs 09:30 $12,294.26 (session -246.33) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.55 | ▼ 09:30 equity $11,933.57 vs yday $12,027.73 (-94.16) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 58 | $52.03 | $2.20 | $-53.66 | $3,036.09 | ▼ -53.66 after sell → book $11,931.37; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 71 | $41.50 | $2.24 | $-105.97 | $5,980.35 | ▼ -105.97 after sell → book $11,929.13; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 846 | $3.46 | $11.08 | $-165.81 | $8,896.43 | ▼ -165.81 after sell → book $11,918.05; vs 09:30 mark -11.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 382 | $7.91 | $5.02 | $-55.78 | $11,913.04 | ▼ -55.78 after sell → book $11,913.04; vs 09:30 mark -5.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 11 | $263.36 | $2.02 | — | $9,014.05 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2978.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 5 | $513.78 | $2.00 | — | $6,443.15 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $2978.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 657 | $4.53 | $8.48 | — | $3,458.46 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $2978.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 517 | $5.75 | $6.67 | — | $479.04 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $2978.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.04 | ▲ close $12,100.78 vs 09:30 $11,933.57 (session +206.92) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.04 | ▼ 09:30 equity $11,928.07 vs yday $12,100.78 (-172.71) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 11 | $253.72 | $2.06 | $-110.12 | $3,267.91 | ▼ -110.12 after sell → book $11,926.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 5 | $521.15 | $2.04 | $+32.81 | $5,871.62 | ▲ +32.81 after sell → book $11,923.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 657 | $4.53 | $8.61 | $-17.08 | $8,839.23 | ▼ -17.08 after sell → book $11,915.38; vs 09:30 mark -8.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 517 | $5.95 | $6.78 | $+89.95 | $11,908.60 | ▲ +89.95 after sell → book $11,908.60; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,908.60 | ▲ close $11,908.60 vs 09:30 $11,928.07 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,908.60 | ▲ 09:30 equity $11,908.60 vs yday $11,908.60 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,908.60 | ▲ close $11,908.60 vs 09:30 $11,908.60 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,908.60 | ▲ 09:30 equity $11,908.60 vs yday $11,908.60 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,908.60 | ▲ close $11,908.60 vs 09:30 $11,908.60 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,908.60 | ▲ 09:30 equity $11,908.60 vs yday $11,908.60 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 113 | $52.55 | $2.33 | — | $5,968.12 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $5954.30 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 325 | $18.30 | $4.19 | — | $16.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $5954.30 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.42 | ▲ close $12,438.98 vs 09:30 $11,908.60 (session +536.91) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.42 | ▼ 09:30 equity $12,387.12 vs yday $12,438.98 (-51.86) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 113 | $56.90 | $2.40 | $+486.82 | $6,443.72 | ▲ +486.82 after sell → book $12,384.72; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 325 | $18.28 | $4.29 | $-14.99 | $12,380.43 | ▼ -14.99 after sell → book $12,380.43; vs 09:30 mark -4.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,380.43 | ▲ close $12,380.43 vs 09:30 $12,387.12 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,380.43 | ▲ 09:30 equity $12,380.43 vs yday $12,380.43 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,380.43 | ▲ close $12,380.43 vs 09:30 $12,380.43 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,380.43 | ▲ 09:30 equity $12,380.43 vs yday $12,380.43 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 69 | $89.38 | $2.20 | — | $6,211.01 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $6190.22 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 52 | $118.18 | $2.15 | — | $63.51 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $6190.22 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.51 | ▼ close $11,895.66 vs 09:30 $12,380.43 (session -480.43) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.51 | ▲ 09:30 equity $12,024.75 vs yday $11,895.66 (+129.09) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 69 | $86.76 | $2.26 | $-185.23 | $6,047.69 | ▼ -185.23 after sell → book $12,022.49; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 52 | $114.90 | $2.20 | $-174.91 | $12,020.29 | ▼ -174.91 after sell → book $12,020.29; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,020.29 | ▲ close $12,020.29 vs 09:30 $12,024.75 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,020.29 | ▲ 09:30 equity $12,020.29 vs yday $12,020.29 (-0.00) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 27 | $108.55 | $2.07 | — | $9,087.37 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $3005.07 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 35 | $85.00 | $2.10 | — | $6,110.27 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $3005.07 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 87 | $34.44 | $2.25 | — | $3,111.74 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $3005.07 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 203 | $14.79 | $2.62 | — | $106.75 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $3005.07 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.75 | ▼ close $11,731.59 vs 09:30 $12,020.29 (session -279.66) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.75 | ▲ 09:30 equity $11,740.93 vs yday $11,731.59 (+9.34) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 27 | $107.57 | $2.10 | $-30.64 | $3,009.04 | ▼ -30.64 after sell → book $11,738.83; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 35 | $82.83 | $2.13 | $-80.17 | $5,905.96 | ▼ -80.17 after sell → book $11,736.70; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 87 | $33.00 | $2.29 | $-129.82 | $8,774.67 | ▼ -129.82 after sell → book $11,734.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 203 | $14.58 | $2.68 | $-47.92 | $11,731.74 | ▼ -47.92 after sell → book $11,731.74; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,731.74 | ▲ close $11,731.74 vs 09:30 $11,740.93 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,731.74 | ▲ 09:30 equity $11,731.74 vs yday $11,731.74 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,731.74 | ▲ close $11,731.74 vs 09:30 $11,731.74 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,731.74 | ▲ 09:30 equity $11,731.74 vs yday $11,731.74 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 17 | $166.54 | $2.04 | — | $8,898.51 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $2932.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 105 | $27.79 | $2.31 | — | $5,978.26 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $2932.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 298 | $9.81 | $3.84 | — | $3,051.03 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $2932.93 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 144 | $20.25 | $2.42 | — | $132.61 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $2932.93 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.61 | ▼ close $11,414.43 vs 09:30 $11,731.74 (session -306.69) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.61 | ▼ 09:30 equity $11,348.12 vs yday $11,414.43 (-66.31) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 17 | $163.95 | $2.07 | $-48.14 | $2,917.69 | ▼ -48.14 after sell → book $11,346.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 105 | $26.22 | $2.34 | $-169.50 | $5,668.45 | ▼ -169.50 after sell → book $11,343.71; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 298 | $9.67 | $3.92 | $-49.48 | $8,546.19 | ▼ -49.48 after sell → book $11,339.79; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 144 | $19.40 | $2.47 | $-127.29 | $11,337.32 | ▼ -127.29 after sell → book $11,337.32; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,337.32 | ▲ close $11,337.32 vs 09:30 $11,348.12 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,486.40 | ▲ 09:30 equity $9,486.40 vs yday $9,486.40 (+0.00) | 09:30 open · cash $9,486.40 · no holdings · equity $9,486.40 vs prior close $9,486.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 20 | $115.36 | $2.05 | — | $7,177.15 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.1; leftover $2371.60 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 310 | $7.65 | $4.00 | — | $4,801.65 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $2371.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 28 | $83.76 | $2.07 | — | $2,454.30 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2371.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 28 | $83.69 | $2.07 | — | $108.76 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $2371.60 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.76 | ▲ close $9,522.92 vs 09:30 $9,486.40 (session +46.72) | 16:00 close · cash $108.76 · equity $9,522.92 vs 09:30 $9,486.40 (+36.52; session marks +46.72) · 4 name(s) marked open→close (per-name table). HALO×20 09:30 $115.36 → close $113.90 -29.20; MRVI×310 09:30 $7.65 → close $7.60 -15.50; TXG×28 09:30 $83.76 → close $85.71 +54.60; TEM×28 09:30 $83.69 → close $85.01 +36.82 | — |
