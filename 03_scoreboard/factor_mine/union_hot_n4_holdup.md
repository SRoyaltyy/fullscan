# Factor mine action — `union_hot_n4_holdup`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `holdup` · hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book

Cash book **+54.26%** ($15,426) · signal-only (no cash/fees) was +89.81%. Starts YES **29/30**. Fills 92 · skips 79 · realized $+4974.44.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Sort the keepers by how hot the prior tape looked and keep the top 4.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On an UP morning (S > 0), each new long lot stays through the next 09:30 so the overnight gap is marked. Highly positive days in this window were mostly that gap; a same-day 09:30→16:00 book cannot harvest them.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4 (S≥+5 may raise this when S-boost is `holdup`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,246.19.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,771.94 vs 09:30 $10,412.10 (session +359.84) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▼ 09:30 equity $10,732.24 vs yday $10,771.94 (-39.70) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `IREN` | 54 | $45.23 | $2.18 | $-44.83 | $2,440.78 | ▼ -44.83 after sell → book $10,730.06; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TNDM` | 107 | $22.50 | $2.35 | $-93.47 | $4,845.93 | ▼ -93.47 after sell → book $10,727.71; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TPG` | 49 | $52.67 | $2.17 | $+95.99 | $7,424.59 | ▲ +95.99 after sell → book $10,725.54; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 3085 | $1.07 | $40.34 | $+727.52 | $10,685.21 | ▲ +727.52 after sell → book $10,685.21; vs 09:30 mark -40.33 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 637 | $4.19 | $8.22 | — | $8,007.96 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2671.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 388 | $6.87 | $5.01 | — | $5,337.40 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2671.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 195 | $13.64 | $2.58 | — | $2,675.02 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2671.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 64 | $41.23 | $2.18 | — | $34.12 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2671.30 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▲ close $10,695.00 vs 09:30 $10,732.24 (session +27.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▲ 09:30 equity $10,705.35 vs yday $10,695.00 (+10.35) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▲ close $10,781.24 vs 09:30 $10,705.35 (session +75.89) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▲ 09:30 equity $10,929.21 vs yday $10,781.24 (+147.97) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `XHG` | 637 | $4.32 | $8.35 | $+66.25 | $2,777.61 | ▲ +66.25 after sell → book $10,920.86; vs 09:30 mark -8.35 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 388 | $7.19 | $5.09 | $+114.06 | $5,562.24 | ▲ +114.06 after sell → book $10,915.77; vs 09:30 mark -5.09 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `STDN` | 195 | $12.35 | $2.63 | $-256.75 | $7,967.86 | ▼ -256.75 after sell → book $10,913.14; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `HTFL` | 64 | $46.02 | $2.22 | $+302.16 | $10,910.93 | ▲ +302.16 after sell → book $10,910.93; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,910.93 | ▲ close $10,910.93 vs 09:30 $10,929.21 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,910.93 | ▲ 09:30 equity $10,910.93 vs yday $10,910.93 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 18 | $150.14 | $2.04 | — | $8,206.36 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2727.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2371 | $1.15 | $30.59 | — | $5,449.13 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2727.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 230 | $11.81 | $2.97 | — | $2,728.71 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2727.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1973 | $1.37 | $25.45 | — | $0.25 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2727.73 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▼ close $10,723.72 vs 09:30 $10,910.93 (session -126.16) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $11,067.63 vs yday $10,723.72 (+343.91) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▲ close $11,443.86 vs 09:30 $11,067.63 (session +376.23) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $12,311.46 vs yday $11,443.86 (+867.60) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 18 | $142.70 | $2.07 | $-138.04 | $2,566.78 | ▼ -138.04 after sell → book $12,309.39; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2371 | $1.83 | $31.01 | $+1550.68 | $6,874.69 | ▲ +1,550.68 after sell → book $12,278.37; vs 09:30 mark -31.02 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABCL` | 230 | $10.97 | $3.03 | $-200.34 | $9,394.77 | ▼ -200.34 after sell → book $12,275.35; vs 09:30 mark -3.02 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `AZI` | 1973 | $1.46 | $25.80 | $+126.32 | $12,249.54 | ▲ +126.32 after sell → book $12,249.54; vs 09:30 mark -25.81 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,249.54 | ▲ close $12,249.54 vs 09:30 $12,311.46 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,249.54 | ▲ 09:30 equity $12,249.54 vs yday $12,249.54 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 127 | $24.11 | $2.37 | — | $9,185.20 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ret5=+891.7; leftover $3062.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1963 | $1.56 | $25.32 | — | $6,097.60 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3062.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 752 | $4.07 | $9.70 | — | $3,027.26 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $3062.39 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 158 | $19.04 | $2.46 | — | $16.47 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $3062.39 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.47 | ▲ close $13,249.06 vs 09:30 $12,249.54 (session +1,039.38) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▼ 09:30 equity $12,675.62 vs yday $13,249.06 (-573.44) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.47 | ▲ close $13,043.21 vs 09:30 $12,675.62 (session +367.59) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▲ 09:30 equity $13,342.51 vs yday $13,043.21 (+299.30) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 127 | $25.91 | $2.42 | $+223.81 | $3,304.63 | ▲ +223.81 after sell → book $13,340.10; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1963 | $1.75 | $25.68 | $+321.97 | $6,714.20 | ▲ +321.97 after sell → book $13,314.42; vs 09:30 mark -25.68 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 158 | $22.45 | $2.52 | $+533.80 | $10,258.78 | ▲ +533.80 after sell → book $13,311.90; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 372 | $9.19 | $4.80 | — | $6,835.30 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $3419.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 23 | $144.18 | $2.06 | — | $3,517.10 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $3419.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 184 | $18.50 | $2.54 | — | $110.56 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $3419.59 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.56 | ▲ close $13,306.19 vs 09:30 $13,342.51 (session +3.69) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.56 | ▼ 09:30 equity $12,999.97 vs yday $13,306.19 (-306.22) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 752 | $3.69 | $9.85 | $-305.31 | $2,875.59 | ▼ -305.31 after sell → book $12,990.12; vs 09:30 mark -9.85 | dropped from list after 3 sess (min 2) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 184 | $18.15 | $2.60 | $-69.54 | $6,212.59 | ▼ -69.54 after sell → book $12,987.52; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 221 | $14.00 | $2.85 | — | $3,115.74 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $3106.30 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 21 | $146.07 | $2.05 | — | $46.22 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $3106.30 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.22 | ▼ close $12,967.35 vs 09:30 $12,999.97 (session -15.27) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.22 | ▼ 09:30 equity $12,825.16 vs yday $12,967.35 (-142.19) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 372 | $9.50 | $4.89 | $+105.63 | $3,575.33 | ▲ +105.63 after sell → book $12,820.27; vs 09:30 mark -4.89 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 23 | $134.10 | $2.09 | $-235.99 | $6,657.54 | ▼ -235.99 after sell → book $12,818.18; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,657.54 | ▼ close $12,601.52 vs 09:30 $12,825.16 (session -216.66) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,657.54 | ▼ 09:30 equity $12,521.38 vs yday $12,601.52 (-80.14) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 221 | $13.04 | $2.91 | $-217.92 | $9,536.47 | ▼ -217.92 after sell → book $12,518.47; vs 09:30 mark -2.91 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `ANF` | 21 | $142.00 | $2.09 | $-89.61 | $12,516.38 | ▼ -89.61 after sell → book $12,516.38; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,516.38 | ▲ close $12,516.38 vs 09:30 $12,521.38 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,516.38 | ▲ 09:30 equity $12,516.38 vs yday $12,516.38 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,516.38 | ▲ close $12,516.38 vs 09:30 $12,516.38 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,516.38 | ▲ 09:30 equity $12,516.38 vs yday $12,516.38 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1757 | $1.78 | $22.67 | — | $9,366.26 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $3129.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 170 | $18.40 | $2.50 | — | $6,235.76 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $3129.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 228 | $13.71 | $2.94 | — | $3,106.93 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $3129.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 130 | $23.88 | $2.38 | — | $0.15 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $3129.10 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▼ close $11,825.10 vs 09:30 $12,516.38 (session -660.79) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▲ 09:30 equity $11,952.13 vs yday $11,825.10 (+127.03) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 170 | $18.15 | $2.55 | $-47.55 | $3,083.10 | ▼ -47.55 after sell → book $11,949.58; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 228 | $13.89 | $3.00 | $+35.09 | $6,247.02 | ▲ +35.09 after sell → book $11,946.58; vs 09:30 mark -3.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 130 | $23.84 | $2.43 | $-10.01 | $9,343.79 | ▼ -10.01 after sell → book $11,944.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 123 | $25.18 | $2.36 | — | $6,244.29 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $3114.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 395 | $7.87 | $5.10 | — | $3,130.55 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $3114.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 537 | $5.79 | $6.93 | — | $14.39 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $3114.60 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.39 | ▲ close $12,624.05 vs 09:30 $11,952.13 (session +694.28) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.39 | ▼ 09:30 equity $12,201.38 vs yday $12,624.05 (-422.67) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1757 | $1.56 | $22.98 | $-423.40 | $2,741.12 | ▼ -423.40 after sell → book $12,178.41; vs 09:30 mark -22.97 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,741.12 | ▲ close $12,304.38 vs 09:30 $12,201.38 (session +125.97) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,741.12 | ▲ 09:30 equity $12,581.81 vs yday $12,304.38 (+277.43) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ASST` | 123 | $28.00 | $2.41 | $+342.09 | $6,182.71 | ▲ +342.09 after sell → book $12,579.40; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `USDE` | 395 | $8.01 | $5.19 | $+45.02 | $9,341.47 | ▲ +45.02 after sell → book $12,574.21; vs 09:30 mark -5.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `DFDV` | 537 | $6.02 | $7.04 | $+109.54 | $12,567.17 | ▲ +109.54 after sell → book $12,567.17; vs 09:30 mark -7.04 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,567.17 | ▲ close $12,567.17 vs 09:30 $12,581.81 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,567.17 | ▲ 09:30 equity $12,567.17 vs yday $12,567.17 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,567.17 | ▲ close $12,567.17 vs 09:30 $12,567.17 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,567.17 | ▲ 09:30 equity $12,567.17 vs yday $12,567.17 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 1163 | $2.70 | $15.00 | — | $9,412.07 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $3141.79 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 639 | $4.91 | $8.24 | — | $6,266.33 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $3141.79 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 510 | $6.16 | $6.58 | — | $3,118.15 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $3141.79 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 992 | $3.13 | $12.80 | — | $0.40 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $3141.79 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.40 | ▲ close $12,846.47 vs 09:30 $12,567.17 (session +321.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.40 | ▲ 09:30 equity $13,023.09 vs yday $12,846.47 (+176.62) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.40 | ▲ close $13,675.33 vs 09:30 $13,023.09 (session +652.24) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.40 | ▲ 09:30 equity $13,860.17 vs yday $13,675.33 (+184.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 639 | $5.11 | $8.38 | $+111.18 | $3,257.31 | ▲ +111.18 after sell → book $13,851.79; vs 09:30 mark -8.38 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `IRD` | 510 | $5.94 | $6.69 | $-125.47 | $6,280.02 | ▼ -125.47 after sell → book $13,845.10; vs 09:30 mark -6.69 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 992 | $3.64 | $12.99 | $+480.13 | $9,877.91 | ▲ +480.13 after sell → book $13,832.11; vs 09:30 mark -12.99 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,877.91 | ▲ close $14,111.23 vs 09:30 $13,860.17 (session +279.12) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,877.91 | ▲ 09:30 equity $14,134.49 vs yday $14,111.23 (+23.26) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1829 | $1.80 | $23.59 | — | $6,562.12 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $3292.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 141 | $23.29 | $2.41 | — | $3,275.82 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $3292.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 223 | $14.62 | $2.88 | — | $12.68 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $3292.64 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.68 | ▼ close $14,092.74 vs 09:30 $14,134.49 (session -12.87) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.68 | ▲ 09:30 equity $14,158.88 vs yday $14,092.74 (+66.14) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.68 | ▲ close $15,694.91 vs 09:30 $14,158.88 (session +1,536.03) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.68 | ▼ 09:30 equity $15,315.58 vs yday $15,694.91 (-379.33) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1829 | $1.96 | $23.93 | $+245.12 | $3,573.59 | ▲ +245.12 after sell → book $15,291.65; vs 09:30 mark -23.93 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **SELL** | `SSL` | 223 | $13.93 | $2.94 | $-159.69 | $6,677.05 | ▼ -159.69 after sell → book $15,288.72; vs 09:30 mark -2.93 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1100 | $3.04 | $14.19 | — | $3,324.36 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $3338.52 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 40 | $81.40 | $2.11 | — | $66.25 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $3338.52 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.25 | ▲ close $15,360.32 vs 09:30 $15,315.58 (session +87.90) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.25 | ▲ 09:30 equity $15,907.73 vs yday $15,360.32 (+547.41) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 1163 | $3.55 | $15.23 | $+958.32 | $4,179.67 | ▲ +958.32 after sell → book $15,892.50; vs 09:30 mark -15.23 | dropped from list after 6 sess (min 2) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 141 | $29.43 | $2.47 | $+860.86 | $8,326.83 | ▲ +860.86 after sell → book $15,890.03; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 2) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 842 | $2.47 | $10.86 | — | $6,236.23 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $2081.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 123 | $16.91 | $2.36 | — | $4,153.94 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $2081.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1261 | $1.65 | $16.27 | — | $2,057.02 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $2081.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 176 | $11.67 | $2.52 | — | $0.58 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $2081.71 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.58 | ▼ close $15,417.61 vs 09:30 $15,907.73 (session -440.41) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.58 | ▲ 09:30 equity $15,441.97 vs yday $15,417.61 (+24.36) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 1100 | $3.51 | $14.40 | $+493.91 | $3,847.18 | ▲ +493.91 after sell → book $15,427.57; vs 09:30 mark -14.40 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 09:30 ET | **SELL** | `TEM` | 40 | $77.99 | $2.15 | $-140.66 | $6,964.63 | ▼ -140.66 after sell → book $15,425.42; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 254 | $9.11 | $3.28 | — | $4,647.42 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+44.4; leftover $2321.54 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 321 | $7.23 | $4.14 | — | $2,322.45 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $2321.54 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,322.45 | ▼ close $15,198.41 vs 09:30 $15,441.97 (session -219.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,322.45 | ▲ 09:30 equity $15,216.55 vs yday $15,198.41 (+18.14) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 123 | $16.92 | $2.40 | $-3.53 | $4,401.21 | ▼ -3.53 after sell → book $15,214.15; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 1261 | $1.41 | $16.49 | $-335.40 | $6,162.73 | ▼ -335.40 after sell → book $15,197.66; vs 09:30 mark -16.49 | dropped from list after 2 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 176 | $12.80 | $2.57 | $+193.80 | $8,412.96 | ▲ +193.80 after sell → book $15,195.09; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 254 | $8.39 | $3.34 | $-189.49 | $10,540.69 | ▼ -189.49 after sell → book $15,191.76; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 321 | $6.83 | $4.21 | $-136.75 | $12,728.91 | ▼ -136.75 after sell → book $15,187.55; vs 09:30 mark -4.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1571 | $2.70 | $20.27 | — | $8,466.94 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $4242.97 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 101 | $41.76 | $2.29 | — | $4,246.89 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $4242.97 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 943 | $4.49 | $12.16 | — | $0.65 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $4242.97 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.65 | ▼ close $14,802.08 vs 09:30 $15,216.55 (session -350.74) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.65 | ▼ 09:30 equity $14,652.49 vs yday $14,802.08 (-149.59) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 842 | $2.68 | $11.02 | $+154.94 | $2,246.19 | ▲ +154.94 after sell → book $14,641.47; vs 09:30 mark -11.02 | dropped from list after 3 sess (min 2) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,246.19 | ▲ close $18,097.07 vs 09:30 $14,652.49 (session +3,455.61) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,439.20 | ▲ 09:30 equity $16,090.79 vs yday $15,219.62 (+871.17) | 09:30 open · cash $2,439.20 (unchanged overnight, no fees) · equity $16,090.79 vs prior close $15,219.62 (+871.17) · 3 name(s) re-marked at the open (per-name table). GLND×1227 yday $5.35 → 09:30 $6.06 +871.17; VICR×12 yday $276.06 → 09:30 $276.06 +0.00; VKTX×79 yday $36.75 → 09:30 $36.75 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 27 | $29.76 | $2.07 | — | $1,633.61 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $813.07 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 50 | $16.21 | $2.14 | — | $820.97 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $813.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 52 | $15.58 | $2.15 | — | $8.61 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $813.07 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.61 | ▼ close $15,425.64 vs 09:30 $16,090.79 (session -658.80) | 16:00 close · cash $8.61 · equity $15,425.64 vs 09:30 $16,090.79 (-665.15; session marks -658.80) · 6 name(s) marked open→close (per-name table). GLND×1227 09:30 $6.06 → close $5.54 -638.04; VICR×12 09:30 $276.06 → close $276.06 -0.00; VKTX×79 09:30 $36.75 → close $36.75 +0.00; TJGC×27 09:30 $29.76 → close $26.24 -95.04; SECZ×50 09:30 $16.21 → close $15.96 -12.50; USDE×52 09:30 $15.58 → close $17.25 +86.78 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 0.13 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 0.13 < 1 share @ 19.57 |
| 2026-08-14 | `ZENA` | cash | leftover split 0.13 < 1 share @ 2.20 |
| 2026-08-14 | `AIRO` | cash | leftover split 0.13 < 1 share @ 11.12 |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `STDN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `HTFL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `XHG` | cash | leftover split 0.12 < 1 share @ 4.49 |
| 2026-08-21 | `CAPR` | cash | leftover split 0.12 < 1 share @ 6.81 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `BYND` | cash | leftover split 5.49 < 1 share @ 14.11 |
| 2026-08-26 | `USDE` | cash | leftover split 5.49 < 1 share @ 5.81 |
| 2026-08-26 | `PURR` | cash | leftover split 5.49 < 1 share @ 11.59 |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `USDE` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `IRD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `BBNX` | cash | leftover split 6.34 < 1 share @ 22.46 |
| 2026-09-17 | `FPS` | cash | leftover split 6.34 < 1 share @ 36.76 |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 1571 | 2026-09-23 @ $2.70 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $4242.97 |
| `VKTX` | 101 | 2026-09-23 @ $41.76 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $4242.97 |
| `SVIA` | 943 | 2026-09-23 @ $4.49 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $4242.97 |
