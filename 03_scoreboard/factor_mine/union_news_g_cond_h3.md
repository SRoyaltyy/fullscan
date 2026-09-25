# Factor mine action — `union_news_g_cond_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢, rank +G−R

Cash book **-20.82%** ($7,918) · signal-only (no cash/fees) was +177.97%. Starts YES **1/30**. Fills 164 · skips 246 · realized $-1849.04.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `news=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,739.91.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 5 | $46.18 | $2.00 | — | $1,099.83 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $955.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $750.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 5 | $49.00 | $2.00 | — | $503.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 2 | $92.99 | $1.87 | — | $316.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $266.55 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $316.08 | ▼ close $9,987.62 vs 09:30 $10,155.37 (session -158.46) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $316.08 | ▼ 09:30 equity $9,894.82 vs yday $9,987.62 (-92.80) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $316.08 | ▼ close $9,867.93 vs 09:30 $9,894.82 (session -26.89) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $316.08 | ▼ 09:30 equity $9,856.59 vs yday $9,867.93 (-11.34) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $1,526.39 | ▼ -30.89 after sell → book $9,854.30; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $2,757.73 | ▼ -3.75 after sell → book $9,852.10; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $3,952.36 | ▼ -54.24 after sell → book $9,849.81; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VELO` | 81 | $14.51 | $2.26 | $-74.96 | $5,125.41 | ▼ -74.96 after sell → book $9,847.55; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $6,285.37 | ▼ -42.06 after sell → book $9,845.51; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `S` | 52 | $22.37 | $2.17 | $-77.37 | $7,446.44 | ▼ -77.37 after sell → book $9,843.34; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,446.44 | ▼ close $9,767.02 vs 09:30 $9,856.59 (session -76.32) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,446.44 | ▲ 09:30 equity $9,770.75 vs yday $9,767.02 (+3.73) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $8,767.94 | ▲ +67.86 after sell → book $9,766.95; vs 09:30 mark -3.80 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 5 | $49.02 | $2.02 | $+10.17 | $9,011.02 | ▲ +10.17 after sell → book $9,764.93; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $9,160.93 | ▲ +5.71 after sell → book $9,763.39; vs 09:30 mark -1.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,372.43 | ▲ +6.80 after sell → book $9,761.38; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 5 | $40.63 | $2.02 | $-45.88 | $9,573.55 | ▼ -45.88 after sell → book $9,759.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CELC` | 2 | $92.90 | $1.88 | $-3.93 | $9,757.47 | ▼ -3.93 after sell → book $9,757.47; vs 09:30 mark -1.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,572.31 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $7,361.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 493 | $2.47 | $6.36 | — | $6,137.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $4,961.00 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 76 | $16.00 | $2.22 | — | $3,742.78 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,539.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $1,341.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1725 | $0.71 | $17.37 | — | $104.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1219.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.93 | ▼ close $9,533.89 vs 09:30 $9,770.75 (session -187.34) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.93 | ▲ 09:30 equity $9,714.38 vs yday $9,533.89 (+180.49) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 2 | $8.66 | $0.18 | — | $87.43 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $17.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 5 | $3.24 | $0.18 | — | $71.05 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $17.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $59.23 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $17.49 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.23 | ▲ close $9,736.21 vs 09:30 $9,714.38 (session +22.31) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.23 | ▲ 09:30 equity $9,759.77 vs yday $9,736.21 (+23.56) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.23 | ▼ close $9,726.07 vs 09:30 $9,759.77 (session -33.71) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.23 | ▼ 09:30 equity $9,707.78 vs yday $9,726.07 (-18.29) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,303.36 | ▲ +58.97 after sell → book $9,705.74; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $2,418.53 | ▼ -95.42 after sell → book $9,703.64; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 493 | $2.38 | $6.45 | $-57.18 | $3,585.42 | ▼ -57.18 after sell → book $9,697.19; vs 09:30 mark -6.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $4,741.95 | ▼ -20.12 after sell → book $9,695.12; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 76 | $19.04 | $2.24 | $+226.58 | $6,186.75 | ▲ +226.58 after sell → book $9,692.88; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $7,332.71 | ▼ -57.17 after sell → book $9,690.85; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $26.04 | $2.15 | $-28.12 | $8,502.37 | ▼ -28.12 after sell → book $9,688.70; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1725 | $0.66 | $16.91 | $-110.18 | $9,629.13 | ▼ -110.18 after sell → book $9,671.79; vs 09:30 mark -16.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,323.39 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1375.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $7,010.14 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1375.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 39 | $35.05 | $2.11 | — | $5,641.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1375.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 146 | $9.42 | $2.43 | — | $4,263.34 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1375.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 47 | $28.86 | $2.13 | — | $2,904.78 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1375.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.11 | $2.16 | — | $1,528.35 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=+891.7; leftover $1375.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 157 | $8.72 | $2.46 | — | $156.85 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1375.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.85 | ▲ close $10,106.25 vs 09:30 $9,707.78 (session +449.80) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.85 | ▼ 09:30 equity $9,933.05 vs yday $10,106.25 (-173.20) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 2 | $8.84 | $0.20 | $-0.02 | $174.33 | ▼ -0.02 after sell → book $9,932.85; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 5 | $2.95 | $0.18 | $-1.81 | $188.90 | ▼ -1.81 after sell → book $9,932.67; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $200.32 | ▼ -0.40 after sell → book $9,932.53; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 2 | $11.22 | $0.23 | — | $177.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $33.39 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 4 | $8.29 | $0.34 | — | $144.14 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $33.39 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 1 | $17.41 | $0.18 | — | $126.56 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-9.2; leftover $33.39 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 3 | $11.12 | $0.34 | — | $92.85 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $33.39 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.85 | ▼ close $9,808.51 vs 09:30 $9,933.05 (session -122.92) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.85 | ▼ 09:30 equity $9,764.95 vs yday $9,808.51 (-43.56) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.85 | ▼ close $9,665.99 vs 09:30 $9,764.95 (session -98.96) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.85 | ▼ 09:30 equity $9,635.81 vs yday $9,665.99 (-30.18) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $119.19 | $2.04 | $+3.30 | $1,401.90 | ▲ +3.30 after sell → book $9,633.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.57 | $2.06 | $+20.38 | $2,735.53 | ▲ +20.38 after sell → book $9,631.71; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 39 | $34.50 | $2.13 | $-25.68 | $4,078.90 | ▼ -25.68 after sell → book $9,629.58; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 146 | $9.30 | $2.46 | $-22.41 | $5,434.24 | ▼ -22.41 after sell → book $9,627.12; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 47 | $28.91 | $2.15 | $-1.93 | $6,790.86 | ▼ -1.93 after sell → book $9,624.97; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 57 | $23.40 | $2.18 | $-44.81 | $8,122.47 | ▼ -44.81 after sell → book $9,622.78; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EOLS` | 157 | $8.84 | $2.50 | $+13.88 | $9,507.86 | ▲ +13.88 after sell → book $9,620.29; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,532.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1188.48 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,396.53 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1188.48 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $6,593.70 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1188.48 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 4 | $240.22 | $2.00 | — | $5,630.82 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1188.48 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,584.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=+7.8; leftover $1188.48 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $32.90 | $2.10 | — | $3,397.68 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1188.48 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 246 | $4.82 | $3.17 | — | $2,208.78 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1188.48 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,208.78 | ▼ close $9,386.22 vs 09:30 $9,635.81 (session -218.78) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,208.78 | ▼ 09:30 equity $9,373.98 vs yday $9,386.22 (-12.24) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 2 | $11.80 | $0.26 | $+0.67 | $2,232.12 | ▲ +0.67 after sell → book $9,373.72; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 4 | $9.50 | $0.41 | $+4.08 | $2,269.71 | ▲ +4.08 after sell → book $9,373.31; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FWRD` | 1 | $17.03 | $0.19 | $-0.75 | $2,286.54 | ▼ -0.75 after sell → book $9,373.11; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 3 | $10.82 | $0.35 | $-1.60 | $2,318.65 | ▼ -1.60 after sell → book $9,372.76; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,318.65 | ▲ close $9,443.23 vs 09:30 $9,373.98 (session +70.47) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,318.65 | ▼ 09:30 equity $9,324.06 vs yday $9,443.23 (-119.17) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,318.65 | ▼ close $9,272.16 vs 09:30 $9,324.06 (session -51.90) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,318.65 | ▼ 09:30 equity $9,246.61 vs yday $9,272.16 (-25.55) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $3,270.75 | ▼ -23.13 after sell → book $9,244.59; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $4,332.72 | ▼ -74.13 after sell → book $9,242.56; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 2 | $357.25 | $2.02 | $-90.35 | $5,045.20 | ▼ -90.35 after sell → book $9,240.54; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 4 | $219.46 | $2.02 | $-87.06 | $5,921.02 | ▼ -87.06 after sell → book $9,238.52; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $6,905.80 | ▼ -61.86 after sell → book $9,236.50; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $32.42 | $2.12 | $-21.50 | $8,070.80 | ▼ -21.50 after sell → book $9,234.38; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 246 | $4.73 | $3.22 | $-28.54 | $9,231.16 | ▼ -28.54 after sell → book $9,231.16; vs 09:30 mark -3.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,231.16 | ▲ close $9,231.16 vs 09:30 $9,246.61 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,231.16 | ▲ 09:30 equity $9,231.16 vs yday $9,231.16 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,173.94 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,199.32 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 35 | $32.31 | $2.10 | — | $6,066.38 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 72 | $15.87 | $2.21 | — | $4,921.53 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 48 | $23.88 | $2.13 | — | $3,773.16 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,067.91 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $47.60 | $2.06 | — | $1,923.45 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1153.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 35 | $32.88 | $2.10 | — | $770.56 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1153.89 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $770.56 | ▲ close $9,564.76 vs 09:30 $9,231.16 (session +350.18) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $770.56 | ▼ 09:30 equity $9,494.67 vs yday $9,564.76 (-70.09) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 2 | $75.65 | $1.52 | — | $617.74 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $154.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 79 | $1.94 | $1.77 | — | $462.71 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $154.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 1 | $137.35 | $1.38 | — | $323.98 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $154.11 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $323.98 | ▼ close $9,468.83 vs 09:30 $9,494.67 (session -21.17) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $323.98 | ▲ 09:30 equity $9,484.11 vs yday $9,468.83 (+15.28) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $323.98 | ▼ close $9,451.33 vs 09:30 $9,484.11 (session -32.78) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $323.98 | ▲ 09:30 equity $9,478.08 vs yday $9,451.33 (+26.75) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,420.65 | ▲ +39.45 after sell → book $9,476.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,495.58 | ▲ +100.31 after sell → book $9,474.04; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 35 | $35.09 | $2.12 | $+93.09 | $3,721.61 | ▲ +93.09 after sell → book $9,471.93; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 72 | $15.96 | $2.23 | $+2.05 | $4,868.50 | ▲ +2.05 after sell → book $9,469.70; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 48 | $23.22 | $2.15 | $-35.97 | $5,980.91 | ▼ -35.97 after sell → book $9,467.54; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $6,660.22 | ▼ -25.94 after sell → book $9,465.53; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 24 | $56.94 | $2.08 | $+220.02 | $8,024.69 | ▲ +220.02 after sell → book $9,463.45; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNXC` | 35 | $28.13 | $2.12 | $-170.46 | $9,007.13 | ▼ -170.46 after sell → book $9,461.33; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,007.13 | ▼ close $9,446.90 vs 09:30 $9,478.08 (session -14.44) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,007.13 | ▼ 09:30 equity $9,441.20 vs yday $9,446.90 (-5.70) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 2 | $75.00 | $1.53 | $-4.35 | $9,155.60 | ▼ -4.35 after sell → book $9,439.67; vs 09:30 mark -1.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 79 | $1.97 | $1.82 | $-1.22 | $9,309.42 | ▼ -1.22 after sell → book $9,437.86; vs 09:30 mark -1.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 1 | $128.44 | $1.31 | $-11.59 | $9,436.55 | ▼ -11.59 after sell → book $9,436.55; vs 09:30 mark -1.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,436.55 | ▲ close $9,436.55 vs 09:30 $9,441.20 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,436.55 | ▲ 09:30 equity $9,436.55 vs yday $9,436.55 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $7,954.66 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1572.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $6,499.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1572.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 104 | $15.01 | $2.30 | — | $4,936.29 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1572.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 741 | $2.12 | $9.56 | — | $3,355.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1572.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 770 | $2.04 | $9.93 | — | $1,775.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1572.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 11 | $135.71 | $2.02 | — | $280.25 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-9.2; leftover $1572.76 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $280.25 | ▼ close $9,250.14 vs 09:30 $9,436.55 (session -158.57) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $280.25 | ▼ 09:30 equity $9,180.72 vs yday $9,250.14 (-69.42) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $280.25 | ▲ close $9,220.38 vs 09:30 $9,180.72 (session +39.66) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $280.25 | ▼ 09:30 equity $9,176.66 vs yday $9,220.38 (-43.72) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $280.25 | ▼ close $9,095.65 vs 09:30 $9,176.66 (session -81.01) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $280.25 | ▼ 09:30 equity $8,875.47 vs yday $9,095.65 (-220.18) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 9 | $140.03 | $2.04 | $-223.65 | $1,538.48 | ▼ -223.65 after sell → book $8,873.43; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 6 | $253.34 | $2.03 | $+62.98 | $3,056.49 | ▲ +62.98 after sell → book $8,871.40; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 741 | $1.84 | $9.69 | $-226.73 | $4,410.24 | ▼ -226.73 after sell → book $8,861.71; vs 09:30 mark -9.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 770 | $1.89 | $10.07 | $-135.51 | $5,855.46 | ▼ -135.51 after sell → book $8,851.63; vs 09:30 mark -10.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 11 | $125.55 | $2.04 | $-115.83 | $7,234.47 | ▼ -115.83 after sell → book $8,849.59; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 68 | $26.27 | $2.19 | — | $5,445.92 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $1808.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 9 | $189.17 | $2.02 | — | $3,741.37 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1808.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 45 | $39.99 | $2.12 | — | $1,939.69 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1808.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 260 | $6.95 | $3.35 | — | $129.34 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-5.8; leftover $1808.62 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.34 | ▼ close $8,772.06 vs 09:30 $8,875.47 (session -67.84) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.34 | ▲ 09:30 equity $8,870.26 vs yday $8,772.06 (+98.20) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 1 | $17.72 | $0.18 | — | $111.44 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $21.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 12 | $1.77 | $0.25 | — | $89.95 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-10.2; leftover $21.56 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.95 | ▼ close $8,809.62 vs 09:30 $8,870.26 (session -60.21) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.95 | ▲ 09:30 equity $8,843.81 vs yday $8,809.62 (+34.19) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 104 | $15.87 | $2.33 | $+84.81 | $1,738.10 | ▲ +84.81 after sell → book $8,841.48; vs 09:30 mark -2.33 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 16 | $20.91 | $2.04 | — | $1,401.50 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $347.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 15 | $22.90 | $2.04 | — | $1,055.97 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $347.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 23 | $14.79 | $2.06 | — | $713.74 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $347.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 24 | $14.07 | $2.06 | — | $373.99 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $347.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 46 | $7.54 | $2.13 | — | $25.26 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $347.62 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.26 | ▼ close $8,296.11 vs 09:30 $8,843.81 (session -535.05) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.26 | ▲ 09:30 equity $8,324.35 vs yday $8,296.11 (+28.24) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 68 | $25.94 | $2.22 | $-26.85 | $1,786.96 | ▼ -26.85 after sell → book $8,322.13; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 9 | $180.61 | $2.04 | $-81.10 | $3,410.41 | ▼ -81.10 after sell → book $8,320.09; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 45 | $35.91 | $2.15 | $-187.87 | $5,024.21 | ▼ -187.87 after sell → book $8,317.94; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $4,101.21 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $1004.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $3,147.70 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1004.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 38 | $25.95 | $2.10 | — | $2,159.50 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1004.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 72 | $13.94 | $2.21 | — | $1,153.61 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1004.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 467 | $2.15 | $6.02 | — | $143.54 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1004.84 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.54 | ▼ close $8,147.27 vs 09:30 $8,324.35 (session -156.33) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.54 | ▲ 09:30 equity $8,152.56 vs yday $8,147.27 (+5.29) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 260 | $5.99 | $3.41 | $-256.36 | $1,697.53 | ▼ -256.36 after sell → book $8,149.15; vs 09:30 mark -3.41 | dropped from list after 4 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 1 | $168.50 | $1.69 | — | $1,527.34 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $282.92 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 280 | $1.01 | $3.61 | — | $1,240.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $282.92 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 65 | $4.30 | $2.19 | — | $959.24 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+16.9; leftover $282.92 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $959.24 | ▲ close $8,190.68 vs 09:30 $8,152.56 (session +49.02) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $959.24 | ▲ 09:30 equity $8,372.50 vs yday $8,190.68 (+181.82) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TNDM` | 1 | $17.44 | $0.20 | $-0.66 | $976.49 | ▼ -0.66 after sell → book $8,372.30; vs 09:30 mark -0.20 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BAK` | 12 | $1.68 | $0.26 | $-1.59 | $996.39 | ▼ -1.59 after sell → book $8,372.04; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 16 | $21.15 | $2.06 | $-0.26 | $1,332.73 | ▼ -0.26 after sell → book $8,369.99; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GME` | 15 | $23.94 | $2.06 | $+11.51 | $1,689.78 | ▲ +11.51 after sell → book $8,367.93; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 23 | $15.40 | $2.08 | $+9.89 | $2,041.90 | ▲ +9.89 after sell → book $8,365.85; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 24 | $14.84 | $2.08 | $+14.34 | $2,395.97 | ▲ +14.34 after sell → book $8,363.77; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FLNC` | 46 | $7.52 | $2.15 | $-4.97 | $2,739.75 | ▼ -4.97 after sell → book $8,361.62; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 2 | $196.78 | $2.00 | — | $2,344.19 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $456.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 57 | $7.95 | $2.16 | — | $1,888.88 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $456.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 29 | $15.72 | $2.08 | — | $1,430.92 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $456.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 351 | $1.30 | $4.53 | — | $970.09 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $456.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 374 | $1.22 | $4.82 | — | $508.99 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $456.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 11 | $40.00 | $2.02 | — | $66.97 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $456.62 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.97 | ▼ close $8,151.47 vs 09:30 $8,372.50 (session -192.54) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.97 | ▼ 09:30 equity $8,003.54 vs yday $8,151.47 (-147.93) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+173.42 | $1,163.38 | ▲ +173.42 after sell → book $8,001.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 5 | $164.04 | $2.02 | $-135.33 | $1,981.56 | ▼ -135.33 after sell → book $7,999.49; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 38 | $25.00 | $2.12 | $-40.52 | $2,929.25 | ▼ -40.52 after sell → book $7,997.37; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 72 | $13.07 | $2.23 | $-67.07 | $3,868.06 | ▼ -67.07 after sell → book $7,995.14; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 467 | $1.88 | $6.11 | $-138.23 | $4,739.91 | ▼ -138.23 after sell → book $7,989.03; vs 09:30 mark -6.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,739.91 | ▲ close $8,058.87 vs 09:30 $8,003.54 (session +69.84) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,751.79 | ▲ 09:30 equity $7,960.58 vs yday $7,938.17 (+22.41) | 09:30 open · cash $5,751.79 (unchanged overnight, no fees) · equity $7,960.58 vs prior close $7,938.17 (+22.41) · 8 name(s) re-marked at the open (per-name table). CMPX×68 yday $1.13 → 09:30 $1.13 +0.00; DGXX×117 yday $4.53 → 09:30 $4.78 +29.25; GRAL×4 yday $125.21 → 09:30 $123.50 -6.84; IVVD×499 yday $0.91 → 09:30 $0.91 +0.00; MRNA×2 yday $194.82 → 09:30 $194.82 +0.00; PGEN×10 yday $7.70 → 09:30 $7.70 +0.00; SGRY×5 yday $14.20 → 09:30 $14.20 +0.00; VERI×64 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `DGXX` | 117 | $4.78 | $2.37 | $+53.79 | $6,308.68 | ▲ +53.79 after sell → book $7,958.21; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 4 | $123.50 | $2.02 | $+62.98 | $6,800.66 | ▲ +62.98 after sell → book $7,956.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 352 | $3.86 | $4.54 | — | $5,437.40 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 4 | $272.16 | $2.00 | — | $4,346.75 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 83 | $16.21 | $2.24 | — | $2,999.09 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,110.09 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 18 | $74.15 | $2.04 | — | $773.35 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+8.5; leftover $1360.13 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $773.35 | ▼ close $7,917.98 vs 09:30 $7,960.58 (session -25.39) | 16:00 close · cash $773.35 · equity $7,917.98 vs 09:30 $7,960.58 (-42.60; session marks -25.39) · 11 name(s) marked open→close (per-name table). CMPX×68 09:30 $1.14 → close $1.14 -0.00; IVVD×499 09:30 $0.91 → close $0.91 -0.00; MRNA×2 09:30 $194.82 → close $194.82 +0.00; PGEN×10 09:30 $7.70 → close $7.70 -0.00; SGRY×5 09:30 $14.20 → close $14.20 -0.00; VERI×64 09:30 $1.33 → close $1.33 +0.00; ZSQR×352 09:30 $3.86 → close $3.78 -28.16; ILMN×4 09:30 $272.16 → close $270.00 -8.64; SECZ×83 09:30 $16.21 → close $15.96 -20.75; COST×1 09:30 $887.00 → close $922.76 +35.76; RKLB×18 09:30 $74.15 → close $73.95 -3.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 17.49 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 17.49 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 17.49 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EOLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 33.39 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 33.39 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EOLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 11.61 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 11.61 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 11.61 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 11.61 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 11.61 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 11.61 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 11.61 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 11.61 < 1 share @ 261.47 |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MPWR` | cash | leftover split 1188.48 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 154.11 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 154.11 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 21.56 < 1 share @ 170.85 |
| 2026-09-17 | `GME` | cash | leftover split 21.56 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 21.56 < 1 share @ 238.60 |
| 2026-09-17 | `LITE` | cash | leftover split 21.56 < 1 share @ 934.88 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TNDM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BAK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DGXX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MRNA` | 1 | 2026-09-22 @ $168.50 | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $282.92 |
| `IVVD` | 280 | 2026-09-22 @ $1.01 | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $282.92 |
| `DGXX` | 65 | 2026-09-22 @ $4.30 | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+16.9; leftover $282.92 |
| `CTAS` | 2 | 2026-09-23 @ $196.78 | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $456.62 |
| `PGEN` | 57 | 2026-09-23 @ $7.95 | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $456.62 |
| `SGRY` | 29 | 2026-09-23 @ $15.72 | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $456.62 |
| `VERI` | 351 | 2026-09-23 @ $1.30 | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $456.62 |
| `CMPX` | 374 | 2026-09-23 @ $1.22 | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $456.62 |
| `BLSH` | 11 | 2026-09-23 @ $40.00 | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $456.62 |
