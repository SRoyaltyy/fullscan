# Factor mine action — `union_news_or_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢

Cash book **-20.82%** ($7,918) · signal-only (no cash/fees) was +179.67%. Starts YES **1/30**. Fills 164 · skips 244 · realized $-1770.27.

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
- Must-have: the morning news packet OR the prior-export headline is green.
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
- **Gate** `news_or_headline=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,770.79.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 5 | $46.18 | $2.00 | — | $1,099.83 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+6.7; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $955.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+5.8; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $750.93 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+8.3; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 5 | $49.00 | $2.00 | — | $503.93 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $266.55 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 2 | $92.99 | $1.87 | — | $316.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $266.55 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,572.31 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $7,361.72 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 493 | $2.47 | $6.36 | — | $6,137.65 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $4,961.00 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 76 | $16.00 | $2.22 | — | $3,742.78 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,539.65 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $1,341.87 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1219.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $122.56 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1219.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.56 | ▼ close $9,601.17 vs 09:30 $9,770.75 (session -135.42) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.56 | ▲ 09:30 equity $9,788.90 vs yday $9,601.17 (+187.73) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 2 | $8.66 | $0.18 | — | $105.06 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $20.43 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 6 | $3.24 | $0.21 | — | $85.41 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $20.43 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $73.59 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $20.43 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.59 | ▲ close $9,848.82 vs 09:30 $9,788.90 (session +60.43) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.59 | ▼ 09:30 equity $9,797.85 vs yday $9,848.82 (-50.97) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.59 | ▲ close $9,800.65 vs 09:30 $9,797.85 (session +2.80) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.59 | ▼ 09:30 equity $9,775.82 vs yday $9,800.65 (-24.83) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,317.72 | ▲ +58.97 after sell → book $9,773.77; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $2,432.89 | ▼ -95.42 after sell → book $9,771.68; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 493 | $2.38 | $6.45 | $-57.18 | $3,599.78 | ▼ -57.18 after sell → book $9,765.23; vs 09:30 mark -6.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $4,756.31 | ▼ -20.12 after sell → book $9,763.16; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 76 | $19.04 | $2.24 | $+226.58 | $6,201.11 | ▲ +226.58 after sell → book $9,760.92; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $7,347.07 | ▼ -57.17 after sell → book $9,758.88; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $26.04 | $2.15 | $-28.12 | $8,516.73 | ▼ -28.12 after sell → book $9,756.74; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEAM` | 7 | $170.64 | $2.03 | $-26.86 | $9,709.18 | ▼ -26.86 after sell → book $9,754.71; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,403.43 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1387.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $7,090.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1387.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 39 | $35.05 | $2.11 | — | $5,721.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1387.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 147 | $9.42 | $2.43 | — | $4,333.95 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1387.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 48 | $28.86 | $2.13 | — | $2,946.54 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1387.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.11 | $2.16 | — | $1,570.11 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=+891.7; leftover $1387.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 159 | $8.72 | $2.47 | — | $181.16 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1387.03 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.16 | ▲ close $10,189.23 vs 09:30 $9,775.82 (session +449.88) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.16 | ▼ 09:30 equity $10,015.66 vs yday $10,189.23 (-173.57) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 2 | $8.84 | $0.20 | $-0.02 | $198.64 | ▼ -0.02 after sell → book $10,015.46; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 6 | $2.95 | $0.21 | $-2.17 | $216.12 | ▼ -2.17 after sell → book $10,015.24; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $227.55 | ▼ -0.40 after sell → book $10,015.11; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 3 | $11.22 | $0.35 | — | $193.54 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $37.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 4 | $8.29 | $0.34 | — | $160.04 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $37.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 2 | $17.41 | $0.35 | — | $124.86 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-9.2; leftover $37.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 3 | $11.12 | $0.34 | — | $91.16 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $37.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.16 | ▼ close $9,892.11 vs 09:30 $10,015.66 (session -121.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.16 | ▼ 09:30 equity $9,848.64 vs yday $9,892.11 (-43.47) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.16 | ▼ close $9,749.23 vs 09:30 $9,848.64 (session -99.41) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.16 | ▼ 09:30 equity $9,718.71 vs yday $9,749.23 (-30.52) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $119.19 | $2.04 | $+3.30 | $1,400.21 | ▲ +3.30 after sell → book $9,716.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.57 | $2.06 | $+20.38 | $2,733.83 | ▲ +20.38 after sell → book $9,714.60; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 39 | $34.50 | $2.13 | $-25.68 | $4,077.21 | ▼ -25.68 after sell → book $9,712.48; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 147 | $9.30 | $2.47 | $-22.54 | $5,441.84 | ▼ -22.54 after sell → book $9,710.01; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 48 | $28.91 | $2.16 | $-1.89 | $6,827.36 | ▼ -1.89 after sell → book $9,707.85; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 57 | $23.40 | $2.18 | $-44.81 | $8,158.98 | ▼ -44.81 after sell → book $9,705.67; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EOLS` | 159 | $8.84 | $2.50 | $+14.11 | $9,562.04 | ▲ +14.11 after sell → book $9,703.17; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,586.81 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1195.25 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,450.72 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1195.25 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $6,647.88 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1195.25 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 4 | $240.22 | $2.00 | — | $5,685.00 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1195.25 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,638.36 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=+7.8; leftover $1195.25 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $32.90 | $2.10 | — | $3,451.86 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1195.25 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 247 | $4.82 | $3.19 | — | $2,258.13 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1195.25 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,258.13 | ▼ close $9,469.17 vs 09:30 $9,718.71 (session -218.70) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,258.13 | ▼ 09:30 equity $9,456.97 vs yday $9,469.17 (-12.20) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 3 | $11.80 | $0.38 | $+1.01 | $2,293.15 | ▲ +1.01 after sell → book $9,456.59; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 4 | $9.50 | $0.41 | $+4.08 | $2,330.74 | ▲ +4.08 after sell → book $9,456.18; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FWRD` | 2 | $17.03 | $0.37 | $-1.48 | $2,364.43 | ▼ -1.48 after sell → book $9,455.81; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 3 | $10.82 | $0.35 | $-1.60 | $2,396.54 | ▼ -1.60 after sell → book $9,455.46; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,396.54 | ▲ close $9,525.94 vs 09:30 $9,456.97 (session +70.48) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,396.54 | ▼ 09:30 equity $9,406.72 vs yday $9,525.94 (-119.22) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,396.54 | ▼ close $9,354.77 vs 09:30 $9,406.72 (session -51.95) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,396.54 | ▼ 09:30 equity $9,329.23 vs yday $9,354.77 (-25.54) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $3,348.64 | ▼ -23.13 after sell → book $9,327.21; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $4,410.60 | ▼ -74.13 after sell → book $9,325.17; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 2 | $357.25 | $2.02 | $-90.35 | $5,123.09 | ▼ -90.35 after sell → book $9,323.16; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 4 | $219.46 | $2.02 | $-87.06 | $5,998.90 | ▼ -87.06 after sell → book $9,321.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $6,983.68 | ▼ -61.86 after sell → book $9,319.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $32.42 | $2.12 | $-21.50 | $8,148.68 | ▼ -21.50 after sell → book $9,316.99; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 247 | $4.73 | $3.24 | $-28.65 | $9,313.76 | ▼ -28.65 after sell → book $9,313.76; vs 09:30 mark -3.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,313.76 | ▲ close $9,313.76 vs 09:30 $9,329.23 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,313.76 | ▲ 09:30 equity $9,313.76 vs yday $9,313.76 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,256.54 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,281.92 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 36 | $32.31 | $2.10 | — | $6,116.66 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 73 | $15.87 | $2.21 | — | $4,955.95 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 48 | $23.88 | $2.13 | — | $3,807.57 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,102.33 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $47.60 | $2.06 | — | $1,957.87 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1164.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 35 | $32.88 | $2.10 | — | $804.97 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1164.22 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $804.97 | ▲ close $9,649.73 vs 09:30 $9,313.76 (session +352.56) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $804.97 | ▼ 09:30 equity $9,578.94 vs yday $9,649.73 (-70.79) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 2 | $75.65 | $1.52 | — | $652.15 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $160.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 82 | $1.94 | $1.84 | — | $491.24 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $160.99 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 1 | $137.35 | $1.38 | — | $352.51 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+5.4; leftover $160.99 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $352.51 | ▼ close $9,554.05 vs 09:30 $9,578.94 (session -20.16) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $352.51 | ▲ 09:30 equity $9,569.69 vs yday $9,554.05 (+15.64) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $352.51 | ▼ close $9,536.66 vs 09:30 $9,569.69 (session -33.03) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $352.51 | ▲ 09:30 equity $9,563.70 vs yday $9,536.66 (+27.04) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,449.18 | ▲ +39.45 after sell → book $9,561.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,524.10 | ▲ +100.31 after sell → book $9,559.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 36 | $35.09 | $2.12 | $+95.86 | $3,785.23 | ▲ +95.86 after sell → book $9,557.55; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 73 | $15.96 | $2.23 | $+2.13 | $4,948.08 | ▲ +2.13 after sell → book $9,555.32; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 48 | $23.22 | $2.15 | $-35.97 | $6,060.48 | ▼ -35.97 after sell → book $9,553.16; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $6,739.79 | ▼ -25.94 after sell → book $9,551.15; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 24 | $56.94 | $2.08 | $+220.02 | $8,104.27 | ▲ +220.02 after sell → book $9,549.07; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNXC` | 35 | $28.13 | $2.12 | $-170.46 | $9,086.70 | ▼ -170.46 after sell → book $9,546.95; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,086.70 | ▼ close $9,532.38 vs 09:30 $9,563.70 (session -14.57) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,086.70 | ▼ 09:30 equity $9,526.68 vs yday $9,532.38 (-5.70) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 2 | $75.00 | $1.53 | $-4.35 | $9,235.17 | ▼ -4.35 after sell → book $9,525.15; vs 09:30 mark -1.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 82 | $1.97 | $1.89 | $-1.26 | $9,394.83 | ▼ -1.26 after sell → book $9,523.27; vs 09:30 mark -1.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 1 | $128.44 | $1.31 | $-11.59 | $9,521.96 | ▼ -11.59 after sell → book $9,521.96; vs 09:30 mark -1.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,521.96 | ▲ close $9,521.96 vs 09:30 $9,526.68 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,521.96 | ▲ 09:30 equity $9,521.96 vs yday $9,521.96 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $8,040.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1586.99 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $6,585.05 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-11.1; leftover $1586.99 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 105 | $15.01 | $2.31 | — | $5,006.69 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1586.99 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 748 | $2.12 | $9.65 | — | $3,411.28 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1586.99 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 777 | $2.04 | $10.02 | — | $1,816.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1586.99 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 11 | $135.71 | $2.02 | — | $321.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-9.2; leftover $1586.99 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.35 | ▼ close $9,334.68 vs 09:30 $9,521.96 (session -159.26) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.35 | ▼ 09:30 equity $9,265.11 vs yday $9,334.68 (-69.57) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.35 | ▲ close $9,304.36 vs 09:30 $9,265.11 (session +39.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.35 | ▼ 09:30 equity $9,260.63 vs yday $9,304.36 (-43.73) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.35 | ▼ close $9,180.26 vs 09:30 $9,260.63 (session -80.37) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.35 | ▼ 09:30 equity $8,958.21 vs yday $9,180.26 (-222.05) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 9 | $140.03 | $2.04 | $-223.65 | $1,579.58 | ▼ -223.65 after sell → book $8,956.17; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 6 | $253.34 | $2.03 | $+62.98 | $3,097.59 | ▲ +62.98 after sell → book $8,954.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 748 | $1.84 | $9.78 | $-228.87 | $4,464.12 | ▼ -228.87 after sell → book $8,944.35; vs 09:30 mark -9.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 777 | $1.89 | $10.16 | $-136.74 | $5,922.49 | ▼ -136.74 after sell → book $8,934.19; vs 09:30 mark -10.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 11 | $125.55 | $2.04 | $-115.83 | $7,301.50 | ▼ -115.83 after sell → book $8,932.15; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 69 | $26.27 | $2.20 | — | $5,486.67 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $1825.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 9 | $189.17 | $2.02 | — | $3,782.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1825.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 45 | $39.99 | $2.12 | — | $1,980.45 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1825.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 262 | $6.95 | $3.38 | — | $156.17 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-5.8; leftover $1825.37 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.17 | ▼ close $8,855.17 vs 09:30 $8,958.21 (session -67.26) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.17 | ▲ 09:30 equity $8,953.95 vs yday $8,855.17 (+98.78) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 1 | $22.12 | $0.22 | — | $133.82 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $26.03 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 1 | $17.72 | $0.18 | — | $115.92 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $26.03 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 14 | $1.77 | $0.29 | — | $90.85 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-10.2; leftover $26.03 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.85 | ▼ close $8,893.60 vs 09:30 $8,953.95 (session -59.65) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.85 | ▲ 09:30 equity $8,928.21 vs yday $8,893.60 (+34.61) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 105 | $15.87 | $2.34 | $+85.66 | $1,754.87 | ▲ +85.66 after sell → book $8,925.88; vs 09:30 mark -2.33 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 20 | $20.91 | $2.05 | — | $1,334.62 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $438.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 29 | $14.79 | $2.08 | — | $903.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $438.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 31 | $14.07 | $2.08 | — | $465.38 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $438.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 58 | $7.54 | $2.16 | — | $26.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $438.72 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.18 | ▼ close $8,376.05 vs 09:30 $8,928.21 (session -541.45) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.18 | ▲ 09:30 equity $8,407.51 vs yday $8,376.05 (+31.46) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 69 | $25.94 | $2.22 | $-27.19 | $1,813.82 | ▼ -27.19 after sell → book $8,405.29; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 9 | $180.61 | $2.04 | $-81.10 | $3,437.27 | ▼ -81.10 after sell → book $8,403.25; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 45 | $35.91 | $2.15 | $-187.87 | $5,051.07 | ▼ -187.87 after sell → book $8,401.10; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $4,128.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $1010.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $3,174.57 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+10.6; leftover $1010.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 38 | $25.95 | $2.10 | — | $2,186.36 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1010.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 72 | $13.94 | $2.21 | — | $1,180.48 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1010.21 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 469 | $2.15 | $6.05 | — | $166.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1010.21 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.08 | ▼ close $8,232.05 vs 09:30 $8,407.51 (session -154.69) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.08 | ▼ 09:30 equity $8,228.94 vs yday $8,232.05 (-3.11) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 262 | $5.99 | $3.44 | $-258.34 | $1,732.02 | ▼ -258.34 after sell → book $8,225.50; vs 09:30 mark -3.44 | dropped from list after 4 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 1 | $23.50 | $0.26 | $+0.90 | $1,755.26 | ▲ +0.90 after sell → book $8,225.24; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 1 | $168.50 | $1.69 | — | $1,585.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+17.9; leftover $292.54 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 289 | $1.01 | $3.73 | — | $1,289.46 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $292.54 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 68 | $4.30 | $2.19 | — | $994.86 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+16.9; leftover $292.54 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $994.86 | ▲ close $8,260.41 vs 09:30 $8,228.94 (session +42.79) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $994.86 | ▲ 09:30 equity $8,449.13 vs yday $8,260.41 (+188.72) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TNDM` | 1 | $17.44 | $0.20 | $-0.66 | $1,012.10 | ▼ -0.66 after sell → book $8,448.93; vs 09:30 mark -0.20 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BAK` | 14 | $1.68 | $0.30 | $-1.85 | $1,035.33 | ▼ -1.85 after sell → book $8,448.63; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 20 | $21.15 | $2.07 | $+0.68 | $1,456.26 | ▲ +0.68 after sell → book $8,446.56; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 29 | $15.40 | $2.10 | $+13.52 | $1,900.76 | ▲ +13.52 after sell → book $8,444.47; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 31 | $14.84 | $2.10 | $+19.68 | $2,358.70 | ▲ +19.68 after sell → book $8,442.36; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FLNC` | 58 | $7.52 | $2.18 | $-5.22 | $2,792.67 | ▼ -5.22 after sell → book $8,440.18; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 2 | $196.78 | $2.00 | — | $2,397.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $465.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 58 | $7.95 | $2.16 | — | $1,933.85 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $465.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 29 | $15.72 | $2.08 | — | $1,475.90 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $465.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 358 | $1.30 | $4.62 | — | $1,005.88 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $465.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 381 | $1.22 | $4.91 | — | $536.14 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $465.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 11 | $40.00 | $2.02 | — | $94.12 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $465.45 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.12 | ▼ close $8,228.05 vs 09:30 $8,449.13 (session -194.33) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.12 | ▼ 09:30 equity $8,079.73 vs yday $8,228.05 (-148.32) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+173.42 | $1,190.54 | ▲ +173.42 after sell → book $8,077.71; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 5 | $164.04 | $2.02 | $-135.33 | $2,008.71 | ▼ -135.33 after sell → book $8,075.68; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 38 | $25.00 | $2.12 | $-40.52 | $2,956.40 | ▼ -40.52 after sell → book $8,073.56; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 72 | $13.07 | $2.23 | $-67.07 | $3,895.21 | ▼ -67.07 after sell → book $8,071.33; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 469 | $1.88 | $6.14 | $-138.82 | $4,770.79 | ▼ -138.82 after sell → book $8,065.19; vs 09:30 mark -6.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,770.79 | ▲ close $8,136.52 vs 09:30 $8,079.73 (session +71.33) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,751.79 | ▲ 09:30 equity $7,960.58 vs yday $7,938.17 (+22.41) | 09:30 open · cash $5,751.79 (unchanged overnight, no fees) · equity $7,960.58 vs prior close $7,938.17 (+22.41) · 8 name(s) re-marked at the open (per-name table). CMPX×68 yday $1.13 → 09:30 $1.13 +0.00; DGXX×117 yday $4.53 → 09:30 $4.78 +29.25; GRAL×4 yday $125.21 → 09:30 $123.50 -6.84; IVVD×499 yday $0.91 → 09:30 $0.91 +0.00; MRNA×2 yday $194.82 → 09:30 $194.82 +0.00; PGEN×10 yday $7.70 → 09:30 $7.70 +0.00; SGRY×5 yday $14.20 → 09:30 $14.20 +0.00; VERI×64 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `DGXX` | 117 | $4.78 | $2.37 | $+53.79 | $6,308.68 | ▲ +53.79 after sell → book $7,958.21; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 4 | $123.50 | $2.02 | $+62.98 | $6,800.66 | ▲ +62.98 after sell → book $7,956.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 352 | $3.86 | $4.54 | — | $5,437.40 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 4 | $272.16 | $2.00 | — | $4,346.75 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 83 | $16.21 | $2.24 | — | $2,999.09 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,110.09 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1360.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 18 | $74.15 | $2.04 | — | $773.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+8.5; leftover $1360.13 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
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
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 20.43 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 20.43 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 20.43 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-26 | `FNV` | cash | leftover split 37.92 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 37.92 < 1 share @ 118.50 |
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
| 2026-08-27 | `ACMR` | cash | leftover split 11.39 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 11.39 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 11.39 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 11.39 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 11.39 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 11.39 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 11.39 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 11.39 < 1 share @ 261.47 |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MPWR` | cash | leftover split 1195.25 < 1 share @ 1306.03 |
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
| 2026-09-04 | `CRM` | cash | leftover split 160.99 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 160.99 < 1 share @ 236.82 |
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
| 2026-09-17 | `SMTC` | cash | leftover split 26.03 < 1 share @ 170.85 |
| 2026-09-17 | `JBHT` | cash | leftover split 26.03 < 1 share @ 238.60 |
| 2026-09-17 | `LITE` | cash | leftover split 26.03 < 1 share @ 934.88 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TNDM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BAK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| `MRNA` | 1 | 2026-09-22 @ $168.50 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+17.9; leftover $292.54 |
| `IVVD` | 289 | 2026-09-22 @ $1.01 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $292.54 |
| `DGXX` | 68 | 2026-09-22 @ $4.30 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+16.9; leftover $292.54 |
| `CTAS` | 2 | 2026-09-23 @ $196.78 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $465.45 |
| `PGEN` | 58 | 2026-09-23 @ $7.95 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $465.45 |
| `SGRY` | 29 | 2026-09-23 @ $15.72 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $465.45 |
| `VERI` | 358 | 2026-09-23 @ $1.30 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $465.45 |
| `CMPX` | 381 | 2026-09-23 @ $1.22 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $465.45 |
| `BLSH` | 11 | 2026-09-23 @ $40.00 | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $465.45 |
