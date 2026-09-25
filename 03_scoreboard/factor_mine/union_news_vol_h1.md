# Factor mine action — `union_news_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-21.62%** ($7,838) · signal-only (no cash/fees) was +0.74%. Starts YES **0/30**. Fills 116 · skips 41 · realized $-847.98.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
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
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,151.99.

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
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,053.48 vs 09:30 $10,000.00 (session +68.63) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,215.56 vs yday $10,053.48 (+162.08) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,488.23 | ▲ +122.49 after sell → book $10,209.48; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 102 | $19.57 | $2.33 | $-4.62 | $4,482.04 | ▼ -4.62 after sell → book $10,207.15; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $6,180.77 | ▲ +49.81 after sell → book $10,205.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 147 | $13.16 | $2.47 | $-62.23 | $8,112.82 | ▼ -62.23 after sell → book $10,202.66; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $10,200.18 | ▲ +94.73 after sell → book $10,200.18; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,215.56 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,200.18 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,200.18 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,924.00 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,720.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1803 | $0.71 | $18.16 | — | $6,427.99 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,150.66 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,884.43 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $2,633.51 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,398.13 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1275.02 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $142.77 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1275.02 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.77 | ▼ close $9,982.45 vs 09:30 $10,200.18 (session -184.48) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.77 | ▲ 09:30 equity $10,233.88 vs yday $9,982.45 (+251.43) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,480.80 | ▲ +61.86 after sell → book $10,231.82; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,543.65 | ▼ -140.29 after sell → book $10,229.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1803 | $0.67 | $17.87 | $-95.53 | $3,741.00 | ▼ -95.53 after sell → book $10,211.92; vs 09:30 mark -17.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,079.74 | ▲ +61.40 after sell → book $10,209.31; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $6,472.62 | ▲ +126.66 after sell → book $10,207.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $7,704.22 | ▼ -19.32 after sell → book $10,204.90; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $8,948.69 | ▼ -10.89 after sell → book $10,202.81; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,752.37 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1278.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $6,468.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1278.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,199.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1278.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,951.19 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1278.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $2,673.57 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1278.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 115 | $11.10 | $2.33 | — | $1,395.31 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $1278.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 394 | $3.24 | $5.08 | — | $113.67 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1278.38 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.67 | ▲ close $10,205.03 vs 09:30 $10,233.88 (session +24.66) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.67 | ▼ 09:30 equity $10,146.19 vs yday $10,205.03 (-58.84) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,316.73 | ▲ +6.74 after sell → book $10,144.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.40 | $6.77 | $-49.62 | $2,550.77 | ▼ -49.62 after sell → book $10,137.39; vs 09:30 mark -6.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,879.72 | ▲ +59.95 after sell → book $10,135.34; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,183.79 | ▲ +55.55 after sell → book $10,133.33; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $6,398.97 | ▼ -62.43 after sell → book $10,130.98; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 115 | $11.48 | $2.36 | $+39.58 | $7,716.81 | ▲ +39.58 after sell → book $10,128.62; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 394 | $2.99 | $5.16 | $-108.74 | $8,889.71 | ▼ -108.74 after sell → book $10,123.46; vs 09:30 mark -5.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,889.71 | ▼ close $10,088.28 vs 09:30 $10,146.19 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,889.71 | ▲ 09:30 equity $10,106.24 vs yday $10,088.28 (+17.96) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $10,104.17 | ▼ -20.93 after sell → book $10,104.17; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 214 | $9.42 | $2.76 | — | $8,085.53 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $2020.83 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 57 | $35.05 | $2.16 | — | $6,085.51 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $2020.83 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 83 | $24.11 | $2.24 | — | $4,082.15 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+891.7; leftover $2020.83 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 17 | $118.52 | $2.04 | — | $2,065.26 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2020.83 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 26 | $77.13 | $2.07 | — | $57.82 | — | combo gate; gate news=good,vol=good; list mover_buy; ⚪; ret5=+13.8; leftover $2020.83 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.82 | ▲ close $10,790.13 vs 09:30 $10,106.24 (session +697.23) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.82 | ▼ 09:30 equity $10,555.77 vs yday $10,790.13 (-234.36) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 214 | $10.07 | $2.81 | $+133.53 | $2,209.98 | ▲ +133.53 after sell → book $10,552.95; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 57 | $35.70 | $2.19 | $+32.70 | $4,242.70 | ▲ +32.70 after sell → book $10,550.77; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 83 | $26.61 | $2.27 | $+202.99 | $6,449.06 | ▲ +202.99 after sell → book $10,548.50; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 17 | $119.80 | $2.07 | $+17.65 | $8,483.59 | ▲ +17.65 after sell → book $10,546.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 26 | $79.34 | $2.09 | $+53.30 | $10,544.33 | ▲ +53.30 after sell → book $10,544.33; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.33 | ▲ close $10,544.33 vs 09:30 $10,555.77 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.33 | ▲ 09:30 equity $10,544.33 vs yday $10,544.33 (+0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.33 | ▲ close $10,544.33 vs 09:30 $10,544.33 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.33 | ▲ 09:30 equity $10,544.33 vs yday $10,544.33 (+0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 45 | $32.90 | $2.12 | — | $9,061.71 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1506.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 154 | $9.73 | $2.45 | — | $7,560.84 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1506.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $6,141.22 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1506.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 78 | $19.25 | $2.22 | — | $4,637.49 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+14.1; leftover $1506.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 80 | $18.75 | $2.23 | — | $3,135.26 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-5.0; leftover $1506.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 52 | $28.91 | $2.15 | — | $1,629.80 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+9.2; leftover $1506.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 79 | $19.00 | $2.23 | — | $126.57 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+7.5; leftover $1506.33 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.57 | ▼ close $10,208.01 vs 09:30 $10,544.33 (session -320.90) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.57 | ▼ 09:30 equity $10,139.17 vs yday $10,208.01 (-68.84) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 45 | $31.15 | $2.15 | $-83.02 | $1,526.17 | ▼ -83.02 after sell → book $10,137.03; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 154 | $9.50 | $2.49 | $-40.36 | $2,986.68 | ▼ -40.36 after sell → book $10,134.54; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $4,307.64 | ▼ -98.66 after sell → book $10,132.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 78 | $17.87 | $2.25 | $-112.11 | $5,699.26 | ▼ -112.11 after sell → book $10,130.25; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 80 | $19.25 | $2.26 | $+35.51 | $7,237.00 | ▲ +35.51 after sell → book $10,127.99; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 52 | $28.06 | $2.17 | $-48.51 | $8,693.95 | ▼ -48.51 after sell → book $10,125.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,693.95 | ▲ close $10,157.03 vs 09:30 $10,139.17 (session +31.20) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,693.95 | ▼ 09:30 equity $10,151.50 vs yday $10,157.03 (-5.53) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 79 | $18.45 | $2.25 | $-47.93 | $10,149.25 | ▼ -47.93 after sell → book $10,149.25; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,149.25 | ▲ close $10,149.25 vs 09:30 $10,151.50 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,149.25 | ▲ 09:30 equity $10,149.25 vs yday $10,149.25 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,149.25 | ▲ close $10,149.25 vs 09:30 $10,149.25 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,149.25 | ▲ 09:30 equity $10,149.25 vs yday $10,149.25 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 85 | $23.88 | $2.25 | — | $8,117.21 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $2029.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $6,708.71 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $2029.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 127 | $15.87 | $2.37 | — | $4,690.85 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2029.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $2,743.61 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+6.1; leftover $2029.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 62 | $32.31 | $2.18 | — | $738.21 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2029.85 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $738.21 | ▲ close $10,452.21 vs 09:30 $10,149.25 (session +313.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $738.21 | ▼ 09:30 equity $10,361.11 vs yday $10,452.21 (-91.10) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 85 | $23.84 | $2.28 | $-7.92 | $2,762.34 | ▼ -7.92 after sell → book $10,358.84; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $4,144.38 | ▼ -26.45 after sell → book $10,356.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 4 | $513.78 | $2.03 | $+105.85 | $6,197.47 | ▲ +105.85 after sell → book $10,354.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 62 | $33.46 | $2.20 | $+66.92 | $8,269.79 | ▲ +66.92 after sell → book $10,352.59; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 4234 | $1.94 | $54.62 | — | $1.21 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $8269.79 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.21 | ▼ close $10,074.84 vs 09:30 $10,361.11 (session -223.13) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.21 | ▲ 09:30 equity $10,341.15 vs yday $10,074.84 (+266.31) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 127 | $16.74 | $2.41 | $+105.71 | $2,124.78 | ▲ +105.71 after sell → book $10,338.74; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 4234 | $1.94 | $55.39 | $-110.01 | $10,283.35 | ▼ -110.01 after sell → book $10,283.35; vs 09:30 mark -55.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.35 | ▲ close $10,283.35 vs 09:30 $10,341.15 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,283.35 | ▲ 09:30 equity $10,283.35 vs yday $10,283.35 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.35 | ▲ close $10,283.35 vs 09:30 $10,283.35 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,283.35 | ▲ 09:30 equity $10,283.35 vs yday $10,283.35 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.35 | ▲ close $10,283.35 vs 09:30 $10,283.35 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,283.35 | ▲ 09:30 equity $10,283.35 vs yday $10,283.35 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 20 | $164.43 | $2.05 | — | $6,992.70 | — | combo gate; gate news=good,vol=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $3427.78 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 14 | $242.17 | $2.03 | — | $3,600.29 | — | combo gate; gate news=good,vol=good; list earn_react; ret5=-11.1; leftover $3427.78 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 25 | $135.71 | $2.06 | — | $205.47 | — | combo gate; gate news=good,vol=good; list earn_react; ret5=-9.2; leftover $3427.78 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.47 | ▼ close $10,094.04 vs 09:30 $10,283.35 (session -183.16) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.47 | ▼ 09:30 equity $9,980.01 vs yday $10,094.04 (-114.03) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 20 | $141.42 | $2.08 | $-464.33 | $3,031.79 | ▼ -464.33 after sell → book $9,977.93; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 14 | $261.51 | $2.07 | $+266.66 | $6,690.86 | ▲ +266.66 after sell → book $9,975.86; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 25 | $131.40 | $2.10 | $-111.92 | $9,973.76 | ▼ -111.92 after sell → book $9,973.76; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,973.76 | ▲ close $9,973.76 vs 09:30 $9,980.01 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,973.76 | ▲ 09:30 equity $9,973.76 vs yday $9,973.76 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,973.76 | ▲ close $9,973.76 vs 09:30 $9,973.76 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,973.76 | ▲ 09:30 equity $9,973.76 vs yday $9,973.76 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 379 | $26.27 | $4.89 | — | $12.54 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+10.0; leftover $9973.76 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.54 | ▲ close $10,090.15 vs 09:30 $9,973.76 (session +121.28) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.54 | ▼ 09:30 equity $10,059.83 vs yday $10,090.15 (-30.32) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 379 | $26.51 | $5.03 | $+81.04 | $10,054.80 | ▲ +81.04 after sell → book $10,054.80; vs 09:30 mark -5.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 19 | $170.85 | $2.05 | — | $6,806.60 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3351.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 189 | $17.72 | $2.56 | — | $3,454.96 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=-8.3; leftover $3351.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 14 | $238.60 | $2.03 | — | $112.53 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=-11.6; leftover $3351.60 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.53 | ▲ close $10,069.81 vs 09:30 $10,059.83 (session +21.65) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.53 | ▲ 09:30 equity $10,129.57 vs yday $10,069.81 (+59.76) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 19 | $182.33 | $2.08 | $+213.99 | $3,574.72 | ▲ +213.99 after sell → book $10,127.49; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 189 | $17.13 | $2.61 | $-116.68 | $6,809.67 | ▼ -116.68 after sell → book $10,124.87; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 14 | $236.80 | $2.07 | $-29.30 | $10,122.80 | ▼ -29.30 after sell → book $10,122.80; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 179 | $14.07 | $2.53 | — | $7,601.75 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2530.70 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 171 | $14.79 | $2.50 | — | $5,070.15 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2530.70 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 335 | $7.54 | $4.32 | — | $2,541.61 | — | combo gate; gate news=good,vol=good; list yday_mover; 🔵; ret5=-20.9; leftover $2530.70 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 121 | $20.91 | $2.35 | — | $9.15 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2530.70 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.15 | ▼ close $9,944.53 vs 09:30 $10,129.57 (session -166.57) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.15 | ▲ 09:30 equity $10,075.67 vs yday $9,944.53 (+131.14) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 179 | $13.90 | $2.58 | $-35.53 | $2,494.67 | ▼ -35.53 after sell → book $10,073.10; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 171 | $14.58 | $2.55 | $-40.96 | $4,985.30 | ▼ -40.96 after sell → book $10,070.55; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 335 | $7.36 | $4.40 | $-67.34 | $7,446.50 | ▼ -67.34 after sell → book $10,066.15; vs 09:30 mark -4.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 121 | $21.65 | $2.39 | $+84.79 | $10,063.76 | ▲ +84.79 after sell → book $10,063.76; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 936 | $2.15 | $12.07 | — | $8,039.28 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $2012.75 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 144 | $13.94 | $2.42 | — | $6,029.50 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $2012.75 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 335 | $6.00 | $4.32 | — | $4,015.18 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=-24.1; leftover $2012.75 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 10 | $190.30 | $2.02 | — | $2,110.16 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+10.6; leftover $2012.75 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 8 | $230.25 | $2.01 | — | $266.14 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+12.5; leftover $2012.75 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.14 | ▼ close $9,709.60 vs 09:30 $10,075.67 (session -331.30) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.14 | ▼ 09:30 equity $9,684.65 vs yday $9,709.60 (-24.95) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 144 | $13.13 | $2.46 | $-121.52 | $2,154.40 | ▼ -121.52 after sell → book $9,682.19; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 335 | $5.99 | $4.39 | $-12.06 | $4,156.66 | ▼ -12.06 after sell → book $9,677.80; vs 09:30 mark -4.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 1028 | $1.01 | $13.26 | — | $3,105.12 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $1039.17 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,105.12 | ▼ close $9,617.25 vs 09:30 $9,684.65 (session -47.29) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,105.12 | ▲ 09:30 equity $9,914.96 vs yday $9,617.25 (+297.71) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 936 | $2.09 | $12.25 | $-80.48 | $5,049.11 | ▼ -80.48 after sell → book $9,902.71; vs 09:30 mark -12.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 10 | $174.50 | $2.04 | $-162.06 | $6,792.07 | ▼ -162.06 after sell → book $9,900.67; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 8 | $266.50 | $2.04 | $+285.94 | $8,922.03 | ▲ +285.94 after sell → book $9,898.63; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 1028 | $0.95 | $13.03 | $-87.97 | $9,885.60 | ▼ -87.97 after sell → book $9,885.60; vs 09:30 mark -13.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 314 | $15.72 | $4.05 | — | $4,945.47 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $4942.80 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 4010 | $1.22 | $51.73 | — | $1.54 | — | combo gate; gate news=good,vol=good; list yday_mover; 🔵; ret5=-33.0; leftover $4942.80 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.54 | ▼ close $9,265.08 vs 09:30 $9,914.96 (session -564.74) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.54 | ▼ 09:30 equity $9,208.56 vs yday $9,265.08 (-56.52) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 314 | $14.38 | $4.14 | $-428.95 | $4,512.72 | ▼ -428.95 after sell → book $9,204.42; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 4010 | $1.17 | $52.43 | $-304.66 | $9,151.99 | ▼ -304.66 after sell → book $9,151.99; vs 09:30 mark -52.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,151.99 | ▲ close $9,151.99 vs 09:30 $9,208.56 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,998.49 | ▲ 09:30 equity $7,998.49 vs yday $7,998.49 (+0.00) | 09:30 open · cash $7,998.49 · no holdings · equity $7,998.49 vs prior close $7,998.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 1036 | $3.86 | $13.36 | — | $3,986.17 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $3999.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 245 | $16.21 | $3.16 | — | $11.56 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $3999.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.56 | ▼ close $7,837.84 vs 09:30 $7,998.49 (session -144.13) | 16:00 close · cash $11.56 · equity $7,837.84 vs 09:30 $7,998.49 (-160.65; session marks -144.13) · 2 name(s) marked open→close (per-name table). ZSQR×1036 09:30 $3.86 → close $3.78 -82.88; SECZ×245 09:30 $16.21 → close $15.96 -61.25 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
