# Factor mine action — `union_news_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-33.10%** ($6,690) · signal-only (no cash/fees) was +0.74%. Starts YES **0/30**. Fills 204 · skips 41 · realized $-847.98.

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
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.56 | ▼ 09:30 equity $7,775.12 vs yday $7,837.84 (-62.72) | 09:30 open · cash $11.56 (unchanged overnight, no fees) · equity $7,775.12 vs prior close $7,837.84 (-62.72) · 2 name(s) re-marked at the open (per-name table). SECZ×245 yday $15.96 → 09:30 $16.00 +9.80; ZSQR×1036 yday $3.78 → 09:30 $3.71 -72.52 | — |
| 2026-09-28 09:30 ET | **SELL** | `SECZ` | 245 | $16.00 | $3.23 | $-57.84 | $3,928.33 | ▼ -57.84 after sell → book $7,771.89; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 1036 | $3.71 | $13.57 | $-182.33 | $7,758.32 | ▼ -182.33 after sell → book $7,758.32; vs 09:30 mark -13.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,758.32 | ▲ close $7,758.32 vs 09:30 $7,775.12 (session +0.00) | 16:00 close · cash $7,758.32 · no lots left · equity $7,758.32. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,758.32 | ▲ 09:30 equity $7,758.32 vs yday $7,758.32 (+0.00) | 09:30 open · cash $7,758.32 · no holdings · equity $7,758.32 vs prior close $7,758.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `GLND` | 974 | $3.98 | $12.56 | — | $3,869.24 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+55.9; leftover $3879.16 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BURL` | 14 | $268.37 | $2.03 | — | $110.02 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+5.8; leftover $3879.16 | join🟢 sector🟡 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.02 | ▲ close $8,517.22 vs 09:30 $7,758.32 (session +773.50) | 16:00 close · cash $110.02 · equity $8,517.22 vs 09:30 $7,758.32 (+758.90; session marks +773.50) · 2 name(s) marked open→close (per-name table). GLND×974 09:30 $3.98 → close $4.75 +749.98; BURL×14 09:30 $268.37 → close $270.05 +23.52 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.02 | ▲ 09:30 equity $8,517.22 vs yday $8,517.22 (+0.00) | 09:30 open · cash $110.02 (unchanged overnight, no fees) · equity $8,517.22 vs prior close $8,517.22 (+0.00) · 2 name(s) re-marked at the open (per-name table). BURL×14 yday $270.05 → 09:30 $270.05 +0.00; GLND×974 yday $4.75 → 09:30 $4.75 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.02 | ▲ close $8,517.22 vs 09:30 $8,517.22 (session +0.00) | 16:00 close · cash $110.02 · equity $8,517.22 vs 09:30 $8,517.22 (+0.00; session marks +0.00) · 2 name(s) marked open→close (per-name table). BURL×14 09:30 $270.05 → close $270.05 +0.00; GLND×974 09:30 $4.75 → close $4.75 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.02 | ▲ 09:30 equity $8,816.22 vs yday $8,517.22 (+299.00) | 09:30 open · cash $110.02 (unchanged overnight, no fees) · equity $8,816.22 vs prior close $8,517.22 (+299.00) · 2 name(s) re-marked at the open (per-name table). BURL×14 yday $270.05 → 09:30 $269.84 -2.94; GLND×974 yday $4.75 → 09:30 $5.06 +301.94 | — |
| 2026-10-01 09:30 ET | **SELL** | `BURL` | 14 | $269.84 | $2.07 | $+16.48 | $3,885.71 | ▲ +16.48 after sell → book $8,814.15; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GLND` | 974 | $5.06 | $12.77 | $+1026.59 | $8,801.38 | ▲ +1,026.59 after sell → book $8,801.38; vs 09:30 mark -12.77 | exit unpriced hold on first bar after 2 sess | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 3964 | $1.11 | $51.14 | — | $4,350.21 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; leftover $4400.69 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PRGS` | 107 | $40.52 | $2.31 | — | $12.26 | — | combo gate; gate news=good,vol=good; list earn_react; 🔵; ret5=-2.6; leftover $4400.69 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.26 | ▼ close $8,445.28 vs 09:30 $8,816.22 (session -302.66) | 16:00 close · cash $12.26 · equity $8,445.28 vs 09:30 $8,816.22 (-370.94; session marks -302.66) · 2 name(s) marked open→close (per-name table). TLSA×3964 09:30 $1.11 → close $1.14 +118.92; PRGS×107 09:30 $40.52 → close $36.58 -421.58 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.26 | ▲ 09:30 equity $8,556.66 vs yday $8,445.28 (+111.38) | 09:30 open · cash $12.26 (unchanged overnight, no fees) · equity $8,556.66 vs prior close $8,445.28 (+111.38) · 2 name(s) re-marked at the open (per-name table). PRGS×107 yday $36.58 → 09:30 $36.88 +32.10; TLSA×3964 yday $1.14 → 09:30 $1.16 +79.28 | — |
| 2026-10-02 09:30 ET | **SELL** | `PRGS` | 107 | $36.88 | $2.36 | $-394.15 | $3,956.06 | ▼ -394.15 after sell → book $8,554.30; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `TLSA` | 3964 | $1.16 | $51.83 | $+95.23 | $8,502.47 | ▲ +95.23 after sell → book $8,502.47; vs 09:30 mark -51.83 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `COHR` | 5 | $316.56 | $2.00 | — | $6,917.64 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+9.8; leftover $1700.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ACN` | 8 | $211.02 | $2.01 | — | $5,227.47 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+19.7; leftover $1700.49 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SNPS` | 3 | $497.86 | $2.00 | — | $3,731.90 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; leftover $1700.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `LITE` | 1 | $1053.26 | $1.99 | — | $2,676.65 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+12.6; leftover $1700.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CMPX` | 1808 | $0.94 | $22.42 | — | $954.53 | — | combo gate; gate news=good,vol=good; list yday_mover; 🔵; ret5=-20.6; leftover $1700.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $954.53 | ▼ close $8,334.96 vs 09:30 $8,556.66 (session -137.08) | 16:00 close · cash $954.53 · equity $8,334.96 vs 09:30 $8,556.66 (-221.70; session marks -137.08) · 5 name(s) marked open→close (per-name table). COHR×5 09:30 $316.56 → close $337.04 +102.38; ACN×8 09:30 $211.02 → close $198.90 -96.96; SNPS×3 09:30 $497.86 → close $489.90 -23.87; LITE×1 09:30 $1053.26 → close $1085.42 +32.16; CMPX×1808 09:30 $0.94 → close $0.86 -150.79 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $954.53 | ▲ 09:30 equity $8,437.80 vs yday $8,334.96 (+102.84) | 09:30 open · cash $954.53 (unchanged overnight, no fees) · equity $8,437.80 vs prior close $8,334.96 (+102.84) · 5 name(s) re-marked at the open (per-name table). ACN×8 yday $198.90 → 09:30 $196.30 -20.80; CMPX×1808 yday $0.86 → 09:30 $0.90 +78.29; COHR×5 yday $337.04 → 09:30 $340.93 +19.42; LITE×1 yday $1085.42 → 09:30 $1092.29 +6.87; SNPS×3 yday $489.90 → 09:30 $496.25 +19.05 | — |
| 2026-10-05 09:30 ET | **SELL** | `ACN` | 8 | $196.30 | $2.04 | $-121.81 | $2,522.89 | ▼ -121.81 after sell → book $8,435.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CMPX` | 1808 | $0.90 | $22.01 | $-116.93 | $4,128.08 | ▼ -116.93 after sell → book $8,413.75; vs 09:30 mark -22.01 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `COHR` | 5 | $340.93 | $2.03 | $+117.77 | $5,830.68 | ▲ +117.77 after sell → book $8,411.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `LITE` | 1 | $1092.29 | $2.01 | $+35.02 | $6,920.96 | ▲ +35.02 after sell → book $8,409.71; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `SNPS` | 3 | $496.25 | $2.02 | $-8.83 | $8,407.69 | ▼ -8.83 after sell → book $8,407.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,407.69 | ▲ close $8,407.69 vs 09:30 $8,437.80 (session +0.00) | 16:00 close · cash $8,407.69 · no lots left · equity $8,407.69. | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,407.69 | ▲ 09:30 equity $8,407.69 vs yday $8,407.69 (+0.00) | 09:30 open · cash $8,407.69 · no holdings · equity $8,407.69 vs prior close $8,407.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-06 09:30 ET | **BUY** | `ALVO` | 692 | $6.07 | $8.93 | — | $4,198.32 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; ret5=-0.7; leftover $4203.85 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TWST` | 20 | $209.55 | $2.05 | — | $5.27 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+14.1; leftover $4203.85 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.27 | ▼ close $7,593.55 vs 09:30 $8,407.69 (session -803.16) | 16:00 close · cash $5.27 · equity $7,593.55 vs 09:30 $8,407.69 (-814.14; session marks -803.16) · 2 name(s) marked open→close (per-name table). ALVO×692 09:30 $6.07 → close $6.14 +48.44; TWST×20 09:30 $209.55 → close $166.97 -851.60 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.27 | ▼ 09:30 equity $7,391.67 vs yday $7,593.55 (-201.88) | 09:30 open · cash $5.27 (unchanged overnight, no fees) · equity $7,391.67 vs prior close $7,593.55 (-201.88) · 2 name(s) re-marked at the open (per-name table). ALVO×692 yday $6.14 → 09:30 $6.10 -27.68; TWST×20 yday $166.97 → 09:30 $158.26 -174.20 | — |
| 2026-10-07 09:30 ET | **SELL** | `ALVO` | 692 | $6.10 | $9.08 | $+2.76 | $4,217.39 | ▲ +2.76 after sell → book $7,382.59; vs 09:30 mark -9.08 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,217.39 | ▼ close $7,330.39 vs 09:30 $7,391.67 (session -52.20) | 16:00 close · cash $4,217.39 · equity $7,330.39 vs 09:30 $7,391.67 (-61.28; session marks -52.20) · 1 name(s) marked open→close (per-name table). TWST×20 09:30 $158.26 → close $155.65 -52.20 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,217.39 | ▲ 09:30 equity $7,330.39 vs yday $7,330.39 (+0.00) | 09:30 open · cash $4,217.39 (unchanged overnight, no fees) · equity $7,330.39 vs prior close $7,330.39 (+0.00) · 1 name(s) re-marked at the open (per-name table). TWST×20 yday $155.65 → 09:30 $155.65 +0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,217.39 | ▲ close $7,330.39 vs 09:30 $7,330.39 (session +0.00) | 16:00 close · cash $4,217.39 · equity $7,330.39 vs 09:30 $7,330.39 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). TWST×20 09:30 $155.65 → close $155.65 +0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,217.39 | ▼ 09:30 equity $7,271.19 vs yday $7,330.39 (-59.20) | 09:30 open · cash $4,217.39 (unchanged overnight, no fees) · equity $7,271.19 vs prior close $7,330.39 (-59.20) · 1 name(s) re-marked at the open (per-name table). TWST×20 yday $155.65 → 09:30 $152.69 -59.20 | — |
| 2026-10-09 09:30 ET | **SELL** | `TWST` | 20 | $152.69 | $2.08 | $-1141.33 | $7,269.11 | ▼ -1,141.33 after sell → book $7,269.11; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,269.11 | ▲ close $7,269.11 vs 09:30 $7,271.19 (session +0.00) | 16:00 close · cash $7,269.11 · no lots left · equity $7,269.11. | — |
| 2026-10-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,269.11 | ▲ 09:30 equity $7,269.11 vs yday $7,269.11 (+0.00) | 09:30 open · cash $7,269.11 · no holdings · equity $7,269.11 vs prior close $7,269.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-12 09:30 ET | **BUY** | `DNA` | 572 | $12.69 | $7.38 | — | $3.05 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.3; leftover $7269.11 | join🔴 sector🟡 gen🔴 news🟢 digest🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.05 | ▼ close $6,746.93 vs 09:30 $7,269.11 (session -514.80) | 16:00 close · cash $3.05 · equity $6,746.93 vs 09:30 $7,269.11 (-522.18; session marks -514.80) · 1 name(s) marked open→close (per-name table). DNA×572 09:30 $12.69 → close $11.79 -514.80 | — |
| 2026-10-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.05 | ▲ 09:30 equity $6,746.93 vs yday $6,746.93 (-0.00) | 09:30 open · cash $3.05 (unchanged overnight, no fees) · equity $6,746.93 vs prior close $6,746.93 (-0.00) · 1 name(s) re-marked at the open (per-name table). DNA×572 yday $11.79 → 09:30 $11.79 +0.00 | — |
| 2026-10-13 09:30 ET | **SELL** | `DNA` | 572 | $11.79 | $7.53 | $-529.71 | $6,739.40 | ▼ -529.71 after sell → book $6,739.40; vs 09:30 mark -7.53 | dropped from list after 1 sess (min 1) | — |
| 2026-10-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,746.93 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-10-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-11-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-11-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-11-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-11-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,739.40 | ▲ close $6,739.40 vs 09:30 $6,739.40 (session +0.00) | 16:00 close · cash $6,739.40 · no lots left · equity $6,739.40. | — |
| 2026-11-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,739.40 | ▲ 09:30 equity $6,739.40 vs yday $6,739.40 (+0.00) | 09:30 open · cash $6,739.40 · no holdings · equity $6,739.40 vs prior close $6,739.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-06 09:30 ET | **BUY** | `APP` | 2 | $355.95 | $2.00 | — | $6,025.50 | — | combo gate; gate news=good,vol=good; list flatten; 🔵; ret5=+39.7; leftover $842.42 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `CRCT` | 93 | $9.00 | $2.27 | — | $5,186.23 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+49.7; leftover $842.42 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `KD` | 53 | $15.83 | $2.15 | — | $4,345.10 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+16.1; leftover $842.42 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-11-06 09:30 ET | **BUY** | `OPRT` | 72 | $11.60 | $2.21 | — | $3,507.69 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+16.2; leftover $842.42 | join🔴 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `ALIT` | 66 | $12.61 | $2.19 | — | $2,673.24 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+39.8; leftover $842.42 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `PCOR` | 11 | $73.99 | $2.02 | — | $1,857.33 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+23.7; leftover $842.42 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `LIVN` | 9 | $90.08 | $2.02 | — | $1,044.55 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+12.2; leftover $842.42 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `AHCO` | 115 | $7.27 | $2.33 | — | $206.16 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+25.0; leftover $842.42 | join🔴 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.16 | ▲ close $6,813.08 vs 09:30 $6,739.40 (session +90.87) | 16:00 close · cash $206.16 · equity $6,813.08 vs 09:30 $6,739.40 (+73.68; session marks +90.87) · 8 name(s) marked open→close (per-name table). APP×2 09:30 $355.95 → close $391.22 +70.54; CRCT×93 09:30 $9.00 → close $8.78 -20.46; KD×53 09:30 $15.83 → close $16.29 +24.38; OPRT×72 09:30 $11.60 → close $11.31 -20.88; ALIT×66 09:30 $12.61 → close $12.86 +16.50; PCOR×11 09:30 $73.99 → close $75.33 +14.74; LIVN×9 09:30 $90.08 → close $89.99 -0.85; AHCO×115 09:30 $7.27 → close $7.33 +6.90 | — |
| 2026-11-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.16 | ▲ 09:30 equity $6,827.45 vs yday $6,813.08 (+14.38) | 09:30 open · cash $206.16 (unchanged overnight, no fees) · equity $6,827.45 vs prior close $6,813.08 (+14.38) · 8 name(s) re-marked at the open (per-name table). AHCO×115 yday $7.33 → 09:30 $7.39 +7.47; ALIT×66 yday $12.86 → 09:30 $12.22 -42.24; APP×2 yday $391.22 → 09:30 $387.71 -7.02; CRCT×93 yday $8.78 → 09:30 $8.82 +3.72; KD×53 yday $16.29 → 09:30 $16.29 +0.00; LIVN×9 yday $89.99 → 09:30 $92.31 +20.88; OPRT×72 yday $11.31 → 09:30 $11.68 +26.28; PCOR×11 yday $75.33 → 09:30 $75.81 +5.28 | — |
| 2026-11-09 09:30 ET | **SELL** | `AHCO` | 115 | $7.39 | $2.36 | $+9.68 | $1,054.22 | ▲ +9.68 after sell → book $6,825.09; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `ALIT` | 66 | $12.22 | $2.21 | $-30.14 | $1,858.53 | ▼ -30.14 after sell → book $6,822.88; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `CRCT` | 93 | $8.82 | $2.29 | $-21.30 | $2,676.50 | ▼ -21.30 after sell → book $6,820.59; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `KD` | 53 | $16.29 | $2.17 | $+20.06 | $3,537.70 | ▲ +20.06 after sell → book $6,818.42; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `LIVN` | 9 | $92.31 | $2.04 | $+15.97 | $4,366.45 | ▲ +15.97 after sell → book $6,816.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `OPRT` | 72 | $11.68 | $2.23 | $+0.97 | $5,204.82 | ▲ +0.97 after sell → book $6,814.15; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `PCOR` | 11 | $75.81 | $2.04 | $+15.95 | $6,036.69 | ▲ +15.95 after sell → book $6,812.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **BUY** | `ALGN` | 5 | $167.90 | $2.00 | — | $5,195.19 | — | combo gate; gate news=good,vol=good; list flatten; 🔵; ⚪; ret5=+10.7; leftover $862.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `CLOV` | 176 | $4.88 | $2.52 | — | $4,333.79 | — | combo gate; gate news=good,vol=good; list flatten; 🔵; ret5=+8.1; leftover $862.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `ZVRA` | 74 | $11.65 | $2.21 | — | $3,469.48 | — | combo gate; gate news=good,vol=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+12.7; leftover $862.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `GCT` | 14 | $60.00 | $2.03 | — | $2,627.44 | — | combo gate; gate news=good,vol=good; list probable; 🔵; ret5=-0.0; leftover $862.38 | join🟢 sector🟡 gen🟢 news🟢 digest🔴 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `APPN` | 16 | $51.46 | $2.04 | — | $1,802.05 | — | combo gate; gate news=good,vol=good; list probable; 🔵; ret5=+5.1; leftover $862.38 | join🟢 sector🟡 gen🟢 news🟢 digest🔴 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `VRSK` | 4 | $204.66 | $2.00 | — | $981.40 | — | combo gate; gate news=good,vol=good; list probable; ret5=+4.2; leftover $862.38 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `COLL` | 35 | $24.41 | $2.10 | — | $124.96 | — | combo gate; gate news=good,vol=good; list probable; 🔵; ⚪; ret5=+1.7; leftover $862.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.96 | ▲ close $6,880.97 vs 09:30 $6,827.45 (session +83.76) | 16:00 close · cash $124.96 · equity $6,880.97 vs 09:30 $6,827.45 (+53.52; session marks +83.76) · 8 name(s) marked open→close (per-name table). APP×2 09:30 $387.71 → close $410.00 +44.58; ALGN×5 09:30 $167.90 → close $174.26 +31.80; CLOV×176 09:30 $4.88 → close $4.83 -8.80; ZVRA×74 09:30 $11.65 → close $12.14 +36.26; GCT×14 09:30 $60.00 → close $59.33 -9.38; APPN×16 09:30 $51.46 → close $51.15 -4.96; VRSK×4 09:30 $204.66 → close $200.60 -16.24; COLL×35 09:30 $24.41 → close $24.71 +10.50 | — |
| 2026-11-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.96 | ▼ 09:30 equity $6,795.38 vs yday $6,880.97 (-85.59) | 09:30 open · cash $124.96 (unchanged overnight, no fees) · equity $6,795.38 vs prior close $6,880.97 (-85.59) · 8 name(s) re-marked at the open (per-name table). ALGN×5 yday $174.26 → 09:30 $171.82 -12.20; APP×2 yday $410.00 → 09:30 $405.59 -8.82; APPN×16 yday $51.15 → 09:30 $50.61 -8.64; CLOV×176 yday $4.83 → 09:30 $4.82 -1.76; COLL×35 yday $24.71 → 09:30 $24.46 -8.75; GCT×14 yday $59.33 → 09:30 $58.50 -11.62; VRSK×4 yday $200.60 → 09:30 $198.44 -8.64; ZVRA×74 yday $12.14 → 09:30 $11.80 -25.16 | — |
| 2026-11-10 09:30 ET | **SELL** | `APP` | 2 | $405.59 | $2.02 | $+95.27 | $934.12 | ▲ +95.27 after sell → book $6,793.36; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `APPN` | 16 | $50.61 | $2.06 | $-17.70 | $1,741.83 | ▼ -17.70 after sell → book $6,791.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `CLOV` | 176 | $4.82 | $2.56 | $-15.64 | $2,587.59 | ▼ -15.64 after sell → book $6,788.75; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `COLL` | 35 | $24.46 | $2.12 | $-2.46 | $3,441.57 | ▼ -2.46 after sell → book $6,786.63; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `GCT` | 14 | $58.50 | $2.05 | $-25.08 | $4,258.52 | ▼ -25.08 after sell → book $6,784.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `VRSK` | 4 | $198.44 | $2.02 | $-28.90 | $5,050.26 | ▼ -28.90 after sell → book $6,782.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **BUY** | `ETON` | 13 | $63.63 | $2.03 | — | $4,221.04 | — | combo gate; gate news=good,vol=good; list flatten; 🔵; ⚪; ret5=+26.6; leftover $841.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `IREN` | 40 | $20.80 | $2.11 | — | $3,386.93 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=-2.8; leftover $841.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `GOGO` | 238 | $3.53 | $3.07 | — | $2,543.72 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+56.7; leftover $841.71 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `SERV` | 255 | $3.29 | $3.29 | — | $1,701.48 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.1; leftover $841.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `AOSL` | 29 | $28.57 | $2.08 | — | $870.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+37.3; leftover $841.71 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `CLNE` | 443 | $1.90 | $5.71 | — | $23.46 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+33.1; leftover $841.71 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.46 | ▼ close $6,724.72 vs 09:30 $6,795.38 (session -39.55) | 16:00 close · cash $23.46 · equity $6,724.72 vs 09:30 $6,795.38 (-70.66; session marks -39.55) · 8 name(s) marked open→close (per-name table). ALGN×5 09:30 $171.82 → close $174.26 +12.20; ZVRA×74 09:30 $11.80 → close $11.92 +8.88; ETON×13 09:30 $63.63 → close $67.54 +50.83; IREN×40 09:30 $20.80 → close $19.73 -42.80; GOGO×238 09:30 $3.53 → close $3.60 +16.66; SERV×255 09:30 $3.29 → close $3.11 -45.90; AOSL×29 09:30 $28.57 → close $28.28 -8.41; CLNE×443 09:30 $1.90 → close $1.83 -31.01 | — |
| 2026-11-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.46 | ▲ 09:30 equity $6,878.28 vs yday $6,724.72 (+153.56) | 09:30 open · cash $23.46 (unchanged overnight, no fees) · equity $6,878.28 vs prior close $6,724.72 (+153.56) · 8 name(s) re-marked at the open (per-name table). ALGN×5 yday $174.26 → 09:30 $174.30 +0.20; AOSL×29 yday $28.28 → 09:30 $30.00 +49.88; CLNE×443 yday $1.83 → 09:30 $1.86 +13.29; ETON×13 yday $67.54 → 09:30 $66.94 -7.80; GOGO×238 yday $3.60 → 09:30 $3.58 -4.76; IREN×40 yday $19.73 → 09:30 $20.96 +49.20; SERV×255 yday $3.11 → 09:30 $3.32 +53.55; ZVRA×74 yday $11.92 → 09:30 $11.92 +0.00 | — |
| 2026-11-11 09:30 ET | **SELL** | `ALGN` | 5 | $174.30 | $2.02 | $+27.97 | $892.94 | ▲ +27.97 after sell → book $6,876.26; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `CLNE` | 443 | $1.86 | $5.80 | $-29.23 | $1,711.12 | ▼ -29.23 after sell → book $6,870.46; vs 09:30 mark -5.80 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `ETON` | 13 | $66.94 | $2.05 | $+38.95 | $2,579.29 | ▲ +38.95 after sell → book $6,868.41; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `GOGO` | 238 | $3.58 | $3.12 | $+5.71 | $3,428.21 | ▲ +5.71 after sell → book $6,865.29; vs 09:30 mark -3.12 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `IREN` | 40 | $20.96 | $2.13 | $+2.16 | $4,264.48 | ▲ +2.16 after sell → book $6,863.16; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `SERV` | 255 | $3.32 | $3.34 | $+1.02 | $5,107.74 | ▲ +1.02 after sell → book $6,859.82; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `ZVRA` | 74 | $11.92 | $2.23 | $+15.53 | $5,987.58 | ▲ +15.53 after sell → book $6,857.58; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-11-11 09:30 ET | **BUY** | `RC` | 988 | $1.01 | $12.75 | — | $4,976.96 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-7.3; leftover $997.93 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `CECO` | 17 | $58.47 | $2.04 | — | $3,980.93 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $997.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `FRVO` | 84 | $11.88 | $2.24 | — | $2,981.18 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+22.1; leftover $997.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `LINC` | 42 | $23.32 | $2.12 | — | $1,999.63 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $997.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `SOC` | 1366 | $0.73 | $14.07 | — | $988.24 | — | combo gate; gate news=good,vol=good; list yday_mover; 🔵; ret5=-69.7; leftover $997.93 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `LOAR` | 12 | $79.53 | $2.03 | — | $31.85 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+16.4; leftover $997.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.85 | ▼ close $6,798.44 vs 09:30 $6,878.28 (session -23.90) | 16:00 close · cash $31.85 · equity $6,798.44 vs 09:30 $6,878.28 (-79.84; session marks -23.90) · 7 name(s) marked open→close (per-name table). AOSL×29 09:30 $30.00 → close $30.86 +24.94; RC×988 09:30 $1.01 → close $0.93 -78.94; CECO×17 09:30 $58.47 → close $53.50 -84.49; FRVO×84 09:30 $11.88 → close $12.94 +89.46; LINC×42 09:30 $23.32 → close $23.51 +7.98; SOC×1366 09:30 $0.73 → close $0.75 +20.63; LOAR×12 09:30 $79.53 → close $79.24 -3.48 | — |
| 2026-11-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.85 | ▼ 09:30 equity $6,683.21 vs yday $6,798.44 (-115.23) | 09:30 open · cash $31.85 (unchanged overnight, no fees) · equity $6,683.21 vs prior close $6,798.44 (-115.23) · 7 name(s) re-marked at the open (per-name table). AOSL×29 yday $30.86 → 09:30 $29.12 -50.40; CECO×17 yday $53.50 → 09:30 $54.34 +14.28; FRVO×84 yday $12.94 → 09:30 $12.33 -51.24; LINC×42 yday $23.51 → 09:30 $23.30 -8.82; LOAR×12 yday $79.24 → 09:30 $78.31 -11.16; RC×988 yday $0.93 → 09:30 $0.93 -0.10; SOC×1366 yday $0.75 → 09:30 $0.74 -7.79 | — |
| 2026-11-12 09:30 ET | **SELL** | `AOSL` | 29 | $29.12 | $2.10 | $+11.83 | $874.29 | ▲ +11.83 after sell → book $6,681.11; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `CECO` | 17 | $54.34 | $2.06 | $-74.31 | $1,796.01 | ▼ -74.31 after sell → book $6,679.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `FRVO` | 84 | $12.33 | $2.27 | $+33.71 | $2,829.46 | ▲ +33.71 after sell → book $6,676.78; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `LINC` | 42 | $23.30 | $2.14 | $-5.09 | $3,805.93 | ▼ -5.09 after sell → book $6,674.65; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `LOAR` | 12 | $78.31 | $2.05 | $-18.71 | $4,743.60 | ▼ -18.71 after sell → book $6,672.60; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `RC` | 988 | $0.93 | $12.33 | $-104.11 | $5,650.12 | ▼ -104.11 after sell → book $6,660.27; vs 09:30 mark -12.33 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `SOC` | 1366 | $0.74 | $14.44 | $-15.67 | $6,645.84 | ▼ -15.67 after sell → book $6,645.84; vs 09:30 mark -14.43 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,645.84 | ▲ close $6,645.84 vs 09:30 $6,683.21 (session +0.00) | 16:00 close · cash $6,645.84 · no lots left · equity $6,645.84. | — |
| 2026-11-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,645.84 | ▲ 09:30 equity $6,645.84 vs yday $6,645.84 (+0.00) | 09:30 open · cash $6,645.84 · no holdings · equity $6,645.84 vs prior close $6,645.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-13 09:30 ET | **BUY** | `ETON` | 13 | $61.25 | $2.03 | — | $5,847.56 | — | combo gate; gate news=good,vol=good; list flatten; ret5=+30.3; leftover $830.73 | join🟢 sector🟡 gen🔴 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `REZI` | 38 | $21.52 | $2.10 | — | $5,027.70 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.5; leftover $830.73 | join🔴 sector🟢 gen🔴 news🟢 digest🟡 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `BGS` | 252 | $3.29 | $3.25 | — | $4,195.37 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+35.6; leftover $830.73 | join🔴 sector🟡 gen🔴 news🟢 digest🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `CAPR` | 81 | $10.21 | $2.23 | — | $3,366.12 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+19.1; leftover $830.73 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `AZTA` | 18 | $44.26 | $2.04 | — | $2,567.40 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+17.3; leftover $830.73 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `RUM` | 145 | $5.71 | $2.42 | — | $1,737.02 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.6; leftover $830.73 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `LGN` | 23 | $36.11 | $2.06 | — | $904.44 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+42.7; leftover $830.73 | join🔴 sector🟢 gen🔴 news🟢 digest🟡 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `PLX` | 282 | $2.94 | $3.64 | — | $71.72 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+17.9; leftover $830.73 | join🟢 sector🟡 gen🔴 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.72 | ▲ close $6,690.35 vs 09:30 $6,645.84 (session +64.29) | 16:00 close · cash $71.72 · equity $6,690.35 vs 09:30 $6,645.84 (+44.51; session marks +64.29) · 8 name(s) marked open→close (per-name table). ETON×13 09:30 $61.25 → close $63.93 +34.84; REZI×38 09:30 $21.52 → close $21.32 -7.60; BGS×252 09:30 $3.29 → close $3.50 +52.92; CAPR×81 09:30 $10.21 → close $9.53 -55.08; AZTA×18 09:30 $44.26 → close $45.65 +25.02; RUM×145 09:30 $5.71 → close $5.77 +8.70; LGN×23 09:30 $36.11 → close $35.00 -25.53; PLX×282 09:30 $2.94 → close $3.05 +31.02 | — |

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
