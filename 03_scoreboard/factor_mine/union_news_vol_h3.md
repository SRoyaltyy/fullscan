# Factor mine action — `union_news_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-18.05%** ($8,194) · signal-only (no cash/fees) was +139.18%. Starts YES **0/30**. Fills 94 · skips 133 · realized $-1334.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,352.19.

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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,230.40 vs 09:30 $10,215.56 (session +14.85) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▼ 09:30 equity $10,119.58 vs yday $10,230.40 (-110.82) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▼ close $10,082.08 vs 09:30 $10,119.58 (session -37.50) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,122.41 vs yday $10,082.08 (+40.33) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 102 | $19.58 | $2.33 | $-3.60 | $2,354.74 | ▼ -3.60 after sell → book $10,120.08; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $4,035.13 | ▲ +31.47 after sell → book $10,118.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 147 | $13.01 | $2.47 | $-84.28 | $5,945.13 | ▼ -84.28 after sell → book $10,115.59; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 151 | $12.90 | $2.48 | $-47.21 | $7,890.55 | ▼ -47.21 after sell → book $10,113.11; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,890.55 | ▼ close $10,024.95 vs 09:30 $10,122.41 (session -88.16) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,890.55 | ▼ 09:30 equity $10,011.03 vs yday $10,024.95 (-13.92) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 464 | $4.57 | $6.08 | $+108.57 | $10,004.95 | ▲ +108.57 after sell → book $10,004.95; vs 09:30 mark -6.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,819.79 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,616.65 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1768 | $0.71 | $17.80 | — | $6,348.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 189 | $6.61 | $2.56 | — | $5,097.97 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 78 | $16.00 | $2.22 | — | $3,847.75 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $2,596.83 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,361.44 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1250.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $150.85 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1250.62 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.85 | ▼ close $9,786.14 vs 09:30 $10,011.03 (session -185.93) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.85 | ▲ 09:30 equity $10,032.67 vs yday $9,786.14 (+246.53) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $130.87 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $21.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $119.05 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $21.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 1 | $11.10 | $0.11 | — | $107.84 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $21.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 6 | $3.24 | $0.21 | — | $88.19 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $21.55 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.19 | ▲ close $10,063.19 vs 09:30 $10,032.67 (session +31.18) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.19 | ▲ 09:30 equity $10,094.01 vs yday $10,063.19 (+30.82) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.19 | ▼ close $10,078.10 vs 09:30 $10,094.01 (session -15.90) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.19 | ▼ 09:30 equity $10,031.12 vs yday $10,078.10 (-46.98) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,332.32 | ▲ +58.97 after sell → book $10,029.08; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $2,478.29 | ▼ -57.17 after sell → book $10,027.04; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1768 | $0.66 | $17.33 | $-112.93 | $3,633.14 | ▼ -112.93 after sell → book $10,009.71; vs 09:30 mark -17.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 189 | $6.75 | $2.60 | $+22.25 | $4,906.29 | ▲ +22.25 after sell → book $10,007.11; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 78 | $19.04 | $2.25 | $+232.65 | $6,389.16 | ▲ +232.65 after sell → book $10,004.86; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $26.04 | $2.15 | $-29.19 | $7,610.89 | ▼ -29.19 after sell → book $10,002.71; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $8,825.35 | ▼ -20.93 after sell → book $10,000.64; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $9,940.52 | ▼ -95.42 after sell → book $9,998.55; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 211 | $9.42 | $2.72 | — | $7,950.18 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1988.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 56 | $35.05 | $2.16 | — | $5,985.22 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1988.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 82 | $24.11 | $2.24 | — | $4,005.96 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+891.7; leftover $1988.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 16 | $118.52 | $2.04 | — | $2,107.61 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1988.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 25 | $77.13 | $2.06 | — | $177.29 | — | combo gate; gate news=good,vol=good; list mover_buy; ⚪; ret5=+13.8; leftover $1988.10 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.29 | ▲ close $10,672.71 vs 09:30 $10,031.12 (session +685.38) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.29 | ▼ 09:30 equity $10,443.17 vs yday $10,672.71 (-229.54) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $196.33 | ▼ -0.94 after sell → book $10,442.93; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $207.76 | ▼ -0.40 after sell → book $10,442.80; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTDR` | 1 | $11.05 | $0.13 | $-0.29 | $218.67 | ▼ -0.29 after sell → book $10,442.66; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 6 | $2.95 | $0.21 | $-2.17 | $236.16 | ▼ -2.17 after sell → book $10,442.45; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.16 | ▼ close $10,157.82 vs 09:30 $10,443.17 (session -284.63) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.16 | ▼ 09:30 equity $10,092.70 vs yday $10,157.82 (-65.12) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.16 | ▼ close $9,945.41 vs 09:30 $10,092.70 (session -147.29) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.16 | ▼ 09:30 equity $9,920.55 vs yday $9,945.41 (-24.86) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 211 | $9.30 | $2.77 | $-30.81 | $2,195.68 | ▼ -30.81 after sell → book $9,917.77; vs 09:30 mark -2.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 56 | $34.50 | $2.18 | $-35.14 | $4,125.50 | ▼ -35.14 after sell → book $9,915.59; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 82 | $23.40 | $2.27 | $-62.72 | $6,042.04 | ▼ -62.72 after sell → book $9,913.33; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 16 | $119.19 | $2.06 | $+6.62 | $7,947.01 | ▲ +6.62 after sell → book $9,911.26; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 25 | $78.57 | $2.09 | $+31.84 | $9,909.17 | ▲ +31.84 after sell → book $9,909.17; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $8,492.35 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1415.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 145 | $9.73 | $2.42 | — | $7,079.08 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1415.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $5,801.22 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1415.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 73 | $19.25 | $2.21 | — | $4,393.76 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+14.1; leftover $1415.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 75 | $18.75 | $2.21 | — | $2,985.30 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-5.0; leftover $1415.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 48 | $28.91 | $2.13 | — | $1,595.48 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+9.2; leftover $1415.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 74 | $19.00 | $2.21 | — | $187.27 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+7.5; leftover $1415.60 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.27 | ▼ close $9,596.33 vs 09:30 $9,920.55 (session -297.51) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.27 | ▼ 09:30 equity $9,531.31 vs yday $9,596.33 (-65.02) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.27 | ▲ close $9,736.61 vs 09:30 $9,531.31 (session +205.30) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.27 | ▼ 09:30 equity $9,731.75 vs yday $9,736.61 (-4.86) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.27 | ▼ close $9,606.79 vs 09:30 $9,731.75 (session -124.96) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.27 | ▲ 09:30 equity $9,628.56 vs yday $9,606.79 (+21.77) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 43 | $32.42 | $2.14 | $-24.90 | $1,579.19 | ▼ -24.90 after sell → book $9,626.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 145 | $10.07 | $2.46 | $+44.41 | $3,036.88 | ▲ +44.41 after sell → book $9,623.96; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $4,231.84 | ▼ -82.89 after sell → book $9,621.92; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 73 | $16.97 | $2.23 | $-170.88 | $5,468.42 | ▼ -170.88 after sell → book $9,619.69; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 75 | $18.41 | $2.24 | $-29.95 | $6,846.93 | ▼ -29.95 after sell → book $9,617.45; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 48 | $30.00 | $2.16 | $+48.03 | $8,284.78 | ▲ +48.03 after sell → book $9,615.30; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 74 | $17.98 | $2.23 | $-79.93 | $9,613.06 | ▼ -79.93 after sell → book $9,613.06; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,613.06 | ▲ close $9,613.06 vs 09:30 $9,628.56 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,613.06 | ▲ 09:30 equity $9,613.06 vs yday $9,613.06 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 80 | $23.88 | $2.23 | — | $7,700.43 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1922.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $6,291.94 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1922.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 121 | $15.87 | $2.35 | — | $4,369.31 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1922.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $2,908.38 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+6.1; leftover $1922.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 59 | $32.31 | $2.17 | — | $999.93 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1922.61 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $999.93 | ▲ close $9,875.96 vs 09:30 $9,613.06 (session +273.64) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $999.93 | ▼ 09:30 equity $9,791.07 vs yday $9,875.96 (-84.89) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 512 | $1.94 | $6.60 | — | $0.04 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $999.93 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.04 | ▲ close $9,811.80 vs 09:30 $9,791.07 (session +27.34) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.04 | ▲ 09:30 equity $9,844.44 vs yday $9,811.80 (+32.64) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.04 | ▼ close $9,814.52 vs 09:30 $9,844.44 (session -29.92) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.04 | ▲ 09:30 equity $9,868.84 vs yday $9,814.52 (+54.32) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 80 | $23.22 | $2.26 | $-57.29 | $1,855.38 | ▼ -57.29 after sell → book $9,866.58; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $3,216.01 | ▼ -47.87 after sell → book $9,864.57; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 121 | $15.96 | $2.39 | $+6.15 | $5,144.78 | ▲ +6.15 after sell → book $9,862.18; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $6,758.17 | ▲ +152.46 after sell → book $9,860.16; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 59 | $35.09 | $2.19 | $+159.66 | $8,826.28 | ▲ +159.66 after sell → book $9,857.96; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,826.28 | ▼ close $9,834.92 vs 09:30 $9,868.84 (session -23.04) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,826.28 | ▲ 09:30 equity $9,834.92 vs yday $9,834.92 (+0.00) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 512 | $1.97 | $6.70 | $+2.06 | $9,828.22 | ▲ +2.06 after sell → book $9,828.22; vs 09:30 mark -6.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,828.22 | ▲ close $9,828.22 vs 09:30 $9,834.92 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,828.22 | ▲ 09:30 equity $9,828.22 vs yday $9,828.22 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 19 | $164.43 | $2.05 | — | $6,702.01 | — | combo gate; gate news=good,vol=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $3276.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 13 | $242.17 | $2.03 | — | $3,551.77 | — | combo gate; gate news=good,vol=good; list earn_react; ret5=-11.1; leftover $3276.07 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 24 | $135.71 | $2.06 | — | $292.67 | — | combo gate; gate news=good,vol=good; list earn_react; ret5=-9.2; leftover $3276.07 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $292.67 | ▼ close $9,644.66 vs 09:30 $9,828.22 (session -177.43) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $292.67 | ▼ 09:30 equity $9,532.88 vs yday $9,644.66 (-111.78) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $292.67 | ▲ close $9,716.56 vs 09:30 $9,532.88 (session +183.68) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $292.67 | ▼ 09:30 equity $9,626.19 vs yday $9,716.56 (-90.37) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $292.67 | ▼ close $9,306.12 vs 09:30 $9,626.19 (session -320.07) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $292.67 | ▼ 09:30 equity $9,259.86 vs yday $9,306.12 (-46.26) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 19 | $140.03 | $2.08 | $-467.73 | $2,951.16 | ▼ -467.73 after sell → book $9,257.78; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 13 | $253.34 | $2.07 | $+141.12 | $6,242.51 | ▲ +141.12 after sell → book $9,255.71; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 24 | $125.55 | $2.10 | $-248.00 | $9,253.62 | ▼ -248.00 after sell → book $9,253.62; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 352 | $26.27 | $4.54 | — | $2.04 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+10.0; leftover $9253.62 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▲ close $9,361.72 vs 09:30 $9,259.86 (session +112.64) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▼ 09:30 equity $9,333.56 vs yday $9,361.72 (-28.16) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▲ close $9,333.56 vs 09:30 $9,333.56 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $9,488.44 vs yday $9,333.56 (+154.88) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $9,034.36 vs 09:30 $9,488.44 (session -454.08) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $9,132.92 vs yday $9,034.36 (+98.56) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 352 | $25.94 | $4.67 | $-125.37 | $9,128.24 | ▼ -125.37 after sell → book $9,128.24; vs 09:30 mark -4.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 849 | $2.15 | $10.95 | — | $7,291.94 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1825.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 130 | $13.94 | $2.38 | — | $5,477.36 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1825.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 304 | $6.00 | $3.92 | — | $3,649.44 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=-24.1; leftover $1825.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 9 | $190.30 | $2.02 | — | $1,934.72 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+10.6; leftover $1825.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $320.96 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+12.5; leftover $1825.65 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.96 | ▼ close $8,809.40 vs 09:30 $9,132.92 (session -297.56) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.96 | ▼ 09:30 equity $8,786.86 vs yday $8,809.40 (-22.54) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 79 | $1.01 | $1.03 | — | $240.14 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $80.24 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.14 | ▲ close $8,856.31 vs 09:30 $8,786.86 (session +70.49) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.14 | ▲ 09:30 equity $9,107.22 vs yday $8,856.31 (+250.91) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 7 | $15.72 | $1.12 | — | $128.98 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $120.07 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 98 | $1.22 | $1.49 | — | $7.93 | — | combo gate; gate news=good,vol=good; list yday_mover; 🔵; ret5=-33.0; leftover $120.07 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.93 | ▼ close $8,827.15 vs 09:30 $9,107.22 (session -277.45) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.93 | ▼ 09:30 equity $8,663.36 vs yday $8,827.15 (-163.79) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 849 | $1.88 | $11.11 | $-251.29 | $1,592.94 | ▼ -251.29 after sell → book $8,652.25; vs 09:30 mark -11.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 130 | $13.07 | $2.42 | $-117.90 | $3,289.62 | ▼ -117.90 after sell → book $8,649.83; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SION` | 304 | $5.50 | $3.99 | $-159.91 | $4,957.64 | ▼ -159.91 after sell → book $8,645.85; vs 09:30 mark -3.98 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 9 | $164.04 | $2.04 | $-240.40 | $6,431.96 | ▼ -240.40 after sell → book $8,643.81; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 7 | $274.61 | $2.04 | $+306.47 | $8,352.19 | ▲ +306.47 after sell → book $8,641.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,352.19 | ▼ close $8,634.97 vs 09:30 $8,663.36 (session -6.80) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,908.82 | ▼ 09:30 equity $8,360.31 vs yday $8,363.73 (-3.42) | 09:30 open · cash $7,908.82 (unchanged overnight, no fees) · equity $8,360.31 vs prior close $8,363.73 (-3.42) · 3 name(s) re-marked at the open (per-name table). CMPX×4 yday $1.13 → 09:30 $1.13 +0.00; GRAL×2 yday $125.21 → 09:30 $123.50 -3.42; IVVD×219 yday $0.91 → 09:30 $0.91 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 2 | $123.50 | $2.02 | $+29.49 | $8,153.80 | ▲ +29.49 after sell → book $8,358.29; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 1056 | $3.86 | $13.62 | — | $4,064.02 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $4076.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 250 | $16.21 | $3.23 | — | $8.30 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $4076.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.30 | ▼ close $8,194.46 vs 09:30 $8,360.31 (session -146.98) | 16:00 close · cash $8.30 · equity $8,194.46 vs 09:30 $8,360.31 (-165.85; session marks -146.98) · 4 name(s) marked open→close (per-name table). CMPX×4 09:30 $1.14 → close $1.14 -0.00; IVVD×219 09:30 $0.91 → close $0.91 -0.00; ZSQR×1056 09:30 $3.86 → close $3.78 -84.48; SECZ×250 09:30 $16.21 → close $15.96 -62.50 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.55 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 21.55 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 21.55 < 1 share @ 623.26 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 0.68 < 1 share @ 170.85 |
| 2026-09-17 | `TNDM` | cash | leftover split 0.68 < 1 share @ 17.72 |
| 2026-09-17 | `JBHT` | cash | leftover split 0.68 < 1 share @ 238.60 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BHVN` | cash | leftover split 0.51 < 1 share @ 14.07 |
| 2026-09-18 | `RARE` | cash | leftover split 0.51 < 1 share @ 14.79 |
| 2026-09-18 | `FLNC` | cash | leftover split 0.51 < 1 share @ 7.54 |
| 2026-09-18 | `TH` | cash | leftover split 0.51 < 1 share @ 20.91 |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `IVVD` | 79 | 2026-09-22 @ $1.01 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $80.24 |
| `SGRY` | 7 | 2026-09-23 @ $15.72 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $120.07 |
| `CMPX` | 98 | 2026-09-23 @ $1.22 | combo gate; gate news=good,vol=good; list yday_mover; 🔵; ret5=-33.0; leftover $120.07 |
