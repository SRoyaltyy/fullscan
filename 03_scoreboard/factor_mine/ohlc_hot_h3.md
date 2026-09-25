# Factor mine action — `ohlc_hot_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-10.07%** ($8,993) · signal-only (no cash/fees) was +107.52%. Starts YES **4/30**. Fills 188 · skips 271 · realized $-671.83.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names that looked hot on the prior price/volume tape and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: names that looked hot on the prior price/volume tape.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on names that looked hot on the prior price/volume tape that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,807.81.

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
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $250.70 | ▼ close $9,881.56 vs 09:30 $10,000.00 (session -101.60) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.70 | ▲ 09:30 equity $9,917.58 vs yday $9,881.56 (+36.02) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $232.27 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $35.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $199.54 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $35.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 2 | $14.94 | $0.30 | — | $169.36 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $35.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 2 | $16.25 | $0.33 | — | $136.53 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $35.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 7 | $4.59 | $0.34 | — | $104.06 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $35.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 5 | $6.84 | $0.36 | — | $69.50 | — | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $35.81 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.50 | ▲ close $10,176.53 vs 09:30 $9,917.58 (session +260.80) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.50 | ▼ 09:30 equity $9,776.58 vs yday $10,176.53 (-399.95) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.50 | ▲ close $9,860.01 vs 09:30 $9,776.58 (session +83.43) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.50 | ▲ 09:30 equity $9,914.71 vs yday $9,860.01 (+54.70) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $1,241.01 | ▼ -68.20 after sell → book $9,912.47; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANRO` | 39 | $35.00 | $2.13 | $+121.74 | $2,603.88 | ▲ +121.74 after sell → book $9,910.34; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LIFE` | 35 | $34.37 | $2.12 | $-27.66 | $3,804.72 | ▼ -27.66 after sell → book $9,908.23; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VOYG` | 28 | $41.93 | $2.09 | $-75.85 | $4,976.66 | ▼ -75.85 after sell → book $9,906.13; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 65 | $18.98 | $2.21 | $-16.74 | $6,208.16 | ▼ -16.74 after sell → book $9,903.93; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETA` | 49 | $26.80 | $2.16 | $+73.62 | $7,519.20 | ▲ +73.62 after sell → book $9,901.77; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `FORM` | 9 | $126.03 | $2.04 | $-35.10 | $8,651.43 | ▼ -35.10 after sell → book $9,899.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ENTG` | 7 | $152.52 | $2.03 | $-73.55 | $9,717.04 | ▼ -73.55 after sell → book $9,897.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,717.04 | ▼ close $9,895.67 vs 09:30 $9,914.71 (session -2.03) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,717.04 | ▼ 09:30 equity $9,894.83 vs yday $9,895.67 (-0.84) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 1 | $14.10 | $0.16 | $-4.49 | $9,730.98 | ▼ -4.49 after sell → book $9,894.67; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 2 | $15.81 | $0.34 | $-1.45 | $9,762.26 | ▼ -1.45 after sell → book $9,894.33; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `LPTH` | 2 | $13.09 | $0.29 | $-4.29 | $9,788.15 | ▼ -4.29 after sell → book $9,894.04; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CLYM` | 2 | $17.16 | $0.37 | $+1.12 | $9,822.10 | ▲ +1.12 after sell → book $9,893.67; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 7 | $4.46 | $0.35 | $-1.61 | $9,852.97 | ▼ -1.61 after sell → book $9,893.32; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `IOVA` | 5 | $8.07 | $0.44 | $+5.35 | $9,892.88 | ▲ +5.35 after sell → book $9,892.88; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 6 | $204.45 | $2.01 | — | $8,664.17 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 9 | $136.84 | $2.02 | — | $7,430.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 146 | $8.46 | $2.43 | — | $6,193.00 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 61 | $20.25 | $2.17 | — | $4,955.58 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 163 | $7.55 | $2.48 | — | $3,722.45 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 40 | $30.65 | $2.11 | — | $2,494.34 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 104 | $11.81 | $2.30 | — | $1,263.28 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1236.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 138 | $8.91 | $2.40 | — | $31.30 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1236.61 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.30 | ▲ close $9,944.36 vs 09:30 $9,894.83 (session +69.40) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.30 | ▲ 09:30 equity $10,146.87 vs yday $9,944.36 (+202.51) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 6 | $0.86 | $0.07 | — | $26.04 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $5.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 2 | $1.94 | $0.04 | — | $22.12 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $5.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 1 | $4.49 | $0.05 | — | $17.58 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $5.22 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.58 | ▲ close $10,156.25 vs 09:30 $10,146.87 (session +9.55) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.58 | ▲ 09:30 equity $10,162.68 vs yday $10,156.25 (+6.43) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.58 | ▼ close $10,128.67 vs 09:30 $10,162.68 (session -34.01) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.58 | ▼ 09:30 equity $10,096.32 vs yday $10,128.67 (-32.35) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 6 | $212.00 | $2.03 | $+41.26 | $1,287.55 | ▲ +41.26 after sell → book $10,094.29; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TWST` | 9 | $145.73 | $2.04 | $+75.96 | $2,597.08 | ▲ +75.96 after sell → book $10,092.25; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABTC` | 146 | $8.62 | $2.46 | $+18.47 | $3,853.14 | ▲ +18.47 after sell → book $10,089.79; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HL` | 61 | $19.95 | $2.19 | $-22.67 | $5,067.90 | ▼ -22.67 after sell → book $10,087.60; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SBET` | 163 | $8.05 | $2.52 | $+76.50 | $6,377.53 | ▲ +76.50 after sell → book $10,085.08; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `PPC` | 40 | $31.47 | $2.13 | $+28.56 | $7,634.20 | ▲ +28.56 after sell → book $10,082.95; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 104 | $11.00 | $2.33 | $-89.39 | $8,775.87 | ▼ -89.39 after sell → book $10,080.62; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 138 | $9.36 | $2.44 | $+57.26 | $10,065.12 | ▲ +57.26 after sell → book $10,078.19; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 198 | $7.25 | $2.58 | — | $8,627.03 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1437.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 76 | $18.72 | $2.22 | — | $7,202.09 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $1437.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 152 | $9.42 | $2.45 | — | $5,767.81 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1437.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 75 | $19.00 | $2.21 | — | $4,340.59 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+11.2; leftover $1437.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 105 | $13.62 | $2.31 | — | $2,907.66 | — | baseline list, no extra gate; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1437.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `CELH` | 40 | $35.23 | $2.11 | — | $1,496.35 | — | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.0; leftover $1437.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 17 | $83.15 | $2.04 | — | $80.76 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.5; leftover $1437.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.76 | ▲ close $10,456.35 vs 09:30 $10,096.32 (session +394.08) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.76 | ▼ 09:30 equity $10,390.00 vs yday $10,456.35 (-66.35) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 6 | $0.80 | $0.09 | $-0.56 | $85.45 | ▼ -0.56 after sell → book $10,389.91; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `TRON` | 2 | $2.08 | $0.07 | $+0.17 | $89.54 | ▲ +0.17 after sell → book $10,389.84; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 1 | $13.63 | $0.14 | — | $75.78 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $14.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 6 | $2.29 | $0.16 | — | $61.88 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $14.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 1 | $14.11 | $0.14 | — | $47.63 | — | baseline list, no extra gate; list ohlc_hot; ret5=+11.4; leftover $14.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 1 | $14.00 | $0.14 | — | $33.48 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.8; leftover $14.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.48 | ▲ close $10,448.49 vs 09:30 $10,390.00 (session +59.23) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.48 | ▼ 09:30 equity $10,416.74 vs yday $10,448.49 (-31.75) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 2 | $2.60 | $0.06 | — | $28.22 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+13.0; leftover $6.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 1 | $4.81 | $0.05 | — | $23.36 | — | baseline list, no extra gate; list ohlc_hot; ret5=+14.8; leftover $6.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 1 | $5.39 | $0.06 | — | $17.92 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.4; leftover $6.70 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.92 | ▲ close $10,601.69 vs 09:30 $10,416.74 (session +185.11) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.92 | ▼ 09:30 equity $10,515.04 vs yday $10,601.69 (-86.65) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 1 | $3.69 | $0.06 | $-0.91 | $21.55 | ▼ -0.91 after sell → book $10,514.98; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 198 | $9.73 | $2.63 | $+485.82 | $1,945.45 | ▲ +485.82 after sell → book $10,512.34; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `JANX` | 76 | $18.57 | $2.24 | $-15.86 | $3,354.53 | ▼ -15.86 after sell → book $10,510.10; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 152 | $9.30 | $2.48 | $-23.17 | $4,765.65 | ▼ -23.17 after sell → book $10,507.62; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NIQ` | 75 | $19.37 | $2.24 | $+23.30 | $6,216.16 | ▲ +23.30 after sell → book $10,505.38; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVAH` | 105 | $13.90 | $2.33 | $+24.24 | $7,673.33 | ▲ +24.24 after sell → book $10,503.05; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CELH` | 40 | $32.77 | $2.13 | $-102.64 | $8,982.00 | ▼ -102.64 after sell → book $10,500.92; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `WIX` | 17 | $85.32 | $2.06 | $+32.79 | $10,430.37 | ▲ +32.79 after sell → book $10,498.85; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 12 | $137.19 | $2.03 | — | $8,782.07 | — | baseline list, no extra gate; list ohlc_hot; ret5=+7.1; leftover $1738.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `SBET` | 200 | $8.65 | $2.59 | — | $7,049.48 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.0; leftover $1738.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRCL` | 18 | $92.61 | $2.04 | — | $5,380.45 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.6; leftover $1738.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 3 | $461.85 | $2.00 | — | $3,992.91 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.8; leftover $1738.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 80 | $21.49 | $2.23 | — | $2,271.48 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.3; leftover $1738.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 94 | $18.36 | $2.27 | — | $543.36 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.8; leftover $1738.40 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $543.36 | ▼ close $10,168.48 vs 09:30 $10,515.04 (session -317.21) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $543.36 | ▼ 09:30 equity $10,063.56 vs yday $10,168.48 (-104.92) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KURA` | 1 | $12.71 | $0.15 | $-1.21 | $555.92 | ▼ -1.21 after sell → book $10,063.41; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CNTN` | 6 | $2.23 | $0.17 | $-0.69 | $569.13 | ▼ -0.69 after sell → book $10,063.24; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 1 | $12.77 | $0.15 | $-1.52 | $581.75 | ▼ -1.52 after sell → book $10,063.09; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $581.75 | ▲ close $10,424.57 vs 09:30 $10,063.56 (session +361.49) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $581.75 | ▼ 09:30 equity $10,224.89 vs yday $10,424.57 (-199.68) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 1 | $13.04 | $0.15 | $-1.37 | $594.64 | ▼ -1.37 after sell → book $10,224.74; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 2 | $2.67 | $0.08 | $+0.00 | $599.90 | ▼ +0.00 after sell → book $10,224.66; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `OABI` | 1 | $4.35 | $0.07 | $-0.58 | $604.18 | ▼ -0.58 after sell → book $10,224.59; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `AQST` | 1 | $5.15 | $0.07 | $-0.37 | $609.26 | ▼ -0.37 after sell → book $10,224.52; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $609.26 | ▲ close $10,292.30 vs 09:30 $10,224.89 (session +67.78) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $609.26 | ▼ 09:30 equity $10,190.99 vs yday $10,292.30 (-101.31) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SBET` | 200 | $8.01 | $2.64 | $-133.23 | $2,208.62 | ▼ -133.23 after sell → book $10,188.35; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRCL` | 18 | $87.75 | $2.07 | $-91.50 | $3,786.14 | ▼ -91.50 after sell → book $10,186.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 3 | $413.78 | $2.02 | $-148.23 | $5,025.47 | ▼ -148.23 after sell → book $10,184.27; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SRPT` | 80 | $21.33 | $2.26 | $-17.29 | $6,729.61 | ▼ -17.29 after sell → book $10,182.01; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NEO` | 94 | $17.40 | $2.30 | $-94.81 | $8,362.91 | ▼ -94.81 after sell → book $10,179.71; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,362.91 | ▼ close $10,172.63 vs 09:30 $10,190.99 (session -7.08) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,362.91 | ▼ 09:30 equity $10,114.25 vs yday $10,172.63 (-58.38) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 12 | $145.94 | $2.05 | $+100.98 | $10,112.20 | ▲ +100.98 after sell → book $10,112.20; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 348 | $3.63 | $4.49 | — | $8,844.47 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $8.03 | $2.46 | — | $7,581.30 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $6,321.33 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 929 | $1.36 | $11.98 | — | $5,045.91 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 121 | $10.42 | $2.35 | — | $3,782.74 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 654 | $1.93 | $8.44 | — | $2,512.08 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 68 | $18.40 | $2.19 | — | $1,258.68 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-32.2; leftover $1264.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 91 | $13.71 | $2.26 | — | $8.81 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $1264.02 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.81 | ▼ close $9,821.17 vs 09:30 $10,114.25 (session -254.63) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.81 | ▼ 09:30 equity $9,779.47 vs yday $9,821.17 (-41.70) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.81 | ▲ close $9,967.85 vs 09:30 $9,779.47 (session +188.38) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.81 | ▼ 09:30 equity $9,963.53 vs yday $9,967.85 (-4.32) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.81 | ▼ close $9,879.61 vs 09:30 $9,963.53 (session -83.92) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.81 | ▲ 09:30 equity $9,886.46 vs yday $9,879.61 (+6.85) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 348 | $3.28 | $4.56 | $-130.85 | $1,145.69 | ▼ -130.85 after sell → book $9,881.90; vs 09:30 mark -4.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 157 | $8.01 | $2.50 | $-8.10 | $2,400.77 | ▼ -8.10 after sell → book $9,879.41; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 75 | $15.46 | $2.24 | $-102.70 | $3,558.03 | ▼ -102.70 after sell → book $9,877.17; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SID` | 929 | $1.28 | $12.15 | $-98.45 | $4,735.00 | ▼ -98.45 after sell → book $9,865.02; vs 09:30 mark -12.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 121 | $10.02 | $2.38 | $-53.14 | $5,945.04 | ▼ -53.14 after sell → book $9,862.64; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 654 | $1.94 | $8.56 | $-10.45 | $7,205.24 | ▼ -10.45 after sell → book $9,854.08; vs 09:30 mark -8.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `REAX` | 68 | $20.70 | $2.22 | $+151.99 | $8,610.63 | ▲ +151.99 after sell → book $9,851.87; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNH` | 91 | $13.64 | $2.29 | $-10.92 | $9,849.58 | ▼ -10.92 after sell → book $9,849.58; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,849.58 | ▲ close $9,849.58 vs 09:30 $9,886.46 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,849.58 | ▲ 09:30 equity $9,849.58 vs yday $9,849.58 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,849.58 | ▲ close $9,849.58 vs 09:30 $9,849.58 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,849.58 | ▲ 09:30 equity $9,849.58 vs yday $9,849.58 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 603 | $2.04 | $7.78 | — | $8,611.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 259 | $4.75 | $3.34 | — | $7,378.09 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 580 | $2.12 | $7.48 | — | $6,141.01 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 208 | $5.91 | $2.68 | — | $4,909.04 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $3,727.23 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $2,596.86 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 879 | $1.40 | $11.34 | — | $1,354.92 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-17.2; leftover $1231.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 67 | $18.30 | $2.19 | — | $126.63 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1231.20 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.63 | ▲ close $9,869.05 vs 09:30 $9,849.58 (session +58.34) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.63 | ▼ 09:30 equity $9,769.85 vs yday $9,869.05 (-99.20) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.63 | ▲ close $9,775.07 vs 09:30 $9,769.85 (session +5.22) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.63 | ▼ 09:30 equity $9,744.48 vs yday $9,775.07 (-30.59) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.63 | ▲ close $9,798.12 vs 09:30 $9,744.48 (session +53.65) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.63 | ▼ 09:30 equity $9,631.09 vs yday $9,798.12 (-167.03) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 603 | $1.89 | $7.89 | $-106.12 | $1,258.41 | ▼ -106.12 after sell → book $9,623.20; vs 09:30 mark -7.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 259 | $4.73 | $3.39 | $-11.92 | $2,480.09 | ▼ -11.92 after sell → book $9,619.81; vs 09:30 mark -3.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 580 | $1.84 | $7.59 | $-177.47 | $3,539.70 | ▼ -177.47 after sell → book $9,612.22; vs 09:30 mark -7.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 208 | $6.25 | $2.73 | $+65.31 | $4,836.97 | ▲ +65.31 after sell → book $9,609.49; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SWKS` | 14 | $89.38 | $2.05 | $+67.46 | $6,086.24 | ▲ +67.46 after sell → book $9,607.44; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `GPRO` | 879 | $1.31 | $11.49 | $-101.94 | $7,226.24 | ▼ -101.94 after sell → book $9,595.95; vs 09:30 mark -11.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 67 | $17.73 | $2.21 | $-42.59 | $8,411.93 | ▼ -42.59 after sell → book $9,593.73; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,253.10 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+7.2; leftover $1201.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 44 | $27.09 | $2.12 | — | $6,059.02 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1201.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 60 | $19.75 | $2.17 | — | $4,871.85 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1201.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 76 | $15.75 | $2.22 | — | $3,672.63 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1201.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 42 | $28.16 | $2.12 | — | $2,487.79 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1201.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $1,358.74 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1201.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 82 | $14.62 | $2.24 | — | $157.66 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+13.6; leftover $1201.70 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.66 | ▼ close $9,567.66 vs 09:30 $9,631.09 (session -11.16) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.66 | ▲ 09:30 equity $9,629.59 vs yday $9,567.66 (+61.93) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $+16.59 | $1,304.62 | ▲ +16.59 after sell → book $9,627.55; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 1 | $151.43 | $1.52 | — | $1,151.68 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $186.37 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 18 | $10.25 | $1.90 | — | $965.28 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $186.37 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 24 | $7.59 | $1.89 | — | $781.22 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $186.37 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 7 | $25.95 | $1.84 | — | $597.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $186.37 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 77 | $2.40 | $2.08 | — | $410.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $186.37 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 5 | $36.76 | $1.85 | — | $225.20 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $186.37 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.20 | ▲ close $9,739.38 vs 09:30 $9,629.59 (session +122.91) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.20 | ▼ 09:30 equity $9,683.23 vs yday $9,739.38 (-56.15) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 5 | $5.83 | $0.31 | — | $195.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $32.17 | — |
| 2026-09-18 09:30 ET | **BUY** | `CHPT` | 3 | $10.00 | $0.31 | — | $165.44 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $32.17 | — |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 5 | $5.49 | $0.29 | — | $137.72 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.2; leftover $32.17 | — |
| 2026-09-18 09:30 ET | **BUY** | `DNA` | 4 | $7.83 | $0.33 | — | $106.08 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $32.17 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.08 | ▼ close $9,549.42 vs 09:30 $9,683.23 (session -132.59) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.08 | ▲ 09:30 equity $9,635.76 vs yday $9,549.42 (+86.34) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $1,248.07 | ▼ -16.84 after sell → book $9,633.71; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 44 | $28.69 | $2.14 | $+66.14 | $2,508.29 | ▲ +66.14 after sell → book $9,631.57; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FTRE` | 60 | $20.29 | $2.19 | $+28.04 | $3,723.50 | ▲ +28.04 after sell → book $9,629.38; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `REF` | 76 | $14.79 | $2.24 | $-77.42 | $4,845.30 | ▼ -77.42 after sell → book $9,627.14; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 42 | $30.23 | $2.14 | $+82.69 | $6,112.82 | ▲ +82.69 after sell → book $9,625.00; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RVTY` | 8 | $144.53 | $2.03 | $+25.15 | $7,267.03 | ▲ +25.15 after sell → book $9,622.97; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SSL` | 82 | $13.87 | $2.26 | $-66.00 | $8,402.11 | ▼ -66.00 after sell → book $9,620.71; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 77 | $13.47 | $2.22 | — | $7,362.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 127 | $8.22 | $2.37 | — | $6,316.00 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 6 | $164.58 | $2.01 | — | $5,326.52 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 75 | $13.94 | $2.21 | — | $4,278.80 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `KEEL` | 251 | $4.17 | $3.24 | — | $3,227.64 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+12.3; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 12 | $83.53 | $2.03 | — | $2,223.25 | — | baseline list, no extra gate; list ohlc_hot; ret5=+8.8; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $1,269.75 | — | baseline list, no extra gate; list ohlc_hot; ret5=+10.6; leftover $1050.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $346.74 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.5; leftover $1050.26 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $346.74 | ▼ close $9,443.72 vs 09:30 $9,635.76 (session -158.88) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $346.74 | ▼ 09:30 equity $9,405.92 vs yday $9,443.72 (-37.80) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 18 | $10.18 | $1.91 | $-5.07 | $528.08 | ▼ -5.07 after sell → book $9,404.02; vs 09:30 mark -1.90 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `FPS` | 5 | $37.41 | $1.91 | $-0.51 | $713.22 | ▼ -0.51 after sell → book $9,402.11; vs 09:30 mark -1.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 3 | $28.02 | $0.85 | — | $628.31 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $101.89 | — |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 32 | $3.10 | $1.09 | — | $528.02 | — | baseline list, no extra gate; list ohlc_hot; ret5=-1.6; leftover $101.89 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $528.02 | ▲ close $9,497.72 vs 09:30 $9,405.92 (session +97.54) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $528.02 | ▲ 09:30 equity $9,638.28 vs yday $9,497.72 (+140.57) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TWST` | 1 | $164.35 | $1.67 | $+9.74 | $690.71 | ▲ +9.74 after sell → book $9,636.62; vs 09:30 mark -1.66 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 24 | $7.95 | $2.00 | $+4.75 | $879.51 | ▲ +4.75 after sell → book $9,634.62; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 7 | $27.79 | $1.99 | $+9.06 | $1,072.05 | ▲ +9.06 after sell → book $9,632.63; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 77 | $2.24 | $1.98 | $-16.38 | $1,242.55 | ▼ -16.38 after sell → book $9,630.65; vs 09:30 mark -1.98 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 5 | $6.29 | $0.35 | $+1.64 | $1,273.65 | ▲ +1.64 after sell → book $9,630.30; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CHPT` | 3 | $9.76 | $0.32 | $-1.35 | $1,302.61 | ▼ -1.35 after sell → book $9,629.98; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SATL` | 5 | $5.74 | $0.32 | $+0.66 | $1,330.99 | ▲ +0.66 after sell → book $9,629.66; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DNA` | 4 | $9.13 | $0.40 | $+4.48 | $1,367.11 | ▲ +4.48 after sell → book $9,629.26; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 175 | $1.30 | $2.52 | — | $1,137.10 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.3; leftover $227.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 11 | $19.70 | $2.02 | — | $918.38 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $227.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `VNET` | 32 | $7.06 | $2.09 | — | $690.37 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $227.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `GRPN` | 10 | $21.29 | $2.02 | — | $475.45 | — | baseline list, no extra gate; list ohlc_hot; ret5=+15.4; leftover $227.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `GME` | 9 | $23.94 | $2.02 | — | $257.97 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $227.85 | — |
| 2026-09-23 09:30 ET | **BUY** | `RXT` | 55 | $4.07 | $2.15 | — | $31.97 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $227.85 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.97 | ▼ close $9,485.91 vs 09:30 $9,638.28 (session -130.54) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.97 | ▼ 09:30 equity $9,316.65 vs yday $9,485.91 (-169.26) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 77 | $12.26 | $2.24 | $-98.02 | $973.74 | ▼ -98.02 after sell → book $9,314.40; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FWDI` | 127 | $7.94 | $2.40 | $-40.33 | $1,979.72 | ▼ -40.33 after sell → book $9,312.00; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MSTR` | 6 | $161.00 | $2.03 | $-25.52 | $2,943.69 | ▼ -25.52 after sell → book $9,309.97; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 75 | $13.07 | $2.24 | $-69.70 | $3,921.71 | ▼ -69.70 after sell → book $9,307.74; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `KEEL` | 251 | $3.93 | $3.29 | $-68.02 | $4,904.85 | ▼ -68.02 after sell → book $9,304.45; vs 09:30 mark -3.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MXL` | 12 | $82.53 | $2.05 | $-16.01 | $5,893.22 | ▼ -16.01 after sell → book $9,302.40; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 5 | $164.04 | $2.02 | $-135.33 | $6,711.40 | ▼ -135.33 after sell → book $9,300.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+173.42 | $7,807.81 | ▲ +173.42 after sell → book $9,298.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,807.81 | ▲ close $9,352.98 vs 09:30 $9,316.65 (session +54.63) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,640.61 | ▲ 09:30 equity $8,828.81 vs yday $8,828.80 (+0.01) | 09:30 open · cash $8,640.61 (unchanged overnight, no fees) · equity $8,828.81 vs prior close $8,828.80 (+0.01) · 7 name(s) re-marked at the open (per-name table). AMRX×1 yday $19.81 → 09:30 $19.81 +0.00; FSLY×1 yday $26.68 → 09:30 $26.68 +0.00; GCTS×16 yday $2.24 → 09:30 $2.24 +0.00; GRPN×1 yday $20.89 → 09:30 $20.89 +0.00; INDP×11 yday $4.00 → 09:30 $4.00 +0.00; VERI×10 yday $1.33 → 09:30 $1.33 +0.00; VNET×4 yday $6.92 → 09:30 $6.92 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 41 | $26.27 | $2.11 | — | $7,561.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1080.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $6,554.28 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1080.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 490 | $2.20 | $6.32 | — | $5,469.96 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1080.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 180 | $6.00 | $2.53 | — | $4,387.43 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1080.08 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $3,381.06 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1080.08 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 17 | $61.33 | $2.04 | — | $2,336.41 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; leftover $1080.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 687 | $1.57 | $8.86 | — | $1,248.96 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $1080.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `P` | 8 | $122.88 | $2.01 | — | $263.91 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+17.2; leftover $1080.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.91 | ▲ close $8,992.85 vs 09:30 $8,828.81 (session +191.98) | 16:00 close · cash $263.91 · equity $8,992.85 vs 09:30 $8,828.81 (+164.04; session marks +191.98) · 15 name(s) marked open→close (per-name table). AMRX×1 09:30 $19.80 → close $19.80 +0.00; FSLY×1 09:30 $26.68 → close $26.68 +0.00; GCTS×16 09:30 $2.24 → close $2.24 +0.00; GRPN×1 09:30 $20.89 → close $20.89 -0.00; INDP×11 09:30 $4.00 → close $4.00 +0.00; VERI×10 09:30 $1.33 → close $1.33 +0.00; VNET×4 09:30 $6.92 → close $6.92 +0.00; WRBY×41 09:30 $26.27 → close $26.71 +18.04; TXG×12 09:30 $83.76 → close $85.71 +23.40; HLP×490 09:30 $2.20 → close $2.21 +4.90; SATL×180 09:30 $6.00 → close $6.17 +30.60; TEM×12 09:30 $83.69 → close $85.01 +15.78; CDNA×17 09:30 $61.33 → close $63.68 +39.95; PACB×687 09:30 $1.57 → close $1.62 +34.35; P×8 09:30 $122.88 → close $126.00 +24.96 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 35.81 < 1 share @ 152.64 |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PPC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `GRAL` | cash | leftover split 5.22 < 1 share @ 78.88 |
| 2026-08-21 | `MSTR` | cash | leftover split 5.22 < 1 share @ 119.69 |
| 2026-08-21 | `AUGO` | cash | leftover split 5.22 < 1 share @ 89.10 |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PPC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TRON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `TRON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CELH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `WIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FIGR` | cash | leftover split 14.92 < 1 share @ 40.50 |
| 2026-08-26 | `FUTU` | cash | leftover split 14.92 < 1 share @ 124.67 |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CELH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `WIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CNTN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MRNA` | cash | leftover split 6.70 < 1 share @ 144.18 |
| 2026-08-27 | `FUTU` | cash | leftover split 6.70 < 1 share @ 128.00 |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CNTN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SRPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SRPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 1.10 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 1.10 < 1 share @ 82.70 |
| 2026-09-04 | `ASST` | cash | leftover split 1.10 < 1 share @ 25.18 |
| 2026-09-04 | `USDE` | cash | leftover split 1.10 < 1 share @ 7.87 |
| 2026-09-04 | `DFDV` | cash | leftover split 1.10 < 1 share @ 5.79 |
| 2026-09-04 | `HOOD` | cash | leftover split 1.10 < 1 share @ 120.47 |
| 2026-09-04 | `GORO` | cash | leftover split 1.10 < 1 share @ 3.95 |
| 2026-09-04 | `RSKD` | cash | leftover split 1.10 < 1 share @ 6.84 |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GALT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNDK` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PAYP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HYLN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SEDG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GPRO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SION` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 186.37 < 1 share @ 233.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DELL` | cash | leftover split 32.17 < 1 share @ 593.15 |
| 2026-09-18 | `SMTC` | cash | leftover split 32.17 < 1 share @ 182.33 |
| 2026-09-18 | `CRWD` | cash | leftover split 32.17 < 1 share @ 246.98 |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TWST` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CHPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `KEEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MXL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-22 | `VNET` | no_price | no 09:30 open |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `KEEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MXL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FSLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FSLY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `AMRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VNET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GRPN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `RXT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ASPN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CTKB` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CAI` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ILMN` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RBRK` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FSLY` | 3 | 2026-09-22 @ $28.02 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $101.89 |
| `INDP` | 32 | 2026-09-22 @ $3.10 | baseline list, no extra gate; list ohlc_hot; ret5=-1.6; leftover $101.89 |
| `VERI` | 175 | 2026-09-23 @ $1.30 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.3; leftover $227.85 |
| `AMRX` | 11 | 2026-09-23 @ $19.70 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $227.85 |
| `VNET` | 32 | 2026-09-23 @ $7.06 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $227.85 |
| `GRPN` | 10 | 2026-09-23 @ $21.29 | baseline list, no extra gate; list ohlc_hot; ret5=+15.4; leftover $227.85 |
| `GME` | 9 | 2026-09-23 @ $23.94 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $227.85 |
| `RXT` | 55 | 2026-09-23 @ $4.07 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $227.85 |
