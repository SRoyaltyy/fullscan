# Factor mine action — `ohlc_hot_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+0.19%** ($10,019) · signal-only (no cash/fees) was +6.95%. Starts YES **0/30**. Fills 231 · skips 93 · realized $-1456.67.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names that looked hot on the prior price/volume tape and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,044.84.

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
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,428.21 | ▼ -62.20 after sell → book $9,915.34; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANRO` | 39 | $32.15 | $2.13 | $+10.59 | $2,679.93 | ▲ +10.59 after sell → book $9,913.21; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 35 | $34.03 | $2.12 | $-39.56 | $3,868.87 | ▼ -39.56 after sell → book $9,911.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $5,046.14 | ▼ -70.53 after sell → book $9,909.01; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 49 | $24.61 | $2.16 | $-33.69 | $6,249.87 | ▼ -33.69 after sell → book $9,906.85; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 9 | $134.05 | $2.04 | $+37.08 | $7,454.28 | ▲ +37.08 after sell → book $9,904.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENTG` | 7 | $162.04 | $2.03 | $-6.91 | $8,586.53 | ▼ -6.91 after sell → book $9,902.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 67 | $18.24 | $2.19 | — | $7,362.26 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1226.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 75 | $16.20 | $2.21 | — | $6,145.04 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1226.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 82 | $14.94 | $2.24 | — | $4,917.73 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1226.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $3,694.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1226.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 75 | $16.25 | $2.21 | — | $2,473.63 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1226.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 267 | $4.59 | $3.44 | — | $1,244.66 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1226.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 179 | $6.84 | $2.53 | — | $17.77 | — | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $1226.65 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.77 | ▲ close $9,949.63 vs 09:30 $9,917.58 (session +63.69) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.77 | ▼ 09:30 equity $9,598.26 vs yday $9,949.63 (-351.37) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `LUNR` | 65 | $19.31 | $2.21 | $+4.71 | $1,270.71 | ▲ +4.71 after sell → book $9,596.05; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 67 | $16.20 | $2.21 | $-141.08 | $2,353.90 | ▼ -141.08 after sell → book $9,593.84; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 75 | $15.78 | $2.24 | $-35.95 | $3,535.16 | ▼ -35.95 after sell → book $9,591.60; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 82 | $14.01 | $2.26 | $-80.76 | $4,681.72 | ▼ -80.76 after sell → book $9,589.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 75 | $16.90 | $2.24 | $+44.30 | $5,946.99 | ▲ +44.30 after sell → book $9,587.11; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 267 | $4.56 | $3.50 | $-14.95 | $7,161.01 | ▼ -14.95 after sell → book $9,583.61; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,161.01 | ▼ close $9,470.66 vs 09:30 $9,598.26 (session -112.95) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,161.01 | ▲ 09:30 equity $9,536.61 vs yday $9,470.66 (+65.95) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `AAOI` | 8 | $135.85 | $2.03 | $-138.37 | $8,245.77 | ▼ -138.37 after sell → book $9,534.57; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `IOVA` | 179 | $7.20 | $2.57 | $+59.35 | $9,532.01 | ▲ +59.35 after sell → book $9,532.01; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,532.01 | ▲ close $9,532.01 vs 09:30 $9,536.61 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,532.01 | ▲ 09:30 equity $9,532.01 vs yday $9,532.01 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $8,507.75 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 8 | $136.84 | $2.01 | — | $7,411.02 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 140 | $8.46 | $2.41 | — | $6,224.21 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 58 | $20.25 | $2.16 | — | $5,047.54 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 157 | $7.55 | $2.46 | — | $3,859.73 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 38 | $30.65 | $2.10 | — | $2,692.93 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $1,509.14 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1191.50 | — |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $321.72 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1191.50 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.72 | ▲ close $9,574.73 vs 09:30 $9,532.01 (session +60.56) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.72 | ▲ 09:30 equity $9,764.65 vs yday $9,574.73 (+189.92) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `TWST` | 8 | $138.43 | $2.03 | $+8.67 | $1,427.13 | ▲ +8.67 after sell → book $9,762.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HL` | 58 | $21.33 | $2.18 | $+58.29 | $2,662.08 | ▲ +58.29 after sell → book $9,760.43; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 157 | $7.87 | $2.50 | $+45.28 | $3,895.17 | ▲ +45.28 after sell → book $9,757.93; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PPC` | 38 | $31.13 | $2.12 | $+14.01 | $5,075.99 | ▲ +14.01 after sell → book $9,755.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $6,230.67 | ▼ -29.11 after sell → book $9,753.49; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $7,457.17 | ▲ +39.08 after sell → book $9,751.07; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1438 | $0.86 | $16.74 | — | $6,198.00 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1242.86 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $5,012.77 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1242.86 | — |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 10 | $119.69 | $2.02 | — | $3,813.85 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1242.86 | — |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 640 | $1.94 | $8.26 | — | $2,563.99 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1242.86 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 276 | $4.49 | $3.56 | — | $1,321.19 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1242.86 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 13 | $89.10 | $2.03 | — | $160.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1242.86 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.86 | ▼ close $9,640.34 vs 09:30 $9,764.65 (session -76.09) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.86 | ▲ 09:30 equity $9,729.20 vs yday $9,640.34 (+88.86) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,243.99 | ▲ +58.87 after sell → book $9,727.18; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 140 | $8.00 | $2.44 | $-69.25 | $2,361.54 | ▼ -69.25 after sell → book $9,724.73; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1438 | $0.89 | $17.36 | $+3.29 | $3,624.00 | ▲ +3.29 after sell → book $9,707.37; vs 09:30 mark -17.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $4,850.00 | ▲ +40.76 after sell → book $9,705.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MSTR` | 10 | $121.84 | $2.04 | $+17.44 | $6,066.36 | ▲ +17.44 after sell → book $9,703.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 640 | $2.02 | $8.37 | $+34.57 | $7,350.78 | ▲ +34.57 after sell → book $9,694.90; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUGO` | 13 | $88.60 | $2.05 | $-10.58 | $8,500.54 | ▼ -10.58 after sell → book $9,692.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,500.54 | ▼ close $9,632.14 vs 09:30 $9,729.20 (session -60.72) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,500.54 | ▼ 09:30 equity $9,623.86 vs yday $9,632.14 (-8.28) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 167 | $7.25 | $2.49 | — | $7,287.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1214.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 64 | $18.72 | $2.18 | — | $6,087.03 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $1214.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 128 | $9.42 | $2.37 | — | $4,878.90 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1214.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 63 | $19.00 | $2.18 | — | $3,679.72 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+11.2; leftover $1214.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 89 | $13.62 | $2.26 | — | $2,464.84 | — | baseline list, no extra gate; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1214.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `CELH` | 34 | $35.23 | $2.09 | — | $1,264.93 | — | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.0; leftover $1214.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `WIX` | 14 | $83.15 | $2.03 | — | $98.79 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.5; leftover $1214.36 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.79 | ▲ close $9,925.66 vs 09:30 $9,623.86 (session +317.41) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.79 | ▼ 09:30 equity $9,812.73 vs yday $9,925.66 (-112.93) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `JANX` | 64 | $18.59 | $2.20 | $-12.70 | $1,286.35 | ▼ -12.70 after sell → book $9,810.53; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 128 | $10.07 | $2.41 | $+78.42 | $2,572.91 | ▲ +78.42 after sell → book $9,808.13; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NIQ` | 63 | $19.20 | $2.20 | $+8.22 | $3,780.31 | ▲ +8.22 after sell → book $9,805.93; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 89 | $13.65 | $2.28 | $-2.31 | $4,992.87 | ▼ -2.31 after sell → book $9,803.64; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CELH` | 34 | $35.25 | $2.11 | $-3.52 | $6,189.26 | ▼ -3.52 after sell → book $9,801.53; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WIX` | 14 | $84.02 | $2.05 | $+8.10 | $7,363.49 | ▲ +8.10 after sell → book $9,799.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 90 | $13.63 | $2.26 | — | $6,134.53 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1227.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 535 | $2.29 | $6.90 | — | $4,902.48 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $1227.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 86 | $14.11 | $2.25 | — | $3,686.77 | — | baseline list, no extra gate; list ohlc_hot; ret5=+11.4; leftover $1227.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 30 | $40.50 | $2.08 | — | $2,469.69 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1227.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 87 | $14.00 | $2.25 | — | $1,249.44 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.8; leftover $1227.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 9 | $124.67 | $2.02 | — | $125.39 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.7; leftover $1227.25 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.39 | ▼ close $9,758.55 vs 09:30 $9,812.73 (session -23.17) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.39 | ▼ 09:30 equity $9,719.75 vs yday $9,758.55 (-38.80) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 90 | $12.98 | $2.28 | $-63.04 | $1,291.31 | ▼ -63.04 after sell → book $9,717.47; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CNTN` | 535 | $2.21 | $7.00 | $-56.70 | $2,466.66 | ▼ -56.70 after sell → book $9,710.47; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 30 | $37.42 | $2.10 | $-96.58 | $3,587.16 | ▼ -96.58 after sell → book $9,708.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 87 | $12.56 | $2.28 | $-129.81 | $4,677.60 | ▼ -129.81 after sell → book $9,706.09; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 449 | $2.60 | $5.79 | — | $3,504.41 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+13.0; leftover $1169.40 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 8 | $144.18 | $2.01 | — | $2,348.96 | — | baseline list, no extra gate; list ohlc_hot; ret5=-14.2; leftover $1169.40 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 243 | $4.81 | $3.13 | — | $1,176.99 | — | baseline list, no extra gate; list ohlc_hot; ret5=+14.8; leftover $1169.40 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 216 | $5.39 | $2.79 | — | $9.96 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.4; leftover $1169.40 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.96 | ▼ close $9,626.22 vs 09:30 $9,719.75 (session -66.14) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.96 | ▼ 09:30 equity $9,483.56 vs yday $9,626.22 (-142.66) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 276 | $3.69 | $3.62 | $-227.98 | $1,024.79 | ▼ -227.98 after sell → book $9,479.95; vs 09:30 mark -3.61 | dropped from list after 5 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 167 | $9.73 | $2.53 | $+409.14 | $2,647.17 | ▲ +409.14 after sell → book $9,477.42; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FUTU` | 9 | $124.27 | $2.04 | $-7.65 | $3,763.56 | ▼ -7.65 after sell → book $9,475.38; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OABI` | 243 | $4.54 | $3.19 | $-71.93 | $4,863.60 | ▼ -71.93 after sell → book $9,472.20; vs 09:30 mark -3.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 216 | $5.11 | $2.83 | $-66.10 | $5,964.52 | ▼ -66.10 after sell → book $9,469.36; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SBET` | 137 | $8.65 | $2.40 | — | $4,777.07 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.0; leftover $1192.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRCL` | 12 | $92.61 | $2.03 | — | $3,663.73 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.6; leftover $1192.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $2,738.03 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.8; leftover $1192.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 55 | $21.49 | $2.15 | — | $1,553.92 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.3; leftover $1192.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 64 | $18.36 | $2.18 | — | $376.70 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.8; leftover $1192.90 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $376.70 | ▼ close $9,174.33 vs 09:30 $9,483.56 (session -284.27) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $376.70 | ▼ 09:30 equity $9,112.92 vs yday $9,174.33 (-61.41) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 449 | $2.58 | $5.88 | $-20.65 | $1,529.25 | ▼ -20.65 after sell → book $9,107.05; vs 09:30 mark -5.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SBET` | 137 | $8.24 | $2.43 | $-61.00 | $2,655.69 | ▼ -61.00 after sell → book $9,104.61; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRCL` | 12 | $87.04 | $2.05 | $-70.91 | $3,698.13 | ▼ -70.91 after sell → book $9,102.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $4,572.01 | ▼ -51.81 after sell → book $9,100.55; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 55 | $20.56 | $2.17 | $-55.48 | $5,700.64 | ▼ -55.48 after sell → book $9,098.38; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 64 | $17.77 | $2.20 | $-42.14 | $6,835.71 | ▼ -42.14 after sell → book $9,096.17; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,835.71 | ▲ close $9,102.23 vs 09:30 $9,112.92 (session +6.06) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,835.71 | ▼ 09:30 equity $9,079.15 vs yday $9,102.23 (-23.08) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 86 | $13.04 | $2.27 | $-96.54 | $7,954.88 | ▼ -96.54 after sell → book $9,076.88; vs 09:30 mark -2.27 | dropped from list after 4 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,954.88 | ▲ close $9,189.04 vs 09:30 $9,079.15 (session +112.16) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,954.88 | ▼ 09:30 equity $9,166.08 vs yday $9,189.04 (-22.96) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,954.88 | ▼ close $9,161.36 vs 09:30 $9,166.08 (session -4.72) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,954.88 | ▼ 09:30 equity $9,122.44 vs yday $9,161.36 (-38.92) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 8 | $145.94 | $2.03 | $+10.07 | $9,120.41 | ▲ +10.07 after sell → book $9,120.41; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 314 | $3.63 | $4.05 | — | $7,976.54 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 141 | $8.03 | $2.41 | — | $6,841.89 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 67 | $16.77 | $2.19 | — | $5,716.11 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 838 | $1.36 | $10.81 | — | $4,565.62 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 109 | $10.42 | $2.32 | — | $3,427.52 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 590 | $1.93 | $7.61 | — | $2,281.21 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 61 | $18.40 | $2.17 | — | $1,156.64 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-32.2; leftover $1140.05 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 83 | $13.71 | $2.24 | — | $16.47 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $1140.05 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.47 | ▼ close $8,857.85 vs 09:30 $9,122.44 (session -228.75) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▼ 09:30 equity $8,820.35 vs yday $8,857.85 (-37.50) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 314 | $3.46 | $4.11 | $-61.54 | $1,098.80 | ▼ -61.54 after sell → book $8,816.24; vs 09:30 mark -4.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 141 | $7.91 | $2.45 | $-21.78 | $2,211.66 | ▼ -21.78 after sell → book $8,813.79; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 67 | $15.61 | $2.21 | $-82.12 | $3,255.32 | ▼ -82.12 after sell → book $8,811.58; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 838 | $1.23 | $10.96 | $-130.71 | $4,275.10 | ▼ -130.71 after sell → book $8,800.62; vs 09:30 mark -10.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 109 | $10.50 | $2.35 | $+4.06 | $5,417.26 | ▲ +4.06 after sell → book $8,798.28; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 590 | $1.90 | $7.72 | $-33.03 | $6,530.54 | ▼ -33.03 after sell → book $8,790.56; vs 09:30 mark -7.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 61 | $18.15 | $2.19 | $-19.62 | $7,635.49 | ▼ -19.62 after sell → book $8,788.36; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 83 | $13.89 | $2.26 | $+10.44 | $8,786.10 | ▲ +10.44 after sell → book $8,786.10; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $7,756.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 13 | $82.70 | $2.03 | — | $6,679.42 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 43 | $25.18 | $2.12 | — | $5,594.56 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+16.0; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 139 | $7.87 | $2.41 | — | $4,498.22 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+8.7; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 189 | $5.79 | $2.56 | — | $3,401.35 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 9 | $120.47 | $2.02 | — | $2,315.06 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+13.6; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 278 | $3.95 | $3.59 | — | $1,213.38 | — | baseline list, no extra gate; list ohlc_hot; ret5=+6.9; leftover $1098.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 160 | $6.84 | $2.47 | — | $116.51 | — | baseline list, no extra gate; list ohlc_hot; ret5=+13.2; leftover $1098.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.51 | ▲ close $9,017.94 vs 09:30 $8,820.35 (session +251.02) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.51 | ▼ 09:30 equity $8,945.54 vs yday $9,017.94 (-72.40) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,156.79 | ▲ +10.73 after sell → book $8,943.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 13 | $89.67 | $2.05 | $+86.53 | $2,320.45 | ▲ +86.53 after sell → book $8,941.47; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 43 | $26.44 | $2.14 | $+49.92 | $3,455.23 | ▲ +49.92 after sell → book $8,939.33; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 139 | $7.76 | $2.44 | $-20.14 | $4,531.43 | ▼ -20.14 after sell → book $8,936.89; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HOOD` | 9 | $125.07 | $2.04 | $+37.30 | $5,655.02 | ▲ +37.30 after sell → book $8,934.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 278 | $4.13 | $3.64 | $+42.81 | $6,799.52 | ▲ +42.81 after sell → book $8,931.21; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 160 | $6.46 | $2.51 | $-65.78 | $7,830.62 | ▼ -65.78 after sell → book $8,928.71; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,830.62 | ▲ close $8,962.73 vs 09:30 $8,945.54 (session +34.02) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,830.62 | ▲ 09:30 equity $8,968.40 vs yday $8,962.73 (+5.67) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `DFDV` | 189 | $6.02 | $2.60 | $+38.31 | $8,965.80 | ▲ +38.31 after sell → book $8,965.80; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,965.80 | ▲ close $8,965.80 vs 09:30 $8,968.40 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,965.80 | ▲ 09:30 equity $8,965.80 vs yday $8,965.80 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,965.80 | ▲ close $8,965.80 vs 09:30 $8,965.80 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,965.80 | ▲ 09:30 equity $8,965.80 vs yday $8,965.80 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 549 | $2.04 | $7.08 | — | $7,838.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 235 | $4.75 | $3.03 | — | $6,719.47 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 528 | $2.12 | $6.81 | — | $5,593.30 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 189 | $5.91 | $2.56 | — | $4,473.75 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $3,376.22 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 9 | $112.83 | $2.02 | — | $2,358.68 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 800 | $1.40 | $10.32 | — | $1,228.36 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-17.2; leftover $1120.72 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 61 | $18.30 | $2.17 | — | $109.89 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1120.72 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.89 | ▲ close $8,983.49 vs 09:30 $8,965.80 (session +53.72) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.89 | ▼ 09:30 equity $8,892.87 vs yday $8,983.49 (-90.62) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 549 | $2.01 | $7.18 | $-30.74 | $1,206.20 | ▼ -30.74 after sell → book $8,885.69; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 235 | $4.82 | $3.08 | $+10.34 | $2,335.82 | ▲ +10.34 after sell → book $8,882.61; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 528 | $2.05 | $6.91 | $-50.68 | $3,411.31 | ▼ -50.68 after sell → book $8,875.70; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 189 | $5.86 | $2.60 | $-14.61 | $4,516.25 | ▼ -14.61 after sell → book $8,873.10; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 13 | $86.06 | $2.05 | $+19.19 | $5,632.98 | ▲ +19.19 after sell → book $8,871.05; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 61 | $18.28 | $2.19 | $-5.59 | $6,745.87 | ▼ -5.59 after sell → book $8,868.86; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,745.87 | ▼ close $8,797.69 vs 09:30 $8,892.87 (session -71.17) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,745.87 | ▼ 09:30 equity $8,797.47 vs yday $8,797.69 (-0.22) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `QRVO` | 9 | $108.40 | $2.04 | $-43.97 | $7,719.43 | ▼ -43.97 after sell → book $8,795.43; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,719.43 | ▼ close $8,775.43 vs 09:30 $8,797.47 (session -20.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,719.43 | ▼ 09:30 equity $8,767.43 vs yday $8,775.43 (-8.00) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `GPRO` | 800 | $1.31 | $10.46 | $-92.78 | $8,756.97 | ▼ -92.78 after sell → book $8,756.97; vs 09:30 mark -10.46 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $7,675.26 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+7.2; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 40 | $27.09 | $2.11 | — | $6,589.55 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 55 | $19.75 | $2.15 | — | $5,501.14 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 69 | $15.75 | $2.20 | — | $4,412.19 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 38 | $28.16 | $2.10 | — | $3,340.01 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 9 | $118.18 | $2.02 | — | $2,274.37 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 7 | $140.88 | $2.01 | — | $1,286.20 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1094.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 74 | $14.62 | $2.21 | — | $202.11 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+13.6; leftover $1094.62 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.11 | ▼ close $8,728.83 vs 09:30 $8,767.43 (session -11.30) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.11 | ▲ 09:30 equity $8,785.13 vs yday $8,728.83 (+56.30) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 14 | $76.44 | $2.05 | $-13.60 | $1,270.22 | ▼ -13.60 after sell → book $8,783.08; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 40 | $28.23 | $2.13 | $+41.36 | $2,397.29 | ▲ +41.36 after sell → book $8,780.95; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 55 | $20.31 | $2.17 | $+26.47 | $3,512.16 | ▲ +26.47 after sell → book $8,778.77; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `REF` | 69 | $15.85 | $2.22 | $+2.48 | $4,603.59 | ▲ +2.48 after sell → book $8,776.55; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 38 | $28.59 | $2.12 | $+12.30 | $5,688.08 | ▲ +12.30 after sell → book $8,774.43; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 9 | $114.90 | $2.04 | $-33.57 | $6,720.14 | ▼ -33.57 after sell → book $8,772.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 74 | $13.77 | $2.23 | $-67.35 | $7,736.89 | ▼ -67.35 after sell → book $8,770.16; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 4 | $233.85 | $2.00 | — | $6,799.49 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+11.7; leftover $1105.27 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 7 | $151.43 | $2.01 | — | $5,737.47 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1105.27 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 107 | $10.25 | $2.31 | — | $4,638.41 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1105.27 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 145 | $7.59 | $2.42 | — | $3,535.43 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1105.27 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 42 | $25.95 | $2.12 | — | $2,443.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1105.27 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 460 | $2.40 | $5.93 | — | $1,333.48 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1105.27 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 30 | $36.76 | $2.08 | — | $228.60 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $1105.27 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $228.60 | ▲ close $8,858.96 vs 09:30 $8,785.13 (session +107.68) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $228.60 | ▲ 09:30 equity $8,933.12 vs yday $8,858.96 (+74.16) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 7 | $146.50 | $2.03 | $+35.30 | $1,252.07 | ▲ +35.30 after sell → book $8,931.09; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 4 | $249.13 | $2.02 | $+57.10 | $2,246.57 | ▲ +57.10 after sell → book $8,929.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 7 | $158.04 | $2.03 | $+42.23 | $3,350.82 | ▲ +42.23 after sell → book $8,927.04; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 107 | $10.12 | $2.34 | $-18.56 | $4,431.32 | ▼ -18.56 after sell → book $8,924.70; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 42 | $26.14 | $2.14 | $+3.73 | $5,527.06 | ▲ +3.73 after sell → book $8,922.56; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 460 | $2.29 | $6.02 | $-62.55 | $6,574.44 | ▼ -62.55 after sell → book $8,916.54; vs 09:30 mark -6.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 30 | $39.50 | $2.10 | $+78.02 | $7,757.34 | ▲ +78.02 after sell → book $8,914.44; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $7,162.20 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+16.1; leftover $1108.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 190 | $5.83 | $2.56 | — | $6,051.94 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1108.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `CHPT` | 110 | $10.00 | $2.32 | — | $4,949.62 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $1108.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `SMTC` | 6 | $182.33 | $2.01 | — | $3,853.63 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.0; leftover $1108.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 202 | $5.49 | $2.61 | — | $2,743.05 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.2; leftover $1108.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 4 | $246.98 | $2.00 | — | $1,753.13 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1108.19 | — |
| 2026-09-18 09:30 ET | **BUY** | `DNA` | 141 | $7.83 | $2.41 | — | $646.69 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1108.19 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $646.69 | ▼ close $8,818.15 vs 09:30 $8,933.12 (session -80.39) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $646.69 | ▲ 09:30 equity $8,974.00 vs yday $8,818.15 (+155.85) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 145 | $7.84 | $2.46 | $+31.37 | $1,781.03 | ▲ +31.37 after sell → book $8,971.54; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 1 | $586.77 | $2.01 | $-10.39 | $2,365.79 | ▼ -10.39 after sell → book $8,969.53; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 190 | $6.42 | $2.60 | $+105.99 | $3,582.04 | ▲ +105.99 after sell → book $8,966.93; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CHPT` | 110 | $10.32 | $2.35 | $+30.53 | $4,714.89 | ▲ +30.53 after sell → book $8,964.58; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SATL` | 202 | $5.18 | $2.65 | $-66.87 | $5,758.60 | ▼ -66.87 after sell → book $8,961.93; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 4 | $231.62 | $2.02 | $-65.46 | $6,683.06 | ▼ -65.46 after sell → book $8,959.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DNA` | 141 | $8.05 | $2.45 | $+26.16 | $7,815.66 | ▲ +26.16 after sell → book $8,957.46; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 82 | $13.47 | $2.24 | — | $6,708.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1116.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 135 | $8.22 | $2.40 | — | $5,596.38 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1116.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 6 | $164.58 | $2.01 | — | $4,606.89 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1116.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 80 | $13.94 | $2.23 | — | $3,489.46 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1116.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `KEEL` | 267 | $4.17 | $3.44 | — | $2,371.29 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+12.3; leftover $1116.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 13 | $83.53 | $2.03 | — | $1,283.37 | — | baseline list, no extra gate; list ohlc_hot; ret5=+8.8; leftover $1116.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $360.37 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.5; leftover $1116.52 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $360.37 | ▼ close $8,778.45 vs 09:30 $8,974.00 (session -162.66) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $360.37 | ▼ 09:30 equity $8,742.06 vs yday $8,778.45 (-36.39) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MSTR` | 6 | $167.55 | $2.03 | $+13.78 | $1,363.64 | ▲ +13.78 after sell → book $8,740.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 80 | $13.13 | $2.25 | $-69.28 | $2,411.79 | ▼ -69.28 after sell → book $8,737.78; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `KEEL` | 267 | $4.00 | $3.50 | $-53.67 | $3,476.29 | ▼ -53.67 after sell → book $8,734.28; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 17 | $28.02 | $2.04 | — | $2,997.91 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $496.61 | — |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 160 | $3.10 | $2.47 | — | $2,499.44 | — | baseline list, no extra gate; list ohlc_hot; ret5=-1.6; leftover $496.61 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,499.44 | ▲ close $8,839.19 vs 09:30 $8,742.06 (session +109.42) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,499.44 | ▲ 09:30 equity $8,966.83 vs yday $8,839.19 (+127.64) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 6 | $174.50 | $2.03 | $-51.02 | $3,544.41 | ▼ -51.02 after sell → book $8,964.80; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 82 | $12.84 | $2.26 | $-56.57 | $4,595.03 | ▼ -56.57 after sell → book $8,962.54; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 13 | $86.57 | $2.05 | $+35.44 | $5,718.39 | ▲ +35.44 after sell → book $8,960.49; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 4 | $266.50 | $2.02 | $+140.98 | $6,782.37 | ▲ +140.98 after sell → book $8,958.47; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 17 | $25.90 | $2.06 | $-40.14 | $7,220.61 | ▼ -40.14 after sell → book $8,956.41; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 925 | $1.30 | $11.93 | — | $6,006.18 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.3; leftover $1203.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 61 | $19.70 | $2.17 | — | $4,802.30 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1203.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `VNET` | 170 | $7.06 | $2.50 | — | $3,599.60 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $1203.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `GRPN` | 56 | $21.29 | $2.16 | — | $2,405.21 | — | baseline list, no extra gate; list ohlc_hot; ret5=+15.4; leftover $1203.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `GME` | 50 | $23.94 | $2.14 | — | $1,206.07 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1203.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `RXT` | 295 | $4.07 | $3.81 | — | $1.61 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1203.43 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.61 | ▼ close $8,768.68 vs 09:30 $8,966.83 (session -163.02) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.61 | ▼ 09:30 equity $8,675.50 vs yday $8,768.68 (-93.18) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FWDI` | 135 | $7.94 | $2.43 | $-42.62 | $1,071.08 | ▼ -42.62 after sell → book $8,673.07; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 925 | $1.27 | $12.10 | $-51.78 | $2,233.74 | ▼ -51.78 after sell → book $8,660.98; vs 09:30 mark -12.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMRX` | 61 | $19.29 | $2.19 | $-29.38 | $3,408.23 | ▼ -29.38 after sell → book $8,658.78; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VNET` | 170 | $6.82 | $2.54 | $-45.84 | $4,565.10 | ▼ -45.84 after sell → book $8,656.25; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GRPN` | 56 | $19.90 | $2.18 | $-82.18 | $5,677.32 | ▼ -82.18 after sell → book $8,654.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GME` | 50 | $23.93 | $2.16 | $-4.80 | $6,871.66 | ▼ -4.80 after sell → book $8,651.91; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `RXT` | 295 | $3.99 | $3.86 | $-31.27 | $8,044.84 | ▼ -31.27 after sell → book $8,648.04; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,044.84 | ▲ close $8,684.84 vs 09:30 $8,675.50 (session +36.80) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,288.15 | ▲ 09:30 equity $9,860.15 vs yday $9,860.15 (+0.00) | 09:30 open · cash $8,288.15 (unchanged overnight, no fees) · equity $9,860.15 vs prior close $9,860.15 (+0.00) · 1 name(s) re-marked at the open (per-name table). INDP×393 yday $4.00 → 09:30 $4.00 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $7,261.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1036.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $6,254.37 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1036.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 470 | $2.20 | $6.06 | — | $5,214.30 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1036.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 172 | $6.00 | $2.51 | — | $4,179.80 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1036.02 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $3,173.43 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1036.02 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 16 | $61.33 | $2.04 | — | $2,190.11 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; leftover $1036.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 659 | $1.57 | $8.50 | — | $1,146.98 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $1036.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `P` | 8 | $122.88 | $2.01 | — | $161.93 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+17.2; leftover $1036.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.93 | ▲ close $10,018.66 vs 09:30 $9,860.15 (session +185.79) | 16:00 close · cash $161.93 · equity $10,018.66 vs 09:30 $9,860.15 (+158.51; session marks +185.79) · 9 name(s) marked open→close (per-name table). INDP×393 09:30 $4.00 → close $4.00 +0.00; WRBY×39 09:30 $26.27 → close $26.71 +17.16; TXG×12 09:30 $83.76 → close $85.71 +23.40; HLP×470 09:30 $2.20 → close $2.21 +4.70; SATL×172 09:30 $6.00 → close $6.17 +29.24; TEM×12 09:30 $83.69 → close $85.01 +15.78; CDNA×16 09:30 $61.33 → close $63.68 +37.60; PACB×659 09:30 $1.57 → close $1.62 +32.95; P×8 09:30 $122.88 → close $126.00 +24.96 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `BMEA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GALT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VSTM` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SION` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-22 | `VNET` | no_price | no 09:30 open |
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
| `INDP` | 160 | 2026-09-22 @ $3.10 | baseline list, no extra gate; list ohlc_hot; ret5=-1.6; leftover $496.61 |
