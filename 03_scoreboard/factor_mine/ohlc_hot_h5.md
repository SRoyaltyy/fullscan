# Factor mine action — `ohlc_hot_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-4.28%** ($9,572) · signal-only (no cash/fees) was +88.18%. Starts YES **4/30**. Fills 174 · skips 429 · realized $-1050.25.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names that looked hot on the prior price/volume tape and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $200.89.

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
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.50 | ▼ close $9,542.55 vs 09:30 $9,914.71 (session -372.16) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.50 | ▼ 09:30 equity $9,474.24 vs yday $9,542.55 (-68.31) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 1 | $8.46 | $0.09 | — | $60.95 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $8.69 | — |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 1 | $7.55 | $0.08 | — | $53.32 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $8.69 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.32 | ▲ close $9,482.15 vs 09:30 $9,474.24 (session +8.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.32 | ▲ 09:30 equity $9,624.15 vs yday $9,482.15 (+142.00) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $1,251.09 | ▼ -41.95 after sell → book $9,621.92; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANRO` | 39 | $34.44 | $2.13 | $+99.90 | $2,592.12 | ▲ +99.90 after sell → book $9,619.79; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LIFE` | 35 | $33.90 | $2.12 | $-44.11 | $3,776.50 | ▼ -44.11 after sell → book $9,617.67; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VOYG` | 28 | $38.84 | $2.09 | $-162.37 | $4,861.93 | ▼ -162.37 after sell → book $9,615.58; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LUNR` | 65 | $18.74 | $2.21 | $-32.34 | $6,077.82 | ▼ -32.34 after sell → book $9,613.37; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BETA` | 49 | $25.56 | $2.16 | $+12.86 | $7,328.11 | ▲ +12.86 after sell → book $9,611.22; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FORM` | 9 | $117.69 | $2.04 | $-110.16 | $8,385.28 | ▼ -110.16 after sell → book $9,609.18; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ENTG` | 7 | $145.64 | $2.03 | $-121.71 | $9,402.73 | ▼ -121.71 after sell → book $9,607.15; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $8,102.92 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1343.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1554 | $0.86 | $18.09 | — | $6,742.17 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1343.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 17 | $78.88 | $2.04 | — | $5,399.17 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1343.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 11 | $119.69 | $2.02 | — | $4,080.56 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1343.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 692 | $1.94 | $8.93 | — | $2,729.15 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1343.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 299 | $4.49 | $3.86 | — | $1,382.79 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1343.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 15 | $89.10 | $2.04 | — | $44.25 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1343.25 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.25 | ▲ close $9,593.53 vs 09:30 $9,624.15 (session +25.36) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.25 | ▲ 09:30 equity $9,681.38 vs yday $9,593.53 (+87.85) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $57.69 | ▼ -4.98 after sell → book $9,681.22; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.70 | $0.40 | $+4.27 | $94.69 | ▲ +4.27 after sell → book $9,680.82; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `LPTH` | 2 | $13.92 | $0.30 | $-2.65 | $122.23 | ▼ -2.65 after sell → book $9,680.52; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CLYM` | 2 | $17.27 | $0.37 | $+1.34 | $156.40 | ▲ +1.34 after sell → book $9,680.15; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BORR` | 7 | $4.50 | $0.36 | $-1.33 | $187.54 | ▼ -1.33 after sell → book $9,679.79; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 5 | $8.08 | $0.44 | $+5.40 | $227.50 | ▲ +5.40 after sell → book $9,679.35; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.50 | ▼ close $9,456.03 vs 09:30 $9,681.38 (session -223.32) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.50 | ▼ 09:30 equity $9,340.83 vs yday $9,456.03 (-115.20) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 4 | $7.25 | $0.30 | — | $198.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $32.50 | — |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 1 | $18.72 | $0.19 | — | $179.29 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.4; leftover $32.50 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 3 | $9.42 | $0.29 | — | $150.74 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $32.50 | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 1 | $19.00 | $0.19 | — | $131.55 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+11.2; leftover $32.50 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 2 | $13.62 | $0.28 | — | $104.02 | — | baseline list, no extra gate; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $32.50 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.02 | ▲ close $9,655.88 vs 09:30 $9,340.83 (session +316.30) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.02 | ▼ 09:30 equity $9,436.43 vs yday $9,655.88 (-219.45) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 1 | $13.63 | $0.14 | — | $90.25 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $17.34 | — |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 7 | $2.29 | $0.18 | — | $74.04 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $17.34 | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 1 | $14.11 | $0.14 | — | $59.78 | — | baseline list, no extra gate; list ohlc_hot; ret5=+11.4; leftover $17.34 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 1 | $14.00 | $0.14 | — | $45.64 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.8; leftover $17.34 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.64 | ▲ close $9,528.01 vs 09:30 $9,436.43 (session +92.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.64 | ▲ 09:30 equity $9,623.01 vs yday $9,528.01 (+95.00) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `ABTC` | 1 | $8.41 | $0.11 | $-0.24 | $53.94 | ▼ -0.24 after sell → book $9,622.90; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SBET` | 1 | $8.45 | $0.11 | $+0.71 | $62.28 | ▲ +0.71 after sell → book $9,622.80; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 4 | $2.60 | $0.12 | — | $51.77 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+13.0; leftover $12.46 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 2 | $4.81 | $0.10 | — | $42.05 | — | baseline list, no extra gate; list ohlc_hot; ret5=+14.8; leftover $12.46 | — |
| 2026-08-27 09:30 ET | **BUY** | `AQST` | 2 | $5.39 | $0.11 | — | $31.15 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.4; leftover $12.46 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.15 | ▲ close $9,687.41 vs 09:30 $9,623.01 (session +64.94) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.15 | ▼ 09:30 equity $9,630.52 vs yday $9,687.41 (-56.89) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 6 | $216.31 | $2.03 | $-3.98 | $1,326.98 | ▼ -3.98 after sell → book $9,628.49; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1554 | $0.83 | $17.89 | $-82.60 | $2,605.13 | ▼ -82.60 after sell → book $9,610.60; vs 09:30 mark -17.89 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRAL` | 17 | $80.18 | $2.06 | $+18.00 | $3,966.13 | ▲ +18.00 after sell → book $9,608.54; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MSTR` | 11 | $134.00 | $2.04 | $+153.34 | $5,438.08 | ▲ +153.34 after sell → book $9,606.49; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRON` | 692 | $2.18 | $9.05 | $+148.10 | $6,937.59 | ▲ +148.10 after sell → book $9,597.44; vs 09:30 mark -9.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 299 | $3.69 | $3.92 | $-246.97 | $8,036.98 | ▼ -246.97 after sell → book $9,593.52; vs 09:30 mark -3.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUGO` | 15 | $89.21 | $2.06 | $-2.44 | $9,373.08 | ▼ -2.44 after sell → book $9,591.47; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 11 | $137.19 | $2.02 | — | $7,861.96 | — | baseline list, no extra gate; list ohlc_hot; ret5=+7.1; leftover $1562.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `SBET` | 180 | $8.65 | $2.53 | — | $6,302.43 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.0; leftover $1562.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRCL` | 16 | $92.61 | $2.04 | — | $4,818.64 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.6; leftover $1562.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 3 | $461.85 | $2.00 | — | $3,431.09 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.8; leftover $1562.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 72 | $21.49 | $2.21 | — | $1,881.60 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.3; leftover $1562.18 | — |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 85 | $18.36 | $2.25 | — | $318.76 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.8; leftover $1562.18 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $318.76 | ▼ close $9,284.26 vs 09:30 $9,630.52 (session -294.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $318.76 | ▼ 09:30 equity $9,186.43 vs yday $9,284.26 (-97.83) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $318.76 | ▲ close $9,514.65 vs 09:30 $9,186.43 (session +328.22) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $318.76 | ▼ 09:30 equity $9,334.21 vs yday $9,514.65 (-180.44) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 4 | $10.77 | $0.46 | $+13.32 | $361.37 | ▲ +13.32 after sell → book $9,333.74; vs 09:30 mark -0.47 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `JANX` | 1 | $18.15 | $0.20 | $-0.96 | $379.32 | ▼ -0.96 after sell → book $9,333.54; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RUM` | 3 | $8.69 | $0.29 | $-2.77 | $405.10 | ▼ -2.77 after sell → book $9,333.25; vs 09:30 mark -0.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 1 | $19.00 | $0.21 | $-0.41 | $423.89 | ▼ -0.41 after sell → book $9,333.04; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `AVAH` | 2 | $13.38 | $0.29 | $-1.06 | $450.35 | ▼ -1.06 after sell → book $9,332.74; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $450.35 | ▲ close $9,392.52 vs 09:30 $9,334.21 (session +59.78) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $450.35 | ▼ 09:30 equity $9,300.73 vs yday $9,392.52 (-91.79) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KURA` | 1 | $12.89 | $0.15 | $-1.03 | $463.09 | ▼ -1.03 after sell → book $9,300.58; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `CNTN` | 7 | $2.26 | $0.20 | $-0.59 | $478.71 | ▼ -0.59 after sell → book $9,300.38; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `BYND` | 1 | $12.35 | $0.15 | $-2.05 | $490.91 | ▼ -2.05 after sell → book $9,300.23; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `MNRO` | 1 | $12.54 | $0.15 | $-1.75 | $503.31 | ▼ -1.75 after sell → book $9,300.09; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $503.31 | ▲ close $9,466.97 vs 09:30 $9,300.73 (session +166.88) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $503.31 | ▲ 09:30 equity $9,525.62 vs yday $9,466.97 (+58.65) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 4 | $2.49 | $0.13 | $-0.69 | $513.13 | ▼ -0.69 after sell → book $9,525.49; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `OABI` | 2 | $5.08 | $0.13 | $+0.31 | $523.17 | ▲ +0.31 after sell → book $9,525.36; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AQST` | 2 | $5.40 | $0.13 | $-0.23 | $533.83 | ▼ -0.23 after sell → book $9,525.23; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 18 | $3.63 | $0.71 | — | $467.79 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 8 | $8.03 | $0.67 | — | $402.88 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 3 | $16.77 | $0.51 | — | $352.06 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 49 | $1.36 | $0.81 | — | $284.60 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 6 | $10.42 | $0.64 | — | $221.44 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 34 | $1.93 | $0.76 | — | $155.06 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 3 | $18.40 | $0.56 | — | $99.30 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-32.2; leftover $66.73 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 4 | $13.71 | $0.56 | — | $43.90 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $66.73 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.90 | ▲ close $9,810.30 vs 09:30 $9,525.62 (session +290.30) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.90 | ▼ 09:30 equity $9,661.79 vs yday $9,810.30 (-148.51) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 11 | $153.62 | $2.05 | $+176.66 | $1,731.67 | ▲ +176.66 after sell → book $9,659.74; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SBET` | 180 | $8.60 | $2.57 | $-14.10 | $3,277.10 | ▼ -14.10 after sell → book $9,657.17; vs 09:30 mark -2.57 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRCL` | 16 | $97.98 | $2.06 | $+81.82 | $4,842.72 | ▲ +81.82 after sell → book $9,655.11; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SNPS` | 3 | $418.12 | $2.02 | $-135.21 | $6,095.06 | ▼ -135.21 after sell → book $9,653.09; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SRPT` | 72 | $22.58 | $2.23 | $+74.04 | $7,718.59 | ▲ +74.04 after sell → book $9,650.86; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `NEO` | 85 | $17.20 | $2.27 | $-103.12 | $9,178.32 | ▼ -103.12 after sell → book $9,648.59; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $8,148.76 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 13 | $82.70 | $2.03 | — | $7,071.64 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 45 | $25.18 | $2.12 | — | $5,936.41 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+16.0; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 145 | $7.87 | $2.42 | — | $4,792.84 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+8.7; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 198 | $5.79 | $2.58 | — | $3,643.83 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 9 | $120.47 | $2.02 | — | $2,557.54 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+13.6; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 290 | $3.95 | $3.74 | — | $1,408.30 | — | baseline list, no extra gate; list ohlc_hot; ret5=+6.9; leftover $1147.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 167 | $6.84 | $2.49 | — | $263.53 | — | baseline list, no extra gate; list ohlc_hot; ret5=+13.2; leftover $1147.29 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.53 | ▲ close $9,894.27 vs 09:30 $9,661.79 (session +265.09) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.53 | ▼ 09:30 equity $9,818.38 vs yday $9,894.27 (-75.89) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.53 | ▼ close $9,649.59 vs 09:30 $9,818.38 (session -168.78) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.53 | ▲ 09:30 equity $9,795.41 vs yday $9,649.59 (+145.82) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.53 | ▼ close $9,278.88 vs 09:30 $9,795.41 (session -516.53) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.53 | ▼ 09:30 equity $9,036.30 vs yday $9,278.88 (-242.58) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.53 | ▼ close $9,026.48 vs 09:30 $9,036.30 (session -9.83) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.53 | ▲ 09:30 equity $9,179.02 vs yday $9,026.48 (+152.54) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 18 | $2.77 | $0.57 | $-16.76 | $312.82 | ▼ -16.76 after sell → book $9,178.45; vs 09:30 mark -0.57 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 8 | $7.70 | $0.66 | $-3.97 | $373.76 | ▼ -3.97 after sell → book $9,177.79; vs 09:30 mark -0.66 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 3 | $14.06 | $0.45 | $-9.09 | $415.48 | ▼ -9.09 after sell → book $9,177.34; vs 09:30 mark -0.45 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SID` | 49 | $1.32 | $0.81 | $-3.59 | $479.35 | ▼ -3.59 after sell → book $9,176.52; vs 09:30 mark -0.82 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `NVAX` | 6 | $9.29 | $0.60 | $-8.02 | $534.50 | ▼ -8.02 after sell → book $9,175.93; vs 09:30 mark -0.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `BMEA` | 34 | $1.75 | $0.72 | $-7.60 | $593.28 | ▼ -7.60 after sell → book $9,175.21; vs 09:30 mark -0.72 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `REAX` | 3 | $18.56 | $0.59 | $-0.67 | $648.37 | ▼ -0.67 after sell → book $9,174.63; vs 09:30 mark -0.58 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CNH` | 4 | $13.76 | $0.58 | $-0.94 | $702.83 | ▼ -0.94 after sell → book $9,174.04; vs 09:30 mark -0.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 43 | $2.04 | $1.01 | — | $614.10 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $87.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 18 | $4.75 | $0.91 | — | $527.69 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $87.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 41 | $2.12 | $0.99 | — | $439.78 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $87.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 14 | $5.91 | $0.87 | — | $356.17 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $87.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 1 | $84.27 | $0.85 | — | $271.06 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $87.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `GPRO` | 62 | $1.40 | $1.05 | — | $183.20 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=-17.2; leftover $87.85 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 4 | $18.30 | $0.74 | — | $109.26 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $87.85 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.26 | ▲ close $9,238.97 vs 09:30 $9,179.02 (session +71.35) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.26 | ▼ 09:30 equity $9,091.52 vs yday $9,238.97 (-147.45) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TARS` | 13 | $79.69 | $2.05 | $-43.21 | $1,143.18 | ▼ -43.21 after sell → book $9,089.47; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASST` | 45 | $27.73 | $2.15 | $+110.48 | $2,388.89 | ▲ +110.48 after sell → book $9,087.33; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `USDE` | 145 | $6.96 | $2.46 | $-136.83 | $3,395.63 | ▼ -136.83 after sell → book $9,084.87; vs 09:30 mark -2.46 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `DFDV` | 198 | $4.91 | $2.63 | $-179.45 | $4,365.18 | ▼ -179.45 after sell → book $9,082.24; vs 09:30 mark -2.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `HOOD` | 9 | $112.26 | $2.04 | $-77.99 | $5,373.48 | ▼ -77.99 after sell → book $9,080.20; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `GORO` | 290 | $3.52 | $3.80 | $-132.24 | $6,390.48 | ▼ -132.24 after sell → book $9,076.40; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `RSKD` | 167 | $6.14 | $2.53 | $-121.92 | $7,413.33 | ▼ -121.92 after sell → book $9,073.87; vs 09:30 mark -2.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,413.33 | ▼ close $9,069.05 vs 09:30 $9,091.52 (session -4.82) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,413.33 | ▲ 09:30 equity $9,081.45 vs yday $9,069.05 (+12.40) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `DELL` | 2 | $541.55 | $2.02 | $+51.53 | $8,494.42 | ▲ +51.53 after sell → book $9,079.44; vs 09:30 mark -2.01 | dropped from list after 6 sess (min 5) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,494.42 | ▼ close $9,077.15 vs 09:30 $9,081.45 (session -2.29) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,494.42 | ▼ 09:30 equity $9,065.29 vs yday $9,077.15 (-11.86) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 13 | $77.12 | $2.03 | — | $7,489.83 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+7.2; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 39 | $27.09 | $2.11 | — | $6,431.21 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 53 | $19.75 | $2.15 | — | $5,382.31 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 67 | $15.75 | $2.19 | — | $4,324.87 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 37 | $28.16 | $2.10 | — | $3,280.85 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 8 | $118.18 | $2.01 | — | $2,333.40 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 7 | $140.88 | $2.01 | — | $1,345.23 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1061.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 72 | $14.62 | $2.21 | — | $290.38 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+13.6; leftover $1061.80 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $290.38 | ▼ close $9,027.33 vs 09:30 $9,065.29 (session -21.15) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.38 | ▲ 09:30 equity $9,088.25 vs yday $9,027.33 (+60.92) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 4 | $10.25 | $0.42 | — | $248.96 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $41.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 5 | $7.59 | $0.39 | — | $210.61 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $41.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 1 | $25.95 | $0.26 | — | $184.40 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $41.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 17 | $2.40 | $0.46 | — | $143.14 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $41.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 1 | $36.76 | $0.37 | — | $106.01 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $41.48 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.01 | ▲ close $9,227.02 vs 09:30 $9,088.25 (session +140.69) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.01 | ▼ 09:30 equity $9,180.65 vs yday $9,227.02 (-46.37) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 43 | $1.90 | $0.97 | $-7.99 | $186.75 | ▼ -7.99 after sell → book $9,179.69; vs 09:30 mark -0.96 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 18 | $4.50 | $0.88 | $-6.29 | $266.86 | ▼ -6.29 after sell → book $9,178.80; vs 09:30 mark -0.89 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 41 | $1.77 | $0.87 | $-16.21 | $338.56 | ▼ -16.21 after sell → book $9,177.93; vs 09:30 mark -0.87 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `DBI` | 14 | $6.07 | $0.91 | $+0.46 | $422.63 | ▲ +0.46 after sell → book $9,177.02; vs 09:30 mark -0.91 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SWKS` | 1 | $92.05 | $0.94 | $+5.99 | $513.74 | ▲ +5.99 after sell → book $9,176.08; vs 09:30 mark -0.94 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `GPRO` | 62 | $1.34 | $1.04 | $-5.81 | $595.78 | ▼ -5.81 after sell → book $9,175.04; vs 09:30 mark -1.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAYP` | 4 | $17.91 | $0.75 | $-3.05 | $666.67 | ▼ -3.05 after sell → book $9,174.29; vs 09:30 mark -0.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 16 | $5.83 | $0.98 | — | $572.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $95.24 | — |
| 2026-09-18 09:30 ET | **BUY** | `CHPT` | 9 | $10.00 | $0.93 | — | $481.48 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $95.24 | — |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 17 | $5.49 | $0.98 | — | $387.26 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.2; leftover $95.24 | — |
| 2026-09-18 09:30 ET | **BUY** | `DNA` | 12 | $7.83 | $0.98 | — | $292.32 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $95.24 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $292.32 | ▼ close $9,024.43 vs 09:30 $9,180.65 (session -146.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $292.32 | ▲ 09:30 equity $9,110.54 vs yday $9,024.43 (+86.11) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 2 | $13.47 | $0.28 | — | $265.10 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $36.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 4 | $8.22 | $0.34 | — | $231.87 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $36.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 2 | $13.94 | $0.28 | — | $203.71 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $36.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `KEEL` | 8 | $4.17 | $0.36 | — | $169.95 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+12.3; leftover $36.54 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.95 | ▼ close $8,892.99 vs 09:30 $9,110.54 (session -216.29) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.95 | ▼ 09:30 equity $8,887.26 vs yday $8,892.99 (-5.73) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 7 | $3.10 | $0.24 | — | $148.01 | — | baseline list, no extra gate; list ohlc_hot; ret5=-1.6; leftover $24.28 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.01 | ▲ close $8,901.93 vs 09:30 $8,887.26 (session +14.91) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.01 | ▲ 09:30 equity $8,997.51 vs yday $8,901.93 (+95.58) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 13 | $73.61 | $2.05 | $-49.71 | $1,102.89 | ▼ -49.71 after sell → book $8,995.46; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 39 | $27.74 | $2.13 | $+21.12 | $2,182.63 | ▲ +21.12 after sell → book $8,993.34; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `FTRE` | 53 | $20.25 | $2.17 | $+22.18 | $3,253.71 | ▲ +22.18 after sell → book $8,991.17; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `REF` | 67 | $13.55 | $2.21 | $-151.80 | $4,159.35 | ▼ -151.80 after sell → book $8,988.96; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `CAI` | 37 | $31.12 | $2.12 | $+105.30 | $5,308.67 | ▲ +105.30 after sell → book $8,986.84; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QRVO` | 8 | $118.44 | $2.03 | $-1.97 | $6,254.15 | ▼ -1.97 after sell → book $8,984.80; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 7 | $142.40 | $2.03 | $+6.60 | $7,248.92 | ▲ +6.60 after sell → book $8,982.77; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `SSL` | 72 | $13.97 | $2.23 | $-51.23 | $8,252.53 | ▼ -51.23 after sell → book $8,980.54; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1058 | $1.30 | $13.65 | — | $6,863.48 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.3; leftover $1375.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 69 | $19.70 | $2.20 | — | $5,501.99 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1375.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `VNET` | 194 | $7.06 | $2.57 | — | $4,129.78 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $1375.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `GRPN` | 64 | $21.29 | $2.18 | — | $2,765.03 | — | baseline list, no extra gate; list ohlc_hot; ret5=+15.4; leftover $1375.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `GME` | 57 | $23.94 | $2.16 | — | $1,398.29 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1375.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `RXT` | 337 | $4.07 | $4.35 | — | $22.35 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1375.42 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.35 | ▼ close $8,791.17 vs 09:30 $8,997.51 (session -162.26) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.35 | ▼ 09:30 equity $8,695.49 vs yday $8,791.17 (-95.68) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 4 | $10.39 | $0.45 | $-0.31 | $63.47 | ▼ -0.31 after sell → book $8,695.05; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 5 | $7.38 | $0.40 | $-1.85 | $99.96 | ▼ -1.85 after sell → book $8,694.64; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 1 | $26.22 | $0.29 | $-0.28 | $125.90 | ▼ -0.28 after sell → book $8,694.36; vs 09:30 mark -0.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `SABR` | 17 | $2.17 | $0.44 | $-4.81 | $162.35 | ▼ -4.81 after sell → book $8,693.92; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `FPS` | 1 | $38.95 | $0.41 | $+1.41 | $200.89 | ▲ +1.41 after sell → book $8,693.51; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $200.89 | ▲ close $9,043.25 vs 09:30 $8,695.49 (session +349.75) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.10 | ▼ 09:30 equity $9,656.75 vs yday $9,659.53 (-2.78) | 09:30 open · cash $282.10 (unchanged overnight, no fees) · equity $9,656.75 vs prior close $9,659.53 (-2.78) · 17 name(s) re-marked at the open (per-name table). BAND×23 yday $61.83 → 09:30 $61.83 +0.00; BNC×6 yday $6.26 → 09:30 $6.26 +0.00; CTKB×7 yday $5.73 → 09:30 $5.73 +0.00; DNA×5 yday $10.25 → 09:30 $10.20 -0.25; FWDI×1 yday $8.35 → 09:30 $8.35 +0.00; GCTS×5 yday $2.24 → 09:30 $2.24 +0.00; GME×55 yday $25.02 → 09:30 $24.96 -3.30; INDP×3 yday $4.00 → 09:30 $4.00 +0.00; KEEL×2 yday $3.90 → 09:30 $3.90 +0.00; LTRX×6 yday $6.60 → 09:30 $6.66 +0.36; MAZE×47 yday $26.21 → 09:30 $26.21 +0.00; MNRO×94 yday $13.98 → 09:30 $13.98 +0.00; SATL×7 yday $5.94 → 09:30 $6.00 +0.42; SGRY×85 yday $14.20 → 09:30 $14.20 +0.00; TWLO×4 yday $299.66 → 09:30 $299.66 +0.00; VERI×1031 yday $1.33 → 09:30 $1.33 +0.00; VNET×1 yday $6.92 → 09:30 $6.92 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `DNA` | 5 | $10.20 | $0.55 | $+10.90 | $332.56 | ▲ +10.90 after sell → book $9,656.21; vs 09:30 mark -0.54 | dropped from list after 5 sess (min 5) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `LTRX` | 6 | $6.66 | $0.44 | $+3.81 | $372.08 | ▲ +3.81 after sell → book $9,655.77; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 2 | $26.27 | $0.53 | — | $319.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $53.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 24 | $2.20 | $0.60 | — | $265.61 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $53.15 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 33 | $1.57 | $0.62 | — | $213.18 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $53.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.18 | ▼ close $9,571.63 vs 09:30 $9,656.75 (session -82.39) | 16:00 close · cash $213.18 · equity $9,571.63 vs 09:30 $9,656.75 (-85.12; session marks -82.39) · 18 name(s) marked open→close (per-name table). BAND×23 09:30 $61.83 → close $61.83 +0.00; BNC×6 09:30 $6.26 → close $6.26 +0.00; CTKB×7 09:30 $5.73 → close $5.73 +0.00; FWDI×1 09:30 $8.35 → close $8.35 +0.00; GCTS×5 09:30 $2.24 → close $2.24 +0.00; GME×55 09:30 $24.96 → close $23.39 -86.35; INDP×3 09:30 $4.00 → close $4.00 +0.00; KEEL×2 09:30 $3.90 → close $3.90 +0.00; MAZE×47 09:30 $26.21 → close $26.21 -0.00; MNRO×94 09:30 $13.98 → close $13.98 -0.00; SATL×7 09:30 $6.00 → close $6.17 +1.19; SGRY×85 09:30 $14.20 → close $14.20 -0.00; TWLO×4 09:30 $299.66 → close $299.66 +0.00; VERI×1031 09:30 $1.33 → close $1.33 +0.00; VNET×1 09:30 $6.92 → close $6.92 +0.00; WRBY×2 09:30 $26.27 → close $26.71 +0.88; HLP×24 09:30 $2.20 → close $2.21 +0.24; PACB×33 09:30 $1.57 → close $1.62 +1.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 35.81 < 1 share @ 152.64 |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LIFE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `VOYG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LUNR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BETA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `FORM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ENTG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LIFE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `VOYG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LUNR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BETA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `FORM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ENTG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `OCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ALM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `LPTH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `CLYM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BORR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `AEM` | cash | leftover split 8.69 < 1 share @ 204.45 |
| 2026-08-20 | `TWST` | cash | leftover split 8.69 < 1 share @ 136.84 |
| 2026-08-20 | `HL` | cash | leftover split 8.69 < 1 share @ 20.25 |
| 2026-08-20 | `PPC` | cash | leftover split 8.69 < 1 share @ 30.65 |
| 2026-08-20 | `ABCL` | cash | leftover split 8.69 < 1 share @ 11.81 |
| 2026-08-20 | `SENS` | cash | leftover split 8.69 < 1 share @ 8.91 |
| 2026-08-21 | `OCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ALM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `LPTH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `CLYM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BORR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GRAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `TRON` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GRAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MSTR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `TRON` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CELH` | cash | leftover split 32.50 < 1 share @ 35.23 |
| 2026-08-25 | `WIX` | cash | leftover split 32.50 < 1 share @ 83.15 |
| 2026-08-26 | `ABTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `SBET` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GRAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `MSTR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `TRON` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUGO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NIQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `FIGR` | cash | leftover split 17.34 < 1 share @ 40.50 |
| 2026-08-26 | `FUTU` | cash | leftover split 17.34 < 1 share @ 124.67 |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GRAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `MSTR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `TRON` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUGO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `CNTN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `MNRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `MRNA` | cash | leftover split 12.46 < 1 share @ 144.18 |
| 2026-08-27 | `FUTU` | cash | leftover split 12.46 < 1 share @ 128.00 |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `JANX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RUM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NIQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CNTN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `MNRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `JANX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RUM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NIQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CNTN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `MNRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `AQST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CRCL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SRPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `NEO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `CNTN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `BYND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `MNRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `AQST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CRCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SRPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `NEO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `AQST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CRCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SNPS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SRPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `NEO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `MRNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SBET` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CRCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SNPS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SRPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `NEO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `REAX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CNH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `REAX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CNH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `TARS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `USDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `HOOD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `GORO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `RSKD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GALT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNDK` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `NVAX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `REAX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CNH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `TARS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ASST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `USDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `HOOD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `GORO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `RSKD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `PAYP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HYLN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SEDG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `NVAX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `REAX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CNH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `TARS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ASST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `USDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DFDV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `HOOD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `GORO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `RSKD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GPRO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `TARS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ASST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `USDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `DFDV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `HOOD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `GORO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `RSKD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `QRVO` | cash | leftover split 87.85 < 1 share @ 112.83 |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TJGC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SION` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `DBI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SWKS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `GPRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAYP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `DBI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `GPRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAYP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `REF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 41.48 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 41.48 < 1 share @ 151.43 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `REF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `SSL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DELL` | cash | leftover split 95.24 < 1 share @ 593.15 |
| 2026-09-18 | `SMTC` | cash | leftover split 95.24 < 1 share @ 182.33 |
| 2026-09-18 | `CRWD` | cash | leftover split 95.24 < 1 share @ 246.98 |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `FTRE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `REF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `CAI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QRVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `SSL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `FPS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `CHPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SATL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `MSTR` | cash | leftover split 36.54 < 1 share @ 164.58 |
| 2026-09-21 | `MXL` | cash | leftover split 36.54 < 1 share @ 83.53 |
| 2026-09-21 | `SMTC` | cash | leftover split 36.54 < 1 share @ 190.30 |
| 2026-09-21 | `VICR` | cash | leftover split 36.54 < 1 share @ 230.25 |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ADPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `FTRE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `REF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `CAI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QRVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `SSL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `ARQT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `SABR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `FPS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `CHPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SATL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `KEEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `FSLY` | cash | leftover split 24.28 < 1 share @ 28.02 |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-22 | `VNET` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `PGEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `ARQT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `SABR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `FPS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `CHPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SATL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `KEEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `BNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `CHPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SATL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `FWDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `KEEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `AMRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `VNET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `GRPN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `GME` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `RXT` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `BNC` | 16 | 2026-09-18 @ $5.83 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $95.24 |
| `CHPT` | 9 | 2026-09-18 @ $10.00 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+11.1; leftover $95.24 |
| `SATL` | 17 | 2026-09-18 @ $5.49 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.2; leftover $95.24 |
| `DNA` | 12 | 2026-09-18 @ $7.83 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $95.24 |
| `BTDR` | 2 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $36.54 |
| `FWDI` | 4 | 2026-09-21 @ $8.22 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $36.54 |
| `MARA` | 2 | 2026-09-21 @ $13.94 | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $36.54 |
| `KEEL` | 8 | 2026-09-21 @ $4.17 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+12.3; leftover $36.54 |
| `INDP` | 7 | 2026-09-22 @ $3.10 | baseline list, no extra gate; list ohlc_hot; ret5=-1.6; leftover $24.28 |
| `VERI` | 1058 | 2026-09-23 @ $1.30 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.3; leftover $1375.42 |
| `AMRX` | 69 | 2026-09-23 @ $19.70 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1375.42 |
| `VNET` | 194 | 2026-09-23 @ $7.06 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+17.5; leftover $1375.42 |
| `GRPN` | 64 | 2026-09-23 @ $21.29 | baseline list, no extra gate; list ohlc_hot; ret5=+15.4; leftover $1375.42 |
| `GME` | 57 | 2026-09-23 @ $23.94 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1375.42 |
| `RXT` | 337 | 2026-09-23 @ $4.07 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1375.42 |
