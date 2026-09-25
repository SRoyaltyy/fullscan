# Factor mine action — `probable_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-7.10%** ($9,290) · signal-only (no cash/fees) was -1.12%. Starts YES **3/30**. Fills 252 · skips 107 · realized $-305.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's 'likely to keep moving' list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on yesterday's 'likely to keep moving' list that pass the must-haves.
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

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,694.16.

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
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | — | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | — | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.08 | ▲ close $9,985.46 vs 09:30 $10,000.00 (session +9.14) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.08 | ▲ 09:30 equity $10,059.20 vs yday $9,985.46 (+73.74) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,599.28 | ▲ +76.56 after sell → book $10,055.40; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,855.89 | ▲ +18.44 after sell → book $10,053.21; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,077.88 | ▼ -31.69 after sell → book $10,049.30; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $5,126.92 | ▲ +40.05 after sell → book $10,047.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FOSL` | 221 | $5.50 | $2.90 | $-36.69 | $6,339.52 | ▼ -36.69 after sell → book $10,044.38; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $7,517.04 | ▼ -62.20 after sell → book $10,042.15; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRS` | 370 | $3.40 | $4.84 | $-0.37 | $8,768.34 | ▼ -0.37 after sell → book $10,037.30; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $10,035.21 | ▲ +31.11 after sell → book $10,035.21; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $8,797.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $7,545.93 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 56 | $22.37 | $2.16 | — | $6,291.05 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $5,036.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,826.05 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 121 | $10.35 | $2.35 | — | $2,571.34 | — | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $1,328.83 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1254.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $79.21 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1254.40 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.21 | ▼ close $9,888.06 vs 09:30 $10,059.20 (session -129.60) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.21 | ▼ 09:30 equity $9,722.67 vs yday $9,888.06 (-165.39) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $1,365.77 | ▲ +49.13 after sell → book $9,720.56; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $2,600.45 | ▼ -17.16 after sell → book $9,718.13; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 56 | $21.18 | $2.18 | $-70.98 | $3,784.35 | ▼ -70.98 after sell → book $9,715.95; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $5,034.62 | ▼ -3.84 after sell → book $9,713.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,233.51 | ▼ -12.01 after sell → book $9,711.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BW` | 121 | $9.60 | $2.38 | $-95.49 | $7,392.73 | ▼ -95.49 after sell → book $9,709.39; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $8,492.12 | ▼ -143.13 after sell → book $9,707.18; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $9,704.93 | ▼ -36.80 after sell → book $9,704.93; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,704.93 | ▲ close $9,704.93 vs 09:30 $9,722.67 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,704.93 | ▲ 09:30 equity $9,704.93 vs yday $9,704.93 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,704.93 | ▲ close $9,704.93 vs 09:30 $9,704.93 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,704.93 | ▲ 09:30 equity $9,704.93 vs yday $9,704.93 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 163 | $7.44 | $2.48 | — | $8,489.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 162 | $7.45 | $2.48 | — | $7,280.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $6,146.04 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $4,937.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $3,724.22 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 144 | $8.38 | $2.42 | — | $2,515.07 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $1,315.76 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1213.12 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $151.87 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.4; leftover $1213.12 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.87 | ▲ close $9,816.80 vs 09:30 $9,704.93 (session +130.11) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.87 | ▲ 09:30 equity $10,180.70 vs yday $9,816.80 (+363.90) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 163 | $8.28 | $2.52 | $+131.92 | $1,498.99 | ▲ +131.92 after sell → book $10,178.18; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 162 | $7.09 | $2.51 | $-63.31 | $2,645.06 | ▼ -63.31 after sell → book $10,175.67; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $3,839.92 | ▲ +60.54 after sell → book $10,173.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $5,107.64 | ▲ +59.16 after sell → book $10,171.27; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $6,418.52 | ▲ +97.62 after sell → book $10,168.87; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 144 | $9.02 | $2.46 | $+87.28 | $7,714.94 | ▲ +87.28 after sell → book $10,166.41; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $8,932.56 | ▲ +18.30 after sell → book $10,164.28; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $10,162.23 | ▲ +65.78 after sell → book $10,162.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 765 | $1.66 | $9.87 | — | $8,882.46 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 742 | $1.71 | $9.57 | — | $7,604.07 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $6,355.55 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 84 | $14.96 | $2.24 | — | $5,096.67 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1470 | $0.86 | $17.11 | — | $3,809.48 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 408 | $3.11 | $5.26 | — | $2,535.34 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 408 | $3.11 | $5.26 | — | $1,261.19 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1270.28 | — |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $112.31 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1270.28 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.31 | ▼ close $10,073.54 vs 09:30 $10,180.70 (session -35.36) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.31 | ▲ 09:30 equity $10,137.11 vs yday $10,073.54 (+63.57) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 765 | $1.55 | $10.01 | $-104.02 | $1,288.05 | ▼ -104.02 after sell → book $10,127.10; vs 09:30 mark -10.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 742 | $1.74 | $9.71 | $+2.98 | $2,569.42 | ▲ +2.98 after sell → book $10,117.39; vs 09:30 mark -9.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,873.49 | ▲ +55.55 after sell → book $10,115.38; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 84 | $14.74 | $2.27 | $-22.99 | $5,109.38 | ▼ -22.99 after sell → book $10,113.11; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1470 | $0.89 | $17.75 | $+3.36 | $6,399.93 | ▲ +3.36 after sell → book $10,095.36; vs 09:30 mark -17.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 408 | $3.20 | $5.34 | $+26.12 | $7,700.19 | ▲ +26.12 after sell → book $10,090.02; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QTRX` | 408 | $2.99 | $5.34 | $-59.56 | $8,914.77 | ▼ -59.56 after sell → book $10,084.68; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $10,082.65 | ▲ +18.99 after sell → book $10,082.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,082.65 | ▲ close $10,082.65 vs 09:30 $10,137.11 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,082.65 | ▲ 09:30 equity $10,082.65 vs yday $10,082.65 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 173 | $7.25 | $2.51 | — | $8,825.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3520 | $0.36 | $23.16 | — | $7,542.56 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 113 | $11.12 | $2.33 | — | $6,283.68 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 92 | $13.59 | $2.27 | — | $5,031.13 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 132 | $9.49 | $2.39 | — | $3,776.06 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 34 | $36.96 | $2.09 | — | $2,517.33 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 276 | $4.55 | $3.56 | — | $1,257.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1260.33 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 57 | $21.79 | $2.16 | — | $13.78 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.1; leftover $1260.33 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.78 | ▲ close $10,284.93 vs 09:30 $10,082.65 (session +242.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.78 | ▼ 09:30 equity $10,227.52 vs yday $10,284.93 (-57.41) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 173 | $8.29 | $2.55 | $+174.86 | $1,445.40 | ▲ +174.86 after sell → book $10,224.97; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3520 | $0.35 | $23.58 | $-64.34 | $2,664.38 | ▼ -64.34 after sell → book $10,201.39; vs 09:30 mark -23.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 113 | $11.03 | $2.36 | $-14.86 | $3,908.41 | ▼ -14.86 after sell → book $10,199.03; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 92 | $13.63 | $2.29 | $-0.88 | $5,160.08 | ▼ -0.88 after sell → book $10,196.74; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 132 | $9.89 | $2.42 | $+48.00 | $6,463.14 | ▲ +48.00 after sell → book $10,194.32; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 34 | $38.24 | $2.11 | $+39.32 | $7,761.19 | ▲ +39.32 after sell → book $10,192.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 276 | $4.31 | $3.62 | $-73.42 | $8,947.14 | ▼ -73.42 after sell → book $10,188.60; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 57 | $21.78 | $2.18 | $-4.91 | $10,186.41 | ▼ -4.91 after sell → book $10,186.41; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 40 | $31.21 | $2.11 | — | $8,935.90 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 114 | $11.12 | $2.33 | — | $7,665.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 129 | $9.83 | $2.38 | — | $6,395.45 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 72 | $17.51 | $2.21 | — | $5,132.52 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 105 | $12.04 | $2.31 | — | $3,866.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 134 | $9.48 | $2.39 | — | $2,593.30 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 5 | $213.94 | $2.00 | — | $1,521.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1273.30 | — |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 19 | $65.34 | $2.05 | — | $278.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1273.30 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $278.09 | ▲ close $10,257.19 vs 09:30 $10,227.52 (session +88.55) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $278.09 | ▲ 09:30 equity $10,417.87 vs yday $10,257.19 (+160.68) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AXTI` | 19 | $70.30 | $2.07 | $+90.13 | $1,611.72 | ▲ +90.13 after sell → book $10,415.80; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 123 | $12.98 | $2.36 | — | $12.82 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1611.72 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.82 | ▲ close $10,495.87 vs 09:30 $10,417.87 (session +82.43) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.82 | ▼ 09:30 equity $10,428.21 vs yday $10,495.87 (-67.66) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 40 | $30.53 | $2.13 | $-31.44 | $1,231.89 | ▼ -31.44 after sell → book $10,426.08; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 114 | $11.27 | $2.36 | $+12.41 | $2,514.31 | ▲ +12.41 after sell → book $10,423.72; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 129 | $9.88 | $2.41 | $+1.66 | $3,786.42 | ▲ +1.66 after sell → book $10,421.31; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVEX` | 72 | $18.75 | $2.23 | $+84.85 | $5,134.19 | ▲ +84.85 after sell → book $10,419.08; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ITG` | 105 | $12.79 | $2.33 | $+74.11 | $6,474.81 | ▲ +74.11 after sell → book $10,416.75; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SENS` | 134 | $9.39 | $2.42 | $-16.88 | $7,730.65 | ▼ -16.88 after sell → book $10,414.32; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 5 | $215.71 | $2.02 | $+4.79 | $8,807.15 | ▲ +4.79 after sell → book $10,412.30; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 123 | $13.05 | $2.39 | $+3.86 | $10,409.91 | ▲ +3.86 after sell → book $10,409.91; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $9,124.70 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 83 | $15.66 | $2.24 | — | $7,822.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $6,549.92 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 391 | $3.32 | $5.04 | — | $5,246.76 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAFX` | 3565 | $0.36 | $23.71 | — | $3,921.82 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.6; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $2,658.62 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 151 | $8.61 | $2.44 | — | $1,356.07 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.7; leftover $1301.24 | — |
| 2026-08-28 09:30 ET | **BUY** | `XPOF` | 241 | $5.38 | $3.11 | — | $56.38 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.5; leftover $1301.24 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.38 | ▼ close $10,141.80 vs 09:30 $10,428.21 (session -225.41) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.38 | ▼ 09:30 equity $10,114.46 vs yday $10,141.80 (-27.34) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $1,269.10 | ▼ -72.48 after sell → book $10,112.33; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 83 | $14.44 | $2.26 | $-105.76 | $2,465.36 | ▼ -105.76 after sell → book $10,110.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $3,750.34 | ▲ +12.22 after sell → book $10,108.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 391 | $3.20 | $5.12 | $-57.08 | $4,996.42 | ▼ -57.08 after sell → book $10,102.89; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAFX` | 3565 | $0.36 | $24.20 | $-58.60 | $6,262.75 | ▼ -58.60 after sell → book $10,078.69; vs 09:30 mark -24.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $7,495.97 | ▼ -29.98 after sell → book $10,076.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 151 | $8.52 | $2.48 | $-18.51 | $8,780.02 | ▼ -18.51 after sell → book $10,074.19; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `XPOF` | 241 | $5.37 | $3.16 | $-8.68 | $10,071.03 | ▼ -8.68 after sell → book $10,071.03; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,071.03 | ▲ close $10,071.03 vs 09:30 $10,114.46 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,071.03 | ▲ 09:30 equity $10,071.03 vs yday $10,071.03 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,071.03 | ▲ close $10,071.03 vs 09:30 $10,071.03 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,071.03 | ▲ 09:30 equity $10,071.03 vs yday $10,071.03 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,071.03 | ▲ close $10,071.03 vs 09:30 $10,071.03 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,071.03 | ▲ 09:30 equity $10,071.03 vs yday $10,071.03 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 81 | $15.45 | $2.23 | — | $8,817.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,647.77 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $6,387.80 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.85 | $2.24 | — | $5,138.16 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $3,916.87 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-25.9; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 577 | $2.18 | $7.44 | — | $2,651.56 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 90 | $13.96 | $2.26 | — | $1,392.90 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $1258.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3339 | $0.38 | $22.61 | — | $111.49 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-2.3; leftover $1258.88 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.49 | ▼ close $9,986.27 vs 09:30 $10,071.03 (session -41.69) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.49 | ▲ 09:30 equity $9,995.07 vs yday $9,986.27 (+8.80) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 81 | $15.00 | $2.26 | $-40.94 | $1,324.24 | ▼ -40.94 after sell → book $9,992.81; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $2,551.16 | ▲ +57.35 after sell → book $9,990.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $15.61 | $2.24 | $-91.45 | $3,719.68 | ▼ -91.45 after sell → book $9,988.54; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 84 | $14.63 | $2.27 | $-22.99 | $4,946.33 | ▼ -22.99 after sell → book $9,986.27; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $6,171.63 | ▲ +4.01 after sell → book $9,984.20; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 577 | $2.16 | $7.55 | $-26.53 | $7,410.41 | ▼ -26.53 after sell → book $9,976.65; vs 09:30 mark -7.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 90 | $14.49 | $2.29 | $+43.15 | $8,712.22 | ▲ +43.15 after sell → book $9,974.36; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3339 | $0.38 | $23.20 | $-42.47 | $9,951.16 | ▼ -42.47 after sell → book $9,951.16; vs 09:30 mark -23.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 493 | $2.52 | $6.36 | — | $8,702.44 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 260 | $4.78 | $3.35 | — | $7,456.29 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 78 | $15.90 | $2.22 | — | $6,213.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 353 | $3.52 | $4.55 | — | $4,966.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $3,937.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 44 | $28.00 | $2.12 | — | $2,703.07 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 124 | $10.02 | $2.36 | — | $1,458.23 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1243.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 236 | $5.25 | $3.04 | — | $216.18 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $1243.89 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.18 | ▼ close $9,901.00 vs 09:30 $9,995.07 (session -24.14) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.18 | ▼ 09:30 equity $9,876.41 vs yday $9,901.00 (-24.59) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 493 | $2.38 | $6.45 | $-81.83 | $1,383.07 | ▼ -81.83 after sell → book $9,869.96; vs 09:30 mark -6.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 260 | $4.30 | $3.41 | $-131.56 | $2,497.67 | ▼ -131.56 after sell → book $9,866.56; vs 09:30 mark -3.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HQ` | 78 | $15.40 | $2.25 | $-43.47 | $3,696.62 | ▼ -43.47 after sell → book $9,864.31; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 353 | $3.99 | $4.62 | $+156.73 | $5,100.46 | ▲ +156.73 after sell → book $9,859.68; vs 09:30 mark -4.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $6,140.75 | ▲ +10.73 after sell → book $9,857.67; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 44 | $28.03 | $2.14 | $-2.94 | $7,371.93 | ▼ -2.94 after sell → book $9,855.53; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 124 | $9.98 | $2.39 | $-9.71 | $8,607.05 | ▼ -9.71 after sell → book $9,853.13; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UAMY` | 236 | $5.28 | $3.09 | $+0.94 | $9,850.04 | ▲ +0.94 after sell → book $9,850.04; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.04 | ▲ close $9,850.04 vs 09:30 $9,876.41 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.04 | ▲ 09:30 equity $9,850.04 vs yday $9,850.04 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.04 | ▲ close $9,850.04 vs 09:30 $9,850.04 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.04 | ▲ 09:30 equity $9,850.04 vs yday $9,850.04 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.04 | ▲ close $9,850.04 vs 09:30 $9,850.04 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.04 | ▲ 09:30 equity $9,850.04 vs yday $9,850.04 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 603 | $2.04 | $7.78 | — | $8,612.14 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 259 | $4.75 | $3.34 | — | $7,378.55 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 580 | $2.12 | $7.48 | — | $6,141.47 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 52 | $23.63 | $2.15 | — | $4,910.56 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-6.3; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 106 | $11.55 | $2.31 | — | $3,683.95 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 7 | $157.55 | $2.01 | — | $2,579.09 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $1,417.11 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+2.5; leftover $1231.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 23 | $52.55 | $2.06 | — | $206.40 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1231.26 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.40 | ▼ close $9,797.40 vs 09:30 $9,850.04 (session -23.48) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.40 | ▲ 09:30 equity $9,872.77 vs yday $9,797.40 (+75.37) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 603 | $2.01 | $7.89 | $-33.76 | $1,410.54 | ▼ -33.76 after sell → book $9,864.88; vs 09:30 mark -7.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 259 | $4.82 | $3.39 | $+11.39 | $2,655.53 | ▲ +11.39 after sell → book $9,861.49; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 580 | $2.05 | $7.59 | $-55.67 | $3,836.94 | ▼ -55.67 after sell → book $9,853.90; vs 09:30 mark -7.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 52 | $23.20 | $2.17 | $-26.67 | $5,041.17 | ▼ -26.67 after sell → book $9,851.73; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 106 | $11.56 | $2.34 | $-3.58 | $6,264.20 | ▼ -3.58 after sell → book $9,849.40; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 7 | $160.00 | $2.03 | $+13.11 | $7,382.17 | ▲ +13.11 after sell → book $9,847.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 15 | $77.10 | $2.06 | $-7.54 | $8,536.61 | ▼ -7.54 after sell → book $9,845.31; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 23 | $56.90 | $2.08 | $+95.91 | $9,843.23 | ▲ +95.91 after sell → book $9,843.23; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,843.23 | ▲ close $9,843.23 vs 09:30 $9,872.77 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,843.23 | ▲ 09:30 equity $9,843.23 vs yday $9,843.23 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,843.23 | ▲ close $9,843.23 vs 09:30 $9,843.23 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,843.23 | ▲ 09:30 equity $9,843.23 vs yday $9,843.23 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 66 | $18.61 | $2.19 | — | $8,612.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 67 | $18.21 | $2.19 | — | $7,390.52 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $6,219.05 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 209 | $5.87 | $2.70 | — | $4,989.53 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 452 | $2.72 | $5.83 | — | $3,754.25 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.4; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $2,528.62 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 32 | $38.01 | $2.09 | — | $1,310.22 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $1230.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 45 | $27.09 | $2.12 | — | $89.04 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1230.40 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.04 | ▲ close $10,071.77 vs 09:30 $9,843.23 (session +249.73) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.04 | ▲ 09:30 equity $10,262.56 vs yday $10,071.77 (+190.79) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 66 | $22.46 | $2.21 | $+249.70 | $1,569.19 | ▲ +249.70 after sell → book $10,260.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 67 | $19.59 | $2.21 | $+88.06 | $2,879.51 | ▲ +88.06 after sell → book $10,258.14; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 17 | $72.70 | $2.06 | $+62.37 | $4,113.35 | ▲ +62.37 after sell → book $10,256.08; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 209 | $5.58 | $2.74 | $-66.05 | $5,276.83 | ▼ -66.05 after sell → book $10,253.34; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 452 | $2.94 | $5.92 | $+87.69 | $6,599.79 | ▲ +87.69 after sell → book $10,247.42; vs 09:30 mark -5.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $7,762.54 | ▼ -62.88 after sell → book $10,245.37; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 32 | $37.89 | $2.11 | $-8.03 | $8,972.91 | ▼ -8.03 after sell → book $10,243.26; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 45 | $28.23 | $2.15 | $+47.03 | $10,241.12 | ▲ +47.03 after sell → book $10,241.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7530 | $0.17 | $35.39 | — | $8,925.63 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 80 | $15.87 | $2.23 | — | $7,653.80 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $6,429.37 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 49 | $25.95 | $2.14 | — | $5,155.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $3,957.72 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 533 | $2.40 | $6.88 | — | $2,671.65 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 70 | $18.04 | $2.20 | — | $1,407.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1280.14 | — |
| 2026-09-17 09:30 ET | **BUY** | `EROC` | 101 | $12.64 | $2.29 | — | $128.06 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.6; leftover $1280.14 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.06 | ▼ close $10,155.89 vs 09:30 $10,262.56 (session -30.04) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.06 | ▲ 09:30 equity $10,395.06 vs yday $10,155.89 (+239.17) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7530 | $0.17 | $36.65 | $-72.04 | $1,371.51 | ▼ -72.04 after sell → book $10,358.41; vs 09:30 mark -36.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 80 | $17.44 | $2.25 | $+121.12 | $2,764.46 | ▲ +121.12 after sell → book $10,356.16; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $4,017.36 | ▲ +28.47 after sell → book $10,354.10; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 49 | $26.14 | $2.16 | $+5.02 | $5,296.06 | ▲ +5.02 after sell → book $10,351.94; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $6,570.34 | ▲ +76.32 after sell → book $10,349.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 533 | $2.29 | $6.97 | $-72.48 | $7,783.93 | ▼ -72.48 after sell → book $10,342.93; vs 09:30 mark -6.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 70 | $17.80 | $2.22 | $-20.87 | $9,027.71 | ▼ -20.87 after sell → book $10,340.71; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EROC` | 101 | $13.00 | $2.32 | $+31.75 | $10,338.39 | ▲ +31.75 after sell → book $10,338.39; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1332 | $0.97 | $16.92 | — | $9,029.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 621 | $2.08 | $8.01 | — | $7,729.74 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 327 | $3.95 | $4.22 | — | $6,433.88 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 91 | $14.07 | $2.26 | — | $5,151.24 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 221 | $5.83 | $2.85 | — | $3,859.96 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 360 | $3.58 | $4.64 | — | $2,566.52 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1520 | $0.85 | $17.48 | — | $1,257.04 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+3.6; leftover $1292.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 84 | $14.79 | $2.24 | — | $12.44 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1292.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.44 | ▼ close $10,215.54 vs 09:30 $10,395.06 (session -64.23) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.44 | ▲ 09:30 equity $10,421.37 vs yday $10,215.54 (+205.83) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1332 | $0.94 | $16.75 | $-73.62 | $1,247.77 | ▼ -73.62 after sell → book $10,404.62; vs 09:30 mark -16.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 621 | $2.15 | $8.12 | $+27.33 | $2,574.79 | ▲ +27.33 after sell → book $10,396.50; vs 09:30 mark -8.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 327 | $3.87 | $4.28 | $-34.66 | $3,836.00 | ▼ -34.66 after sell → book $10,392.21; vs 09:30 mark -4.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 91 | $13.90 | $2.29 | $-20.02 | $5,098.61 | ▼ -20.02 after sell → book $10,389.93; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 221 | $6.42 | $2.90 | $+123.54 | $6,513.43 | ▲ +123.54 after sell → book $10,387.03; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DDD` | 360 | $3.71 | $4.71 | $+37.44 | $7,844.31 | ▲ +37.44 after sell → book $10,382.31; vs 09:30 mark -4.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RANI` | 1520 | $0.86 | $17.96 | $-14.16 | $9,139.64 | ▼ -14.16 after sell → book $10,364.36; vs 09:30 mark -17.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 84 | $14.58 | $2.27 | $-22.15 | $10,362.09 | ▼ -22.15 after sell → book $10,362.09; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 139 | $9.31 | $2.41 | — | $9,065.59 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1295.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 96 | $13.47 | $2.28 | — | $7,769.72 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1295.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1166 | $1.11 | $15.04 | — | $6,460.42 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1295.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 129 | $9.99 | $2.38 | — | $5,169.33 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1295.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 709 | $1.82 | $9.15 | — | $3,866.26 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1295.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 127 | $10.13 | $2.37 | — | $2,576.74 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.9; leftover $1295.26 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 49 | $25.95 | $2.14 | — | $1,303.05 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1295.26 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,303.05 | ▼ close $10,218.77 vs 09:30 $10,421.37 (session -107.56) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,303.05 | ▼ 09:30 equity $10,192.02 vs yday $10,218.77 (-26.75) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1166 | $1.05 | $15.24 | $-100.25 | $2,512.11 | ▼ -100.25 after sell → book $10,176.77; vs 09:30 mark -15.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 129 | $9.91 | $2.41 | $-15.11 | $3,788.09 | ▼ -15.11 after sell → book $10,174.37; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 709 | $1.79 | $9.27 | $-39.69 | $5,051.47 | ▼ -39.69 after sell → book $10,165.09; vs 09:30 mark -9.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 1088 | $0.58 | $9.57 | — | $4,410.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $631.43 | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 67 | $9.40 | $2.19 | — | $3,778.87 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+9.5; leftover $631.43 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,778.87 | ▼ close $10,074.14 vs 09:30 $10,192.02 (session -79.19) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,778.87 | ▲ 09:30 equity $10,159.35 vs yday $10,074.14 (+85.21) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 139 | $9.50 | $2.44 | $+21.56 | $5,096.93 | ▲ +21.56 after sell → book $10,156.91; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 96 | $12.84 | $2.30 | $-65.54 | $6,327.26 | ▼ -65.54 after sell → book $10,154.60; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 127 | $10.26 | $2.40 | $+11.10 | $7,627.88 | ▲ +11.10 after sell → book $10,152.20; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 49 | $26.58 | $2.16 | $+26.58 | $8,928.14 | ▲ +26.58 after sell → book $10,150.04; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 1088 | $0.57 | $9.71 | $-24.73 | $9,544.03 | ▼ -24.73 after sell → book $10,140.33; vs 09:30 mark -9.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALOY` | 67 | $8.90 | $2.21 | $-37.90 | $10,138.12 | ▼ -37.90 after sell → book $10,138.12; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 61 | $20.65 | $2.17 | — | $8,876.30 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 322 | $3.93 | $4.15 | — | $7,606.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 44 | $28.30 | $2.12 | — | $6,359.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 80 | $15.72 | $2.23 | — | $5,099.53 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 49 | $25.40 | $2.14 | — | $3,852.79 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 81 | $15.55 | $2.23 | — | $2,591.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1650 | $0.77 | $17.62 | — | $1,306.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1267.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 10 | $116.00 | $2.02 | — | $144.17 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1267.26 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.17 | ▼ close $9,784.98 vs 09:30 $10,159.35 (session -318.45) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.17 | ▼ 09:30 equity $9,728.96 vs yday $9,784.98 (-56.02) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 61 | $20.52 | $2.19 | $-12.30 | $1,393.70 | ▼ -12.30 after sell → book $9,726.77; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 322 | $3.77 | $4.22 | $-59.89 | $2,603.42 | ▼ -59.89 after sell → book $9,722.55; vs 09:30 mark -4.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MAZE` | 44 | $28.15 | $2.14 | $-10.86 | $3,839.88 | ▼ -10.86 after sell → book $9,720.41; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 80 | $14.38 | $2.25 | $-111.68 | $4,988.02 | ▼ -111.68 after sell → book $9,718.15; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 49 | $23.99 | $2.16 | $-73.38 | $6,161.38 | ▼ -73.38 after sell → book $9,716.00; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 81 | $14.82 | $2.26 | $-63.62 | $7,359.54 | ▼ -63.62 after sell → book $9,713.74; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1650 | $0.75 | $17.54 | $-71.46 | $8,572.90 | ▼ -71.46 after sell → book $9,696.20; vs 09:30 mark -17.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 10 | $112.33 | $2.04 | $-40.76 | $9,694.16 | ▼ -40.76 after sell → book $9,694.16; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,694.16 | ▲ close $9,694.16 vs 09:30 $9,728.96 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,348.61 | ▲ 09:30 equity $9,348.61 vs yday $9,348.61 (+0.00) | 09:30 open · cash $9,348.61 · no holdings · equity $9,348.61 vs prior close $9,348.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 44 | $26.27 | $2.12 | — | $8,190.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1168.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $7,099.70 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1168.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 129 | $9.05 | $2.38 | — | $5,929.87 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $1168.58 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 49 | $23.58 | $2.14 | — | $4,772.32 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1168.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 531 | $2.20 | $6.85 | — | $3,597.27 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1168.58 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 194 | $6.00 | $2.57 | — | $2,430.69 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1168.58 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 65 | $17.91 | $2.19 | — | $1,264.36 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.7; leftover $1168.58 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 13 | $83.69 | $2.03 | — | $174.29 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1168.58 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.29 | ▼ close $9,290.15 vs 09:30 $9,348.61 (session -36.15) | 16:00 close · cash $174.29 · equity $9,290.15 vs 09:30 $9,348.61 (-58.46; session marks -36.15) · 8 name(s) marked open→close (per-name table). WRBY×44 09:30 $26.27 → close $26.71 +19.36; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×129 09:30 $9.05 → close $9.36 +39.99; BRVE×49 09:30 $23.58 → close $20.62 -145.04; HLP×531 09:30 $2.20 → close $2.21 +5.31; SATL×194 09:30 $6.00 → close $6.17 +32.98; PL×65 09:30 $17.91 → close $17.43 -31.20; TEM×13 09:30 $83.69 → close $85.01 +17.10 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1295.26 < 1 share @ 1826.00 |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
