# Factor mine action — `overnight_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-17.05%** ($8,295) · signal-only (no cash/fees) was -40.60%. Starts YES **0/30**. Fills 93 · skips 188 · realized $-1635.94.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: names the prior Finviz calendar said report AMC today or BMO next session (print not in yet).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) that pass the must-haves.
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

- **Universe** `overnight` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,339.43.

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
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 265 | $9.43 | $3.42 | — | $7,497.63 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+7.7; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HTHT` | 61 | $40.88 | $2.17 | — | $5,001.78 | — | baseline list, no extra gate; list overnight; ret5=-5.4; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NUAI` | 494 | $5.06 | $6.37 | — | $2,495.77 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=-3.2; leftover $2500.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 973 | $2.55 | $12.55 | — | $2.06 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+21.5; leftover $2500.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,005.27 vs 09:30 $10,000.00 (session +29.79) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,423.70 vs yday $10,005.27 (+418.43) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,733.27 vs 09:30 $10,423.70 (session +309.57) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,841.31 vs yday $10,733.27 (+108.04) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▼ close $10,782.87 vs 09:30 $10,841.31 (session -58.44) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,828.95 vs yday $10,782.87 (+46.08) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `DUOT` | 265 | $11.52 | $3.49 | $+546.94 | $3,051.38 | ▲ +546.94 after sell → book $10,825.47; vs 09:30 mark -3.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HTHT` | 61 | $46.82 | $2.21 | $+357.96 | $5,905.19 | ▲ +357.96 after sell → book $10,823.26; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NUAI` | 494 | $5.13 | $6.47 | $+21.73 | $8,432.94 | ▲ +21.73 after sell → book $10,816.79; vs 09:30 mark -6.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SIDU` | 973 | $2.45 | $12.73 | $-122.58 | $10,804.05 | ▼ -122.58 after sell → book $10,804.05; vs 09:30 mark -12.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,804.05 | ▲ close $10,804.05 vs 09:30 $10,828.95 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,804.05 | ▲ 09:30 equity $10,804.05 vs yday $10,804.05 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 126 | $17.04 | $2.37 | — | $8,654.65 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.2; leftover $2160.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 24 | $88.91 | $2.06 | — | $6,518.74 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.0; leftover $2160.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 50 | $42.60 | $2.14 | — | $4,386.60 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.6; leftover $2160.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 290 | $7.43 | $3.74 | — | $2,228.16 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+4.0; leftover $2160.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 9 | $229.55 | $2.02 | — | $160.20 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $2160.81 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.20 | ▼ close $10,744.05 vs 09:30 $10,804.05 (session -47.68) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.20 | ▲ 09:30 equity $11,025.18 vs yday $10,744.05 (+281.13) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 6 | $12.29 | $0.76 | — | $85.70 | — | baseline list, no extra gate; list overnight; ret5=+1.9; leftover $80.10 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.70 | ▲ close $11,066.78 vs 09:30 $11,025.18 (session +42.36) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.70 | ▲ 09:30 equity $11,132.21 vs yday $11,066.78 (+65.43) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.70 | ▲ close $11,253.20 vs 09:30 $11,132.21 (session +120.99) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.70 | ▼ 09:30 equity $11,218.31 vs yday $11,253.20 (-34.89) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BEKE` | 126 | $17.63 | $2.41 | $+69.57 | $2,304.67 | ▲ +69.57 after sell → book $11,215.90; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BJ` | 24 | $97.63 | $2.09 | $+205.13 | $4,645.70 | ▲ +205.13 after sell → book $11,213.81; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BKE` | 50 | $44.50 | $2.17 | $+90.69 | $6,868.54 | ▲ +90.69 after sell → book $11,211.65; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FLO` | 290 | $7.25 | $3.81 | $-59.75 | $8,967.23 | ▼ -59.75 after sell → book $11,207.84; vs 09:30 mark -3.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ROST` | 9 | $241.50 | $2.04 | $+103.49 | $11,138.69 | ▲ +103.49 after sell → book $11,205.80; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 12 | $112.17 | $2.03 | — | $9,790.62 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 72 | $19.16 | $2.21 | — | $8,408.89 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.0; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 41 | $33.33 | $2.11 | — | $7,040.25 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.7; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 14 | $93.64 | $2.03 | — | $5,727.26 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.4; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $4,554.60 | — | baseline list, no extra gate; list overnight; ret5=-12.0; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 273 | $5.10 | $3.52 | — | $3,158.78 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $2,085.33 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1392.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $990.28 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1392.34 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $990.28 | ▼ close $10,856.12 vs 09:30 $11,218.31 (session -331.78) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $990.28 | ▲ 09:30 equity $11,138.94 vs yday $10,856.12 (+282.82) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `XPEV` | 6 | $11.90 | $0.75 | $-3.85 | $1,060.93 | ▼ -3.85 after sell → book $11,138.19; vs 09:30 mark -0.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 9 | $13.95 | $1.28 | — | $934.09 | — | baseline list, no extra gate; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $132.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 1 | $85.19 | $0.85 | — | $848.05 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.3; leftover $132.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 8 | $16.22 | $1.32 | — | $716.97 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.5; leftover $132.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 1 | $118.50 | $1.19 | — | $597.28 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $132.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 7 | $17.91 | $1.27 | — | $470.64 | — | baseline list, no extra gate; list overnight; ret5=+3.9; leftover $132.62 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $470.64 | ▲ close $11,295.06 vs 09:30 $11,138.94 (session +162.79) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $470.64 | ▼ 09:30 equity $11,261.41 vs yday $11,295.06 (-33.65) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 2 | $20.75 | $0.42 | — | $428.71 | — | baseline list, no extra gate; list ohlc_hot,overnight; ret5=+5.2; leftover $58.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 3 | $14.96 | $0.46 | — | $383.38 | — | baseline list, no extra gate; list overnight; ret5=+3.0; leftover $58.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 5 | $10.54 | $0.54 | — | $330.13 | — | baseline list, no extra gate; list overnight; ret5=+2.5; leftover $58.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 7 | $7.91 | $0.57 | — | $274.19 | — | baseline list, no extra gate; list overnight; ret5=-1.8; leftover $58.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `IREN` | 1 | $40.65 | $0.41 | — | $233.13 | — | baseline list, no extra gate; list overnight; ret5=-7.6; leftover $58.83 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.13 | ▼ close $11,244.79 vs 09:30 $11,261.41 (session -14.21) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.13 | ▲ 09:30 equity $11,269.91 vs yday $11,244.79 (+25.12) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ANF` | 12 | $146.07 | $2.05 | $+402.72 | $1,983.92 | ▲ +402.72 after sell → book $11,267.86; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBWI` | 72 | $18.75 | $2.23 | $-33.95 | $3,331.69 | ▼ -33.95 after sell → book $11,265.63; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 41 | $34.75 | $2.13 | $+53.97 | $4,754.31 | ▲ +53.97 after sell → book $11,263.49; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DCI` | 14 | $92.25 | $2.05 | $-23.54 | $6,043.76 | ▼ -23.54 after sell → book $11,261.44; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DY` | 3 | $306.34 | $2.02 | $-255.66 | $6,960.76 | ▼ -255.66 after sell → book $11,259.42; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FSCO` | 273 | $5.12 | $3.58 | $-1.64 | $8,354.94 | ▼ -1.64 after sell → book $11,255.84; vs 09:30 mark -3.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HEI` | 3 | $339.95 | $2.02 | $-55.62 | $9,372.77 | ▼ -55.62 after sell → book $11,253.82; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INTU` | 3 | $347.82 | $2.02 | $-53.61 | $10,414.21 | ▼ -53.61 after sell → book $11,251.80; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4488 | $1.16 | $57.90 | — | $5,150.23 | — | baseline list, no extra gate; list overnight; ret5=-13.8; leftover $5207.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $99.19 | — | baseline list, no extra gate; list overnight; ret5=+2.1; leftover $5207.10 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.19 | ▼ close $11,131.50 vs 09:30 $11,269.91 (session -60.31) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.19 | ▼ 09:30 equity $10,938.98 vs yday $11,131.50 (-192.52) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `STDN` | 9 | $14.35 | $1.34 | $+0.98 | $227.00 | ▲ +0.98 after sell → book $10,937.64; vs 09:30 mark -1.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBY` | 1 | $81.94 | $0.84 | $-4.95 | $308.10 | ▼ -4.95 after sell → book $10,936.80; vs 09:30 mark -0.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BILI` | 8 | $16.53 | $1.37 | $-0.21 | $438.97 | ▼ -0.21 after sell → book $10,935.43; vs 09:30 mark -1.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CM` | 1 | $114.46 | $1.17 | $-6.40 | $552.26 | ▼ -6.40 after sell → book $10,934.26; vs 09:30 mark -1.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CMBT` | 7 | $18.82 | $1.36 | $+3.74 | $682.64 | ▲ +3.74 after sell → book $10,932.90; vs 09:30 mark -1.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $682.64 | ▼ close $10,729.77 vs 09:30 $10,938.98 (session -203.14) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $682.64 | ▼ 09:30 equity $10,411.62 vs yday $10,729.77 (-318.15) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `GAP` | 2 | $22.05 | $0.47 | $+1.71 | $726.28 | ▲ +1.71 after sell → book $10,411.16; vs 09:30 mark -0.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBAR` | 3 | $14.82 | $0.47 | $-1.35 | $770.26 | ▼ -1.35 after sell → book $10,410.68; vs 09:30 mark -0.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CHA` | 5 | $11.63 | $0.62 | $+4.29 | $827.80 | ▲ +4.29 after sell → book $10,410.07; vs 09:30 mark -0.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HAFN` | 7 | $8.56 | $0.64 | $+3.34 | $887.08 | ▲ +3.34 after sell → book $10,409.43; vs 09:30 mark -0.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `IREN` | 1 | $36.08 | $0.38 | $-5.36 | $922.77 | ▼ -5.36 after sell → book $10,409.04; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $922.77 | ▼ close $9,825.61 vs 09:30 $10,411.62 (session -583.43) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $922.77 | ▲ 09:30 equity $9,906.80 vs yday $9,825.61 (+81.19) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `LX` | 4488 | $0.91 | $54.90 | $-1252.75 | $4,934.00 | ▼ -1,252.75 after sell → book $9,851.90; vs 09:30 mark -54.90 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SAIC` | 39 | $126.10 | $2.16 | $-135.30 | $9,849.74 | ▼ -135.30 after sell → book $9,849.74; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,849.74 | ▲ close $9,849.74 vs 09:30 $9,906.80 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,849.74 | ▲ 09:30 equity $9,849.74 vs yday $9,849.74 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 18 | $66.61 | $2.04 | — | $8,648.72 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-3.6; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 121 | $10.16 | $2.35 | — | $7,417.01 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.8; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 18 | $67.06 | $2.04 | — | $6,207.88 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+10.2; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 325 | $3.78 | $4.19 | — | $4,975.19 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.4; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,785.18 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+0.9; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 32 | $37.69 | $2.09 | — | $2,577.01 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-7.7; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,363.49 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.2; leftover $1231.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 78 | $15.62 | $2.22 | — | $142.91 | — | baseline list, no extra gate; list overnight; ret5=-6.7; leftover $1231.22 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.91 | ▲ close $9,843.59 vs 09:30 $9,849.74 (session +12.82) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.91 | ▼ 09:30 equity $9,394.13 vs yday $9,843.59 (-449.46) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 1 | $46.79 | $0.47 | — | $95.65 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $71.46 | — |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 1 | $43.80 | $0.44 | — | $51.41 | — | baseline list, no extra gate; list overnight; ret5=-7.7; leftover $71.46 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.41 | ▼ close $9,282.30 vs 09:30 $9,394.13 (session -110.92) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.41 | ▼ 09:30 equity $9,223.04 vs yday $9,282.30 (-59.26) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.41 | ▼ close $9,193.32 vs 09:30 $9,223.04 (session -29.72) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.41 | ▼ 09:30 equity $9,113.71 vs yday $9,193.32 (-79.61) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AMBA` | 18 | $63.07 | $2.06 | $-67.83 | $1,184.60 | ▼ -67.83 after sell → book $9,111.64; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ASAN` | 121 | $8.64 | $2.38 | $-188.66 | $2,227.66 | ▼ -188.66 after sell → book $9,109.26; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DOCU` | 18 | $64.64 | $2.06 | $-47.67 | $3,389.12 | ▼ -47.67 after sell → book $9,107.20; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DOMO` | 325 | $3.86 | $4.26 | $+17.55 | $4,639.36 | ▲ +17.55 after sell → book $9,102.94; vs 09:30 mark -4.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GWRE` | 6 | $147.85 | $2.03 | $-304.94 | $5,524.43 | ▼ -304.94 after sell → book $9,100.91; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `IOT` | 32 | $39.60 | $2.11 | $+56.93 | $6,789.53 | ▲ +56.93 after sell → book $9,098.81; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LULU` | 10 | $101.90 | $2.04 | $-196.56 | $7,806.49 | ▼ -196.56 after sell → book $9,096.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MAMA` | 78 | $15.31 | $2.25 | $-28.65 | $8,998.42 | ▼ -28.65 after sell → book $9,094.52; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,998.42 | ▼ close $9,092.77 vs 09:30 $9,113.71 (session -1.75) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,998.42 | ▲ 09:30 equity $9,093.05 vs yday $9,092.77 (+0.28) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ABM` | 1 | $49.74 | $0.52 | $+1.96 | $9,047.64 | ▲ +1.96 after sell → book $9,092.53; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `UNFI` | 1 | $44.89 | $0.47 | $+0.18 | $9,092.06 | ▲ +0.18 after sell → book $9,092.06; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,092.06 | ▲ close $9,092.06 vs 09:30 $9,093.05 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,092.06 | ▲ 09:30 equity $9,092.06 vs yday $9,092.06 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,092.06 | ▲ close $9,092.06 vs 09:30 $9,092.06 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,092.06 | ▲ 09:30 equity $9,092.06 vs yday $9,092.06 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,092.06 | ▲ close $9,092.06 vs 09:30 $9,092.06 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,092.06 | ▲ 09:30 equity $9,092.06 vs yday $9,092.06 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,092.06 | ▲ close $9,092.06 vs 09:30 $9,092.06 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,092.06 | ▲ 09:30 equity $9,092.06 vs yday $9,092.06 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 330 | $13.75 | $4.26 | — | $4,550.30 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.4; leftover $4546.03 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 56 | $80.63 | $2.16 | — | $32.86 | — | baseline list, no extra gate; list overnight; ret5=-0.4; leftover $4546.03 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.86 | ▼ close $8,852.92 vs 09:30 $9,092.06 (session -232.72) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.86 | ▼ 09:30 equity $8,268.16 vs yday $8,852.92 (-584.76) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.86 | ▲ close $8,305.91 vs 09:30 $8,268.16 (session +37.75) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.86 | ▼ 09:30 equity $8,256.06 vs yday $8,305.91 (-49.85) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.86 | ▲ close $8,512.19 vs 09:30 $8,256.06 (session +256.13) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.86 | ▲ 09:30 equity $8,674.99 vs yday $8,512.19 (+162.80) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 330 | $13.12 | $4.35 | $-214.85 | $4,359.77 | ▼ -214.85 after sell → book $8,670.65; vs 09:30 mark -4.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `LEN` | 56 | $76.98 | $2.20 | $-208.76 | $8,668.44 | ▼ -208.76 after sell → book $8,668.44; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 27 | $105.72 | $2.07 | — | $5,811.93 | — | baseline list, no extra gate; list overnight; ret5=-11.5; leftover $2889.48 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 138 | $20.85 | $2.40 | — | $2,932.23 | — | baseline list, no extra gate; list overnight; ret5=-2.5; leftover $2889.48 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 42 | $68.39 | $2.12 | — | $57.73 | — | baseline list, no extra gate; list overnight; ret5=-7.0; leftover $2889.48 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.73 | ▼ close $8,592.52 vs 09:30 $8,674.99 (session -69.33) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.73 | ▲ 09:30 equity $8,592.52 vs yday $8,592.52 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.73 | ▲ close $8,592.52 vs 09:30 $8,592.52 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.73 | ▼ 09:30 equity $8,437.93 vs yday $8,592.52 (-154.59) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `BB` | 1 | $8.60 | $0.09 | — | $49.04 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.4; leftover $9.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `NEOV` | 2 | $3.40 | $0.07 | — | $42.17 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.8; leftover $9.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `SFIX` | 3 | $2.99 | $0.10 | — | $33.10 | — | baseline list, no extra gate; list overnight; ret5=+1.7; leftover $9.62 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.10 | ▼ close $8,400.33 vs 09:30 $8,437.93 (session -37.34) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.10 | ▼ 09:30 equity $8,366.78 vs yday $8,400.33 (-33.55) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ABVX` | 27 | $92.97 | $2.10 | $-348.42 | $2,541.19 | ▼ -348.42 after sell → book $8,364.68; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MLKN` | 138 | $19.96 | $2.45 | $-127.67 | $5,293.22 | ▼ -127.67 after sell → book $8,362.23; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `THO` | 42 | $72.58 | $2.15 | $+171.71 | $8,339.43 | ▲ +171.71 after sell → book $8,360.08; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,339.43 | ▼ close $8,359.58 vs 09:30 $8,366.78 (session -0.50) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,266.49 | ▼ 09:30 equity $8,295.78 vs yday $8,295.85 (-0.07) | 09:30 open · cash $8,266.49 (unchanged overnight, no fees) · equity $8,295.78 vs prior close $8,295.85 (-0.07) · 3 name(s) re-marked at the open (per-name table). BB×1 yday $8.73 → 09:30 $8.73 +0.00; NEOV×4 yday $2.39 → 09:30 $2.39 -0.02; SFIX×5 yday $2.21 → 09:30 $2.20 -0.05 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,266.49 | ▼ close $8,294.75 vs 09:30 $8,295.78 (session -1.03) | 16:00 close · cash $8,266.49 · equity $8,294.75 vs 09:30 $8,295.78 (-1.03; session marks -1.03) · 3 name(s) marked open→close (per-name table). BB×1 09:30 $8.73 → close $8.73 -0.00; NEOV×4 09:30 $2.39 → close $2.19 -0.80; SFIX×5 09:30 $2.20 → close $2.15 -0.23 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `DUOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HTHT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SIDU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AS` | cash | leftover split 0.26 < 1 share @ 32.88 |
| 2026-08-17 | `BIDU` | cash | leftover split 0.26 < 1 share @ 102.83 |
| 2026-08-17 | `FN` | cash | leftover split 0.26 < 1 share @ 583.15 |
| 2026-08-17 | `HD` | cash | leftover split 0.26 < 1 share @ 334.71 |
| 2026-08-17 | `HSAI` | cash | leftover split 0.26 < 1 share @ 18.32 |
| 2026-08-17 | `IQ` | cash | leftover split 0.26 < 1 share @ 1.35 |
| 2026-08-17 | `KLAR` | cash | leftover split 0.26 < 1 share @ 20.67 |
| 2026-08-17 | `PONY` | cash | leftover split 0.26 < 1 share @ 8.16 |
| 2026-08-18 | `DUOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HTHT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SIDU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ZIM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BEKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ROST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PDD` | cash | leftover split 80.10 < 1 share @ 90.03 |
| 2026-08-24 | `BEKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BJ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ROST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XPEV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GRRR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `XPEV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FSCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `A` | cash | leftover split 132.62 < 1 share @ 152.45 |
| 2026-08-26 | `CRM` | cash | leftover split 132.62 < 1 share @ 199.94 |
| 2026-08-26 | `CRWD` | cash | leftover split 132.62 < 1 share @ 182.75 |
| 2026-08-27 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FSCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `STDN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BILI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CMBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ADSK` | cash | leftover split 58.83 < 1 share @ 261.47 |
| 2026-08-27 | `AFRM` | cash | leftover split 58.83 < 1 share @ 76.90 |
| 2026-08-27 | `ESTC` | cash | leftover split 58.83 < 1 share @ 82.65 |
| 2026-08-28 | `STDN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BILI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CMBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAIC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAIC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVGO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HPE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AMBA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOCU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GWRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `IOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LULU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AMBA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOCU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GWRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LULU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ABM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UNFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CGNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHWY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GME` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UNFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AEO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WLTH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CPRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HITI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ALMU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ABVX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MLKN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `CTAS` | no_price | no 09:30 open |
| 2026-09-22 | `GIS` | cash | leftover split 11.55 < 1 share @ 35.96 |
| 2026-09-22 | `KBH` | cash | leftover split 11.55 < 1 share @ 49.39 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `ABVX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MLKN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DRI` | cash | leftover split 9.62 < 1 share @ 215.10 |
| 2026-09-23 | `FUL` | cash | leftover split 9.62 < 1 share @ 50.51 |
| 2026-09-23 | `SNX` | cash | leftover split 9.62 < 1 share @ 283.46 |
| 2026-09-24 | `BB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NEOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SFIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BB` | 1 | 2026-09-23 @ $8.60 | baseline list, no extra gate; list overnight; 🔵; ret5=-0.4; leftover $9.62 |
| `NEOV` | 2 | 2026-09-23 @ $3.40 | baseline list, no extra gate; list overnight; 🔵; ret5=-4.8; leftover $9.62 |
| `SFIX` | 3 | 2026-09-23 @ $2.99 | baseline list, no extra gate; list overnight; ret5=+1.7; leftover $9.62 |
