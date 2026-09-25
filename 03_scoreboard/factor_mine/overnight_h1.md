# Factor mine action — `overnight_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-21.61%** ($7,840) · signal-only (no cash/fees) was -22.61%. Starts YES **0/30**. Fills 132 · skips 80 · realized $-2514.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `overnight` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,485.66.

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
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 265 | $10.35 | $3.48 | $+236.90 | $2,741.33 | ▲ +236.90 after sell → book $10,420.22; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HTHT` | 61 | $45.49 | $2.21 | $+276.83 | $5,514.01 | ▲ +276.83 after sell → book $10,418.01; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NUAI` | 494 | $5.20 | $6.48 | $+56.31 | $8,076.34 | ▲ +56.31 after sell → book $10,411.54; vs 09:30 mark -6.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SIDU` | 973 | $2.40 | $12.73 | $-171.23 | $10,398.81 | ▼ -171.23 after sell → book $10,398.81; vs 09:30 mark -12.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `AS` | 39 | $32.88 | $2.11 | — | $9,114.38 | — | baseline list, no extra gate; list overnight; ret5=-10.8; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `BIDU` | 12 | $102.83 | $2.03 | — | $7,878.39 | — | baseline list, no extra gate; list overnight; ⚪; ret5=-5.5; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `FN` | 2 | $583.15 | $2.00 | — | $6,710.10 | — | baseline list, no extra gate; list overnight; ret5=+1.4; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `HD` | 3 | $334.71 | $2.00 | — | $5,703.97 | — | baseline list, no extra gate; list overnight,overnight_mega; ret5=-4.7; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `HSAI` | 70 | $18.32 | $2.20 | — | $4,419.37 | — | baseline list, no extra gate; list overnight; ret5=-5.3; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 962 | $1.35 | $12.41 | — | $3,108.26 | — | baseline list, no extra gate; list overnight; ⚪; ret5=+1.5; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLAR` | 62 | $20.67 | $2.18 | — | $1,824.54 | — | baseline list, no extra gate; list overnight; ret5=+4.5; leftover $1299.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `PONY` | 159 | $8.16 | $2.47 | — | $524.64 | — | baseline list, no extra gate; list overnight; ⚪; ret5=-0.1; leftover $1299.85 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $524.64 | ▼ close $10,277.91 vs 09:30 $10,423.70 (session -93.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $524.64 | ▼ 09:30 equity $9,509.77 vs yday $10,277.91 (-768.14) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `AS` | 39 | $33.89 | $2.13 | $+35.16 | $1,844.22 | ▲ +35.16 after sell → book $9,507.64; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BIDU` | 12 | $94.35 | $2.05 | $-105.83 | $2,974.37 | ▼ -105.83 after sell → book $9,505.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FN` | 2 | $513.70 | $2.02 | $-142.91 | $3,999.76 | ▼ -142.91 after sell → book $9,503.58; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HD` | 3 | $336.78 | $2.02 | $+2.19 | $5,008.08 | ▲ +2.19 after sell → book $9,501.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HSAI` | 70 | $15.77 | $2.22 | $-183.27 | $6,109.41 | ▼ -183.27 after sell → book $9,499.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 962 | $1.27 | $12.58 | $-101.95 | $7,318.57 | ▼ -101.95 after sell → book $9,486.76; vs 09:30 mark -12.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLAR` | 62 | $15.66 | $2.20 | $-314.99 | $8,287.29 | ▼ -314.99 after sell → book $9,484.56; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `PONY` | 159 | $7.53 | $2.50 | $-105.14 | $9,482.06 | ▼ -105.14 after sell → book $9,482.06; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,482.06 | ▲ close $9,482.06 vs 09:30 $9,509.77 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,482.06 | ▲ 09:30 equity $9,482.06 vs yday $9,482.06 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,482.06 | ▲ close $9,482.06 vs 09:30 $9,482.06 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,482.06 | ▲ 09:30 equity $9,482.06 vs yday $9,482.06 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 111 | $17.04 | $2.32 | — | $7,588.29 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.2; leftover $1896.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 21 | $88.91 | $2.05 | — | $5,719.13 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.0; leftover $1896.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 44 | $42.60 | $2.12 | — | $3,842.61 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.6; leftover $1896.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 255 | $7.43 | $3.29 | — | $1,944.67 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+4.0; leftover $1896.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 8 | $229.55 | $2.01 | — | $106.26 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1896.41 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.26 | ▼ close $9,428.03 vs 09:30 $9,482.06 (session -42.23) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.26 | ▲ 09:30 equity $9,676.44 vs yday $9,428.03 (+248.41) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BEKE` | 111 | $17.93 | $2.36 | $+94.66 | $2,094.68 | ▲ +94.66 after sell → book $9,674.08; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BJ` | 21 | $93.98 | $2.08 | $+102.34 | $4,066.18 | ▲ +102.34 after sell → book $9,672.00; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BKE` | 44 | $43.08 | $2.15 | $+16.85 | $5,959.56 | ▲ +16.85 after sell → book $9,669.86; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FLO` | 255 | $6.90 | $3.35 | $-141.79 | $7,715.71 | ▼ -141.79 after sell → book $9,666.51; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 8 | $243.85 | $2.04 | $+110.35 | $9,664.47 | ▲ +110.35 after sell → book $9,664.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 53 | $90.03 | $2.15 | — | $4,890.73 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $4832.24 | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 393 | $12.29 | $5.07 | — | $55.69 | — | baseline list, no extra gate; list overnight; ret5=+1.9; leftover $4832.24 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.69 | ▼ close $9,530.50 vs 09:30 $9,676.44 (session -126.75) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.69 | ▼ 09:30 equity $9,525.23 vs yday $9,530.50 (-5.27) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 53 | $90.95 | $2.20 | $+44.41 | $4,873.85 | ▲ +44.41 after sell → book $9,523.04; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XPEV` | 393 | $11.83 | $5.17 | $-191.02 | $9,517.86 | ▼ -191.02 after sell → book $9,517.86; vs 09:30 mark -5.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,517.86 | ▲ close $9,517.86 vs 09:30 $9,525.23 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,517.86 | ▲ 09:30 equity $9,517.86 vs yday $9,517.86 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 10 | $112.17 | $2.02 | — | $8,394.14 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 62 | $19.16 | $2.18 | — | $7,204.05 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.0; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 35 | $33.33 | $2.10 | — | $6,035.40 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.7; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 12 | $93.64 | $2.03 | — | $4,909.70 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.4; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $3,737.04 | — | baseline list, no extra gate; list overnight; ret5=-12.0; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 233 | $5.10 | $3.01 | — | $2,545.73 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,472.28 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1189.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $377.23 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1189.73 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $377.23 | ▼ close $9,192.55 vs 09:30 $9,517.86 (session -307.99) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $377.23 | ▲ 09:30 equity $9,409.89 vs yday $9,192.55 (+217.34) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 10 | $131.37 | $2.04 | $+187.94 | $1,688.89 | ▲ +187.94 after sell → book $9,407.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BBWI` | 62 | $18.26 | $2.20 | $-60.17 | $2,818.82 | ▼ -60.17 after sell → book $9,405.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BOX` | 35 | $34.30 | $2.12 | $+29.74 | $4,017.20 | ▲ +29.74 after sell → book $9,403.54; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DCI` | 12 | $95.13 | $2.05 | $+13.81 | $5,156.72 | ▲ +13.81 after sell → book $9,401.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DY` | 3 | $326.91 | $2.02 | $-193.95 | $6,135.43 | ▼ -193.95 after sell → book $9,399.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FSCO` | 233 | $5.08 | $3.05 | $-10.72 | $7,316.01 | ▼ -10.72 after sell → book $9,396.42; vs 09:30 mark -3.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HEI` | 3 | $370.00 | $2.02 | $+34.53 | $8,423.99 | ▲ +34.53 after sell → book $9,394.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 3 | $323.47 | $2.02 | $-126.66 | $9,392.38 | ▼ -126.66 after sell → book $9,392.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 84 | $13.95 | $2.24 | — | $8,218.34 | — | baseline list, no extra gate; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `A` | 7 | $152.45 | $2.01 | — | $7,149.18 | — | baseline list, no extra gate; list overnight; ret5=+4.3; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 13 | $85.19 | $2.03 | — | $6,039.68 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.3; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 72 | $16.22 | $2.21 | — | $4,869.64 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.5; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 9 | $118.50 | $2.02 | — | $3,801.12 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 65 | $17.91 | $2.19 | — | $2,634.78 | — | baseline list, no extra gate; list overnight; ret5=+3.9; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 5 | $199.94 | $2.00 | — | $1,633.08 | — | baseline list, no extra gate; list overnight,overnight_mega; ret5=+2.1; leftover $1174.05 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 6 | $182.75 | $2.01 | — | $534.57 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-12.9; leftover $1174.05 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.57 | ▲ close $9,444.68 vs 09:30 $9,409.89 (session +69.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.57 | ▲ 09:30 equity $9,649.72 vs yday $9,444.68 (+205.04) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `STDN` | 84 | $13.84 | $2.27 | $-13.75 | $1,694.87 | ▼ -13.75 after sell → book $9,647.46; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `A` | 7 | $159.35 | $2.03 | $+44.26 | $2,808.28 | ▲ +44.26 after sell → book $9,645.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBY` | 13 | $80.60 | $2.05 | $-63.75 | $3,854.04 | ▼ -63.75 after sell → book $9,643.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BILI` | 72 | $16.18 | $2.23 | $-7.31 | $5,016.77 | ▼ -7.31 after sell → book $9,641.15; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 9 | $118.77 | $2.04 | $-1.62 | $6,083.66 | ▼ -1.62 after sell → book $9,639.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CMBT` | 65 | $17.78 | $2.21 | $-12.84 | $7,237.15 | ▼ -12.84 after sell → book $9,636.90; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 5 | $230.05 | $2.02 | $+146.52 | $8,385.38 | ▲ +146.52 after sell → book $9,634.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 6 | $208.25 | $2.03 | $+148.96 | $9,632.85 | ▲ +148.96 after sell → book $9,632.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 58 | $20.75 | $2.16 | — | $8,427.19 | — | baseline list, no extra gate; list ohlc_hot,overnight; ret5=+5.2; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $7,379.31 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `AFRM` | 15 | $76.90 | $2.04 | — | $6,223.77 | — | baseline list, no extra gate; list overnight; ret5=-1.1; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 80 | $14.96 | $2.23 | — | $5,024.74 | — | baseline list, no extra gate; list overnight; ret5=+3.0; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 114 | $10.54 | $2.33 | — | $3,820.85 | — | baseline list, no extra gate; list overnight; ret5=+2.5; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `ESTC` | 14 | $82.65 | $2.03 | — | $2,661.72 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-9.3; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 152 | $7.91 | $2.45 | — | $1,456.95 | — | baseline list, no extra gate; list overnight; ret5=-1.8; leftover $1204.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `IREN` | 29 | $40.65 | $2.08 | — | $276.02 | — | baseline list, no extra gate; list overnight; ret5=-7.6; leftover $1204.11 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.02 | ▲ close $9,682.22 vs 09:30 $9,649.72 (session +66.69) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.02 | ▲ 09:30 equity $10,233.05 vs yday $9,682.22 (+550.83) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 58 | $24.69 | $2.19 | $+224.17 | $1,705.86 | ▲ +224.17 after sell → book $10,230.86; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 4 | $261.16 | $2.02 | $-5.26 | $2,748.48 | ▼ -5.26 after sell → book $10,228.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AFRM` | 15 | $86.00 | $2.06 | $+132.41 | $4,036.42 | ▲ +132.41 after sell → book $10,226.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBAR` | 80 | $15.01 | $2.25 | $-0.48 | $5,234.97 | ▼ -0.48 after sell → book $10,224.53; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CHA` | 114 | $10.30 | $2.36 | $-32.05 | $6,406.81 | ▼ -32.05 after sell → book $10,222.17; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ESTC` | 14 | $103.89 | $2.05 | $+293.27 | $7,859.21 | ▲ +293.27 after sell → book $10,220.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HAFN` | 152 | $8.35 | $2.48 | $+61.95 | $9,125.93 | ▲ +61.95 after sell → book $10,217.64; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `IREN` | 29 | $37.65 | $2.10 | $-91.32 | $10,215.54 | ▼ -91.32 after sell → book $10,215.54; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4403 | $1.16 | $56.80 | — | $5,051.26 | — | baseline list, no extra gate; list overnight; ret5=-13.8; leftover $5107.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $0.21 | — | baseline list, no extra gate; list overnight; ret5=+2.1; leftover $5107.77 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.21 | ▼ close $10,108.19 vs 09:30 $10,233.05 (session -48.44) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.21 | ▼ 09:30 equity $9,922.45 vs yday $10,108.19 (-185.74) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `LX` | 4403 | $1.01 | $57.57 | $-774.81 | $4,389.68 | ▼ -774.81 after sell → book $9,864.89; vs 09:30 mark -57.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SAIC` | 39 | $140.39 | $2.16 | $+422.00 | $9,862.73 | ▲ +422.00 after sell → book $9,862.73; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.73 | ▲ close $9,862.73 vs 09:30 $9,922.45 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.73 | ▲ 09:30 equity $9,862.73 vs yday $9,862.73 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.73 | ▲ close $9,862.73 vs 09:30 $9,862.73 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.73 | ▲ 09:30 equity $9,862.73 vs yday $9,862.73 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.73 | ▲ close $9,862.73 vs 09:30 $9,862.73 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.73 | ▲ 09:30 equity $9,862.73 vs yday $9,862.73 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 18 | $66.61 | $2.04 | — | $8,661.70 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-3.6; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 121 | $10.16 | $2.35 | — | $7,429.99 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.8; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 18 | $67.06 | $2.04 | — | $6,220.87 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+10.2; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 326 | $3.78 | $4.21 | — | $4,984.38 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.4; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,794.37 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+0.9; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 32 | $37.69 | $2.09 | — | $2,586.21 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-7.7; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,372.69 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.2; leftover $1232.84 | — |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 78 | $15.62 | $2.22 | — | $152.10 | — | baseline list, no extra gate; list overnight; ret5=-6.7; leftover $1232.84 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.10 | ▲ close $9,856.57 vs 09:30 $9,862.73 (session +12.83) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.10 | ▼ 09:30 equity $9,406.93 vs yday $9,856.57 (-449.64) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AMBA` | 18 | $63.18 | $2.06 | $-65.85 | $1,287.28 | ▼ -65.85 after sell → book $9,404.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ASAN` | 121 | $8.74 | $2.38 | $-176.56 | $2,342.44 | ▼ -176.56 after sell → book $9,402.49; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DOCU` | 18 | $68.52 | $2.06 | $+22.17 | $3,573.73 | ▲ +22.17 after sell → book $9,400.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DOMO` | 326 | $3.62 | $4.27 | $-62.26 | $4,747.95 | ▼ -62.26 after sell → book $9,396.15; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GWRE` | 6 | $167.55 | $2.03 | $-186.74 | $5,751.23 | ▼ -186.74 after sell → book $9,394.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `IOT` | 32 | $44.90 | $2.11 | $+226.53 | $7,185.92 | ▲ +226.53 after sell → book $9,392.02; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LULU` | 10 | $98.15 | $2.04 | $-234.06 | $8,165.38 | ▼ -234.06 after sell → book $9,389.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MAMA` | 78 | $15.70 | $2.25 | $+1.77 | $9,387.73 | ▲ +1.77 after sell → book $9,387.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 100 | $46.79 | $2.29 | — | $4,706.44 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $4693.87 | — |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 107 | $43.80 | $2.31 | — | $17.53 | — | baseline list, no extra gate; list overnight; ret5=-7.7; leftover $4693.87 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.53 | ▲ close $9,423.04 vs 09:30 $9,406.93 (session +39.91) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.53 | ▲ 09:30 equity $9,436.00 vs yday $9,423.04 (+12.96) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ABM` | 100 | $45.81 | $2.34 | $-102.63 | $4,596.19 | ▼ -102.63 after sell → book $9,433.66; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UNFI` | 107 | $45.21 | $2.37 | $+146.19 | $9,431.29 | ▲ +146.19 after sell → book $9,431.29; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,436.00 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 342 | $13.75 | $4.41 | — | $4,724.38 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.4; leftover $4715.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 58 | $80.63 | $2.16 | — | $45.67 | — | baseline list, no extra gate; list overnight; ret5=-0.4; leftover $4715.64 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.67 | ▼ close $9,183.61 vs 09:30 $9,431.29 (session -241.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.67 | ▼ 09:30 equity $8,577.49 vs yday $9,183.61 (-606.12) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `ALMU` | 342 | $11.21 | $4.50 | $-877.59 | $3,874.99 | ▼ -877.59 after sell → book $8,572.99; vs 09:30 mark -4.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 58 | $81.00 | $2.21 | $+17.08 | $8,570.78 | ▲ +17.08 after sell → book $8,570.78; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,570.78 | ▲ close $8,570.78 vs 09:30 $8,577.49 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,570.78 | ▲ 09:30 equity $8,570.78 vs yday $8,570.78 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,570.78 | ▲ close $8,570.78 vs 09:30 $8,570.78 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,570.78 | ▲ 09:30 equity $8,570.78 vs yday $8,570.78 (+0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 27 | $105.72 | $2.07 | — | $5,714.27 | — | baseline list, no extra gate; list overnight; ret5=-11.5; leftover $2856.93 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 137 | $20.85 | $2.40 | — | $2,855.42 | — | baseline list, no extra gate; list overnight; ret5=-2.5; leftover $2856.93 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 41 | $68.39 | $2.11 | — | $49.32 | — | baseline list, no extra gate; list overnight; ret5=-7.0; leftover $2856.93 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▼ close $8,493.85 vs 09:30 $8,570.78 (session -70.35) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▲ 09:30 equity $8,493.85 vs yday $8,493.85 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▲ close $8,493.85 vs 09:30 $8,493.85 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▼ 09:30 equity $8,338.35 vs yday $8,493.85 (-155.50) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ABVX` | 27 | $98.30 | $2.10 | $-204.51 | $2,701.32 | ▼ -204.51 after sell → book $8,336.25; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MLKN` | 137 | $19.76 | $2.45 | $-154.18 | $5,405.99 | ▼ -154.18 after sell → book $8,333.80; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `THO` | 41 | $71.41 | $2.15 | $+119.56 | $8,331.65 | ▲ +119.56 after sell → book $8,331.65; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `BB` | 161 | $8.60 | $2.47 | — | $6,944.58 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.4; leftover $1388.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `DRI` | 6 | $215.10 | $2.01 | — | $5,651.97 | — | baseline list, no extra gate; list overnight; ret5=-0.6; leftover $1388.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `FUL` | 27 | $50.51 | $2.07 | — | $4,286.13 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.6; leftover $1388.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `NEOV` | 408 | $3.40 | $5.26 | — | $2,893.67 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.8; leftover $1388.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `SFIX` | 464 | $2.99 | $5.99 | — | $1,500.32 | — | baseline list, no extra gate; list overnight; ret5=+1.7; leftover $1388.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `SNX` | 4 | $283.46 | $2.00 | — | $364.48 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1388.61 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.48 | ▼ close $8,104.87 vs 09:30 $8,338.35 (session -206.98) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.48 | ▼ 09:30 equity $7,505.73 vs yday $8,104.87 (-599.14) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BB` | 161 | $8.42 | $2.51 | $-33.96 | $1,717.59 | ▼ -33.96 after sell → book $7,503.22; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DRI` | 6 | $208.88 | $2.03 | $-41.36 | $2,968.84 | ▼ -41.36 after sell → book $7,501.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FUL` | 27 | $49.29 | $2.09 | $-37.10 | $4,297.58 | ▼ -37.10 after sell → book $7,499.10; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NEOV` | 408 | $2.65 | $5.34 | $-316.60 | $5,373.44 | ▼ -316.60 after sell → book $7,493.76; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SFIX` | 464 | $2.31 | $6.07 | $-327.58 | $6,439.21 | ▼ -327.58 after sell → book $7,487.69; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SNX` | 4 | $262.12 | $2.02 | $-89.38 | $7,485.66 | ▼ -89.38 after sell → book $7,485.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,485.66 | ▲ close $7,485.66 vs 09:30 $7,505.73 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,839.54 | ▲ 09:30 equity $7,839.54 vs yday $7,839.54 (+0.00) | 09:30 open · cash $7,839.54 · no holdings · equity $7,839.54 vs prior close $7,839.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,839.54 | ▲ close $7,839.54 vs 09:30 $7,839.54 (session +0.00) | 16:00 close · cash $7,839.54 · no lots left · equity $7,839.54. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GRRR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CGNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHWY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GME` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-22 | `ABVX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `CTAS` | no_price | no 09:30 open |
| 2026-09-22 | `GIS` | cash | leftover split 9.86 < 1 share @ 35.96 |
| 2026-09-22 | `KBH` | cash | leftover split 9.86 < 1 share @ 49.39 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new buys |
