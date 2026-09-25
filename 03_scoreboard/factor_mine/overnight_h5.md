# Factor mine action — `overnight_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-22.21%** ($7,779) · signal-only (no cash/fees) was -37.95%. Starts YES **0/30**. Fills 30 · skips 179 · realized $-881.39.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `overnight` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $110.45.

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
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,866.25 vs 09:30 $10,828.95 (session +37.30) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▼ 09:30 equity $10,771.91 vs yday $10,866.25 (-94.34) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,811.05 vs 09:30 $10,771.91 (session +39.14) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,882.73 vs yday $10,811.05 (+71.68) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `DUOT` | 265 | $10.56 | $3.48 | $+292.55 | $2,796.98 | ▲ +292.55 after sell → book $10,879.25; vs 09:30 mark -3.48 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 61 | $49.58 | $2.21 | $+526.32 | $5,819.15 | ▲ +526.32 after sell → book $10,877.04; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NUAI` | 494 | $5.61 | $6.48 | $+258.85 | $8,584.02 | ▲ +258.85 after sell → book $10,870.57; vs 09:30 mark -6.47 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SIDU` | 973 | $2.35 | $12.73 | $-219.88 | $10,857.83 | ▼ -219.88 after sell → book $10,857.83; vs 09:30 mark -12.74 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 60 | $90.03 | $2.17 | — | $5,453.86 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $5428.92 | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 441 | $12.29 | $5.69 | — | $28.28 | — | baseline list, no extra gate; list overnight; ret5=+1.9; leftover $5428.92 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,706.87 vs 09:30 $10,882.73 (session -143.10) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▼ 09:30 equity $10,702.31 vs yday $10,706.87 (-4.56) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,169.63 vs 09:30 $10,702.31 (session -532.68) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▼ 09:30 equity $10,159.87 vs yday $10,169.63 (-9.76) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▲ close $10,408.88 vs 09:30 $10,159.87 (session +249.01) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▲ 09:30 equity $10,545.98 vs yday $10,408.88 (+137.10) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,396.79 vs 09:30 $10,545.98 (session -149.19) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▼ 09:30 equity $10,292.99 vs yday $10,396.79 (-103.80) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,106.21 vs 09:30 $10,292.99 (session -186.78) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▲ 09:30 equity $10,252.07 vs yday $10,106.21 (+145.86) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `PDD` | 60 | $85.21 | $2.22 | $-293.59 | $5,138.66 | ▼ -293.59 after sell → book $10,249.85; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `XPEV` | 441 | $11.59 | $5.80 | $-320.19 | $10,244.05 | ▼ -320.19 after sell → book $10,244.05; vs 09:30 mark -5.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4415 | $1.16 | $56.95 | — | $5,065.70 | — | baseline list, no extra gate; list overnight; ret5=-13.8; leftover $5122.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $14.65 | — | baseline list, no extra gate; list overnight; ret5=+2.1; leftover $5122.03 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $10,136.79 vs 09:30 $10,252.07 (session -48.20) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▼ 09:30 equity $9,949.01 vs yday $10,136.79 (-187.78) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $9,739.28 vs 09:30 $9,949.01 (session -209.73) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▼ 09:30 equity $9,427.19 vs yday $9,739.28 (-312.09) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $8,853.10 vs 09:30 $9,427.19 (session -574.09) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▲ 09:30 equity $8,932.54 vs yday $8,853.10 (+79.44) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $8,599.39 vs 09:30 $8,932.54 (session -333.16) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▲ 09:30 equity $8,640.70 vs yday $8,599.39 (+41.31) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▲ close $8,709.73 vs 09:30 $8,640.70 (session +69.04) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▼ 09:30 equity $8,705.41 vs yday $8,709.73 (-4.32) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 4415 | $0.86 | $51.89 | $-1442.17 | $3,750.83 | ▼ -1,442.17 after sell → book $8,653.52; vs 09:30 mark -51.89 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAIC` | 39 | $125.71 | $2.16 | $-150.51 | $8,651.37 | ▼ -150.51 after sell → book $8,651.37; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 92 | $46.79 | $2.27 | — | $4,344.42 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $4325.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 98 | $43.80 | $2.28 | — | $49.74 | — | baseline list, no extra gate; list overnight; ret5=-7.7; leftover $4325.68 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▲ close $8,683.48 vs 09:30 $8,705.41 (session +36.66) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $8,694.84 vs yday $8,683.48 (+11.36) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▲ close $9,108.08 vs 09:30 $8,694.84 (session +413.24) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,163.94 vs yday $9,108.08 (+55.86) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▼ close $8,997.60 vs 09:30 $9,163.94 (session -166.34) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,025.04 vs yday $8,997.60 (+27.44) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▼ close $8,886.96 vs 09:30 $9,025.04 (session -138.08) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,011.90 vs yday $8,886.96 (+124.94) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▼ close $8,953.40 vs 09:30 $9,011.90 (session -58.50) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,036.48 vs yday $8,953.40 (+83.08) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ABM` | 92 | $49.63 | $2.32 | $+256.70 | $4,613.38 | ▲ +256.70 after sell → book $9,034.16; vs 09:30 mark -2.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `UNFI` | 98 | $45.11 | $2.34 | $+123.76 | $9,031.82 | ▲ +123.76 after sell → book $9,031.82; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,031.82 | ▲ close $9,031.82 vs 09:30 $9,036.48 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,031.82 | ▲ 09:30 equity $9,031.82 vs yday $9,031.82 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,031.82 | ▲ close $9,031.82 vs 09:30 $9,031.82 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,031.82 | ▲ 09:30 equity $9,031.82 vs yday $9,031.82 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 328 | $13.75 | $4.23 | — | $4,517.59 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.4; leftover $4515.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 56 | $80.63 | $2.16 | — | $0.15 | — | baseline list, no extra gate; list overnight; ret5=-0.4; leftover $4515.91 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▼ close $8,793.35 vs 09:30 $9,031.82 (session -232.08) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▼ 09:30 equity $8,213.03 vs yday $8,793.35 (-580.32) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,250.11 vs 09:30 $8,213.03 (session +37.08) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▼ 09:30 equity $8,200.07 vs yday $8,250.11 (-50.04) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,454.03 vs 09:30 $8,200.07 (session +253.96) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▲ 09:30 equity $8,616.03 vs yday $8,454.03 (+162.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,836.71 vs 09:30 $8,616.03 (session +220.68) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▲ 09:30 equity $8,836.71 vs yday $8,836.71 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,836.71 vs 09:30 $8,836.71 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▲ 09:30 equity $9,125.11 vs yday $8,836.71 (+288.40) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 328 | $13.82 | $4.32 | $+14.41 | $4,528.79 | ▲ +14.41 after sell → book $9,120.79; vs 09:30 mark -4.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 56 | $82.00 | $2.20 | $+72.36 | $9,118.59 | ▲ +72.36 after sell → book $9,118.59; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `BB` | 176 | $8.60 | $2.52 | — | $7,602.47 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.4; leftover $1519.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `DRI` | 7 | $215.10 | $2.01 | — | $6,094.76 | — | baseline list, no extra gate; list overnight; ret5=-0.6; leftover $1519.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `FUL` | 30 | $50.51 | $2.08 | — | $4,577.38 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.6; leftover $1519.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `NEOV` | 446 | $3.40 | $5.75 | — | $3,055.22 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.8; leftover $1519.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `SFIX` | 508 | $2.99 | $6.55 | — | $1,529.75 | — | baseline list, no extra gate; list overnight; ret5=+1.7; leftover $1519.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `SNX` | 5 | $283.46 | $2.00 | — | $110.45 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1519.76 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.45 | ▼ close $8,873.29 vs 09:30 $9,125.11 (session -224.38) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.45 | ▼ 09:30 equity $8,199.21 vs yday $8,873.29 (-674.08) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.45 | ▼ close $8,085.81 vs 09:30 $8,199.21 (session -113.40) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $263.29 | ▼ 09:30 equity $7,888.88 vs yday $7,896.02 (-7.14) | 09:30 open · cash $263.29 (unchanged overnight, no fees) · equity $7,888.88 vs prior close $7,896.02 (-7.14) · 6 name(s) re-marked at the open (per-name table). BB×172 yday $8.73 → 09:30 $8.73 +0.00; DRI×6 yday $207.24 → 09:30 $207.24 +0.00; FUL×29 yday $50.00 → 09:30 $50.00 +0.00; NEOV×436 yday $2.39 → 09:30 $2.39 -2.18; SFIX×496 yday $2.21 → 09:30 $2.20 -4.96; SNX×5 yday $259.47 → 09:30 $259.47 +0.00 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $263.29 | ▼ close $7,779.36 vs 09:30 $7,888.88 (session -109.52) | 16:00 close · cash $263.29 · equity $7,779.36 vs 09:30 $7,888.88 (-109.52; session marks -109.52) · 6 name(s) marked open→close (per-name table). BB×172 09:30 $8.73 → close $8.73 -0.00; DRI×6 09:30 $207.24 → close $207.24 +0.00; FUL×29 09:30 $50.00 → close $50.00 +0.00; NEOV×436 09:30 $2.39 → close $2.19 -87.20; SFIX×496 09:30 $2.20 → close $2.15 -22.32; SNX×5 09:30 $259.47 → close $259.47 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `DUOT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HTHT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NUAI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `SIDU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AS` | cash | leftover split 0.26 < 1 share @ 32.88 |
| 2026-08-17 | `BIDU` | cash | leftover split 0.26 < 1 share @ 102.83 |
| 2026-08-17 | `FN` | cash | leftover split 0.26 < 1 share @ 583.15 |
| 2026-08-17 | `HD` | cash | leftover split 0.26 < 1 share @ 334.71 |
| 2026-08-17 | `HSAI` | cash | leftover split 0.26 < 1 share @ 18.32 |
| 2026-08-17 | `IQ` | cash | leftover split 0.26 < 1 share @ 1.35 |
| 2026-08-17 | `KLAR` | cash | leftover split 0.26 < 1 share @ 20.67 |
| 2026-08-17 | `PONY` | cash | leftover split 0.26 < 1 share @ 8.16 |
| 2026-08-18 | `DUOT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HTHT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NUAI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `SIDU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ZIM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DUOT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HTHT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NUAI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `SIDU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `DUOT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HTHT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NUAI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `SIDU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BEKE` | cash | leftover split 0.41 < 1 share @ 17.04 |
| 2026-08-20 | `BJ` | cash | leftover split 0.41 < 1 share @ 88.91 |
| 2026-08-20 | `BKE` | cash | leftover split 0.41 < 1 share @ 42.60 |
| 2026-08-20 | `FLO` | cash | leftover split 0.41 < 1 share @ 7.43 |
| 2026-08-20 | `ROST` | cash | leftover split 0.41 < 1 share @ 229.55 |
| 2026-08-24 | `PDD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `XPEV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GRRR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PDD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `XPEV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ANF` | cash | leftover split 3.54 < 1 share @ 112.17 |
| 2026-08-25 | `BBWI` | cash | leftover split 3.54 < 1 share @ 19.16 |
| 2026-08-25 | `BOX` | cash | leftover split 3.54 < 1 share @ 33.33 |
| 2026-08-25 | `DCI` | cash | leftover split 3.54 < 1 share @ 93.64 |
| 2026-08-25 | `DY` | cash | leftover split 3.54 < 1 share @ 390.22 |
| 2026-08-25 | `FSCO` | cash | leftover split 3.54 < 1 share @ 5.10 |
| 2026-08-25 | `HEI` | cash | leftover split 3.54 < 1 share @ 357.15 |
| 2026-08-25 | `INTU` | cash | leftover split 3.54 < 1 share @ 364.35 |
| 2026-08-26 | `PDD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `XPEV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `STDN` | cash | leftover split 3.54 < 1 share @ 13.95 |
| 2026-08-26 | `A` | cash | leftover split 3.54 < 1 share @ 152.45 |
| 2026-08-26 | `BBY` | cash | leftover split 3.54 < 1 share @ 85.19 |
| 2026-08-26 | `BILI` | cash | leftover split 3.54 < 1 share @ 16.22 |
| 2026-08-26 | `CM` | cash | leftover split 3.54 < 1 share @ 118.50 |
| 2026-08-26 | `CMBT` | cash | leftover split 3.54 < 1 share @ 17.91 |
| 2026-08-26 | `CRM` | cash | leftover split 3.54 < 1 share @ 199.94 |
| 2026-08-26 | `CRWD` | cash | leftover split 3.54 < 1 share @ 182.75 |
| 2026-08-27 | `PDD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `XPEV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GAP` | cash | leftover split 3.54 < 1 share @ 20.75 |
| 2026-08-27 | `ADSK` | cash | leftover split 3.54 < 1 share @ 261.47 |
| 2026-08-27 | `AFRM` | cash | leftover split 3.54 < 1 share @ 76.90 |
| 2026-08-27 | `BBAR` | cash | leftover split 3.54 < 1 share @ 14.96 |
| 2026-08-27 | `CHA` | cash | leftover split 3.54 < 1 share @ 10.54 |
| 2026-08-27 | `ESTC` | cash | leftover split 3.54 < 1 share @ 82.65 |
| 2026-08-27 | `HAFN` | cash | leftover split 3.54 < 1 share @ 7.91 |
| 2026-08-27 | `IREN` | cash | leftover split 3.54 < 1 share @ 40.65 |
| 2026-08-31 | `LX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SAIC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SAIC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `LX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SAIC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVGO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HPE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `LX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SAIC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AMBA` | cash | leftover split 1.83 < 1 share @ 66.61 |
| 2026-09-03 | `ASAN` | cash | leftover split 1.83 < 1 share @ 10.16 |
| 2026-09-03 | `DOCU` | cash | leftover split 1.83 < 1 share @ 67.06 |
| 2026-09-03 | `DOMO` | cash | leftover split 1.83 < 1 share @ 3.78 |
| 2026-09-03 | `GWRE` | cash | leftover split 1.83 < 1 share @ 198.00 |
| 2026-09-03 | `IOT` | cash | leftover split 1.83 < 1 share @ 37.69 |
| 2026-09-03 | `LULU` | cash | leftover split 1.83 < 1 share @ 121.15 |
| 2026-09-03 | `MAMA` | cash | leftover split 1.83 < 1 share @ 15.62 |
| 2026-09-08 | `ABM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `UNFI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CGNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHWY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GME` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `UNFI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `AEO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WLTH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ABM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UNFI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CPRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ABM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `UNFI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HITI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ALMU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `ALMU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ABVX` | cash | leftover split 0.05 < 1 share @ 105.72 |
| 2026-09-21 | `MLKN` | cash | leftover split 0.05 < 1 share @ 20.85 |
| 2026-09-21 | `THO` | cash | leftover split 0.05 < 1 share @ 68.39 |
| 2026-09-22 | `ALMU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `LEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `CTAS` | no_price | no 09:30 open |
| 2026-09-22 | `GIS` | cash | leftover split 0.03 < 1 share @ 35.96 |
| 2026-09-22 | `KBH` | cash | leftover split 0.03 < 1 share @ 49.39 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-24 | `BB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `DRI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FUL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `NEOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `SFIX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `SNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BB` | 176 | 2026-09-23 @ $8.60 | baseline list, no extra gate; list overnight; 🔵; ret5=-0.4; leftover $1519.76 |
| `DRI` | 7 | 2026-09-23 @ $215.10 | baseline list, no extra gate; list overnight; ret5=-0.6; leftover $1519.76 |
| `FUL` | 30 | 2026-09-23 @ $50.51 | baseline list, no extra gate; list overnight; 🔵; ret5=-2.6; leftover $1519.76 |
| `NEOV` | 446 | 2026-09-23 @ $3.40 | baseline list, no extra gate; list overnight; 🔵; ret5=-4.8; leftover $1519.76 |
| `SFIX` | 508 | 2026-09-23 @ $2.99 | baseline list, no extra gate; list overnight; ret5=+1.7; leftover $1519.76 |
| `SNX` | 5 | 2026-09-23 @ $283.46 | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1519.76 |
