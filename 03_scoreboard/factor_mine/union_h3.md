# Factor mine action — `union_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-19.42%** ($8,058) · signal-only (no cash/fees) was -4.92%. Starts YES **0/30**. Fills 186 · skips 294 · realized $-475.74.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
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

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,232.36.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | baseline list, no extra gate; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | baseline list, no extra gate; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | baseline list, no extra gate; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | baseline list, no extra gate; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | baseline list, no extra gate; list flatten; ⚪; ret5=-11.4; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,525.15 vs 09:30 $10,414.78 (session +110.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.37 | ▼ -0.12 after sell → book $10,389.73; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.40 | ▼ -69.50 after sell → book $10,387.64; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.80 | ▲ +23.38 after sell → book $10,385.56; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.71 | ▼ -14.65 after sell → book $10,383.47; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.34 | ▲ +97.12 after sell → book $10,381.14; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.90 | ▼ -83.63 after sell → book $10,379.00; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.75 | ▲ +471.89 after sell → book $10,358.83; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,309.06 | ▼ -66.33 after sell → book $10,356.66; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.06 | ▼ close $10,355.74 vs 09:30 $10,391.80 (session -0.92) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.06 | ▲ 09:30 equity $10,355.88 vs yday $10,355.74 (+0.14) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.85 | ▼ -0.31 after sell → book $10,355.76; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,329.12 | ▼ -1.08 after sell → book $10,355.59; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.32 | ▼ -0.94 after sell → book $10,355.43; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,340.32 | ▲ close $10,355.75 vs 09:30 $10,355.88 (session +0.32) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.32 | ▼ 09:30 equity $10,355.62 vs yday $10,355.75 (-0.13) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,344.18 | ▼ -0.24 after sell → book $10,355.56; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.49 | ▼ -0.24 after sell → book $10,355.47; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 1 | $4.98 | $0.07 | $+0.05 | $10,355.40 | ▲ +0.05 after sell → book $10,355.40; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.98 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.64 | ▲ close $10,569.98 vs 09:30 $10,355.62 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.04 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.21 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▲ close $10,844.89 vs 09:30 $10,845.87 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,956.63 vs yday $10,844.89 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▼ close $10,921.87 vs 09:30 $10,956.63 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▼ 09:30 equity $10,751.25 vs yday $10,921.87 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,351.68 | ▼ -18.63 after sell → book $10,749.05; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,691.67 | ▲ +63.82 after sell → book $10,747.00; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,958.61 | ▼ -15.53 after sell → book $10,744.80; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,194.39 | ▼ -59.59 after sell → book $10,741.86; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.84 | ▲ +98.31 after sell → book $10,739.66; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,958.46 | ▲ +111.41 after sell → book $10,737.52; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.89 | ▲ +91.65 after sell → book $10,727.85; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.94 | ▲ +91.71 after sell → book $10,725.82; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,293.43 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+13.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 120 | $10.98 | $2.35 | — | $7,973.48 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+1.2; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,686.44 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+7.4; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.35 | $2.46 | — | $5,364.67 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $4.94 | $3.46 | — | $4,037.30 | — | baseline list, no extra gate; list flatten; ret5=+7.1; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,754.39 | — | baseline list, no extra gate; list flatten; ret5=+6.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 182 | $7.25 | $2.54 | — | $1,432.35 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3702 | $0.36 | $24.36 | — | $82.68 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1325.37 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.68 | ▲ close $10,935.51 vs 09:30 $10,751.25 (session +251.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.68 | ▼ 09:30 equity $10,932.90 vs yday $10,935.51 (-2.61) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $99.09 | ▼ -0.96 after sell → book $10,932.71; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $129.45 | ▲ +7.88 after sell → book $10,932.38; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $153.26 | ▼ -1.17 after sell → book $10,932.09; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $179.33 | ▲ +0.69 after sell → book $10,931.77; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $209.35 | ▲ +4.63 after sell → book $10,931.38; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 3 | $31.21 | $0.95 | — | $114.77 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $104.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 9 | $11.12 | $1.03 | — | $13.67 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $104.67 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.67 | ▲ close $11,216.12 vs 09:30 $10,932.90 (session +286.71) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.67 | ▼ 09:30 equity $11,207.19 vs yday $11,216.12 (-8.93) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1 | $2.60 | $0.03 | — | $11.04 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+13.0; leftover $2.73 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.04 | ▼ close $11,194.56 vs 09:30 $11,207.19 (session -12.60) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.04 | ▼ 09:30 equity $11,139.52 vs yday $11,194.56 (-55.04) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 120 | $10.97 | $2.38 | $-5.93 | $1,325.06 | ▼ -5.93 after sell → book $11,137.14; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $60.52 | $2.07 | $-18.20 | $2,593.90 | ▼ -18.20 after sell → book $11,135.06; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.28 | $2.50 | $-16.02 | $3,899.64 | ▼ -16.02 after sell → book $11,132.56; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 268 | $4.95 | $3.51 | $-4.29 | $5,222.73 | ▼ -4.29 after sell → book $11,129.05; vs 09:30 mark -3.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $6,491.99 | ▼ -13.65 after sell → book $11,127.03; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 182 | $9.73 | $2.58 | $+446.24 | $8,260.27 | ▲ +446.24 after sell → book $11,124.45; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3702 | $0.36 | $25.24 | $-23.69 | $9,586.26 | ▼ -23.69 after sell → book $11,099.21; vs 09:30 mark -25.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 38 | $41.74 | $2.10 | — | $7,998.03 | — | baseline list, no extra gate; list flatten; ret5=+2.4; leftover $1597.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 109 | $14.63 | $2.32 | — | $6,401.05 | — | baseline list, no extra gate; list flatten; ret5=+5.8; leftover $1597.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 48 | $32.90 | $2.13 | — | $4,819.71 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1597.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 102 | $15.66 | $2.30 | — | $3,220.10 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1597.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 20 | $79.42 | $2.05 | — | $1,629.65 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1597.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 481 | $3.32 | $6.20 | — | $26.52 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1597.71 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.52 | ▼ close $10,801.52 vs 09:30 $11,139.52 (session -280.58) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.52 | ▲ 09:30 equity $10,815.64 vs yday $10,801.52 (+14.12) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.68 | $2.18 | $-9.28 | $1,326.75 | ▼ -9.28 after sell → book $10,813.47; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 3 | $29.94 | $0.93 | $-5.68 | $1,415.64 | ▼ -5.68 after sell → book $10,812.54; vs 09:30 mark -0.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 9 | $10.82 | $1.02 | $-4.75 | $1,512.00 | ▼ -4.75 after sell → book $10,811.52; vs 09:30 mark -1.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,512.00 | ▲ close $10,909.62 vs 09:30 $10,815.64 (session +98.10) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,512.00 | ▲ 09:30 equity $11,007.87 vs yday $10,909.62 (+98.25) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 1 | $2.67 | $0.05 | $-0.01 | $1,514.62 | ▼ -0.01 after sell → book $11,007.82; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,514.62 | ▲ close $11,109.14 vs 09:30 $11,007.87 (session +101.32) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,514.62 | ▼ 09:30 equity $11,037.97 vs yday $11,109.14 (-71.17) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 38 | $42.10 | $2.13 | $+9.45 | $3,112.29 | ▲ +9.45 after sell → book $11,035.84; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 109 | $15.70 | $2.35 | $+111.96 | $4,821.24 | ▲ +111.96 after sell → book $11,033.49; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 48 | $32.42 | $2.16 | $-27.33 | $6,375.25 | ▼ -27.33 after sell → book $11,031.34; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 102 | $13.92 | $2.32 | $-182.10 | $7,792.76 | ▼ -182.10 after sell → book $11,029.01; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 20 | $78.84 | $2.07 | $-15.72 | $9,367.49 | ▼ -15.72 after sell → book $11,026.94; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 481 | $3.45 | $6.30 | $+50.03 | $11,020.64 | ▲ +50.03 after sell → book $11,020.64; vs 09:30 mark -6.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,020.64 | ▲ close $11,020.64 vs 09:30 $11,037.97 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,020.64 | ▲ 09:30 equity $11,020.64 vs yday $11,020.64 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $9,643.69 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $8,267.85 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 379 | $3.63 | $4.89 | — | $6,887.19 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 171 | $8.03 | $2.50 | — | $5,511.55 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,185.03 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 89 | $15.45 | $2.26 | — | $2,807.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,492.21 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1377.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 82 | $16.77 | $2.24 | — | $114.83 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1377.58 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.83 | ▼ close $10,754.41 vs 09:30 $11,020.64 (session -246.15) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.83 | ▲ 09:30 equity $10,757.46 vs yday $10,754.41 (+3.05) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 7 | $2.52 | $0.20 | — | $96.99 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $19.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $83.43 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $19.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 10 | $1.90 | $0.22 | — | $64.21 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $19.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $4.78 | $0.20 | — | $44.89 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $19.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 12 | $1.59 | $0.23 | — | $25.58 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $19.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $14.16 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $19.14 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.16 | ▲ close $10,789.56 vs 09:30 $10,757.46 (session +33.20) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.16 | ▲ 09:30 equity $10,823.43 vs yday $10,789.56 (+33.87) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.16 | ▼ close $10,639.73 vs 09:30 $10,823.43 (session -183.70) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.16 | ▼ 09:30 equity $10,587.07 vs yday $10,639.73 (-52.66) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 26 | $53.16 | $2.09 | $+3.12 | $1,394.23 | ▲ +3.12 after sell → book $10,584.98; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 32 | $42.01 | $2.11 | $-33.63 | $2,736.44 | ▼ -33.63 after sell → book $10,582.88; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 171 | $8.01 | $2.54 | $-8.47 | $4,103.61 | ▼ -8.47 after sell → book $10,580.33; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 10 | $125.77 | $2.04 | $-70.86 | $5,359.27 | ▼ -70.86 after sell → book $10,578.29; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 89 | $15.16 | $2.28 | $-30.35 | $6,706.23 | ▼ -30.35 after sell → book $10,576.01; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 9 | $140.29 | $2.04 | $-54.90 | $7,966.84 | ▼ -54.90 after sell → book $10,573.97; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 82 | $15.46 | $2.26 | $-111.92 | $9,232.30 | ▼ -111.92 after sell → book $10,571.71; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,232.30 | ▼ close $10,427.20 vs 09:30 $10,587.07 (session -144.51) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,232.30 | ▼ 09:30 equity $10,403.13 vs yday $10,427.20 (-24.07) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 379 | $2.85 | $4.96 | $-305.47 | $10,307.49 | ▼ -305.47 after sell → book $10,398.17; vs 09:30 mark -4.96 | dropped from list after 4 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 7 | $2.22 | $0.20 | $-2.49 | $10,322.83 | ▼ -2.49 after sell → book $10,397.97; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $10,334.91 | ▼ -1.49 after sell → book $10,397.82; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 10 | $1.83 | $0.23 | $-1.15 | $10,352.97 | ▼ -1.15 after sell → book $10,397.59; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 4 | $3.92 | $0.19 | $-3.82 | $10,368.47 | ▼ -3.82 after sell → book $10,397.40; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 12 | $1.53 | $0.24 | $-1.19 | $10,386.59 | ▼ -1.19 after sell → book $10,397.16; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $10,397.03 | ▼ -0.98 after sell → book $10,397.03; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.03 | ▲ close $10,397.03 vs 09:30 $10,403.13 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.03 | ▲ 09:30 equity $10,397.03 vs yday $10,397.03 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 79 | $16.28 | $2.23 | — | $9,108.69 | — | baseline list, no extra gate; list flatten; 🔵; ret5=-1.1; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 476 | $2.73 | $6.14 | — | $7,803.07 | — | baseline list, no extra gate; list flatten; 🔵; ret5=-3.0; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,560.02 | — | baseline list, no extra gate; list flatten; ret5=+8.3; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,407.00 | — | baseline list, no extra gate; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,142.74 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+4.7; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,850.62 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+19.6; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 637 | $2.04 | $8.22 | — | $1,542.92 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1299.63 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 273 | $4.75 | $3.52 | — | $242.65 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1299.63 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.65 | ▼ close $10,351.06 vs 09:30 $10,397.03 (session -17.78) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.65 | ▼ 09:30 equity $10,046.86 vs yday $10,351.06 (-304.20) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.65 | ▼ close $9,989.93 vs 09:30 $10,046.86 (session -56.93) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.65 | ▲ 09:30 equity $10,022.28 vs yday $9,989.93 (+32.35) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.65 | ▼ close $9,751.55 vs 09:30 $10,022.28 (session -270.73) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.65 | ▲ 09:30 equity $9,820.47 vs yday $9,751.55 (+68.92) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 79 | $16.16 | $2.25 | $-13.96 | $1,517.04 | ▼ -13.96 after sell → book $9,818.22; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 476 | $2.72 | $6.23 | $-17.13 | $2,805.53 | ▼ -17.13 after sell → book $9,811.99; vs 09:30 mark -6.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 6 | $194.84 | $2.03 | $-76.04 | $3,972.54 | ▼ -76.04 after sell → book $9,809.96; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $4,950.72 | ▼ -174.84 after sell → book $9,807.93; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 8 | $147.79 | $2.03 | $-83.97 | $6,131.00 | ▼ -83.97 after sell → book $9,805.89; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 23 | $51.29 | $2.08 | $-114.54 | $7,308.59 | ▼ -114.54 after sell → book $9,803.81; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 637 | $1.89 | $8.33 | $-112.10 | $8,504.19 | ▼ -112.10 after sell → book $9,795.48; vs 09:30 mark -8.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 273 | $4.73 | $3.58 | $-12.56 | $9,791.90 | ▼ -12.56 after sell → book $9,791.90; vs 09:30 mark -3.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,706.34 | — | baseline list, no extra gate; list flatten; ret5=+4.0; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,547.51 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+7.2; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 85 | $14.31 | $2.25 | — | $6,328.91 | — | baseline list, no extra gate; list flatten; ret5=+4.8; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $5,123.64 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+2.9; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 65 | $18.61 | $2.19 | — | $3,911.81 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 67 | $18.21 | $2.19 | — | $2,689.55 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $1,518.08 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1223.99 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 208 | $5.87 | $2.68 | — | $294.43 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1223.99 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $294.43 | ▲ close $9,967.32 vs 09:30 $9,820.47 (session +192.89) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $294.43 | ▲ 09:30 equity $10,130.76 vs yday $9,967.32 (+163.44) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $263.37 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $36.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $232.69 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $36.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $197.41 | — | baseline list, no extra gate; list flatten; ret5=+1.6; leftover $36.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 216 | $0.17 | $1.02 | — | $159.67 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $36.80 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 2 | $15.87 | $0.32 | — | $127.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $36.80 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.61 | ▲ close $10,183.77 vs 09:30 $10,130.76 (session +55.33) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.61 | ▲ 09:30 equity $10,212.20 vs yday $10,183.77 (+28.43) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 16 | $0.97 | $0.20 | — | $111.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $15.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 7 | $2.08 | $0.17 | — | $97.16 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $15.95 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.16 | ▼ close $10,136.20 vs 09:30 $10,212.20 (session -75.63) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.16 | ▲ 09:30 equity $10,203.89 vs yday $10,136.20 (+67.69) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,162.18 | ▼ -20.54 after sell → book $10,201.87; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $2,304.17 | ▼ -16.84 after sell → book $10,199.81; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 85 | $13.65 | $2.27 | $-60.61 | $3,462.15 | ▼ -60.61 after sell → book $10,197.54; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 33 | $36.70 | $2.11 | $+3.72 | $4,671.14 | ▲ +3.72 after sell → book $10,195.43; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 65 | $22.11 | $2.21 | $+223.11 | $6,106.09 | ▲ +223.11 after sell → book $10,193.23; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 67 | $20.55 | $2.21 | $+152.38 | $7,480.72 | ▲ +152.38 after sell → book $10,191.01; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+170.83 | $8,823.02 | ▲ +170.83 after sell → book $10,188.95; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 208 | $5.62 | $2.73 | $-57.41 | $9,989.25 | ▼ -57.41 after sell → book $10,186.22; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 10 | $157.87 | $2.02 | — | $8,408.53 | — | baseline list, no extra gate; list flatten; ret5=+6.5; leftover $1664.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 4 | $386.20 | $2.00 | — | $6,861.73 | — | baseline list, no extra gate; list flatten; ret5=-5.8; leftover $1664.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 18 | $88.83 | $2.04 | — | $5,260.75 | — | baseline list, no extra gate; list flatten; ret5=+7.6; leftover $1664.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 123 | $13.47 | $2.36 | — | $3,601.58 | — | baseline list, no extra gate; list flatten; ret5=+3.6; leftover $1664.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 416 | $4.00 | $5.37 | — | $1,932.21 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1664.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 178 | $9.31 | $2.52 | — | $272.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1664.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $272.51 | ▼ close $9,854.29 vs 09:30 $10,203.89 (session -315.62) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.51 | ▲ 09:30 equity $9,900.02 vs yday $9,854.29 (+45.73) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 3 | $10.18 | $0.33 | $-0.86 | $302.71 | ▼ -0.86 after sell → book $9,899.68; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 216 | $0.16 | $1.04 | $-4.21 | $336.24 | ▼ -4.21 after sell → book $9,898.65; vs 09:30 mark -1.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 72 | $0.58 | $0.63 | — | $293.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $42.03 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.84 | ▲ close $9,985.93 vs 09:30 $9,900.02 (session +87.92) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.84 | ▲ 09:30 equity $10,009.88 vs yday $9,985.93 (+23.95) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 1 | $34.78 | $0.37 | $-0.87 | $328.25 | ▼ -0.87 after sell → book $10,009.51; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 2 | $17.10 | $0.37 | $+1.77 | $362.08 | ▲ +1.77 after sell → book $10,009.14; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 16 | $0.89 | $0.21 | $-1.69 | $376.11 | ▼ -1.69 after sell → book $10,008.93; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 7 | $2.16 | $0.19 | $+0.20 | $391.04 | ▲ +0.20 after sell → book $10,008.74; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 2 | $27.79 | $0.56 | — | $334.90 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+7.0; leftover $78.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 7 | $9.81 | $0.71 | — | $265.52 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+4.0; leftover $78.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 3 | $20.25 | $0.62 | — | $204.15 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+15.0; leftover $78.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 3 | $20.65 | $0.63 | — | $141.58 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $78.21 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.58 | ▼ close $9,697.87 vs 09:30 $10,009.88 (session -308.35) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.58 | ▼ 09:30 equity $9,526.58 vs yday $9,697.87 (-171.29) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 4 | $7.38 | $0.33 | $-1.48 | $170.77 | ▼ -1.48 after sell → book $9,526.26; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 10 | $163.95 | $2.04 | $+56.74 | $1,808.23 | ▲ +56.74 after sell → book $9,524.21; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 4 | $374.54 | $2.02 | $-50.67 | $3,304.36 | ▼ -50.67 after sell → book $9,522.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 18 | $87.67 | $2.07 | $-24.90 | $4,880.45 | ▼ -24.90 after sell → book $9,520.12; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 123 | $11.42 | $2.39 | $-256.90 | $6,282.71 | ▼ -256.90 after sell → book $9,517.73; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 416 | $3.40 | $5.45 | $-260.41 | $7,691.67 | ▼ -260.41 after sell → book $9,512.29; vs 09:30 mark -5.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 178 | $8.67 | $2.57 | $-119.01 | $9,232.36 | ▼ -119.01 after sell → book $9,509.72; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,232.36 | ▲ close $9,510.30 vs 09:30 $9,526.58 (session +0.58) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,492.73 | ▲ 09:30 equity $8,032.53 vs yday $8,031.57 (+0.96) | 09:30 open · cash $7,492.73 (unchanged overnight, no fees) · equity $8,032.53 vs prior close $8,031.57 (+0.96) · 11 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×131 yday $0.53 → 09:30 $0.53 +0.00; DLO×5 yday $13.88 → 09:30 $13.88 +0.00; FJET×38 yday $1.80 → 09:30 $1.80 +0.00; FTRE×2 yday $20.02 → 09:30 $20.02 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×5 yday $7.70 → 09:30 $7.70 +0.00; TDC×2 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $6,686.87 | — | baseline list, no extra gate; list flatten; ret5=+0.8; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $5,646.61 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $4,604.77 | — | baseline list, no extra gate; list flatten; ret5=+4.7; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 139 | $7.65 | $2.41 | — | $3,539.01 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $2,486.10 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,478.96 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 118 | $9.05 | $2.34 | — | $408.71 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $1070.39 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $408.71 | ▲ close $8,057.62 vs 09:30 $8,032.53 (session +40.06) | 16:00 close · cash $408.71 · equity $8,057.62 vs 09:30 $8,032.53 (+25.09; session marks +40.06) · 18 name(s) marked open→close (per-name table). ADMA×4 09:30 $9.52 → close $9.52 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×131 09:30 $0.53 → close $0.53 +0.00; DLO×5 09:30 $13.88 → close $13.88 +0.00; FJET×38 09:30 $1.80 → close $1.80 -0.00; FTRE×2 09:30 $20.02 → close $20.02 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×5 09:30 $7.70 → close $7.70 -0.00; TDC×2 09:30 $29.46 → close $29.46 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×139 09:30 $7.65 → close $7.60 -6.95; WRBY×40 09:30 $26.27 → close $26.71 +17.60; TXG×12 09:30 $83.76 → close $85.71 +23.40; AEHL×118 09:30 $9.05 → close $9.36 +36.58 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 2.73 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 2.73 < 1 share @ 14.42 |
| 2026-08-27 | `KURA` | cash | leftover split 2.73 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 2.73 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 36.80 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 36.80 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 36.80 < 1 share @ 147.61 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 15.95 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 15.95 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 15.95 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 15.95 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 15.95 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 15.95 < 1 share @ 34.44 |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 42.03 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 78.21 < 1 share @ 116.85 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 72 | 2026-09-22 @ $0.58 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $42.03 |
| `ARQT` | 2 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+7.0; leftover $78.21 |
| `ADMA` | 7 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+4.0; leftover $78.21 |
| `FTRE` | 3 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+15.0; leftover $78.21 |
| `OMER` | 3 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $78.21 |
