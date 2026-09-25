# Factor mine action — `union_h3_exit_news_r`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hold 3d, sell next 09:30 if news🔴

Cash book **-23.34%** ($7,666) · signal-only (no cash/fees) was +1.29%. Starts YES **0/30**. Fills 192 · skips 301 · realized $-427.03.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- Early exit: sell at the next 09:30 if the news camera turns red, even inside the floor.
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,217.82.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 1 | $5.07 | $0.05 | — | $48.18 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=-4.7; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.18 | ▲ close $10,525.00 vs 09:30 $10,414.78 (session +110.38) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.18 | ▼ 09:30 equity $10,391.53 vs yday $10,525.00 (-133.47) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.11 | ▼ -0.12 after sell → book $10,389.46; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.14 | ▼ -69.50 after sell → book $10,387.37; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.54 | ▲ +23.38 after sell → book $10,385.29; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.45 | ▼ -14.65 after sell → book $10,383.20; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.07 | ▲ +97.12 after sell → book $10,380.86; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.64 | ▼ -83.63 after sell → book $10,378.73; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.48 | ▲ +471.89 after sell → book $10,358.55; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,308.79 | ▼ -66.33 after sell → book $10,356.38; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,308.79 | ▼ close $10,355.26 vs 09:30 $10,391.53 (session -1.13) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,308.79 | ▲ 09:30 equity $10,355.41 vs yday $10,355.26 (+0.15) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.59 | ▼ -0.31 after sell → book $10,355.30; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,328.86 | ▼ -1.08 after sell → book $10,355.13; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.06 | ▼ -0.94 after sell → book $10,354.97; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,340.06 | ▼ close $10,354.93 vs 09:30 $10,355.41 (session -0.04) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.06 | ▼ 09:30 equity $10,354.83 vs yday $10,354.93 (-0.10) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,343.92 | ▼ -0.24 after sell → book $10,354.77; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.23 | ▼ -0.24 after sell → book $10,354.68; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 1 | $4.45 | $0.07 | $-0.74 | $10,354.61 | ▼ -0.74 after sell → book $10,354.61; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,078.34 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.16 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,519.69 | — | hold 3d, sell next 09:30 if news🔴; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,224.32 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.18 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,669.97 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.19 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.86 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.33 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.86 | ▲ close $10,569.20 vs 09:30 $10,354.83 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.86 | ▲ 09:30 equity $10,845.09 vs yday $10,569.20 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $191.48 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $168.99 | — | hold 3d, sell next 09:30 if news🔴; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.02 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $118.64 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.11 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $93.25 | — | hold 3d, sell next 09:30 if news🔴; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.11 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.25 | ▲ close $10,844.10 vs 09:30 $10,845.09 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.25 | ▲ 09:30 equity $10,955.84 vs yday $10,844.10 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.25 | ▼ close $10,921.08 vs 09:30 $10,955.84 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.25 | ▼ 09:30 equity $10,750.46 vs yday $10,921.08 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,350.89 | ▼ -18.63 after sell → book $10,748.26; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,690.88 | ▲ +63.82 after sell → book $10,746.21; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,957.82 | ▼ -15.53 after sell → book $10,744.01; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,193.61 | ▼ -59.59 after sell → book $10,741.08; vs 09:30 mark -2.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.05 | ▲ +98.31 after sell → book $10,738.87; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,957.67 | ▲ +111.41 after sell → book $10,736.73; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.10 | ▲ +91.65 after sell → book $10,727.06; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.15 | ▲ +91.71 after sell → book $10,725.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,292.64 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ⚪; ret5=+13.0; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 120 | $10.98 | $2.35 | — | $7,972.69 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=+1.2; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,685.65 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=+7.4; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.35 | $2.46 | — | $5,363.89 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $4.94 | $3.46 | — | $4,036.51 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+7.1; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,753.60 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+6.0; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 182 | $7.25 | $2.54 | — | $1,431.56 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1325.27 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3701 | $0.36 | $24.35 | — | $82.25 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; ret5=-15.6; leftover $1325.27 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.25 | ▲ close $10,934.74 vs 09:30 $10,750.46 (session +251.08) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.25 | ▼ 09:30 equity $10,932.13 vs yday $10,934.74 (-2.61) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $98.66 | ▼ -0.96 after sell → book $10,931.94; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $129.03 | ▲ +7.88 after sell → book $10,931.60; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $152.84 | ▼ -1.17 after sell → book $10,931.31; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $178.91 | ▲ +0.69 after sell → book $10,930.99; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $208.93 | ▲ +4.63 after sell → book $10,930.61; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 3 | $31.21 | $0.95 | — | $114.35 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $104.46 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 9 | $11.12 | $1.03 | — | $13.24 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $104.46 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.24 | ▲ close $11,215.31 vs 09:30 $10,932.13 (session +286.67) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.24 | ▼ 09:30 equity $11,206.38 vs yday $11,215.31 (-8.93) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1 | $2.60 | $0.03 | — | $10.61 | — | hold 3d, sell next 09:30 if news🔴; list flatten,ohlc_hot; ret5=+13.0; leftover $2.65 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.61 | ▼ close $11,193.77 vs 09:30 $11,206.38 (session -12.57) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.61 | ▼ 09:30 equity $11,138.73 vs yday $11,193.77 (-55.04) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 120 | $10.97 | $2.38 | $-5.93 | $1,324.63 | ▼ -5.93 after sell → book $11,136.35; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $60.52 | $2.07 | $-18.20 | $2,593.48 | ▼ -18.20 after sell → book $11,134.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.28 | $2.50 | $-16.02 | $3,899.22 | ▼ -16.02 after sell → book $11,131.77; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 268 | $4.95 | $3.51 | $-4.29 | $5,222.31 | ▼ -4.29 after sell → book $11,128.26; vs 09:30 mark -3.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $6,491.57 | ▼ -13.65 after sell → book $11,126.24; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 182 | $9.73 | $2.58 | $+446.24 | $8,259.85 | ▲ +446.24 after sell → book $11,123.66; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3701 | $0.36 | $25.24 | $-23.68 | $9,585.48 | ▼ -23.68 after sell → book $11,098.43; vs 09:30 mark -25.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 38 | $41.74 | $2.10 | — | $7,997.25 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+2.4; leftover $1597.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 109 | $14.63 | $2.32 | — | $6,400.26 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+5.8; leftover $1597.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 48 | $32.90 | $2.13 | — | $4,818.93 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1597.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 102 | $15.66 | $2.30 | — | $3,219.31 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1597.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 20 | $79.42 | $2.05 | — | $1,628.86 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1597.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 185 | $8.61 | $2.54 | — | $33.47 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; ret5=-0.7; leftover $1597.58 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.47 | ▼ close $10,831.04 vs 09:30 $11,138.73 (session -253.94) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.47 | ▲ 09:30 equity $10,859.59 vs yday $10,831.04 (+28.55) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.68 | $2.18 | $-9.28 | $1,333.69 | ▼ -9.28 after sell → book $10,857.41; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 3 | $29.94 | $0.93 | $-5.68 | $1,422.59 | ▼ -5.68 after sell → book $10,856.49; vs 09:30 mark -0.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 9 | $10.82 | $1.02 | $-4.75 | $1,518.95 | ▼ -4.75 after sell → book $10,855.47; vs 09:30 mark -1.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,518.95 | ▲ close $10,873.28 vs 09:30 $10,859.59 (session +17.81) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,518.95 | ▲ 09:30 equity $10,925.28 vs yday $10,873.28 (+52.00) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 1 | $2.67 | $0.05 | $-0.01 | $1,521.57 | ▼ -0.01 after sell → book $10,925.23; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,521.57 | ▼ close $10,808.25 vs 09:30 $10,925.28 (session -116.98) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,521.57 | ▼ 09:30 equity $10,726.72 vs yday $10,808.25 (-81.53) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 38 | $42.10 | $2.13 | $+9.45 | $3,119.24 | ▲ +9.45 after sell → book $10,724.59; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 109 | $15.70 | $2.35 | $+111.96 | $4,828.19 | ▲ +111.96 after sell → book $10,722.24; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 48 | $32.42 | $2.16 | $-27.33 | $6,382.19 | ▼ -27.33 after sell → book $10,720.08; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 102 | $13.92 | $2.32 | $-182.10 | $7,799.71 | ▼ -182.10 after sell → book $10,717.76; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 20 | $78.84 | $2.07 | $-15.72 | $9,374.44 | ▼ -15.72 after sell → book $10,715.69; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 185 | $7.25 | $2.59 | $-256.73 | $10,713.10 | ▼ -256.73 after sell → book $10,713.10; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.10 | ▲ close $10,713.10 vs 09:30 $10,726.72 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.10 | ▲ 09:30 equity $10,713.10 vs yday $10,713.10 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,389.04 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,056.12 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 368 | $3.63 | $4.75 | — | $6,715.54 | — | hold 3d, sell next 09:30 if news🔴; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 166 | $8.03 | $2.49 | — | $5,380.07 | — | hold 3d, sell next 09:30 if news🔴; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,053.55 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.45 | $2.25 | — | $2,722.60 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,407.08 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1339.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.77 | $2.23 | — | $80.02 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1339.14 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.02 | ▼ close $10,455.57 vs 09:30 $10,713.10 (session -237.63) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.02 | ▲ 09:30 equity $10,459.68 vs yday $10,455.57 (+4.11) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 5 | $2.52 | $0.14 | — | $67.28 | — | hold 3d, sell next 09:30 if news🔴; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $13.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $60.50 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $13.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 7 | $1.90 | $0.15 | — | $47.05 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $13.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $37.38 | — | hold 3d, sell next 09:30 if news🔴; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $13.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $25.96 | — | hold 3d, sell next 09:30 if news🔴; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $13.34 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.96 | ▲ close $10,489.27 vs 09:30 $10,459.68 (session +30.17) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.96 | ▲ 09:30 equity $10,521.59 vs yday $10,489.27 (+32.32) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.96 | ▼ close $10,342.27 vs 09:30 $10,521.59 (session -179.32) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.96 | ▼ 09:30 equity $10,291.19 vs yday $10,342.27 (-51.08) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 25 | $53.16 | $2.09 | $+2.85 | $1,352.87 | ▲ +2.85 after sell → book $10,289.11; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 31 | $42.01 | $2.10 | $-32.71 | $2,653.08 | ▼ -32.71 after sell → book $10,287.00; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 368 | $3.28 | $4.82 | $-138.37 | $3,855.30 | ▼ -138.37 after sell → book $10,282.19; vs 09:30 mark -4.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 166 | $8.01 | $2.53 | $-8.33 | $5,182.43 | ▼ -8.33 after sell → book $10,279.66; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 10 | $125.77 | $2.04 | $-70.86 | $6,438.09 | ▼ -70.86 after sell → book $10,277.62; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 86 | $15.16 | $2.27 | $-29.46 | $7,739.58 | ▼ -29.46 after sell → book $10,275.35; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 9 | $140.29 | $2.04 | $-54.90 | $9,000.20 | ▼ -54.90 after sell → book $10,273.31; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 79 | $15.46 | $2.25 | $-107.97 | $10,219.29 | ▼ -107.97 after sell → book $10,271.06; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,219.29 | ▼ close $10,268.56 vs 09:30 $10,291.19 (session -2.50) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,219.29 | ▼ 09:30 equity $10,267.72 vs yday $10,268.56 (-0.84) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 5 | $2.22 | $0.15 | $-1.79 | $10,230.24 | ▼ -1.79 after sell → book $10,267.58; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $10,236.27 | ▼ -0.75 after sell → book $10,267.49; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 7 | $1.83 | $0.17 | $-0.81 | $10,248.91 | ▼ -0.81 after sell → book $10,267.32; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $10,256.65 | ▼ -1.92 after sell → book $10,267.22; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $10,267.09 | ▼ -0.98 after sell → book $10,267.09; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,267.09 | ▲ close $10,267.09 vs 09:30 $10,267.72 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,267.09 | ▲ 09:30 equity $10,267.09 vs yday $10,267.09 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 78 | $16.28 | $2.22 | — | $8,995.03 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=-1.1; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 470 | $2.73 | $6.06 | — | $7,705.86 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=-3.0; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,462.82 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+8.3; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,309.80 | — | hold 3d, sell next 09:30 if news🔴; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,045.54 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=+4.7; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $2,809.51 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=+19.6; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 629 | $2.04 | $8.11 | — | $1,518.23 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1283.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 270 | $4.75 | $3.48 | — | $232.25 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1283.39 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.25 | ▼ close $10,220.80 vs 09:30 $10,267.09 (session -18.32) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.25 | ▼ 09:30 equity $9,921.13 vs yday $10,220.80 (-299.67) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.25 | ▼ close $9,865.81 vs 09:30 $9,921.13 (session -55.32) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.25 | ▲ 09:30 equity $9,897.46 vs yday $9,865.81 (+31.65) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.25 | ▼ close $9,629.53 vs 09:30 $9,897.46 (session -267.93) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.25 | ▲ 09:30 equity $9,696.99 vs yday $9,629.53 (+67.46) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 78 | $16.16 | $2.25 | $-13.83 | $1,490.48 | ▼ -13.83 after sell → book $9,694.74; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 470 | $2.72 | $6.15 | $-16.91 | $2,762.73 | ▼ -16.91 after sell → book $9,688.59; vs 09:30 mark -6.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 6 | $194.84 | $2.03 | $-76.04 | $3,929.74 | ▼ -76.04 after sell → book $9,686.56; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $4,907.92 | ▼ -174.84 after sell → book $9,684.53; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 8 | $147.79 | $2.03 | $-83.97 | $6,088.21 | ▼ -83.97 after sell → book $9,682.50; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 22 | $51.29 | $2.08 | $-109.73 | $7,214.51 | ▼ -109.73 after sell → book $9,680.42; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 629 | $1.89 | $8.23 | $-110.69 | $8,395.09 | ▼ -110.69 after sell → book $9,672.19; vs 09:30 mark -8.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 270 | $4.73 | $3.54 | $-12.42 | $9,668.65 | ▼ -12.42 after sell → book $9,668.65; vs 09:30 mark -3.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,583.09 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+4.0; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,424.26 | — | hold 3d, sell next 09:30 if news🔴; list flatten,ohlc_hot; ret5=+7.2; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 84 | $14.31 | $2.24 | — | $6,219.98 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+4.8; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $5,014.71 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ret5=+2.9; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 66 | $18.21 | $2.19 | — | $3,810.66 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; ret5=-19.1; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 17 | $68.79 | $2.04 | — | $2,639.19 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 205 | $5.87 | $2.64 | — | $1,433.19 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1208.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 444 | $2.72 | $5.73 | — | $219.79 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; ret5=-0.4; leftover $1208.58 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.79 | ▲ close $9,693.13 vs 09:30 $9,696.99 (session +45.44) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.79 | ▲ 09:30 equity $9,850.92 vs yday $9,693.13 (+157.79) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $199.07 | — | hold 3d, sell next 09:30 if news🔴; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $27.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $176.07 | — | hold 3d, sell next 09:30 if news🔴; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $27.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 161 | $0.17 | $0.76 | — | $147.94 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $27.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $131.91 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $27.47 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.91 | ▲ close $10,052.01 vs 09:30 $9,850.92 (session +202.46) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.91 | ▲ 09:30 equity $10,087.08 vs yday $10,052.01 (+35.07) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 16 | $0.97 | $0.20 | — | $116.19 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $16.49 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 4 | $3.95 | $0.17 | — | $100.22 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $16.49 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $86.00 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $16.49 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 2 | $5.83 | $0.12 | — | $74.22 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $16.49 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.22 | ▼ close $9,950.89 vs 09:30 $10,087.08 (session -135.55) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.22 | ▲ 09:30 equity $10,030.48 vs yday $9,950.89 (+79.59) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,139.24 | ▼ -20.54 after sell → book $10,028.46; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $2,281.23 | ▼ -16.84 after sell → book $10,026.40; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 84 | $13.65 | $2.27 | $-59.95 | $3,425.57 | ▼ -59.95 after sell → book $10,024.14; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 33 | $36.70 | $2.11 | $+3.72 | $4,634.56 | ▲ +3.72 after sell → book $10,022.03; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 66 | $20.55 | $2.21 | $+150.04 | $5,988.65 | ▲ +150.04 after sell → book $10,019.82; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+170.83 | $7,330.95 | ▲ +170.83 after sell → book $10,017.76; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 205 | $5.62 | $2.69 | $-56.58 | $8,480.36 | ▼ -56.58 after sell → book $10,015.07; vs 09:30 mark -2.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 444 | $3.13 | $5.81 | $+170.50 | $9,864.27 | ▲ +170.50 after sell → book $10,009.26; vs 09:30 mark -5.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $8,757.16 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+6.5; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,596.57 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=-5.8; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $6,439.75 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+7.6; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 132 | $9.31 | $2.39 | — | $5,208.44 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 91 | $13.47 | $2.26 | — | $3,979.95 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1110 | $1.11 | $14.32 | — | $2,733.53 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 123 | $9.99 | $2.36 | — | $1,502.40 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1233.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 675 | $1.82 | $8.71 | — | $261.82 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1233.03 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $261.82 | ▼ close $9,858.36 vs 09:30 $10,030.48 (session -114.81) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $261.82 | ▼ 09:30 equity $9,832.32 vs yday $9,858.36 (-26.04) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $281.95 | ▼ -0.58 after sell → book $9,832.09; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 161 | $0.16 | $0.78 | $-3.14 | $306.94 | ▼ -3.14 after sell → book $9,831.31; vs 09:30 mark -0.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 66 | $0.58 | $0.58 | — | $268.07 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $38.37 | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 4 | $9.40 | $0.39 | — | $230.09 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; ret5=+9.5; leftover $38.37 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.09 | ▲ close $10,042.08 vs 09:30 $9,832.32 (session +211.74) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.09 | ▼ 09:30 equity $10,033.46 vs yday $10,042.08 (-8.62) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $246.99 | ▲ +0.87 after sell → book $10,033.27; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 16 | $0.89 | $0.21 | $-1.69 | $261.02 | ▼ -1.69 after sell → book $10,033.06; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 4 | $4.10 | $0.20 | $+0.23 | $277.23 | ▲ +0.23 after sell → book $10,032.86; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $291.89 | ▲ +0.45 after sell → book $10,032.69; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 2 | $6.29 | $0.15 | $+0.65 | $304.32 | ▲ +0.65 after sell → book $10,032.54; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 2 | $27.79 | $0.56 | — | $248.18 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+7.0; leftover $60.86 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 6 | $9.81 | $0.61 | — | $188.71 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+4.0; leftover $60.86 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 3 | $20.25 | $0.62 | — | $127.35 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+15.0; leftover $60.86 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 2 | $20.65 | $0.42 | — | $85.63 | — | hold 3d, sell next 09:30 if news🔴; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $60.86 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 15 | $3.93 | $0.63 | — | $26.04 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $60.86 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.04 | ▼ close $9,669.02 vs 09:30 $10,033.46 (session -360.68) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.04 | ▼ 09:30 equity $9,588.72 vs yday $9,669.02 (-80.30) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $47.93 | ▼ -1.12 after sell → book $9,588.47; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,193.55 | ▲ +38.52 after sell → book $9,586.44; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $2,315.15 | ▼ -39.00 after sell → book $9,584.42; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $3,452.88 | ▼ -19.09 after sell → book $9,582.37; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 132 | $8.67 | $2.42 | $-89.28 | $4,594.90 | ▼ -89.28 after sell → book $9,579.95; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 91 | $12.26 | $2.29 | $-115.12 | $5,708.27 | ▼ -115.12 after sell → book $9,577.67; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1110 | $1.05 | $14.51 | $-95.43 | $6,859.26 | ▼ -95.43 after sell → book $9,563.15; vs 09:30 mark -14.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 123 | $9.80 | $2.39 | $-28.12 | $8,062.27 | ▼ -28.12 after sell → book $9,560.76; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 675 | $1.73 | $8.83 | $-85.04 | $9,217.82 | ▼ -85.04 after sell → book $9,551.93; vs 09:30 mark -8.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,217.82 | ▲ close $9,556.99 vs 09:30 $9,588.72 (session +5.06) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,319.50 | ▲ 09:30 equity $7,793.86 vs yday $7,793.38 (+0.48) | 09:30 open · cash $7,319.50 (unchanged overnight, no fees) · equity $7,793.86 vs prior close $7,793.38 (+0.48) · 13 name(s) re-marked at the open (per-name table). ADMA×3 yday $9.52 → 09:30 $9.52 +0.00; APPS×4 yday $10.88 → 09:30 $10.88 +0.00; ARHS×6 yday $9.47 → 09:30 $9.47 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×100 yday $0.53 → 09:30 $0.53 +0.00; DLO×4 yday $13.88 → 09:30 $13.88 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; INDP×7 yday $4.00 → 09:30 $4.00 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×3 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $6,513.64 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+0.8; leftover $1045.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $5,471.80 | — | hold 3d, sell next 09:30 if news🔴; list flatten; ret5=+4.7; leftover $1045.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 136 | $7.65 | $2.40 | — | $4,429.00 | — | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1045.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $3,402.36 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1045.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $2,395.22 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1045.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 44 | $23.58 | $2.12 | — | $1,355.57 | — | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1045.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 475 | $2.20 | $6.13 | — | $304.45 | — | hold 3d, sell next 09:30 if news🔴; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1045.64 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.45 | ▼ close $7,666.39 vs 09:30 $7,793.86 (session -108.63) | 16:00 close · cash $304.45 · equity $7,666.39 vs 09:30 $7,793.86 (-127.47; session marks -108.63) · 20 name(s) marked open→close (per-name table). ADMA×3 09:30 $9.52 → close $9.52 +0.00; APPS×4 09:30 $10.88 → close $10.88 +0.00; ARHS×6 09:30 $9.47 → close $9.47 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DEFT×100 09:30 $0.53 → close $0.53 +0.00; DLO×4 09:30 $13.88 → close $13.88 +0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; INDP×7 09:30 $4.00 → close $4.00 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×3 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; REGN×1 09:30 $803.87 → close $788.04 -15.83; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×136 09:30 $7.65 → close $7.60 -6.80; WRBY×39 09:30 $26.27 → close $26.71 +17.16; TXG×12 09:30 $83.76 → close $85.71 +23.40; BRVE×44 09:30 $23.58 → close $20.62 -130.24; HLP×475 09:30 $2.20 → close $2.21 +4.75 | — |

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
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.11 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.11 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.11 < 1 share @ 59.72 |
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
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-27 | `RRC` | cash | leftover split 2.65 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 2.65 < 1 share @ 14.42 |
| 2026-08-27 | `KURA` | cash | leftover split 2.65 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 2.65 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 13.34 < 1 share @ 263.36 |
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
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
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
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 27.47 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 27.47 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 27.47 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 27.47 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 16.49 < 1 share @ 108.55 |
| 2026-09-18 | `GNRC` | cash | leftover split 16.49 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 16.49 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 16.49 < 1 share @ 85.00 |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `DEFT` | 66 | 2026-09-22 @ $0.58 | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $38.37 |
| `ALOY` | 4 | 2026-09-22 @ $9.40 | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer; ret5=+9.5; leftover $38.37 |
| `ARQT` | 2 | 2026-09-23 @ $27.79 | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+7.0; leftover $60.86 |
| `ADMA` | 6 | 2026-09-23 @ $9.81 | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+4.0; leftover $60.86 |
| `FTRE` | 3 | 2026-09-23 @ $20.25 | hold 3d, sell next 09:30 if news🔴; list flatten; 🔵; ⚪; ret5=+15.0; leftover $60.86 |
| `OMER` | 2 | 2026-09-23 @ $20.65 | hold 3d, sell next 09:30 if news🔴; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $60.86 |
| `INDP` | 15 | 2026-09-23 @ $3.93 | hold 3d, sell next 09:30 if news🔴; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $60.86 |
