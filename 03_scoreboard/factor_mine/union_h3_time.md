# Factor mine action — `union_h3_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **-19.42%** ($8,058) · signal-only (no cash/fees) was -4.92%. Starts YES **0/30**. Fills 187 · skips 296 · realized $-285.30.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the timer rings. They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Time-stop: once 3 session(s) are up, sell at 09:30 even if the name is still on the list.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,365.54.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-11.4; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,525.15 vs 09:30 $10,414.78 (session +110.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.37 | ▼ -0.12 after sell → book $10,389.73; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.40 | ▼ -69.50 after sell → book $10,387.64; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.80 | ▲ +23.38 after sell → book $10,385.56; vs 09:30 mark -2.08 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.71 | ▼ -14.65 after sell → book $10,383.47; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.34 | ▲ +97.12 after sell → book $10,381.14; vs 09:30 mark -2.33 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.90 | ▼ -83.63 after sell → book $10,379.00; vs 09:30 mark -2.14 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.75 | ▲ +471.89 after sell → book $10,358.83; vs 09:30 mark -20.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,309.06 | ▼ -66.33 after sell → book $10,356.66; vs 09:30 mark -2.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.06 | ▼ close $10,355.74 vs 09:30 $10,391.80 (session -0.92) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.06 | ▲ 09:30 equity $10,355.88 vs yday $10,355.74 (+0.14) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.85 | ▼ -0.31 after sell → book $10,355.76; vs 09:30 mark -0.12 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,329.12 | ▼ -1.08 after sell → book $10,355.59; vs 09:30 mark -0.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.32 | ▼ -0.94 after sell → book $10,355.43; vs 09:30 mark -0.16 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,340.32 | ▲ close $10,355.75 vs 09:30 $10,355.88 (session +0.32) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.32 | ▼ 09:30 equity $10,355.62 vs yday $10,355.75 (-0.13) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,344.18 | ▼ -0.24 after sell → book $10,355.56; vs 09:30 mark -0.06 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.49 | ▼ -0.24 after sell → book $10,355.47; vs 09:30 mark -0.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 1 | $4.98 | $0.07 | $+0.05 | $10,355.40 | ▲ +0.05 after sell → book $10,355.40; vs 09:30 mark -0.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.98 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.64 | ▲ close $10,569.98 vs 09:30 $10,355.62 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.04 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.21 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▲ close $10,844.89 vs 09:30 $10,845.87 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,956.63 vs yday $10,844.89 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▼ close $10,921.87 vs 09:30 $10,956.63 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▼ 09:30 equity $10,751.25 vs yday $10,921.87 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,351.68 | ▼ -18.63 after sell → book $10,749.05; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,691.67 | ▲ +63.82 after sell → book $10,747.00; vs 09:30 mark -2.05 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,958.61 | ▼ -15.53 after sell → book $10,744.80; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,194.39 | ▼ -59.59 after sell → book $10,741.86; vs 09:30 mark -2.94 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.84 | ▲ +98.31 after sell → book $10,739.66; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,958.46 | ▲ +111.41 after sell → book $10,737.52; vs 09:30 mark -2.14 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.89 | ▲ +91.65 after sell → book $10,727.85; vs 09:30 mark -9.67 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.94 | ▲ +91.71 after sell → book $10,725.82; vs 09:30 mark -2.03 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,293.43 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+13.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 120 | $10.98 | $2.35 | — | $7,973.48 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+1.2; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,686.44 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+7.4; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.35 | $2.46 | — | $5,364.67 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $4.94 | $3.46 | — | $4,037.30 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,754.39 | — | sell at min-hold even if still listed; list flatten; ret5=+6.0; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 182 | $7.25 | $2.54 | — | $1,432.35 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1325.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3702 | $0.36 | $24.36 | — | $82.68 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-15.6; leftover $1325.37 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.68 | ▲ close $10,935.51 vs 09:30 $10,751.25 (session +251.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.68 | ▼ 09:30 equity $10,932.90 vs yday $10,935.51 (-2.61) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $99.09 | ▼ -0.96 after sell → book $10,932.71; vs 09:30 mark -0.19 | time-stop after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $129.45 | ▲ +7.88 after sell → book $10,932.38; vs 09:30 mark -0.33 | time-stop after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $153.26 | ▼ -1.17 after sell → book $10,932.09; vs 09:30 mark -0.29 | time-stop after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $179.33 | ▲ +0.69 after sell → book $10,931.77; vs 09:30 mark -0.32 | time-stop after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $209.35 | ▲ +4.63 after sell → book $10,931.38; vs 09:30 mark -0.39 | time-stop after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 3 | $31.21 | $0.95 | — | $114.77 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $104.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 9 | $11.12 | $1.03 | — | $13.67 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $104.67 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.67 | ▲ close $11,216.12 vs 09:30 $10,932.90 (session +286.71) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.67 | ▼ 09:30 equity $11,207.19 vs yday $11,216.12 (-8.93) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1 | $2.60 | $0.03 | — | $11.04 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+13.0; leftover $2.73 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.04 | ▼ close $11,194.56 vs 09:30 $11,207.19 (session -12.60) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.04 | ▼ 09:30 equity $11,139.52 vs yday $11,194.56 (-55.04) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $23.95 | $2.18 | $+5.57 | $1,326.11 | ▲ +5.57 after sell → book $11,137.34; vs 09:30 mark -2.18 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 120 | $10.97 | $2.38 | $-5.93 | $2,640.13 | ▼ -5.93 after sell → book $11,134.96; vs 09:30 mark -2.38 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $60.52 | $2.07 | $-18.20 | $3,908.98 | ▼ -18.20 after sell → book $11,132.89; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.28 | $2.50 | $-16.02 | $5,214.72 | ▼ -16.02 after sell → book $11,130.39; vs 09:30 mark -2.50 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 268 | $4.95 | $3.51 | $-4.29 | $6,537.80 | ▼ -4.29 after sell → book $11,126.87; vs 09:30 mark -3.52 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $7,807.07 | ▼ -13.65 after sell → book $11,124.86; vs 09:30 mark -2.01 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 182 | $9.73 | $2.58 | $+446.24 | $9,575.34 | ▲ +446.24 after sell → book $11,122.27; vs 09:30 mark -2.59 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3702 | $0.36 | $25.24 | $-23.69 | $10,901.33 | ▼ -23.69 after sell → book $11,097.03; vs 09:30 mark -25.24 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 37 | $41.74 | $2.10 | — | $9,354.85 | — | sell at min-hold even if still listed; list flatten; ret5=+2.4; leftover $1557.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 106 | $14.63 | $2.31 | — | $7,801.76 | — | sell at min-hold even if still listed; list flatten; ret5=+5.8; leftover $1557.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 65 | $23.95 | $2.19 | — | $6,242.83 | — | sell at min-hold even if still listed; list flatten; ret5=+1.8; leftover $1557.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 47 | $32.90 | $2.13 | — | $4,694.40 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1557.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 99 | $15.66 | $2.29 | — | $3,141.77 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1557.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 19 | $79.42 | $2.05 | — | $1,630.74 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1557.33 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 469 | $3.32 | $6.05 | — | $67.61 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=+6.4; leftover $1557.33 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.61 | ▼ close $10,799.79 vs 09:30 $11,139.52 (session -278.13) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.61 | ▲ 09:30 equity $10,814.60 vs yday $10,799.79 (+14.81) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 3 | $29.94 | $0.93 | $-5.68 | $156.51 | ▼ -5.68 after sell → book $10,813.68; vs 09:30 mark -0.92 | time-stop after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 9 | $10.82 | $1.02 | $-4.75 | $252.86 | ▼ -4.75 after sell → book $10,812.65; vs 09:30 mark -1.03 | time-stop after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.86 | ▲ close $10,936.78 vs 09:30 $10,814.60 (session +124.13) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.86 | ▲ 09:30 equity $11,021.31 vs yday $10,936.78 (+84.53) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 1 | $2.67 | $0.05 | $-0.01 | $255.48 | ▼ -0.01 after sell → book $11,021.26; vs 09:30 mark -0.05 | time-stop after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $255.48 | ▲ close $11,174.91 vs 09:30 $11,021.31 (session +153.65) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $255.48 | ▼ 09:30 equity $11,100.71 vs yday $11,174.91 (-74.20) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 37 | $42.10 | $2.12 | $+9.10 | $1,811.06 | ▲ +9.10 after sell → book $11,098.59; vs 09:30 mark -2.12 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 106 | $15.70 | $2.34 | $+108.77 | $3,472.92 | ▲ +108.77 after sell → book $11,096.25; vs 09:30 mark -2.34 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MOS` | 65 | $24.70 | $2.21 | $+44.36 | $5,076.21 | ▲ +44.36 after sell → book $11,094.04; vs 09:30 mark -2.21 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 47 | $32.42 | $2.15 | $-26.84 | $6,597.80 | ▼ -26.84 after sell → book $11,091.89; vs 09:30 mark -2.15 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 99 | $13.92 | $2.31 | $-176.86 | $7,973.57 | ▼ -176.86 after sell → book $11,089.58; vs 09:30 mark -2.31 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 19 | $78.84 | $2.07 | $-15.14 | $9,469.46 | ▼ -15.14 after sell → book $11,087.51; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 469 | $3.45 | $6.14 | $+48.78 | $11,081.37 | ▲ +48.78 after sell → book $11,081.37; vs 09:30 mark -6.14 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,081.37 | ▲ close $11,081.37 vs 09:30 $11,100.71 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,081.37 | ▲ 09:30 equity $11,081.37 vs yday $11,081.37 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $9,704.42 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $8,328.57 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 381 | $3.63 | $4.91 | — | $6,940.63 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 172 | $8.03 | $2.51 | — | $5,556.96 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,230.44 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 89 | $15.45 | $2.26 | — | $2,853.13 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,537.61 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1385.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 82 | $16.77 | $2.24 | — | $160.24 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1385.17 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.24 | ▼ close $10,814.76 vs 09:30 $11,081.37 (session -246.50) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.24 | ▲ 09:30 equity $10,817.70 vs yday $10,814.76 (+2.94) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 10 | $2.52 | $0.28 | — | $134.75 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $26.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 3 | $6.71 | $0.21 | — | $114.41 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $26.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 14 | $1.90 | $0.31 | — | $87.51 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $26.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $4.78 | $0.25 | — | $63.35 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $26.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 16 | $1.59 | $0.30 | — | $37.61 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $26.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 2 | $11.31 | $0.23 | — | $14.76 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $26.71 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.76 | ▲ close $10,849.64 vs 09:30 $10,817.70 (session +33.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.76 | ▲ 09:30 equity $10,882.84 vs yday $10,849.64 (+33.20) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.76 | ▼ close $10,698.29 vs 09:30 $10,882.84 (session -184.55) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.76 | ▼ 09:30 equity $10,645.36 vs yday $10,698.29 (-52.93) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 26 | $53.16 | $2.09 | $+3.12 | $1,394.83 | ▲ +3.12 after sell → book $10,643.27; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 32 | $42.01 | $2.11 | $-33.63 | $2,737.04 | ▼ -33.63 after sell → book $10,641.17; vs 09:30 mark -2.10 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 381 | $3.28 | $4.99 | $-143.25 | $3,981.73 | ▼ -143.25 after sell → book $10,636.18; vs 09:30 mark -4.99 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 172 | $8.01 | $2.55 | $-8.49 | $5,356.91 | ▼ -8.49 after sell → book $10,633.63; vs 09:30 mark -2.55 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 10 | $125.77 | $2.04 | $-70.86 | $6,612.57 | ▼ -70.86 after sell → book $10,631.59; vs 09:30 mark -2.04 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 89 | $15.16 | $2.28 | $-30.35 | $7,959.52 | ▼ -30.35 after sell → book $10,629.31; vs 09:30 mark -2.28 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 9 | $140.29 | $2.04 | $-54.90 | $9,220.14 | ▼ -54.90 after sell → book $10,627.27; vs 09:30 mark -2.04 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 82 | $15.46 | $2.26 | $-111.92 | $10,485.60 | ▼ -111.92 after sell → book $10,625.01; vs 09:30 mark -2.26 | time-stop after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,485.60 | ▼ close $10,618.95 vs 09:30 $10,645.36 (session -6.07) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,485.60 | ▼ 09:30 equity $10,616.98 vs yday $10,618.95 (-1.97) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 10 | $2.22 | $0.27 | $-3.55 | $10,507.53 | ▼ -3.55 after sell → book $10,616.71; vs 09:30 mark -0.27 | time-stop after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 3 | $6.11 | $0.21 | $-2.22 | $10,525.65 | ▼ -2.22 after sell → book $10,616.50; vs 09:30 mark -0.21 | time-stop after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 14 | $1.83 | $0.32 | $-1.61 | $10,550.95 | ▼ -1.61 after sell → book $10,616.18; vs 09:30 mark -0.32 | time-stop after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 5 | $3.92 | $0.23 | $-4.78 | $10,570.33 | ▼ -4.78 after sell → book $10,615.95; vs 09:30 mark -0.23 | time-stop after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 16 | $1.53 | $0.31 | $-1.58 | $10,594.50 | ▼ -1.58 after sell → book $10,615.64; vs 09:30 mark -0.31 | time-stop after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 2 | $10.57 | $0.24 | $-1.95 | $10,615.40 | ▼ -1.95 after sell → book $10,615.40; vs 09:30 mark -0.24 | time-stop after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,615.40 | ▲ close $10,615.40 vs 09:30 $10,616.98 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,615.40 | ▲ 09:30 equity $10,615.40 vs yday $10,615.40 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 81 | $16.28 | $2.23 | — | $9,294.49 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-1.1; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 486 | $2.73 | $6.27 | — | $7,961.44 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-3.0; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,718.39 | — | sell at min-hold even if still listed; list flatten; ret5=+8.3; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $5,400.93 | — | sell at min-hold even if still listed; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,136.68 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+4.7; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,844.55 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+19.6; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 650 | $2.04 | $8.38 | — | $1,510.17 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1326.92 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 279 | $4.75 | $3.60 | — | $181.32 | — | sell at min-hold even if still listed; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1326.92 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.32 | ▼ close $10,554.16 vs 09:30 $10,615.40 (session -32.66) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.32 | ▼ 09:30 equity $10,241.61 vs yday $10,554.16 (-312.55) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.32 | ▼ close $10,189.56 vs 09:30 $10,241.61 (session -52.05) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.32 | ▲ 09:30 equity $10,219.86 vs yday $10,189.56 (+30.30) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.32 | ▼ close $9,942.72 vs 09:30 $10,219.86 (session -277.14) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.32 | ▲ 09:30 equity $10,011.64 vs yday $9,942.72 (+68.92) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 81 | $16.16 | $2.26 | $-14.21 | $1,488.02 | ▼ -14.21 after sell → book $10,009.38; vs 09:30 mark -2.26 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 486 | $2.72 | $6.36 | $-17.49 | $2,803.58 | ▼ -17.49 after sell → book $10,003.02; vs 09:30 mark -6.36 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 6 | $194.84 | $2.03 | $-76.04 | $3,970.59 | ▼ -76.04 after sell → book $10,000.99; vs 09:30 mark -2.03 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 8 | $140.03 | $2.03 | $-199.25 | $5,088.80 | ▼ -199.25 after sell → book $9,998.96; vs 09:30 mark -2.03 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 8 | $147.79 | $2.03 | $-83.97 | $6,269.08 | ▼ -83.97 after sell → book $9,996.92; vs 09:30 mark -2.04 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 23 | $51.29 | $2.08 | $-114.54 | $7,446.68 | ▼ -114.54 after sell → book $9,994.85; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 650 | $1.89 | $8.50 | $-114.39 | $8,666.67 | ▼ -114.39 after sell → book $9,986.34; vs 09:30 mark -8.51 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 279 | $4.73 | $3.66 | $-12.84 | $9,982.69 | ▼ -12.84 after sell → book $9,982.69; vs 09:30 mark -3.65 | time-stop after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,897.12 | — | sell at min-hold even if still listed; list flatten; ret5=+4.0; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,661.17 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+7.2; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 87 | $14.31 | $2.25 | — | $6,413.95 | — | sell at min-hold even if still listed; list flatten; ret5=+4.8; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,172.21 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+2.9; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 67 | $18.61 | $2.19 | — | $3,923.15 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 68 | $18.21 | $2.19 | — | $2,682.68 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-19.1; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $1,442.41 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1247.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 212 | $5.87 | $2.73 | — | $195.24 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1247.84 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.24 | ▲ close $10,164.04 vs 09:30 $10,011.64 (session +198.90) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.24 | ▲ 09:30 equity $10,332.87 vs yday $10,164.04 (+168.83) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $174.53 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $24.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $151.52 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $24.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 143 | $0.17 | $0.67 | — | $126.54 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $24.40 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $110.51 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $24.40 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.51 | ▲ close $10,393.82 vs 09:30 $10,332.87 (session +62.23) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.51 | ▲ 09:30 equity $10,421.42 vs yday $10,393.82 (+27.60) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 14 | $0.97 | $0.18 | — | $96.75 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $13.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 6 | $2.08 | $0.14 | — | $84.13 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $13.81 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.13 | ▼ close $10,343.03 vs 09:30 $10,421.42 (session -78.07) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.13 | ▲ 09:30 equity $10,411.41 vs yday $10,343.03 (+68.38) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,149.15 | ▼ -20.54 after sell → book $10,409.39; vs 09:30 mark -2.02 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 16 | $76.27 | $2.06 | $-17.70 | $2,367.41 | ▼ -17.70 after sell → book $10,407.33; vs 09:30 mark -2.06 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 87 | $13.65 | $2.28 | $-61.95 | $3,552.68 | ▼ -61.95 after sell → book $10,405.05; vs 09:30 mark -2.28 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 34 | $36.70 | $2.11 | $+3.96 | $4,798.37 | ▲ +3.96 after sell → book $10,402.94; vs 09:30 mark -2.11 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 67 | $22.11 | $2.21 | $+230.10 | $6,277.53 | ▲ +230.10 after sell → book $10,400.73; vs 09:30 mark -2.21 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 68 | $20.55 | $2.22 | $+154.71 | $7,672.71 | ▲ +154.71 after sell → book $10,398.51; vs 09:30 mark -2.22 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 18 | $79.08 | $2.07 | $+181.11 | $9,094.08 | ▲ +181.11 after sell → book $10,396.44; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 212 | $5.62 | $2.78 | $-58.51 | $10,282.74 | ▼ -58.51 after sell → book $10,393.66; vs 09:30 mark -2.78 | time-stop after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 10 | $157.87 | $2.02 | — | $8,702.02 | — | sell at min-hold even if still listed; list flatten; ret5=+6.5; leftover $1713.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 4 | $386.20 | $2.00 | — | $7,155.22 | — | sell at min-hold even if still listed; list flatten; ret5=-5.8; leftover $1713.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 19 | $88.83 | $2.05 | — | $5,465.40 | — | sell at min-hold even if still listed; list flatten; ret5=+7.6; leftover $1713.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 127 | $13.47 | $2.37 | — | $3,752.34 | — | sell at min-hold even if still listed; list flatten; ret5=+3.6; leftover $1713.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 428 | $4.00 | $5.52 | — | $2,034.82 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1713.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 184 | $9.31 | $2.54 | — | $319.24 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1713.79 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $319.24 | ▼ close $10,053.38 vs 09:30 $10,411.41 (session -323.78) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $319.24 | ▲ 09:30 equity $10,100.44 vs yday $10,053.38 (+47.06) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $339.37 | ▼ -0.58 after sell → book $10,100.21; vs 09:30 mark -0.23 | time-stop after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 143 | $0.16 | $0.69 | $-2.79 | $361.56 | ▼ -2.79 after sell → book $10,099.52; vs 09:30 mark -0.69 | time-stop after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 77 | $0.58 | $0.68 | — | $316.22 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $45.19 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $316.22 | ▲ close $10,189.15 vs 09:30 $10,100.44 (session +90.31) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $316.22 | ▲ 09:30 equity $10,212.75 vs yday $10,189.15 (+23.60) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 3 | $7.95 | $0.27 | $+0.58 | $339.80 | ▲ +0.58 after sell → book $10,212.48; vs 09:30 mark -0.27 | time-stop after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $356.71 | ▲ +0.87 after sell → book $10,212.29; vs 09:30 mark -0.19 | time-stop after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 14 | $0.89 | $0.19 | $-1.48 | $368.98 | ▼ -1.48 after sell → book $10,212.10; vs 09:30 mark -0.19 | time-stop after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 6 | $2.16 | $0.17 | $+0.17 | $381.78 | ▲ +0.17 after sell → book $10,211.93; vs 09:30 mark -0.17 | time-stop after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 2 | $27.79 | $0.56 | — | $325.63 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+7.0; leftover $63.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 8 | $7.95 | $0.66 | — | $261.37 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+12.4; leftover $63.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 6 | $9.81 | $0.61 | — | $201.91 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+4.0; leftover $63.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 3 | $20.25 | $0.62 | — | $140.54 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+15.0; leftover $63.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 3 | $20.65 | $0.63 | — | $77.96 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $63.63 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.96 | ▼ close $9,887.90 vs 09:30 $10,212.75 (session -320.96) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.96 | ▼ 09:30 equity $9,711.60 vs yday $9,887.90 (-176.30) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 10 | $163.95 | $2.04 | $+56.74 | $1,715.42 | ▲ +56.74 after sell → book $9,709.56; vs 09:30 mark -2.04 | time-stop after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 4 | $374.54 | $2.02 | $-50.67 | $3,211.56 | ▼ -50.67 after sell → book $9,707.53; vs 09:30 mark -2.03 | time-stop after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 19 | $87.67 | $2.07 | $-26.06 | $4,875.31 | ▼ -26.06 after sell → book $9,705.46; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 127 | $11.42 | $2.40 | $-265.12 | $6,323.25 | ▼ -265.12 after sell → book $9,703.06; vs 09:30 mark -2.40 | time-stop after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 428 | $3.40 | $5.60 | $-267.93 | $7,772.84 | ▼ -267.93 after sell → book $9,697.45; vs 09:30 mark -5.61 | time-stop after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 184 | $8.67 | $2.59 | $-122.89 | $9,365.54 | ▼ -122.89 after sell → book $9,694.87; vs 09:30 mark -2.58 | time-stop after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,365.54 | ▲ close $9,698.21 vs 09:30 $9,711.60 (session +3.34) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,492.73 | ▲ 09:30 equity $8,032.53 vs yday $8,031.57 (+0.96) | 09:30 open · cash $7,492.73 (unchanged overnight, no fees) · equity $8,032.53 vs prior close $8,031.57 (+0.96) · 11 name(s) re-marked at the open (per-name table). ADMA×4 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DEFT×131 yday $0.53 → 09:30 $0.53 +0.00; DLO×5 yday $13.88 → 09:30 $13.88 +0.00; FJET×38 yday $1.80 → 09:30 $1.80 +0.00; FTRE×2 yday $20.02 → 09:30 $20.02 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×5 yday $7.70 → 09:30 $7.70 +0.00; TDC×2 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $6,686.87 | — | sell at min-hold even if still listed; list flatten; ret5=+0.8; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $5,646.61 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $4,604.77 | — | sell at min-hold even if still listed; list flatten; ret5=+4.7; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 139 | $7.65 | $2.41 | — | $3,539.01 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $2,486.10 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,478.96 | — | sell at min-hold even if still listed; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1070.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 118 | $9.05 | $2.34 | — | $408.71 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-27.1; leftover $1070.39 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
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
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
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
| 2026-09-17 | `ILMN` | cash | leftover split 24.40 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 24.40 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 24.40 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 24.40 < 1 share @ 34.93 |
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
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 13.81 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 13.81 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 13.81 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 13.81 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 13.81 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 13.81 < 1 share @ 34.44 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
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
| 2026-09-22 | `USFD` | cash | leftover split 45.19 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 63.63 < 1 share @ 116.85 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `DEFT` | 77 | 2026-09-22 @ $0.58 | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $45.19 |
| `ARQT` | 2 | 2026-09-23 @ $27.79 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+7.0; leftover $63.63 |
| `PGEN` | 8 | 2026-09-23 @ $7.95 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+12.4; leftover $63.63 |
| `ADMA` | 6 | 2026-09-23 @ $9.81 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+4.0; leftover $63.63 |
| `FTRE` | 3 | 2026-09-23 @ $20.25 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+15.0; leftover $63.63 |
| `OMER` | 3 | 2026-09-23 @ $20.65 | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $63.63 |
