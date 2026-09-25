# Factor mine action — `union_h5_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **-7.55%** ($9,245) · signal-only (no cash/fees) was +16.74%. Starts YES **12/30**. Fills 167 · skips 463 · realized $+297.82.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the timer rings. They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- Time-stop: once 5 session(s) are up, sell at 09:30 even if the name is still on the list.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $369.42.

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
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,572.37 vs 09:30 $10,391.80 (session +180.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▲ 09:30 equity $10,710.13 vs yday $10,572.37 (+137.76) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $11,031.12 vs 09:30 $10,710.13 (session +321.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,966.31 vs yday $11,031.12 (-64.81) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,219.17 | ▼ -27.32 after sell → book $10,964.24; vs 09:30 mark -2.07 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,363.50 | ▼ -99.20 after sell → book $10,962.15; vs 09:30 mark -2.09 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,634.86 | ▲ +54.34 after sell → book $10,960.07; vs 09:30 mark -2.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,924.02 | ▲ +44.60 after sell → book $10,957.99; vs 09:30 mark -2.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,388.72 | ▲ +222.19 after sell → book $10,955.65; vs 09:30 mark -2.34 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,674.31 | ▲ +34.39 after sell → book $10,953.51; vs 09:30 mark -2.14 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,660.03 | ▲ +718.77 after sell → book $10,933.33; vs 09:30 mark -20.18 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,882.69 | ▼ -15.98 after sell → book $10,931.17; vs 09:30 mark -2.16 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,524.20 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,248.03 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,903.60 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,544.62 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,187.95 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,852.47 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 777 | $1.75 | $10.02 | — | $1,482.70 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $179.82 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1360.34 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.82 | ▲ close $11,161.56 vs 09:30 $10,966.31 (session +256.20) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.82 | ▲ 09:30 equity $11,454.79 vs yday $11,161.56 (+293.23) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $191.38 | ▲ +2.46 after sell → book $11,454.65; vs 09:30 mark -0.14 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $202.48 | ▼ -1.24 after sell → book $11,454.48; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $215.59 | ▲ +0.96 after sell → book $11,454.31; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $198.21 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.72 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.75 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $125.37 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 20 | $1.32 | $0.32 | — | $98.64 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.95 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.64 | ▲ close $11,454.73 vs 09:30 $11,454.79 (session +1.72) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.64 | ▲ 09:30 equity $11,573.07 vs yday $11,454.73 (+118.34) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.62 | $0.07 | $+0.46 | $103.20 | ▲ +0.46 after sell → book $11,573.00; vs 09:30 mark -0.07 | time-stop after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $110.10 | ▲ +0.35 after sell → book $11,572.90; vs 09:30 mark -0.10 | time-stop after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $115.08 | ▲ +0.12 after sell → book $11,572.83; vs 09:30 mark -0.07 | time-stop after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.08 | ▼ close $11,536.82 vs 09:30 $11,573.07 (session -36.01) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.08 | ▼ 09:30 equity $11,355.56 vs yday $11,536.82 (-181.26) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $103.99 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+1.2; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $95.55 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+8.0; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 2 | $4.94 | $0.10 | — | $85.56 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 1 | $7.25 | $0.08 | — | $78.24 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 40 | $0.36 | $0.26 | — | $63.66 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-15.6; leftover $14.38 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.66 | ▲ close $11,813.59 vs 09:30 $11,355.56 (session +458.67) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.66 | ▼ 09:30 equity $11,600.59 vs yday $11,813.59 (-213.00) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 1 | $11.12 | $0.11 | — | $52.42 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $12.73 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.42 | ▼ close $11,441.31 vs 09:30 $11,600.59 (session -159.16) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.42 | ▲ 09:30 equity $11,468.26 vs yday $11,441.31 (+26.95) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.93 | $2.21 | $+20.68 | $1,431.59 | ▲ +20.68 after sell → book $11,466.05; vs 09:30 mark -2.21 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,766.82 | ▲ +59.06 after sell → book $11,464.00; vs 09:30 mark -2.05 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,149.76 | ▲ +38.51 after sell → book $11,461.79; vs 09:30 mark -2.21 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.49 | $3.08 | $-71.91 | $5,436.83 | ▼ -71.91 after sell → book $11,458.71; vs 09:30 mark -3.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.47 | $2.22 | $+122.54 | $6,916.04 | ▲ +122.54 after sell → book $11,456.49; vs 09:30 mark -2.22 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,368.29 | ▲ +116.78 after sell → book $11,454.34; vs 09:30 mark -2.15 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 777 | $1.91 | $10.16 | $+104.13 | $9,842.20 | ▲ +104.13 after sell → book $11,444.18; vs 09:30 mark -10.16 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,243.17 | ▲ +98.09 after sell → book $11,442.14; vs 09:30 mark -2.04 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 38 | $41.44 | $2.10 | — | $9,666.35 | — | sell at min-hold even if still listed; list flatten; ret5=+3.1; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 111 | $14.42 | $2.32 | — | $8,063.40 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 66 | $24.00 | $2.19 | — | $6,477.22 | — | sell at min-hold even if still listed; list flatten; ret5=+8.7; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 617 | $2.60 | $7.96 | — | $4,865.06 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+13.0; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 123 | $12.98 | $2.36 | — | $3,266.16 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 52 | $30.79 | $2.15 | — | $1,662.93 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 165 | $9.68 | $2.48 | — | $63.25 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1606.17 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.25 | ▲ close $11,522.85 vs 09:30 $11,468.26 (session +102.28) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.25 | ▲ 09:30 equity $11,529.20 vs yday $11,522.85 (+6.35) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $79.50 | ▼ -1.12 after sell → book $11,529.01; vs 09:30 mark -0.19 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $110.03 | ▲ +8.04 after sell → book $11,528.68; vs 09:30 mark -0.33 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.35 | $0.28 | $-1.76 | $133.24 | ▼ -1.76 after sell → book $11,528.39; vs 09:30 mark -0.29 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.06 | $0.33 | $+1.07 | $159.69 | ▲ +1.07 after sell → book $11,528.06; vs 09:30 mark -0.33 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 20 | $1.82 | $0.44 | $+9.23 | $195.65 | ▲ +9.23 after sell → book $11,527.62; vs 09:30 mark -0.44 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 1 | $32.90 | $0.33 | — | $162.42 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $48.91 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 3 | $15.66 | $0.48 | — | $114.96 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $48.91 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 14 | $3.32 | $0.51 | — | $67.97 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=+6.4; leftover $48.91 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.97 | ▼ close $11,276.30 vs 09:30 $11,529.20 (session -250.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.97 | ▲ 09:30 equity $11,341.89 vs yday $11,276.30 (+65.59) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.97 | ▲ close $11,367.49 vs 09:30 $11,341.89 (session +25.60) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.97 | ▲ 09:30 equity $11,494.09 vs yday $11,367.49 (+126.60) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $78.26 | ▼ -0.80 after sell → book $11,493.97; vs 09:30 mark -0.12 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $86.41 | ▼ -0.29 after sell → book $11,493.86; vs 09:30 mark -0.11 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 2 | $4.64 | $0.12 | $-0.82 | $95.57 | ▼ -0.82 after sell → book $11,493.74; vs 09:30 mark -0.12 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 1 | $10.77 | $0.13 | $+3.31 | $106.21 | ▲ +3.31 after sell → book $11,493.61; vs 09:30 mark -0.13 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 40 | $0.36 | $0.29 | $-0.27 | $120.52 | ▼ -0.27 after sell → book $11,493.33; vs 09:30 mark -0.28 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.52 | ▼ close $11,488.01 vs 09:30 $11,494.09 (session -5.31) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.52 | ▼ 09:30 equity $11,464.50 vs yday $11,488.01 (-23.51) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `FLNC` | 1 | $10.38 | $0.13 | $-0.98 | $130.78 | ▼ -0.98 after sell → book $11,464.37; vs 09:30 mark -0.13 | time-stop after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.78 | ▲ close $11,564.15 vs 09:30 $11,464.50 (session +99.78) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.78 | ▲ 09:30 equity $11,662.40 vs yday $11,564.15 (+98.25) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 38 | $42.43 | $2.13 | $+33.39 | $1,740.99 | ▲ +33.39 after sell → book $11,660.27; vs 09:30 mark -2.13 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 111 | $15.45 | $2.36 | $+109.65 | $3,453.59 | ▲ +109.65 after sell → book $11,657.91; vs 09:30 mark -2.36 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 66 | $26.12 | $2.21 | $+135.52 | $5,175.30 | ▲ +135.52 after sell → book $11,655.70; vs 09:30 mark -2.21 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 617 | $2.49 | $8.07 | $-83.90 | $6,703.55 | ▼ -83.90 after sell → book $11,647.63; vs 09:30 mark -8.07 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 123 | $13.25 | $2.39 | $+28.46 | $8,330.91 | ▲ +28.46 after sell → book $11,645.24; vs 09:30 mark -2.39 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AVBP` | 52 | $30.58 | $2.17 | $-15.23 | $9,918.90 | ▼ -15.23 after sell → book $11,643.07; vs 09:30 mark -2.17 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 165 | $9.68 | $2.53 | $-5.01 | $11,513.58 | ▼ -5.01 after sell → book $11,640.54; vs 09:30 mark -2.53 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $10,083.75 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $42.93 | $2.09 | — | $8,664.97 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 396 | $3.63 | $5.11 | — | $7,222.38 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 179 | $8.03 | $2.53 | — | $5,782.48 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,455.96 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 93 | $15.45 | $2.27 | — | $3,016.84 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,701.32 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1439.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 85 | $16.77 | $2.25 | — | $273.63 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1439.20 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.63 | ▼ close $11,361.88 vs 09:30 $11,662.40 (session -258.32) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.63 | ▲ 09:30 equity $11,362.68 vs yday $11,361.88 (+0.80) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 1 | $33.86 | $0.36 | $+0.27 | $307.12 | ▲ +0.27 after sell → book $11,362.31; vs 09:30 mark -0.37 | time-stop after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 3 | $13.56 | $0.44 | $-7.21 | $347.37 | ▼ -7.21 after sell → book $11,361.88; vs 09:30 mark -0.43 | time-stop after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 14 | $3.53 | $0.56 | $+1.88 | $396.23 | ▲ +1.88 after sell → book $11,361.32; vs 09:30 mark -0.56 | time-stop after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 26 | $2.52 | $0.73 | — | $329.98 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $66.04 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 9 | $6.71 | $0.63 | — | $268.96 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $66.04 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 34 | $1.90 | $0.75 | — | $203.61 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $66.04 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 13 | $4.78 | $0.66 | — | $140.81 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $66.04 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 41 | $1.59 | $0.77 | — | $74.84 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $66.04 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 5 | $11.31 | $0.58 | — | $17.71 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $66.04 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.71 | ▲ close $11,393.43 vs 09:30 $11,362.68 (session +36.23) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.71 | ▲ 09:30 equity $11,425.87 vs yday $11,393.43 (+32.44) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.71 | ▼ close $11,234.09 vs 09:30 $11,425.87 (session -191.78) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.71 | ▼ 09:30 equity $11,178.68 vs yday $11,234.09 (-55.41) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.71 | ▼ close $10,840.77 vs 09:30 $11,178.68 (session -337.91) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.71 | ▼ 09:30 equity $10,730.60 vs yday $10,840.77 (-110.17) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.71 | ▼ close $10,626.31 vs 09:30 $10,730.60 (session -104.29) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.71 | ▲ 09:30 equity $10,696.83 vs yday $10,626.31 (+70.52) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 27 | $53.53 | $2.09 | $+13.39 | $1,460.93 | ▲ +13.39 after sell → book $10,694.74; vs 09:30 mark -2.09 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 33 | $41.30 | $2.11 | $-57.99 | $2,821.72 | ▼ -57.99 after sell → book $10,692.63; vs 09:30 mark -2.11 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 396 | $2.77 | $5.18 | $-350.85 | $3,913.46 | ▼ -350.85 after sell → book $10,687.44; vs 09:30 mark -5.19 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 179 | $7.70 | $2.57 | $-64.16 | $5,289.19 | ▼ -64.16 after sell → book $10,684.87; vs 09:30 mark -2.57 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 10 | $122.40 | $2.04 | $-104.56 | $6,511.15 | ▼ -104.56 after sell → book $10,682.83; vs 09:30 mark -2.04 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRK` | 93 | $15.03 | $2.30 | $-43.62 | $7,906.64 | ▼ -43.62 after sell → book $10,680.54; vs 09:30 mark -2.29 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 9 | $137.91 | $2.04 | $-76.41 | $9,145.75 | ▼ -76.41 after sell → book $10,678.50; vs 09:30 mark -2.04 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 85 | $14.06 | $2.27 | $-234.86 | $10,338.58 | ▼ -234.86 after sell → book $10,676.23; vs 09:30 mark -2.27 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 79 | $16.28 | $2.23 | — | $9,050.24 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-1.1; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 473 | $2.73 | $6.10 | — | $7,752.84 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-3.0; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,509.80 | — | sell at min-hold even if still listed; list flatten; ret5=+8.3; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,356.77 | — | sell at min-hold even if still listed; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,092.52 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+4.7; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,800.39 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+19.6; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 633 | $2.04 | $8.17 | — | $1,500.91 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1292.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 272 | $4.75 | $3.51 | — | $205.40 | — | sell at min-hold even if still listed; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1292.32 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.40 | ▼ close $10,630.80 vs 09:30 $10,696.83 (session -17.34) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.40 | ▼ 09:30 equity $10,328.59 vs yday $10,630.80 (-302.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 26 | $2.15 | $0.66 | $-11.01 | $260.64 | ▼ -11.01 after sell → book $10,327.94; vs 09:30 mark -0.65 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 9 | $5.93 | $0.58 | $-8.23 | $313.43 | ▼ -8.23 after sell → book $10,327.35; vs 09:30 mark -0.59 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 34 | $1.72 | $0.71 | $-7.74 | $371.03 | ▼ -7.74 after sell → book $10,326.65; vs 09:30 mark -0.70 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 13 | $4.13 | $0.60 | $-9.71 | $424.13 | ▼ -9.71 after sell → book $10,326.05; vs 09:30 mark -0.60 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 41 | $1.59 | $0.79 | $-1.57 | $488.52 | ▼ -1.57 after sell → book $10,325.26; vs 09:30 mark -0.79 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 5 | $10.73 | $0.57 | $-4.05 | $541.60 | ▼ -4.05 after sell → book $10,324.69; vs 09:30 mark -0.57 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $541.60 | ▼ close $10,267.76 vs 09:30 $10,328.59 (session -56.92) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $541.60 | ▲ 09:30 equity $10,300.26 vs yday $10,267.76 (+32.50) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $541.60 | ▼ close $10,030.07 vs 09:30 $10,300.26 (session -270.19) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $541.60 | ▲ 09:30 equity $10,098.97 vs yday $10,030.07 (+68.90) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 4 | $14.31 | $0.58 | — | $483.78 | — | sell at min-hold even if still listed; list flatten; ret5=+4.8; leftover $67.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 1 | $36.46 | $0.37 | — | $446.95 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+2.9; leftover $67.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 3 | $18.61 | $0.57 | — | $390.55 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $67.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 3 | $18.21 | $0.56 | — | $335.37 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-19.1; leftover $67.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 11 | $5.87 | $0.68 | — | $270.12 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $67.70 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $270.12 | ▲ close $10,130.48 vs 09:30 $10,098.97 (session +34.26) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $270.12 | ▲ 09:30 equity $10,332.99 vs yday $10,130.48 (+202.51) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $239.05 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $33.76 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $208.38 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $33.76 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 198 | $0.17 | $0.93 | — | $173.79 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $33.76 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 2 | $15.87 | $0.32 | — | $141.72 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $33.76 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.72 | ▼ close $10,294.13 vs 09:30 $10,332.99 (session -36.97) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.72 | ▲ 09:30 equity $10,327.60 vs yday $10,294.13 (+33.47) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 79 | $16.93 | $2.25 | $+46.87 | $1,476.94 | ▲ +46.87 after sell → book $10,325.35; vs 09:30 mark -2.25 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 473 | $2.68 | $6.19 | $-35.94 | $2,738.39 | ▼ -35.94 after sell → book $10,319.16; vs 09:30 mark -6.19 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 6 | $197.76 | $2.03 | $-58.52 | $3,922.92 | ▼ -58.52 after sell → book $10,317.13; vs 09:30 mark -2.03 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 7 | $150.47 | $2.03 | $-101.76 | $4,974.18 | ▼ -101.76 after sell → book $10,315.10; vs 09:30 mark -2.03 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 8 | $152.71 | $2.03 | $-44.61 | $6,193.83 | ▼ -44.61 after sell → book $10,313.07; vs 09:30 mark -2.03 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 23 | $55.80 | $2.08 | $-10.81 | $7,475.15 | ▼ -10.81 after sell → book $10,310.99; vs 09:30 mark -2.08 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 633 | $1.90 | $8.28 | $-105.07 | $8,669.57 | ▼ -105.07 after sell → book $10,302.71; vs 09:30 mark -8.28 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 272 | $4.50 | $3.56 | $-75.07 | $9,890.00 | ▼ -75.07 after sell → book $10,299.14; vs 09:30 mark -3.57 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $8,693.93 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+21.3; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $7,505.64 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+16.1; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $6,456.03 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $5,355.93 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $4,163.89 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 35 | $34.44 | $2.10 | — | $2,956.40 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1274 | $0.97 | $16.18 | — | $1,704.44 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1236.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 594 | $2.08 | $7.66 | — | $461.26 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1236.25 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $461.26 | ▼ close $10,089.76 vs 09:30 $10,327.60 (session -173.39) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $461.26 | ▲ 09:30 equity $10,219.23 vs yday $10,089.76 (+129.47) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 5 | $13.47 | $0.69 | — | $393.22 | — | sell at min-hold even if still listed; list flatten; ret5=+3.6; leftover $76.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 19 | $4.00 | $0.82 | — | $316.40 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $76.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 8 | $9.31 | $0.77 | — | $241.15 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $76.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.15 | ▼ close $10,177.57 vs 09:30 $10,219.23 (session -39.38) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $241.15 | ▼ 09:30 equity $10,167.62 vs yday $10,177.57 (-9.95) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 51 | $0.58 | $0.45 | — | $211.12 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $30.14 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.12 | ▼ close $10,131.83 vs 09:30 $10,167.62 (session -35.34) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.12 | ▲ 09:30 equity $10,444.11 vs yday $10,131.83 (+312.28) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 4 | $13.12 | $0.56 | $-5.90 | $263.05 | ▼ -5.90 after sell → book $10,443.56; vs 09:30 mark -0.55 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 1 | $38.04 | $0.40 | $+0.81 | $300.68 | ▲ +0.81 after sell → book $10,443.15; vs 09:30 mark -0.41 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 3 | $23.00 | $0.72 | $+11.88 | $368.96 | ▲ +11.88 after sell → book $10,442.43; vs 09:30 mark -0.72 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 3 | $23.30 | $0.73 | $+13.99 | $438.14 | ▲ +13.99 after sell → book $10,441.71; vs 09:30 mark -0.72 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 11 | $5.53 | $0.66 | $-5.08 | $498.30 | ▼ -5.08 after sell → book $10,441.04; vs 09:30 mark -0.67 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 2 | $27.79 | $0.56 | — | $442.16 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+7.0; leftover $71.19 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 7 | $9.81 | $0.71 | — | $372.79 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+4.0; leftover $71.19 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 3 | $20.25 | $0.62 | — | $311.42 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+15.0; leftover $71.19 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 3 | $20.65 | $0.63 | — | $248.84 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $71.19 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.84 | ▲ close $10,808.61 vs 09:30 $10,444.11 (session +370.08) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.84 | ▼ 09:30 equity $10,724.48 vs yday $10,808.61 (-84.13) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 3 | $10.39 | $0.34 | $-0.24 | $279.67 | ▼ -0.24 after sell → book $10,724.14; vs 09:30 mark -0.34 | time-stop after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 4 | $7.38 | $0.33 | $-1.48 | $308.86 | ▼ -1.48 after sell → book $10,723.81; vs 09:30 mark -0.33 | time-stop after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 198 | $0.15 | $0.93 | $-5.82 | $337.63 | ▼ -5.82 after sell → book $10,722.88; vs 09:30 mark -0.93 | time-stop after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 2 | $16.07 | $0.35 | $-0.27 | $369.42 | ▼ -0.27 after sell → book $10,722.53; vs 09:30 mark -0.35 | time-stop after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $369.42 | ▲ close $10,908.26 vs 09:30 $10,724.48 (session +185.73) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.74 | ▲ 09:30 equity $9,287.79 vs yday $9,262.63 (+25.16) | 09:30 open · cash $190.74 (unchanged overnight, no fees) · equity $9,287.79 vs prior close $9,262.63 (+25.16) · 11 name(s) re-marked at the open (per-name table). A×8 yday $172.84 → 09:30 $171.98 -6.88; ADMA×136 yday $9.52 → 09:30 $9.52 +0.00; ARQT×48 yday $26.27 → 09:30 $26.27 +0.00; CYPH×3 yday $4.08 → 09:30 $4.00 -0.22; DEFT×13 yday $0.53 → 09:30 $0.53 +0.00; DXCM×14 yday $87.47 → 09:30 $87.47 +0.00; EYPT×2 yday $3.65 → 09:30 $3.65 +0.00; FJET×3 yday $1.80 → 09:30 $1.80 +0.00; FTRE×66 yday $20.02 → 09:30 $20.02 +0.00; HALO×11 yday $115.22 → 09:30 $115.36 +1.54; OMER×64 yday $20.13 → 09:30 $20.61 +30.72 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 4 | $7.65 | $0.32 | — | $159.82 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.2; leftover $31.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 1 | $26.27 | $0.27 | — | $133.29 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $31.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 3 | $9.05 | $0.28 | — | $105.86 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-27.1; leftover $31.79 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.86 | ▼ close $9,244.94 vs 09:30 $9,287.79 (session -41.98) | 16:00 close · cash $105.86 · equity $9,244.94 vs 09:30 $9,287.79 (-42.85; session marks -41.98) · 14 name(s) marked open→close (per-name table). A×8 09:30 $171.98 → close $172.79 +6.48; ADMA×136 09:30 $9.52 → close $9.52 +0.00; ARQT×48 09:30 $26.27 → close $26.27 +0.00; CYPH×3 09:30 $4.00 → close $4.12 +0.35; DEFT×13 09:30 $0.53 → close $0.53 +0.00; DXCM×14 09:30 $87.47 → close $87.47 +0.00; EYPT×2 09:30 $3.65 → close $3.65 +0.00; FJET×3 09:30 $1.80 → close $1.80 -0.00; FTRE×66 09:30 $20.02 → close $20.02 +0.00; HALO×11 09:30 $115.36 → close $113.90 -16.06; OMER×64 09:30 $20.61 → close $20.08 -33.92; MRVI×4 09:30 $7.65 → close $7.60 -0.20; WRBY×1 09:30 $26.27 → close $26.71 +0.44; AEHL×3 09:30 $9.05 → close $9.36 +0.93 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.95 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.95 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.95 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 14.38 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 14.38 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 14.38 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 12.73 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 12.73 < 1 share @ 24.84 |
| 2026-08-26 | `INSP` | cash | leftover split 12.73 < 1 share @ 60.07 |
| 2026-08-26 | `AVBP` | cash | leftover split 12.73 < 1 share @ 31.21 |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `URBN` | cash | leftover split 48.91 < 1 share @ 79.42 |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `AVBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PYXS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `PYXS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `MRNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `MRNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `OVID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ORCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COHU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `IQV` | cash | leftover split 67.70 < 1 share @ 270.89 |
| 2026-09-16 | `RDNT` | cash | leftover split 67.70 < 1 share @ 77.12 |
| 2026-09-16 | `TEM` | cash | leftover split 67.70 < 1 share @ 68.79 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 33.76 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 33.76 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 33.76 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 33.76 < 1 share @ 34.93 |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 76.88 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 76.88 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 76.88 < 1 share @ 88.83 |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 30.14 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SWRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-23 | `DXCM` | cash | leftover split 71.19 < 1 share @ 89.50 |
| 2026-09-23 | `A` | cash | leftover split 71.19 < 1 share @ 166.54 |
| 2026-09-23 | `HALO` | cash | leftover split 71.19 < 1 share @ 116.85 |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `RBRK` | 11 | 2026-09-18 @ $108.55 | sell at min-hold even if still listed; list flatten; ⚪; ret5=+21.3; leftover $1236.25 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+16.1; leftover $1236.25 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1236.25 |
| `VICR` | 5 | 2026-09-18 @ $219.62 | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1236.25 |
| `ECO` | 14 | 2026-09-18 @ $85.00 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1236.25 |
| `FIVN` | 35 | 2026-09-18 @ $34.44 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1236.25 |
| `TLSA` | 1274 | 2026-09-18 @ $0.97 | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1236.25 |
| `SWRD` | 594 | 2026-09-18 @ $2.08 | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1236.25 |
| `MGTX` | 5 | 2026-09-21 @ $13.47 | sell at min-hold even if still listed; list flatten; ret5=+3.6; leftover $76.88 |
| `CYPH` | 19 | 2026-09-21 @ $4.00 | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $76.88 |
| `BKKT` | 8 | 2026-09-21 @ $9.31 | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $76.88 |
| `DEFT` | 51 | 2026-09-22 @ $0.58 | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $30.14 |
| `ARQT` | 2 | 2026-09-23 @ $27.79 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+7.0; leftover $71.19 |
| `ADMA` | 7 | 2026-09-23 @ $9.81 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+4.0; leftover $71.19 |
| `FTRE` | 3 | 2026-09-23 @ $20.25 | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+15.0; leftover $71.19 |
| `OMER` | 3 | 2026-09-23 @ $20.65 | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $71.19 |
