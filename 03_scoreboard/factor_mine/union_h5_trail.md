# Factor mine action — `union_h5_trail`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `trail` · S-boost `none` · after min-hold, trail 5% off peak

Cash book **-7.55%** ($9,245) · signal-only (no cash/fees) was +16.74%. Starts YES **12/30**. Fills 174 · skips 468 · realized $+274.64.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- After 5 session(s), sell if the 09:30 open is 5% off the best price since entry. Otherwise sell when the name drops off the list.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `trail` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $242.47.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-11.4; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,525.15 vs 09:30 $10,414.78 (session +110.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,572.37 vs 09:30 $10,391.80 (session +180.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▲ 09:30 equity $10,710.13 vs yday $10,572.37 (+137.76) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $11,031.12 vs 09:30 $10,710.13 (session +321.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,966.31 vs yday $11,031.12 (-64.81) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,219.17 | ▼ -27.32 after sell → book $10,964.24; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,363.50 | ▼ -99.20 after sell → book $10,962.15; vs 09:30 mark -2.09 | trail off peak after 5 sess (−5%) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,634.86 | ▲ +54.34 after sell → book $10,960.07; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,924.02 | ▲ +44.60 after sell → book $10,957.99; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,388.72 | ▲ +222.19 after sell → book $10,955.65; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,674.31 | ▲ +34.39 after sell → book $10,953.51; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,660.03 | ▲ +718.77 after sell → book $10,933.33; vs 09:30 mark -20.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,882.69 | ▼ -15.98 after sell → book $10,931.17; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,524.20 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,248.03 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,903.60 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,544.62 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,187.95 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,852.47 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 777 | $1.75 | $10.02 | — | $1,482.70 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $179.82 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1360.34 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.82 | ▲ close $11,161.56 vs 09:30 $10,966.31 (session +256.20) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.82 | ▲ 09:30 equity $11,454.79 vs yday $11,161.56 (+293.23) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $191.38 | ▲ +2.46 after sell → book $11,454.65; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $202.48 | ▼ -1.24 after sell → book $11,454.48; vs 09:30 mark -0.17 | trail off peak after 5 sess (−5%) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $215.59 | ▲ +0.96 after sell → book $11,454.31; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $198.21 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.72 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.75 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $125.37 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 20 | $1.32 | $0.32 | — | $98.64 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.95 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.64 | ▲ close $11,454.73 vs 09:30 $11,454.79 (session +1.72) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.64 | ▲ 09:30 equity $11,573.07 vs yday $11,454.73 (+118.34) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.62 | $0.07 | $+0.46 | $103.20 | ▲ +0.46 after sell → book $11,573.00; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $110.10 | ▲ +0.35 after sell → book $11,572.90; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $115.08 | ▲ +0.12 after sell → book $11,572.83; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.08 | ▼ close $11,536.82 vs 09:30 $11,573.07 (session -36.01) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.08 | ▼ 09:30 equity $11,355.56 vs yday $11,536.82 (-181.26) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $103.99 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+1.2; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $95.55 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+8.0; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 2 | $4.94 | $0.10 | — | $85.56 | — | after min-hold, trail 5% off peak; list flatten; ret5=+7.1; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 1 | $7.25 | $0.08 | — | $78.24 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $14.38 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 40 | $0.36 | $0.26 | — | $63.66 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; ret5=-15.6; leftover $14.38 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.66 | ▲ close $11,813.59 vs 09:30 $11,355.56 (session +458.67) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.66 | ▼ 09:30 equity $11,600.59 vs yday $11,813.59 (-213.00) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 1 | $11.12 | $0.11 | — | $52.42 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $12.73 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.42 | ▼ close $11,441.31 vs 09:30 $11,600.59 (session -159.16) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.42 | ▲ 09:30 equity $11,468.26 vs yday $11,441.31 (+26.95) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.93 | $2.21 | $+20.68 | $1,431.59 | ▲ +20.68 after sell → book $11,466.05; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,766.82 | ▲ +59.06 after sell → book $11,464.00; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,149.76 | ▲ +38.51 after sell → book $11,461.79; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.49 | $3.08 | $-71.91 | $5,436.83 | ▼ -71.91 after sell → book $11,458.71; vs 09:30 mark -3.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.47 | $2.22 | $+122.54 | $6,916.04 | ▲ +122.54 after sell → book $11,456.49; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,368.29 | ▲ +116.78 after sell → book $11,454.34; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 777 | $1.91 | $10.16 | $+104.13 | $9,842.20 | ▲ +104.13 after sell → book $11,444.18; vs 09:30 mark -10.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,243.17 | ▲ +98.09 after sell → book $11,442.14; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 38 | $41.44 | $2.10 | — | $9,666.35 | — | after min-hold, trail 5% off peak; list flatten; ret5=+3.1; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 111 | $14.42 | $2.32 | — | $8,063.40 | — | after min-hold, trail 5% off peak; list flatten; ret5=+7.1; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 66 | $24.00 | $2.19 | — | $6,477.22 | — | after min-hold, trail 5% off peak; list flatten; ret5=+8.7; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 617 | $2.60 | $7.96 | — | $4,865.06 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; ret5=+13.0; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 123 | $12.98 | $2.36 | — | $3,266.16 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 52 | $30.79 | $2.15 | — | $1,662.93 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1606.17 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 165 | $9.68 | $2.48 | — | $63.25 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1606.17 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.25 | ▲ close $11,522.85 vs 09:30 $11,468.26 (session +102.28) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.25 | ▲ 09:30 equity $11,529.20 vs yday $11,522.85 (+6.35) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $79.50 | ▼ -1.12 after sell → book $11,529.01; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $110.03 | ▲ +8.04 after sell → book $11,528.68; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.35 | $0.28 | $-1.76 | $133.24 | ▼ -1.76 after sell → book $11,528.39; vs 09:30 mark -0.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.06 | $0.33 | $+1.07 | $159.69 | ▲ +1.07 after sell → book $11,528.06; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 20 | $1.82 | $0.44 | $+9.23 | $195.65 | ▲ +9.23 after sell → book $11,527.62; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 1 | $32.90 | $0.33 | — | $162.42 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $48.91 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 3 | $15.66 | $0.48 | — | $114.96 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $48.91 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 14 | $3.32 | $0.51 | — | $67.97 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; ret5=+6.4; leftover $48.91 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.97 | ▼ close $11,276.30 vs 09:30 $11,529.20 (session -250.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.97 | ▲ 09:30 equity $11,341.89 vs yday $11,276.30 (+65.59) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.97 | ▲ close $11,367.49 vs 09:30 $11,341.89 (session +25.60) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.97 | ▲ 09:30 equity $11,494.09 vs yday $11,367.49 (+126.60) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $78.26 | ▼ -0.80 after sell → book $11,493.97; vs 09:30 mark -0.12 | trail off peak after 5 sess (−5%) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $86.41 | ▼ -0.29 after sell → book $11,493.86; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 2 | $4.64 | $0.12 | $-0.82 | $95.57 | ▼ -0.82 after sell → book $11,493.74; vs 09:30 mark -0.12 | trail off peak after 5 sess (−5%) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 1 | $10.77 | $0.13 | $+3.31 | $106.21 | ▲ +3.31 after sell → book $11,493.61; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 40 | $0.36 | $0.29 | $-0.27 | $120.52 | ▼ -0.27 after sell → book $11,493.33; vs 09:30 mark -0.28 | trail off peak after 5 sess (−5%) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.52 | ▼ close $11,488.01 vs 09:30 $11,494.09 (session -5.31) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.52 | ▼ 09:30 equity $11,464.50 vs yday $11,488.01 (-23.51) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `FLNC` | 1 | $10.38 | $0.13 | $-0.98 | $130.78 | ▼ -0.98 after sell → book $11,464.37; vs 09:30 mark -0.13 | trail off peak after 5 sess (−5%) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.78 | ▲ close $11,564.15 vs 09:30 $11,464.50 (session +99.78) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.78 | ▲ 09:30 equity $11,662.40 vs yday $11,564.15 (+98.25) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 38 | $42.43 | $2.13 | $+33.39 | $1,740.99 | ▲ +33.39 after sell → book $11,660.27; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 66 | $26.12 | $2.21 | $+135.52 | $3,462.70 | ▲ +135.52 after sell → book $11,658.06; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 617 | $2.49 | $8.07 | $-83.90 | $4,990.96 | ▼ -83.90 after sell → book $11,649.98; vs 09:30 mark -8.08 | trail off peak after 5 sess (−5%) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 123 | $13.25 | $2.39 | $+28.46 | $6,618.32 | ▲ +28.46 after sell → book $11,647.59; vs 09:30 mark -2.39 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AVBP` | 52 | $30.58 | $2.17 | $-15.23 | $8,206.31 | ▼ -15.23 after sell → book $11,645.42; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 165 | $9.68 | $2.53 | $-5.01 | $9,800.98 | ▼ -5.01 after sell → book $11,642.90; vs 09:30 mark -2.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $8,424.03 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1400.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $7,048.19 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1400.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 385 | $3.63 | $4.97 | — | $5,645.67 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1400.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 174 | $8.03 | $2.51 | — | $4,245.94 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1400.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $2,919.42 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1400.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,603.90 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1400.14 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.77 | $2.24 | — | $209.75 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1400.14 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.75 | ▼ close $11,363.48 vs 09:30 $11,662.40 (session -261.51) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.75 | ▲ 09:30 equity $11,366.44 vs yday $11,363.48 (+2.96) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 111 | $15.00 | $2.35 | $+59.70 | $1,872.39 | ▲ +59.70 after sell → book $11,364.08; vs 09:30 mark -2.36 | trail off peak after 6 sess (−5%) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 1 | $33.86 | $0.36 | $+0.27 | $1,905.89 | ▲ +0.27 after sell → book $11,363.72; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 3 | $13.56 | $0.44 | $-7.21 | $1,946.14 | ▼ -7.21 after sell → book $11,363.29; vs 09:30 mark -0.43 | trail off peak after 5 sess (−5%) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 14 | $3.53 | $0.56 | $+1.88 | $1,995.00 | ▲ +1.88 after sell → book $11,362.73; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 131 | $2.52 | $2.38 | — | $1,662.50 | — | after min-hold, trail 5% off peak; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $332.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 49 | $6.71 | $2.14 | — | $1,331.57 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $332.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 174 | $1.90 | $2.51 | — | $998.46 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $332.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 69 | $4.78 | $2.20 | — | $666.44 | — | after min-hold, trail 5% off peak; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $332.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 209 | $1.59 | $2.70 | — | $331.43 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $332.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 29 | $11.31 | $2.08 | — | $1.37 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $332.50 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▲ close $11,349.46 vs 09:30 $11,366.44 (session +0.73) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▼ 09:30 equity $11,338.47 vs yday $11,349.46 (-10.99) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▼ close $11,164.07 vs 09:30 $11,338.47 (session -174.40) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▼ 09:30 equity $11,102.54 vs yday $11,164.07 (-61.53) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▼ close $10,728.93 vs 09:30 $11,102.54 (session -373.61) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▼ 09:30 equity $10,588.40 vs yday $10,728.93 (-140.53) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▼ close $10,437.98 vs 09:30 $10,588.40 (session -150.42) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▲ 09:30 equity $10,540.02 vs yday $10,437.98 (+102.04) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 26 | $53.53 | $2.09 | $+12.74 | $1,391.06 | ▲ +12.74 after sell → book $10,537.93; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 32 | $41.30 | $2.11 | $-56.35 | $2,710.55 | ▼ -56.35 after sell → book $10,535.83; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 385 | $2.77 | $5.04 | $-341.11 | $3,771.96 | ▼ -341.11 after sell → book $10,530.79; vs 09:30 mark -5.04 | trail off peak after 5 sess (−5%) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 174 | $7.70 | $2.55 | $-62.48 | $5,109.21 | ▼ -62.48 after sell → book $10,528.24; vs 09:30 mark -2.55 | trail off peak after 5 sess (−5%) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 10 | $122.40 | $2.04 | $-104.56 | $6,331.17 | ▼ -104.56 after sell → book $10,526.20; vs 09:30 mark -2.04 | trail off peak after 5 sess (−5%) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 9 | $137.91 | $2.04 | $-76.41 | $7,570.28 | ▼ -76.41 after sell → book $10,524.16; vs 09:30 mark -2.04 | trail off peak after 5 sess (−5%) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 83 | $14.06 | $2.26 | $-229.43 | $8,735.00 | ▼ -229.43 after sell → book $10,521.90; vs 09:30 mark -2.26 | trail off peak after 5 sess (−5%) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 67 | $16.28 | $2.19 | — | $7,642.04 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=-1.1; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 399 | $2.73 | $5.15 | — | $6,547.63 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=-3.0; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $5,511.42 | — | after min-hold, trail 5% off peak; list flatten; ret5=+8.3; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 6 | $164.43 | $2.01 | — | $4,522.83 | — | after min-hold, trail 5% off peak; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $3,574.15 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+4.7; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 19 | $56.09 | $2.05 | — | $2,506.39 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+19.6; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 535 | $2.04 | $6.90 | — | $1,408.09 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1091.87 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 229 | $4.75 | $2.95 | — | $317.38 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1091.87 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.38 | ▼ close $10,476.18 vs 09:30 $10,540.02 (session -20.45) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.38 | ▼ 09:30 equity $10,242.24 vs yday $10,476.18 (-233.94) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 131 | $2.15 | $2.41 | $-53.27 | $596.62 | ▼ -53.27 after sell → book $10,239.82; vs 09:30 mark -2.42 | trail off peak after 5 sess (−5%) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 49 | $5.93 | $2.16 | $-42.51 | $885.03 | ▼ -42.51 after sell → book $10,237.67; vs 09:30 mark -2.15 | trail off peak after 5 sess (−5%) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 174 | $1.72 | $2.55 | $-37.25 | $1,180.89 | ▼ -37.25 after sell → book $10,235.12; vs 09:30 mark -2.55 | trail off peak after 5 sess (−5%) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 69 | $4.13 | $2.22 | $-49.27 | $1,463.64 | ▼ -49.27 after sell → book $10,232.90; vs 09:30 mark -2.22 | trail off peak after 5 sess (−5%) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 209 | $1.59 | $2.74 | $-5.44 | $1,793.21 | ▼ -5.44 after sell → book $10,230.16; vs 09:30 mark -2.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 29 | $10.73 | $2.10 | $-20.99 | $2,102.28 | ▼ -20.99 after sell → book $10,228.06; vs 09:30 mark -2.10 | trail off peak after 5 sess (−5%) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,102.28 | ▼ close $10,184.37 vs 09:30 $10,242.24 (session -43.68) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,102.28 | ▲ 09:30 equity $10,207.70 vs yday $10,184.37 (+23.33) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,102.28 | ▼ close $9,983.65 vs 09:30 $10,207.70 (session -224.05) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,102.28 | ▲ 09:30 equity $10,040.23 vs yday $9,983.65 (+56.58) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 3 | $77.12 | $2.00 | — | $1,868.93 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; ret5=+7.2; leftover $262.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 18 | $14.31 | $2.04 | — | $1,609.30 | — | after min-hold, trail 5% off peak; list flatten; ret5=+4.8; leftover $262.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 7 | $36.46 | $2.01 | — | $1,352.07 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+2.9; leftover $262.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 14 | $18.61 | $2.03 | — | $1,089.50 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $262.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 14 | $18.21 | $2.03 | — | $832.53 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; ret5=-19.1; leftover $262.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 3 | $68.79 | $2.00 | — | $624.16 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $262.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 44 | $5.87 | $2.12 | — | $363.76 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $262.79 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $363.76 | ▲ close $10,091.01 vs 09:30 $10,040.23 (session +65.01) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $363.76 | ▲ 09:30 equity $10,283.39 vs yday $10,091.01 (+192.38) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 4 | $10.25 | $0.42 | — | $322.33 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $45.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 5 | $7.59 | $0.39 | — | $283.99 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $45.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $248.71 | — | after min-hold, trail 5% off peak; list flatten; ret5=+1.6; leftover $45.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 267 | $0.17 | $1.25 | — | $202.06 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $45.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 2 | $15.87 | $0.32 | — | $170.00 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $45.47 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $170.00 | ▼ close $10,262.13 vs 09:30 $10,283.39 (session -18.51) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $170.00 | ▲ 09:30 equity $10,294.45 vs yday $10,262.13 (+32.32) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 67 | $16.93 | $2.21 | $+39.15 | $1,302.10 | ▲ +39.15 after sell → book $10,292.24; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 399 | $2.68 | $5.22 | $-30.32 | $2,366.19 | ▼ -30.32 after sell → book $10,287.01; vs 09:30 mark -5.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 5 | $197.76 | $2.02 | $-49.43 | $3,352.97 | ▼ -49.43 after sell → book $10,284.99; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 6 | $150.47 | $2.03 | $-87.80 | $4,253.76 | ▼ -87.80 after sell → book $10,282.96; vs 09:30 mark -2.03 | trail off peak after 5 sess (−5%) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 6 | $152.71 | $2.03 | $-34.46 | $5,167.99 | ▼ -34.46 after sell → book $10,280.93; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 19 | $55.80 | $2.07 | $-9.62 | $6,226.13 | ▼ -9.62 after sell → book $10,278.87; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 535 | $1.90 | $7.00 | $-88.80 | $7,235.62 | ▼ -88.80 after sell → book $10,271.86; vs 09:30 mark -7.01 | trail off peak after 5 sess (−5%) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 229 | $4.50 | $3.00 | $-63.21 | $8,263.12 | ▼ -63.21 after sell → book $10,268.86; vs 09:30 mark -3.00 | trail off peak after 5 sess (−5%) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 9 | $108.55 | $2.02 | — | $7,284.16 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+21.3; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $6,689.01 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; ret5=+16.1; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 4 | $209.52 | $2.00 | — | $5,848.93 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 4 | $219.62 | $2.00 | — | $4,968.45 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 12 | $85.00 | $2.03 | — | $3,946.42 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 29 | $34.44 | $2.08 | — | $2,945.59 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1064 | $0.97 | $13.51 | — | $1,899.99 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1032.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 496 | $2.08 | $6.40 | — | $861.91 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1032.89 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $861.91 | ▼ close $10,096.27 vs 09:30 $10,294.45 (session -140.56) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $861.91 | ▲ 09:30 equity $10,201.81 vs yday $10,096.27 (+105.54) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 1 | $88.83 | $0.89 | — | $772.19 | — | after min-hold, trail 5% off peak; list flatten; ret5=+7.6; leftover $143.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 10 | $13.47 | $1.38 | — | $636.12 | — | after min-hold, trail 5% off peak; list flatten; ret5=+3.6; leftover $143.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 35 | $4.00 | $1.50 | — | $494.61 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $143.65 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 15 | $9.31 | $1.44 | — | $353.52 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $143.65 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $353.52 | ▼ close $10,169.09 vs 09:30 $10,201.81 (session -27.51) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $353.52 | ▼ 09:30 equity $10,163.31 vs yday $10,169.09 (-5.78) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 76 | $0.58 | $0.67 | — | $308.77 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $44.19 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.77 | ▼ close $10,153.49 vs 09:30 $10,163.31 (session -9.15) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.77 | ▲ 09:30 equity $10,411.05 vs yday $10,153.49 (+257.56) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 3 | $73.61 | $2.02 | $-14.55 | $527.58 | ▼ -14.55 after sell → book $10,409.03; vs 09:30 mark -2.02 | trail off peak after 5 sess (−5%) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 18 | $13.12 | $2.06 | $-25.53 | $761.68 | ▼ -25.53 after sell → book $10,406.96; vs 09:30 mark -2.07 | trail off peak after 5 sess (−5%) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 7 | $38.04 | $2.03 | $+7.02 | $1,025.93 | ▲ +7.02 after sell → book $10,404.93; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 14 | $23.00 | $2.05 | $+57.38 | $1,345.87 | ▲ +57.38 after sell → book $10,402.88; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 14 | $23.30 | $2.05 | $+67.18 | $1,670.02 | ▲ +67.18 after sell → book $10,400.83; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 3 | $76.47 | $2.02 | $+19.02 | $1,897.41 | ▲ +19.02 after sell → book $10,398.81; vs 09:30 mark -2.02 | trail off peak after 5 sess (−5%) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 44 | $5.53 | $2.14 | $-19.22 | $2,138.59 | ▼ -19.22 after sell → book $10,396.67; vs 09:30 mark -2.14 | trail off peak after 5 sess (−5%) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 2 | $166.54 | $2.00 | — | $1,803.52 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+10.3; leftover $356.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 3 | $116.85 | $2.00 | — | $1,450.97 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+3.3; leftover $356.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 12 | $27.79 | $2.03 | — | $1,115.46 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+7.0; leftover $356.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 36 | $9.81 | $2.10 | — | $760.20 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+4.0; leftover $356.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 17 | $20.25 | $2.04 | — | $413.91 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+15.0; leftover $356.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 17 | $20.65 | $2.04 | — | $60.82 | — | after min-hold, trail 5% off peak; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $356.43 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.82 | ▲ close $10,635.09 vs 09:30 $10,411.05 (session +250.62) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.82 | ▼ 09:30 equity $10,557.74 vs yday $10,635.09 (-77.35) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 4 | $10.39 | $0.45 | $-0.31 | $101.93 | ▼ -0.31 after sell → book $10,557.29; vs 09:30 mark -0.45 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 5 | $7.38 | $0.40 | $-1.85 | $138.43 | ▼ -1.85 after sell → book $10,556.88; vs 09:30 mark -0.41 | trail off peak after 5 sess (−5%) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 1 | $33.82 | $0.36 | $-1.82 | $171.89 | ▼ -1.82 after sell → book $10,556.52; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 267 | $0.15 | $1.26 | $-7.85 | $210.68 | ▼ -7.85 after sell → book $10,555.27; vs 09:30 mark -1.25 | trail off peak after 5 sess (−5%) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 2 | $16.07 | $0.35 | $-0.27 | $242.47 | ▼ -0.27 after sell → book $10,554.92; vs 09:30 mark -0.35 | trail off peak after 5 sess (−5%) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.47 | ▲ close $10,750.49 vs 09:30 $10,557.74 (session +195.57) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.74 | ▲ 09:30 equity $9,287.79 vs yday $9,262.63 (+25.16) | 09:30 open · cash $190.74 (unchanged overnight, no fees) · equity $9,287.79 vs prior close $9,262.63 (+25.16) · 11 name(s) re-marked at the open (per-name table). A×8 yday $172.84 → 09:30 $171.98 -6.88; ADMA×136 yday $9.52 → 09:30 $9.52 +0.00; ARQT×48 yday $26.27 → 09:30 $26.27 +0.00; CYPH×3 yday $4.08 → 09:30 $4.00 -0.22; DEFT×13 yday $0.53 → 09:30 $0.53 +0.00; DXCM×14 yday $87.47 → 09:30 $87.47 +0.00; EYPT×2 yday $3.65 → 09:30 $3.65 +0.00; FJET×3 yday $1.80 → 09:30 $1.80 +0.00; FTRE×66 yday $20.02 → 09:30 $20.02 +0.00; HALO×11 yday $115.22 → 09:30 $115.36 +1.54; OMER×64 yday $20.13 → 09:30 $20.61 +30.72 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 4 | $7.65 | $0.32 | — | $159.82 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+5.2; leftover $31.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 1 | $26.27 | $0.27 | — | $133.29 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $31.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 3 | $9.05 | $0.28 | — | $105.86 | — | after min-hold, trail 5% off peak; list probable,yday_gainer; ret5=-27.1; leftover $31.79 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
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
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-16 | `IQV` | cash | leftover split 262.79 < 1 share @ 270.89 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 45.47 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 45.47 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 45.47 < 1 share @ 147.61 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-21 | `A` | cash | leftover split 143.65 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 143.65 < 1 share @ 386.20 |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `AMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 44.19 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `AMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `RBRK` | 9 | 2026-09-18 @ $108.55 | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+21.3; leftover $1032.89 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | after min-hold, trail 5% off peak; list flatten,ohlc_hot; ret5=+16.1; leftover $1032.89 |
| `GNRC` | 4 | 2026-09-18 @ $209.52 | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1032.89 |
| `VICR` | 4 | 2026-09-18 @ $219.62 | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1032.89 |
| `ECO` | 12 | 2026-09-18 @ $85.00 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1032.89 |
| `FIVN` | 29 | 2026-09-18 @ $34.44 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1032.89 |
| `TLSA` | 1064 | 2026-09-18 @ $0.97 | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1032.89 |
| `SWRD` | 496 | 2026-09-18 @ $2.08 | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1032.89 |
| `DXCM` | 1 | 2026-09-21 @ $88.83 | after min-hold, trail 5% off peak; list flatten; ret5=+7.6; leftover $143.65 |
| `MGTX` | 10 | 2026-09-21 @ $13.47 | after min-hold, trail 5% off peak; list flatten; ret5=+3.6; leftover $143.65 |
| `CYPH` | 35 | 2026-09-21 @ $4.00 | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $143.65 |
| `BKKT` | 15 | 2026-09-21 @ $9.31 | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $143.65 |
| `DEFT` | 76 | 2026-09-22 @ $0.58 | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $44.19 |
| `A` | 2 | 2026-09-23 @ $166.54 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+10.3; leftover $356.43 |
| `HALO` | 3 | 2026-09-23 @ $116.85 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+3.3; leftover $356.43 |
| `ARQT` | 12 | 2026-09-23 @ $27.79 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+7.0; leftover $356.43 |
| `ADMA` | 36 | 2026-09-23 @ $9.81 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+4.0; leftover $356.43 |
| `FTRE` | 17 | 2026-09-23 @ $20.25 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+15.0; leftover $356.43 |
| `OMER` | 17 | 2026-09-23 @ $20.65 | after min-hold, trail 5% off peak; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $356.43 |
