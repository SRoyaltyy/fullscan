# Factor mine action — `flatten_h5_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **-14.17%** ($8,583) · signal-only (no cash/fees) was +4.47%. Starts YES **4/30**. Fills 127 · skips 349 · realized $+556.94.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the timer rings. They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $308.18.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $7.99 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,524.20 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,248.03 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,903.60 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,544.62 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,187.95 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,852.47 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 777 | $1.75 | $10.02 | — | $1,482.70 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1360.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $179.82 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1360.34 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.82 | ▲ close $11,161.56 vs 09:30 $10,966.31 (session +256.20) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.82 | ▲ 09:30 equity $11,454.79 vs yday $11,161.56 (+293.23) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $191.38 | ▲ +2.46 after sell → book $11,454.65; vs 09:30 mark -0.14 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $202.48 | ▼ -1.24 after sell → book $11,454.48; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $215.59 | ▲ +0.96 after sell → book $11,454.31; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $198.21 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.72 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.75 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $125.37 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $26.95 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 20 | $1.32 | $0.32 | — | $98.64 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $26.95 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.64 | ▲ close $11,454.73 vs 09:30 $11,454.79 (session +1.72) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.64 | ▲ 09:30 equity $11,573.07 vs yday $11,454.73 (+118.34) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.62 | $0.07 | $+0.46 | $103.20 | ▲ +0.46 after sell → book $11,573.00; vs 09:30 mark -0.07 | time-stop after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $110.10 | ▲ +0.35 after sell → book $11,572.90; vs 09:30 mark -0.10 | time-stop after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $115.08 | ▲ +0.12 after sell → book $11,572.83; vs 09:30 mark -0.07 | time-stop after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.08 | ▼ close $11,536.82 vs 09:30 $11,573.07 (session -36.01) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.08 | ▼ 09:30 equity $11,355.56 vs yday $11,536.82 (-181.26) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $103.99 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $19.18 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $87.11 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $19.18 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $4.94 | $0.16 | — | $72.14 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $19.18 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.14 | ▲ close $11,813.19 vs 09:30 $11,355.56 (session +458.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.14 | ▼ 09:30 equity $11,600.27 vs yday $11,813.19 (-212.92) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.14 | ▼ close $11,438.58 vs 09:30 $11,600.27 (session -161.69) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.14 | ▲ 09:30 equity $11,465.15 vs yday $11,438.58 (+26.57) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.93 | $2.21 | $+20.68 | $1,451.31 | ▲ +20.68 after sell → book $11,462.94; vs 09:30 mark -2.20 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,786.53 | ▲ +59.06 after sell → book $11,460.88; vs 09:30 mark -2.06 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,169.48 | ▲ +38.51 after sell → book $11,458.68; vs 09:30 mark -2.20 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.49 | $3.08 | $-71.91 | $5,456.54 | ▼ -71.91 after sell → book $11,455.59; vs 09:30 mark -3.09 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.47 | $2.22 | $+122.54 | $6,935.75 | ▲ +122.54 after sell → book $11,453.37; vs 09:30 mark -2.22 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,388.01 | ▲ +116.78 after sell → book $11,451.23; vs 09:30 mark -2.14 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 777 | $1.91 | $10.16 | $+104.13 | $9,861.91 | ▲ +104.13 after sell → book $11,441.06; vs 09:30 mark -10.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,262.89 | ▲ +98.09 after sell → book $11,439.03; vs 09:30 mark -2.03 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 67 | $41.44 | $2.19 | — | $8,484.21 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $2815.72 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 195 | $14.42 | $2.58 | — | $5,669.74 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $2815.72 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 117 | $24.00 | $2.34 | — | $2,859.40 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.7; leftover $2815.72 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1082 | $2.60 | $13.96 | — | $32.24 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $2815.72 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.24 | ▲ close $11,488.80 vs 09:30 $11,465.15 (session +70.84) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.24 | ▲ 09:30 equity $11,559.94 vs yday $11,488.80 (+71.14) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $48.49 | ▼ -1.12 after sell → book $11,559.75; vs 09:30 mark -0.19 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $79.02 | ▲ +8.04 after sell → book $11,559.42; vs 09:30 mark -0.33 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.35 | $0.28 | $-1.76 | $102.23 | ▼ -1.76 after sell → book $11,559.13; vs 09:30 mark -0.29 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.06 | $0.33 | $+1.07 | $128.69 | ▲ +1.07 after sell → book $11,558.81; vs 09:30 mark -0.32 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 20 | $1.82 | $0.44 | $+9.23 | $164.64 | ▲ +9.23 after sell → book $11,558.36; vs 09:30 mark -0.45 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.64 | ▼ close $11,290.13 vs 09:30 $11,559.94 (session -268.23) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.64 | ▲ 09:30 equity $11,416.83 vs yday $11,290.13 (+126.70) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.64 | ▲ close $11,498.99 vs 09:30 $11,416.83 (session +82.16) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.64 | ▲ 09:30 equity $11,790.28 vs yday $11,498.99 (+291.29) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $174.94 | ▼ -0.80 after sell → book $11,790.16; vs 09:30 mark -0.12 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $191.24 | ▼ -0.56 after sell → book $11,789.96; vs 09:30 mark -0.20 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.64 | $0.17 | $-1.23 | $205.00 | ▼ -1.23 after sell → book $11,789.80; vs 09:30 mark -0.16 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.00 | ▼ close $11,763.14 vs 09:30 $11,790.28 (session -26.66) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.00 | ▼ 09:30 equity $11,671.28 vs yday $11,763.14 (-91.86) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.00 | ▲ close $11,705.34 vs 09:30 $11,671.28 (session +34.06) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.00 | ▲ 09:30 equity $11,810.78 vs yday $11,705.34 (+105.44) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 67 | $42.43 | $2.22 | $+61.91 | $3,045.58 | ▲ +61.91 after sell → book $11,808.55; vs 09:30 mark -2.23 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 195 | $15.45 | $2.63 | $+195.64 | $6,055.70 | ▲ +195.64 after sell → book $11,805.92; vs 09:30 mark -2.63 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 117 | $26.12 | $2.38 | $+243.31 | $9,109.35 | ▲ +243.31 after sell → book $11,803.53; vs 09:30 mark -2.39 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1082 | $2.49 | $14.16 | $-147.14 | $11,789.38 | ▼ -147.14 after sell → book $11,789.38; vs 09:30 mark -14.16 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 44 | $52.88 | $2.12 | — | $9,460.53 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2357.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 54 | $42.93 | $2.15 | — | $7,140.16 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2357.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 649 | $3.63 | $8.37 | — | $4,775.92 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2357.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 293 | $8.03 | $3.78 | — | $2,419.35 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2357.88 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $132.45 | $2.04 | — | $165.66 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2357.88 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.66 | ▼ close $11,551.71 vs 09:30 $11,810.78 (session -219.20) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.66 | ▼ 09:30 equity $11,469.66 vs yday $11,551.71 (-82.05) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 10 | $2.52 | $0.28 | — | $140.18 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $27.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 4 | $6.71 | $0.28 | — | $113.06 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $27.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 14 | $1.90 | $0.31 | — | $86.15 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $27.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $4.78 | $0.25 | — | $61.99 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $27.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 17 | $1.59 | $0.32 | — | $34.64 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $27.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 2 | $11.31 | $0.23 | — | $11.79 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $27.61 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.79 | ▲ close $11,580.10 vs 09:30 $11,469.66 (session +112.12) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.79 | ▲ 09:30 equity $11,643.13 vs yday $11,580.10 (+63.03) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.79 | ▼ close $11,445.75 vs 09:30 $11,643.13 (session -197.38) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.79 | ▼ 09:30 equity $11,380.48 vs yday $11,445.75 (-65.27) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.79 | ▼ close $11,054.10 vs 09:30 $11,380.48 (session -326.39) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.79 | ▼ 09:30 equity $10,934.86 vs yday $11,054.10 (-119.24) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.79 | ▼ close $10,764.05 vs 09:30 $10,934.86 (session -170.81) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.79 | ▲ 09:30 equity $10,869.38 vs yday $10,764.05 (+105.33) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 44 | $53.53 | $2.15 | $+24.33 | $2,364.96 | ▲ +24.33 after sell → book $10,867.23; vs 09:30 mark -2.15 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 54 | $41.30 | $2.18 | $-92.35 | $4,592.98 | ▼ -92.35 after sell → book $10,865.05; vs 09:30 mark -2.18 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 649 | $2.77 | $8.49 | $-575.01 | $6,382.22 | ▼ -575.01 after sell → book $10,856.56; vs 09:30 mark -8.49 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 293 | $7.70 | $3.85 | $-104.32 | $8,634.47 | ▼ -104.32 after sell → book $10,852.71; vs 09:30 mark -3.85 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 17 | $122.40 | $2.07 | $-174.96 | $10,713.20 | ▼ -174.96 after sell → book $10,850.64; vs 09:30 mark -2.07 | time-stop after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 109 | $16.28 | $2.32 | — | $8,936.37 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1785.53 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 654 | $2.73 | $8.44 | — | $7,142.51 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1785.53 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,485.77 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1785.53 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $3,839.45 | — | sell at min-hold even if still listed; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1785.53 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 11 | $157.78 | $2.02 | — | $2,101.85 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1785.53 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 31 | $56.09 | $2.08 | — | $360.98 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1785.53 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $360.98 | ▼ close $10,799.08 vs 09:30 $10,869.38 (session -32.67) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $360.98 | ▼ 09:30 equity $10,383.72 vs yday $10,799.08 (-415.36) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 10 | $2.15 | $0.27 | $-4.25 | $382.21 | ▼ -4.25 after sell → book $10,383.45; vs 09:30 mark -0.27 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 4 | $5.93 | $0.27 | $-3.67 | $405.66 | ▼ -3.67 after sell → book $10,383.18; vs 09:30 mark -0.27 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 14 | $1.72 | $0.30 | $-3.20 | $429.37 | ▼ -3.20 after sell → book $10,382.88; vs 09:30 mark -0.30 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 5 | $4.13 | $0.24 | $-3.75 | $449.78 | ▼ -3.75 after sell → book $10,382.64; vs 09:30 mark -0.24 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 17 | $1.59 | $0.34 | $-0.66 | $476.47 | ▼ -0.66 after sell → book $10,382.30; vs 09:30 mark -0.34 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 2 | $10.73 | $0.24 | $-1.63 | $497.69 | ▼ -1.63 after sell → book $10,382.06; vs 09:30 mark -0.24 | time-stop after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $497.69 | ▼ close $10,256.67 vs 09:30 $10,383.72 (session -125.39) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $497.69 | ▲ 09:30 equity $10,331.91 vs yday $10,256.67 (+75.24) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $497.69 | ▼ close $10,128.45 vs 09:30 $10,331.91 (session -203.46) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $497.69 | ▲ 09:30 equity $10,212.71 vs yday $10,128.45 (+84.26) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 1 | $77.12 | $0.77 | — | $419.80 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $124.42 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 8 | $14.31 | $1.17 | — | $304.15 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $124.42 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 3 | $36.46 | $1.10 | — | $193.66 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $124.42 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.66 | ▲ close $10,297.82 vs 09:30 $10,212.71 (session +88.16) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.66 | ▲ 09:30 equity $10,538.08 vs yday $10,297.82 (+240.26) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $162.60 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $32.28 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $131.92 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $32.28 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.92 | ▲ close $10,552.00 vs 09:30 $10,538.08 (session +14.55) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.92 | ▲ 09:30 equity $10,587.73 vs yday $10,552.00 (+35.73) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 109 | $16.93 | $2.35 | $+66.18 | $1,974.94 | ▲ +66.18 after sell → book $10,585.38; vs 09:30 mark -2.35 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 654 | $2.68 | $8.56 | $-49.70 | $3,719.10 | ▼ -49.70 after sell → book $10,576.82; vs 09:30 mark -8.56 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 8 | $197.76 | $2.04 | $-76.69 | $5,299.15 | ▼ -76.69 after sell → book $10,574.79; vs 09:30 mark -2.03 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 10 | $150.47 | $2.04 | $-143.66 | $6,801.80 | ▼ -143.66 after sell → book $10,572.74; vs 09:30 mark -2.05 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 11 | $152.71 | $2.05 | $-59.84 | $8,479.57 | ▼ -59.84 after sell → book $10,570.70; vs 09:30 mark -2.04 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 31 | $55.80 | $2.11 | $-13.18 | $10,207.26 | ▼ -13.18 after sell → book $10,568.59; vs 09:30 mark -2.11 | time-stop after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 15 | $108.55 | $2.04 | — | $8,576.98 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1701.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $7,388.68 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1701.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 8 | $209.52 | $2.01 | — | $5,710.51 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1701.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 7 | $219.62 | $2.01 | — | $4,171.15 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1701.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 20 | $85.00 | $2.05 | — | $2,469.10 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1701.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 49 | $34.44 | $2.14 | — | $779.41 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1701.21 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $779.41 | ▼ close $10,382.07 vs 09:30 $10,587.73 (session -174.28) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $779.41 | ▲ 09:30 equity $10,490.07 vs yday $10,382.07 (+108.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 1 | $88.83 | $0.89 | — | $689.69 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $155.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 11 | $13.47 | $1.51 | — | $540.00 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $155.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 38 | $4.00 | $1.63 | — | $386.37 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $155.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $386.37 | ▲ close $10,619.81 vs 09:30 $10,490.07 (session +133.78) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $386.37 | ▼ 09:30 equity $10,612.94 vs yday $10,619.81 (-6.87) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $386.37 | ▼ close $10,580.92 vs 09:30 $10,612.94 (session -32.02) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $386.37 | ▲ 09:30 equity $10,875.31 vs yday $10,580.92 (+294.39) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 1 | $73.61 | $0.76 | $-5.04 | $459.22 | ▼ -5.04 after sell → book $10,874.55; vs 09:30 mark -0.76 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 8 | $13.12 | $1.09 | $-11.78 | $563.08 | ▼ -11.78 after sell → book $10,873.46; vs 09:30 mark -1.09 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 3 | $38.04 | $1.17 | $+2.47 | $676.03 | ▲ +2.47 after sell → book $10,872.29; vs 09:30 mark -1.17 | time-stop after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 4 | $27.79 | $1.12 | — | $563.75 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $112.67 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 11 | $9.81 | $1.11 | — | $454.73 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $112.67 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 5 | $20.25 | $1.03 | — | $352.45 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $112.67 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 5 | $20.65 | $1.05 | — | $248.15 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $112.67 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.15 | ▲ close $10,893.77 vs 09:30 $10,875.31 (session +25.79) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.15 | ▼ 09:30 equity $10,710.74 vs yday $10,893.77 (-183.03) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 3 | $10.39 | $0.34 | $-0.24 | $278.98 | ▼ -0.24 after sell → book $10,710.40; vs 09:30 mark -0.34 | time-stop after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 4 | $7.38 | $0.33 | $-1.48 | $308.18 | ▼ -1.48 after sell → book $10,710.07; vs 09:30 mark -0.33 | time-stop after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.18 | ▲ close $10,739.39 vs 09:30 $10,710.74 (session +29.32) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.67 | ▲ 09:30 equity $8,625.53 vs yday $8,601.65 (+23.88) | 09:30 open · cash $290.67 (unchanged overnight, no fees) · equity $8,625.53 vs prior close $8,601.65 (+23.88) · 9 name(s) re-marked at the open (per-name table). A×7 yday $172.84 → 09:30 $171.98 -6.02; ADMA×126 yday $9.52 → 09:30 $9.52 +0.00; ARQT×44 yday $26.27 → 09:30 $26.27 +0.00; CYPH×4 yday $4.08 → 09:30 $4.00 -0.30; DXCM×13 yday $87.47 → 09:30 $87.47 +0.00; FTRE×61 yday $20.02 → 09:30 $20.02 +0.00; HALO×10 yday $115.22 → 09:30 $115.36 +1.40; MGTX×1 yday $11.05 → 09:30 $11.05 +0.00; OMER×60 yday $20.13 → 09:30 $20.61 +28.80 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 2 | $38.51 | $0.78 | — | $212.87 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $96.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 12 | $7.65 | $0.95 | — | $120.12 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $96.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.12 | ▼ close $8,582.89 vs 09:30 $8,625.53 (session -40.91) | 16:00 close · cash $120.12 · equity $8,582.89 vs 09:30 $8,625.53 (-42.64; session marks -40.91) · 11 name(s) marked open→close (per-name table). A×7 09:30 $171.98 → close $172.79 +5.67; ADMA×126 09:30 $9.52 → close $9.52 +0.00; ARQT×44 09:30 $26.27 → close $26.27 +0.00; CYPH×4 09:30 $4.00 → close $4.12 +0.46; DXCM×13 09:30 $87.47 → close $87.47 +0.00; FTRE×61 09:30 $20.02 → close $20.02 +0.00; HALO×10 09:30 $115.36 → close $113.90 -14.60; MGTX×1 09:30 $11.05 → close $11.05 +0.00; OMER×60 09:30 $20.61 → close $20.08 -31.80; BLFS×2 09:30 $38.51 → close $38.49 -0.04; MRVI×12 09:30 $7.65 → close $7.60 -0.60 | — |

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
| 2026-08-25 | `MOS` | cash | leftover split 19.18 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 19.18 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 19.18 < 1 share @ 426.97 |
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
| 2026-08-26 | `HCA` | cash | leftover split 24.05 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 24.05 < 1 share @ 24.84 |
| 2026-08-26 | `INSP` | cash | leftover split 24.05 < 1 share @ 60.07 |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `OVID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ORCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COHU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `IQV` | cash | leftover split 124.42 < 1 share @ 270.89 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 32.28 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 32.28 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 32.28 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 32.28 < 1 share @ 34.93 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 155.88 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 155.88 < 1 share @ 386.20 |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 64.39 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `A` | cash | leftover split 112.67 < 1 share @ 166.54 |
| 2026-09-23 | `HALO` | cash | leftover split 112.67 < 1 share @ 116.85 |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 15 | 2026-09-18 @ $108.55 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1701.21 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1701.21 |
| `GNRC` | 8 | 2026-09-18 @ $209.52 | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1701.21 |
| `VICR` | 7 | 2026-09-18 @ $219.62 | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1701.21 |
| `ECO` | 20 | 2026-09-18 @ $85.00 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1701.21 |
| `FIVN` | 49 | 2026-09-18 @ $34.44 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1701.21 |
| `DXCM` | 1 | 2026-09-21 @ $88.83 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $155.88 |
| `MGTX` | 11 | 2026-09-21 @ $13.47 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $155.88 |
| `CYPH` | 38 | 2026-09-21 @ $4.00 | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $155.88 |
| `ARQT` | 4 | 2026-09-23 @ $27.79 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $112.67 |
| `ADMA` | 11 | 2026-09-23 @ $9.81 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $112.67 |
| `FTRE` | 5 | 2026-09-23 @ $20.25 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $112.67 |
| `OMER` | 5 | 2026-09-23 @ $20.65 | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $112.67 |
