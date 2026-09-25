# Factor mine action — `union_h1_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **-12.29%** ($8,771) · signal-only (no cash/fees) was -0.80%. Starts YES **0/30**. Fills 284 · skips 108 · realized $-163.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the timer rings. They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- Time-stop: once 1 session(s) are up, sell at 09:30 even if the name is still on the list.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,836.91.

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
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.27 | ▲ close $10,193.91 vs 09:30 $10,178.12 (session +90.14) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | sell at min-hold even if still listed; list flatten; ret5=-7.2; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-11.4; leftover $1269.49 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.62 | ▲ close $10,173.09 vs 09:30 $10,196.20 (session +40.17) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.62 | ▼ 09:30 equity $10,124.76 vs yday $10,173.09 (-48.33) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,485.52 | ▲ +44.98 after sell → book $10,122.66; vs 09:30 mark -2.10 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,667.81 | ▲ +38.11 after sell → book $10,120.63; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,919.36 | ▲ +33.34 after sell → book $10,118.60; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,079.62 | ▼ -111.43 after sell → book $10,114.50; vs 09:30 mark -4.10 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,359.65 | ▲ +8.58 after sell → book $10,112.03; vs 09:30 mark -2.47 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,665.76 | ▲ +36.52 after sell → book $10,109.98; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,876.65 | ▼ -60.99 after sell → book $10,104.86; vs 09:30 mark -5.12 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 263 | $4.67 | $3.45 | $-43.66 | $10,101.41 | ▼ -43.66 after sell → book $10,101.41; vs 09:30 mark -3.45 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,124.76 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,101.41 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.57 | ▲ close $10,311.13 vs 09:30 $10,101.41 (session +234.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.57 | ▲ 09:30 equity $10,580.85 vs yday $10,311.13 (+269.72) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,537.28 | ▲ +77.98 after sell → book $10,578.66; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,779.59 | ▲ +57.15 after sell → book $10,576.61; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,104.14 | ▲ +62.73 after sell → book $10,574.41; vs 09:30 mark -2.20 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,337.35 | ▼ -27.47 after sell → book $10,571.56; vs 09:30 mark -2.85 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,690.02 | ▲ +94.17 after sell → book $10,569.35; vs 09:30 mark -2.21 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,039.02 | ▲ +102.43 after sell → book $10,567.21; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,320.18 | ▲ +10.11 after sell → book $10,557.78; vs 09:30 mark -9.43 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,555.75 | ▲ +77.23 after sell → book $10,555.75; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.75 | ▲ close $10,781.93 vs 09:30 $10,580.85 (session +265.42) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.75 | ▲ 09:30 equity $11,161.11 vs yday $10,781.93 (+379.18) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,338.32 | ▲ +7.81 after sell → book $11,159.07; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,595.40 | ▼ -52.34 after sell → book $11,156.83; vs 09:30 mark -2.24 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,895.55 | ▲ +0.34 after sell → book $11,154.80; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.33 | $2.38 | $+254.88 | $5,466.12 | ▲ +254.88 after sell → book $11,152.43; vs 09:30 mark -2.37 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.40 | $6.99 | $-51.26 | $6,740.73 | ▼ -51.26 after sell → book $11,145.44; vs 09:30 mark -6.99 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.88 | $8.93 | $-51.90 | $8,015.83 | ▼ -51.90 after sell → book $11,136.50; vs 09:30 mark -8.94 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.75 | $2.08 | $-25.47 | $9,306.26 | ▼ -25.47 after sell → book $11,134.43; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 999 | $1.83 | $13.07 | $+483.54 | $11,121.36 | ▲ +483.54 after sell → book $11,121.36; vs 09:30 mark -13.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,121.36 | ▲ close $11,121.36 vs 09:30 $11,161.11 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,121.36 | ▲ 09:30 equity $11,121.36 vs yday $11,121.36 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,740.54 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+13.0; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.98 | $2.37 | — | $8,354.69 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+1.2; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $7,006.45 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+7.4; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.35 | $2.49 | — | $5,617.86 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 281 | $4.94 | $3.62 | — | $4,226.10 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,943.19 | — | sell at min-hold even if still listed; list flatten; ret5=+6.0; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 191 | $7.25 | $2.56 | — | $1,555.88 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3883 | $0.36 | $25.55 | — | $140.21 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-15.6; leftover $1390.17 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.21 | ▲ close $11,335.31 vs 09:30 $11,121.36 (session +256.77) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.21 | ▼ 09:30 equity $11,334.01 vs yday $11,335.31 (-1.30) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 58 | $24.84 | $2.19 | $+57.71 | $1,578.75 | ▲ +57.71 after sell → book $11,331.83; vs 09:30 mark -2.18 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-28.71 | $2,935.89 | ▼ -28.71 after sell → book $11,329.43; vs 09:30 mark -2.40 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-28.77 | $4,255.35 | ▼ -28.77 after sell → book $11,327.35; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 166 | $8.60 | $2.53 | $+36.48 | $5,680.42 | ▲ +36.48 after sell → book $11,324.82; vs 09:30 mark -2.53 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RZLT` | 281 | $5.01 | $3.68 | $+12.36 | $7,084.55 | ▲ +12.36 after sell → book $11,321.14; vs 09:30 mark -3.68 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $8,365.03 | ▼ -2.43 after sell → book $11,319.12; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 191 | $8.29 | $2.61 | $+193.47 | $9,945.81 | ▲ +193.47 after sell → book $11,316.51; vs 09:30 mark -2.61 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3883 | $0.35 | $26.01 | $-70.98 | $11,290.50 | ▼ -70.98 after sell → book $11,290.50; vs 09:30 mark -26.01 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 3 | $427.50 | $2.00 | — | $10,006.00 | — | sell at min-hold even if still listed; list flatten; ret5=+4.1; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `MOS` | 56 | $24.84 | $2.16 | — | $8,612.80 | — | sell at min-hold even if still listed; list flatten; ret5=+14.8; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `OCUL` | 130 | $10.79 | $2.38 | — | $7,207.72 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-1.2; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `INSP` | 23 | $60.07 | $2.06 | — | $5,824.06 | — | sell at min-hold even if still listed; list flatten; ret5=+6.4; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 164 | $8.60 | $2.48 | — | $4,411.17 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+4.8; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 281 | $5.01 | $3.62 | — | $2,999.74 | — | sell at min-hold even if still listed; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 45 | $31.21 | $2.12 | — | $1,593.16 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1411.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 126 | $11.12 | $2.37 | — | $189.68 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1411.31 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.68 | ▼ close $11,235.20 vs 09:30 $11,334.01 (session -36.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.68 | ▲ 09:30 equity $11,271.81 vs yday $11,235.20 (+36.61) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-12.69 | $1,461.49 | ▼ -12.69 after sell → book $11,269.79; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 56 | $24.00 | $2.18 | $-51.38 | $2,803.31 | ▼ -51.38 after sell → book $11,267.61; vs 09:30 mark -2.18 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 130 | $10.63 | $2.41 | $-25.59 | $4,182.80 | ▼ -25.59 after sell → book $11,265.20; vs 09:30 mark -2.41 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 23 | $62.10 | $2.08 | $+42.55 | $5,609.02 | ▲ +42.55 after sell → book $11,263.12; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 164 | $8.49 | $2.52 | $-23.04 | $6,998.85 | ▼ -23.04 after sell → book $11,260.59; vs 09:30 mark -2.53 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 281 | $5.07 | $3.68 | $+9.55 | $8,419.84 | ▲ +9.55 after sell → book $11,256.91; vs 09:30 mark -3.68 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 45 | $30.79 | $2.15 | $-23.17 | $9,803.25 | ▼ -23.17 after sell → book $11,254.77; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 126 | $11.52 | $2.40 | $+45.63 | $11,252.37 | ▲ +45.63 after sell → book $11,252.37; vs 09:30 mark -2.40 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $9,882.76 | — | sell at min-hold even if still listed; list flatten; ret5=+3.1; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.42 | $2.28 | — | $8,481.74 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 58 | $24.00 | $2.16 | — | $7,087.57 | — | sell at min-hold even if still listed; list flatten; ret5=+8.7; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 540 | $2.60 | $6.97 | — | $5,676.61 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+13.0; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 108 | $12.98 | $2.31 | — | $4,272.45 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 45 | $30.79 | $2.12 | — | $2,884.78 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `FLNC` | 122 | $11.52 | $2.36 | — | $1,476.98 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=-8.2; leftover $1406.55 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 145 | $9.68 | $2.42 | — | $70.96 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1406.55 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.96 | ▲ close $11,305.15 vs 09:30 $11,271.81 (session +75.50) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.96 | ▼ 09:30 equity $11,294.58 vs yday $11,305.15 (-10.57) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 33 | $41.74 | $2.11 | $+5.70 | $1,446.27 | ▲ +5.70 after sell → book $11,292.47; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 97 | $14.63 | $2.31 | $+15.78 | $2,863.07 | ▲ +15.78 after sell → book $11,290.16; vs 09:30 mark -2.31 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 58 | $23.95 | $2.19 | $-7.25 | $4,249.98 | ▼ -7.25 after sell → book $11,287.97; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 540 | $2.68 | $7.07 | $+29.17 | $5,690.11 | ▲ +29.17 after sell → book $11,280.90; vs 09:30 mark -7.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 108 | $13.05 | $2.34 | $+2.90 | $7,097.17 | ▲ +2.90 after sell → book $11,278.56; vs 09:30 mark -2.34 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 45 | $30.53 | $2.15 | $-15.97 | $8,468.88 | ▼ -15.97 after sell → book $11,276.42; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 122 | $11.27 | $2.39 | $-35.24 | $9,841.43 | ▼ -35.24 after sell → book $11,274.03; vs 09:30 mark -2.39 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 145 | $9.88 | $2.46 | $+24.11 | $11,271.57 | ▲ +24.11 after sell → book $11,271.57; vs 09:30 mark -2.46 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 33 | $41.74 | $2.09 | — | $9,892.06 | — | sell at min-hold even if still listed; list flatten; ret5=+2.4; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 96 | $14.63 | $2.28 | — | $8,485.30 | — | sell at min-hold even if still listed; list flatten; ret5=+5.8; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 58 | $23.95 | $2.16 | — | $7,094.04 | — | sell at min-hold even if still listed; list flatten; ret5=+1.8; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 525 | $2.68 | $6.77 | — | $5,680.26 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+16.3; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $4,296.35 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $2,900.35 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,548.17 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1408.95 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 424 | $3.32 | $5.47 | — | $135.02 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=+6.4; leftover $1408.95 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.02 | ▼ close $10,932.35 vs 09:30 $11,294.58 (session -314.03) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.02 | ▲ 09:30 equity $10,962.54 vs yday $10,932.35 (+30.19) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+4.38 | $1,518.91 | ▲ +4.38 after sell → book $10,960.43; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.54 | $2.31 | $-13.22 | $2,912.45 | ▼ -13.22 after sell → book $10,958.13; vs 09:30 mark -2.30 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-20.01 | $4,283.70 | ▼ -20.01 after sell → book $10,955.94; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 525 | $2.58 | $6.87 | $-66.14 | $5,631.33 | ▼ -66.14 after sell → book $10,949.07; vs 09:30 mark -6.87 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $6,937.49 | ▼ -77.75 after sell → book $10,946.93; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $8,220.37 | ▼ -113.12 after sell → book $10,944.65; vs 09:30 mark -2.28 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $9,585.79 | ▲ +13.24 after sell → book $10,942.59; vs 09:30 mark -2.06 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 424 | $3.20 | $5.55 | $-61.90 | $10,937.04 | ▼ -61.90 after sell → book $10,937.04; vs 09:30 mark -5.55 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,937.04 | ▲ close $10,937.04 vs 09:30 $10,962.54 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,937.04 | ▲ 09:30 equity $10,937.04 vs yday $10,937.04 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,937.04 | ▲ close $10,937.04 vs 09:30 $10,937.04 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,937.04 | ▲ 09:30 equity $10,937.04 vs yday $10,937.04 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,937.04 | ▲ close $10,937.04 vs 09:30 $10,937.04 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,937.04 | ▲ 09:30 equity $10,937.04 vs yday $10,937.04 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,612.97 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,280.06 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 376 | $3.63 | $4.85 | — | $6,910.33 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 170 | $8.03 | $2.50 | — | $5,542.73 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,216.21 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,854.36 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,538.83 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1367.13 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $178.23 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1367.13 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.23 | ▼ close $10,674.56 vs 09:30 $10,937.04 (session -242.45) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.23 | ▲ 09:30 equity $10,678.43 vs yday $10,674.56 (+3.87) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 25 | $52.03 | $2.09 | $-25.40 | $1,476.90 | ▼ -25.40 after sell → book $10,676.35; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $2,761.29 | ▼ -48.52 after sell → book $10,674.24; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 376 | $3.46 | $4.92 | $-73.69 | $4,057.33 | ▼ -73.69 after sell → book $10,669.32; vs 09:30 mark -4.92 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 170 | $7.91 | $2.54 | $-25.44 | $5,399.49 | ▼ -25.44 after sell → book $10,666.78; vs 09:30 mark -2.54 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $6,697.75 | ▼ -28.26 after sell → book $10,664.74; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $8,015.47 | ▼ -44.13 after sell → book $10,662.46; vs 09:30 mark -2.28 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $9,396.01 | ▲ +65.02 after sell → book $10,660.42; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $10,658.17 | ▼ -98.45 after sell → book $10,658.17; vs 09:30 mark -2.25 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 385 | $3.46 | $4.97 | — | $9,321.10 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 528 | $2.52 | $6.81 | — | $7,983.73 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 198 | $6.71 | $2.58 | — | $6,652.56 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 701 | $1.90 | $9.04 | — | $5,311.62 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 278 | $4.78 | $3.59 | — | $3,979.19 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 837 | $1.59 | $10.80 | — | $2,637.57 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 117 | $11.31 | $2.34 | — | $1,311.96 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1332.27 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 25 | $52.03 | $2.06 | — | $9.14 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1332.27 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.14 | ▼ close $10,562.35 vs 09:30 $10,678.43 (session -53.63) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.14 | ▼ 09:30 equity $10,519.39 vs yday $10,562.35 (-42.96) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 385 | $3.43 | $5.04 | $-21.56 | $1,324.65 | ▼ -21.56 after sell → book $10,514.35; vs 09:30 mark -5.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 528 | $2.38 | $6.91 | $-87.64 | $2,574.38 | ▼ -87.64 after sell → book $10,507.44; vs 09:30 mark -6.91 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 198 | $6.57 | $2.63 | $-32.93 | $3,872.61 | ▼ -32.93 after sell → book $10,504.81; vs 09:30 mark -2.63 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 701 | $2.00 | $9.17 | $+51.89 | $5,265.44 | ▲ +51.89 after sell → book $10,495.64; vs 09:30 mark -9.17 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 278 | $4.30 | $3.64 | $-140.67 | $6,457.20 | ▼ -140.67 after sell → book $10,492.00; vs 09:30 mark -3.64 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 837 | $1.63 | $10.95 | $+11.74 | $7,810.56 | ▲ +11.74 after sell → book $10,481.05; vs 09:30 mark -10.95 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 117 | $11.22 | $2.37 | $-15.24 | $9,120.93 | ▼ -15.24 after sell → book $10,478.68; vs 09:30 mark -2.37 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+52.85 | $10,476.60 | ▲ +52.85 after sell → book $10,476.60; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,476.60 | ▲ close $10,476.60 vs 09:30 $10,519.39 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,476.60 | ▲ 09:30 equity $10,476.60 vs yday $10,476.60 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,476.60 | ▲ close $10,476.60 vs 09:30 $10,476.60 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,476.60 | ▲ 09:30 equity $10,476.60 vs yday $10,476.60 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,476.60 | ▲ close $10,476.60 vs 09:30 $10,476.60 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,476.60 | ▲ 09:30 equity $10,476.60 vs yday $10,476.60 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,171.97 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-1.1; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 479 | $2.73 | $6.18 | — | $7,858.12 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-3.0; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,615.07 | — | sell at min-hold even if still listed; list flatten; ret5=+8.3; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,462.05 | — | sell at min-hold even if still listed; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,197.80 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+4.7; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,905.67 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+19.6; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 641 | $2.04 | $8.27 | — | $1,589.76 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1309.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 275 | $4.75 | $3.55 | — | $279.96 | — | sell at min-hold even if still listed; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1309.57 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.96 | ▼ close $10,430.22 vs 09:30 $10,476.60 (session -18.06) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.96 | ▼ 09:30 equity $10,126.15 vs yday $10,430.22 (-304.07) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 80 | $16.03 | $2.25 | $-24.48 | $1,560.11 | ▼ -24.48 after sell → book $10,123.89; vs 09:30 mark -2.26 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 479 | $2.75 | $6.27 | $-0.47 | $2,873.48 | ▼ -0.47 after sell → book $10,117.62; vs 09:30 mark -6.27 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $4,110.45 | ▼ -6.08 after sell → book $10,115.59; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $5,098.36 | ▼ -165.11 after sell → book $10,113.56; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NVT` | 8 | $150.00 | $2.03 | $-66.29 | $6,296.33 | ▼ -66.29 after sell → book $10,111.53; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $7,495.54 | ▼ -92.92 after sell → book $10,109.45; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 641 | $2.01 | $8.39 | $-35.88 | $8,775.56 | ▼ -35.88 after sell → book $10,101.06; vs 09:30 mark -8.39 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 275 | $4.82 | $3.60 | $+12.10 | $10,097.46 | ▲ +12.10 after sell → book $10,097.46; vs 09:30 mark -3.60 | time-stop after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,097.46 | ▲ close $10,097.46 vs 09:30 $10,126.15 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,097.46 | ▲ 09:30 equity $10,097.46 vs yday $10,097.46 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,097.46 | ▲ close $10,097.46 vs 09:30 $10,097.46 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,097.46 | ▲ 09:30 equity $10,097.46 vs yday $10,097.46 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $9,011.90 | — | sell at min-hold even if still listed; list flatten; ret5=+4.0; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,775.94 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+7.2; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 88 | $14.31 | $2.25 | — | $6,514.41 | — | sell at min-hold even if still listed; list flatten; ret5=+4.8; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,272.67 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+2.9; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 67 | $18.61 | $2.19 | — | $4,023.61 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 69 | $18.21 | $2.20 | — | $2,764.93 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-19.1; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $1,524.66 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1262.18 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 215 | $5.87 | $2.77 | — | $259.84 | — | sell at min-hold even if still listed; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1262.18 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $259.84 | ▲ close $10,278.53 vs 09:30 $10,097.46 (session +198.66) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $259.84 | ▲ 09:30 equity $10,448.13 vs yday $10,278.53 (+169.60) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,350.42 | ▲ +5.02 after sell → book $10,446.11; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,571.40 | ▼ -14.98 after sell → book $10,444.05; vs 09:30 mark -2.06 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 88 | $14.33 | $2.28 | $-2.77 | $3,830.16 | ▼ -2.77 after sell → book $10,441.77; vs 09:30 mark -2.28 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $5,074.83 | ▲ +2.94 after sell → book $10,439.66; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 67 | $22.46 | $2.21 | $+253.54 | $6,577.43 | ▲ +253.54 after sell → book $10,437.44; vs 09:30 mark -2.22 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 69 | $19.59 | $2.22 | $+90.80 | $7,926.93 | ▲ +90.80 after sell → book $10,435.23; vs 09:30 mark -2.21 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $9,233.46 | ▲ +66.27 after sell → book $10,433.16; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 215 | $5.58 | $2.82 | $-67.94 | $10,430.34 | ▼ -67.94 after sell → book $10,430.34; vs 09:30 mark -2.82 | time-stop after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $9,259.09 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+11.7; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $8,045.63 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,862.74 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+17.7; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 127 | $10.25 | $2.37 | — | $5,558.62 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 171 | $7.59 | $2.50 | — | $4,258.22 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 37 | $34.93 | $2.10 | — | $2,963.71 | — | sell at min-hold even if still listed; list flatten; ret5=+1.6; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7669 | $0.17 | $36.04 | — | $1,623.94 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 82 | $15.87 | $2.24 | — | $320.36 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1303.79 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.36 | ▲ close $10,456.04 vs 09:30 $10,448.13 (session +76.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.36 | ▲ 09:30 equity $10,663.20 vs yday $10,456.04 (+207.16) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,563.99 | ▲ +72.37 after sell → book $10,661.18; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,826.27 | ▲ +48.83 after sell → book $10,659.14; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $3,996.24 | ▼ -12.93 after sell → book $10,657.11; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 127 | $10.12 | $2.40 | $-21.28 | $5,279.08 | ▼ -21.28 after sell → book $10,654.71; vs 09:30 mark -2.40 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 171 | $7.98 | $2.54 | $+61.64 | $6,641.12 | ▲ +61.64 after sell → book $10,652.17; vs 09:30 mark -2.54 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 37 | $34.52 | $2.12 | $-19.39 | $7,916.23 | ▼ -19.39 after sell → book $10,650.04; vs 09:30 mark -2.13 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7669 | $0.17 | $37.33 | $-73.37 | $9,182.64 | ▼ -73.37 after sell → book $10,612.72; vs 09:30 mark -37.32 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 82 | $17.44 | $2.26 | $+124.24 | $10,610.46 | ▲ +124.24 after sell → book $10,610.46; vs 09:30 mark -2.26 | time-stop after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 12 | $108.55 | $2.03 | — | $9,305.83 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+21.3; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $8,117.53 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+16.1; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $6,858.41 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $5,538.68 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $4,261.64 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 38 | $34.44 | $2.10 | — | $2,950.82 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1367 | $0.97 | $17.36 | — | $1,607.47 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1326.31 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 637 | $2.08 | $8.22 | — | $274.29 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1326.31 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.29 | ▼ close $10,393.52 vs 09:30 $10,663.20 (session -179.18) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.29 | ▲ 09:30 equity $10,531.15 vs yday $10,393.52 (+137.63) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 12 | $107.57 | $2.05 | $-15.83 | $1,563.08 | ▼ -15.83 after sell → book $10,529.10; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 2 | $586.77 | $2.02 | $-16.77 | $2,734.61 | ▼ -16.77 after sell → book $10,527.09; vs 09:30 mark -2.01 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $3,992.58 | ▼ -1.16 after sell → book $10,525.06; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $5,372.05 | ▲ +59.74 after sell → book $10,523.03; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $6,612.45 | ▼ -36.64 after sell → book $10,520.98; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 38 | $33.00 | $2.12 | $-58.95 | $7,864.32 | ▼ -58.95 after sell → book $10,518.85; vs 09:30 mark -2.13 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1367 | $0.94 | $17.19 | $-75.56 | $9,132.11 | ▼ -75.56 after sell → book $10,501.66; vs 09:30 mark -17.19 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 637 | $2.15 | $8.33 | $+28.04 | $10,493.33 | ▲ +28.04 after sell → book $10,493.33; vs 09:30 mark -8.33 | time-stop after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,228.36 | — | sell at min-hold even if still listed; list flatten; ret5=+6.5; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $8,067.76 | — | sell at min-hold even if still listed; list flatten; ret5=-5.8; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,822.10 | — | sell at min-hold even if still listed; list flatten; ret5=+7.6; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 167 | $7.84 | $2.49 | — | $5,510.33 | — | sell at min-hold even if still listed; list flatten; ret5=+13.6; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 125 | $10.43 | $2.37 | — | $4,204.22 | — | sell at min-hold even if still listed; list flatten; ret5=+19.2; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 97 | $13.47 | $2.28 | — | $2,895.35 | — | sell at min-hold even if still listed; list flatten; ret5=+3.6; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 327 | $4.00 | $4.22 | — | $1,583.13 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1311.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 140 | $9.31 | $2.41 | — | $277.32 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1311.67 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.32 | ▼ close $10,176.77 vs 09:30 $10,531.15 (session -296.75) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.32 | ▲ 09:30 equity $10,211.49 vs yday $10,176.77 (+34.72) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 125 | $10.18 | $2.40 | $-36.01 | $1,547.42 | ▼ -36.01 after sell → book $10,209.09; vs 09:30 mark -2.40 | time-stop after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 327 | $3.51 | $4.28 | $-168.73 | $2,690.91 | ▼ -168.73 after sell → book $10,204.81; vs 09:30 mark -4.28 | time-stop after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 3 | $93.97 | $2.00 | — | $2,407.00 | — | sell at min-hold even if still listed; list flatten; ret5=-0.6; leftover $336.36 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 579 | $0.58 | $5.10 | — | $2,066.09 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $336.36 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,066.09 | ▼ close $10,168.65 vs 09:30 $10,211.49 (session -29.07) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,066.09 | ▲ 09:30 equity $10,223.11 vs yday $10,168.65 (+54.46) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 8 | $166.54 | $2.03 | $+65.31 | $3,396.37 | ▲ +65.31 after sell → book $10,221.08; vs 09:30 mark -2.03 | time-stop after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $4,504.35 | ▼ -52.62 after sell → book $10,219.06; vs 09:30 mark -2.02 | time-stop after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DXCM` | 14 | $89.50 | $2.05 | $+5.30 | $5,755.30 | ▲ +5.30 after sell → book $10,217.01; vs 09:30 mark -2.05 | time-stop after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 167 | $7.95 | $2.53 | $+13.35 | $7,080.42 | ▲ +13.35 after sell → book $10,214.48; vs 09:30 mark -2.53 | time-stop after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 97 | $12.26 | $2.31 | $-121.96 | $8,267.33 | ▼ -121.96 after sell → book $10,212.17; vs 09:30 mark -2.31 | time-stop after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 140 | $9.50 | $2.44 | $+21.75 | $9,594.89 | ▲ +21.75 after sell → book $10,209.73; vs 09:30 mark -2.44 | time-stop after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 3 | $93.97 | $2.02 | $-4.02 | $9,874.78 | ▼ -4.02 after sell → book $10,207.71; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 579 | $0.57 | $5.17 | $-13.16 | $10,202.53 | ▼ -13.16 after sell → book $10,202.53; vs 09:30 mark -5.18 | time-stop after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 14 | $89.50 | $2.03 | — | $8,947.50 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $7,779.71 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,609.19 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 45 | $27.79 | $2.12 | — | $5,356.52 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 160 | $7.95 | $2.47 | — | $4,082.05 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 130 | $9.81 | $2.38 | — | $2,804.37 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 62 | $20.25 | $2.18 | — | $1,546.69 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1275.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 61 | $20.65 | $2.17 | — | $284.87 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1275.32 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $284.87 | ▼ close $9,908.61 vs 09:30 $10,223.11 (session -276.54) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $284.87 | ▼ 09:30 equity $9,854.49 vs yday $9,908.61 (-54.12) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-29.63 | $1,510.27 | ▼ -29.63 after sell → book $9,852.44; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $-22.17 | $2,655.88 | ▼ -22.17 after sell → book $9,850.40; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $3,776.04 | ▼ -50.36 after sell → book $9,848.36; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 45 | $26.22 | $2.15 | $-74.92 | $4,953.80 | ▼ -74.92 after sell → book $9,846.22; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 160 | $7.38 | $2.51 | $-96.18 | $6,132.09 | ▼ -96.18 after sell → book $9,843.71; vs 09:30 mark -2.51 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 130 | $9.67 | $2.41 | $-22.99 | $7,386.78 | ▼ -22.99 after sell → book $9,841.30; vs 09:30 mark -2.41 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 62 | $19.40 | $2.20 | $-57.07 | $8,587.38 | ▼ -57.07 after sell → book $9,839.10; vs 09:30 mark -2.20 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 61 | $20.52 | $2.19 | $-12.30 | $9,836.91 | ▼ -12.30 after sell → book $9,836.91; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,836.91 | ▲ close $9,836.91 vs 09:30 $9,854.49 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,772.49 | ▲ 09:30 equity $8,772.49 vs yday $8,772.49 (+0.00) | 09:30 open · cash $8,772.49 · no holdings · equity $8,772.49 vs prior close $8,772.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $7,966.63 | — | sell at min-hold even if still listed; list flatten; ret5=+0.8; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $6,926.37 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 53 | $20.61 | $2.15 | — | $5,831.89 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+9.1; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $4,751.54 | — | sell at min-hold even if still listed; list flatten; ret5=+4.7; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 143 | $7.65 | $2.42 | — | $3,655.17 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 41 | $26.27 | $2.11 | — | $2,575.98 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $1,485.08 | — | sell at min-hold even if still listed; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1096.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 121 | $9.05 | $2.35 | — | $387.67 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-27.1; leftover $1096.56 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $387.67 | ▲ close $8,771.47 vs 09:30 $8,772.49 (session +16.13) | 16:00 close · cash $387.67 · equity $8,771.47 vs 09:30 $8,772.49 (-1.02; session marks +16.13) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×53 09:30 $20.61 → close $20.08 -28.09; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×143 09:30 $7.65 → close $7.60 -7.15; WRBY×41 09:30 $26.27 → close $26.71 +18.04; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×121 09:30 $9.05 → close $9.36 +37.51 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
