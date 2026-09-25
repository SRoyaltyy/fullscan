# Factor mine action — `union_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-12.21%** ($8,779) · signal-only (no cash/fees) was -0.80%. Starts YES **0/30**. Fills 248 · skips 107 · realized $-106.46.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,893.58.

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
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.27 | ▲ close $10,193.91 vs 09:30 $10,178.12 (session +90.14) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | baseline list, no extra gate; list flatten; ret5=-7.2; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | — | baseline list, no extra gate; list flatten; ⚪; ret5=-11.4; leftover $1269.49 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.62 | ▲ close $10,173.09 vs 09:30 $10,196.20 (session +40.17) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.62 | ▼ 09:30 equity $10,124.76 vs yday $10,173.09 (-48.33) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,485.52 | ▲ +44.98 after sell → book $10,122.66; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,667.81 | ▲ +38.11 after sell → book $10,120.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,919.36 | ▲ +33.34 after sell → book $10,118.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,079.62 | ▼ -111.43 after sell → book $10,114.50; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,359.65 | ▲ +8.58 after sell → book $10,112.03; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,665.76 | ▲ +36.52 after sell → book $10,109.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,876.65 | ▼ -60.99 after sell → book $10,104.86; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 263 | $4.67 | $3.45 | $-43.66 | $10,101.41 | ▼ -43.66 after sell → book $10,101.41; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,124.76 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,101.41 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.57 | ▲ close $10,311.13 vs 09:30 $10,101.41 (session +234.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.57 | ▲ 09:30 equity $10,580.85 vs yday $10,311.13 (+269.72) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,537.28 | ▲ +77.98 after sell → book $10,578.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,779.59 | ▲ +57.15 after sell → book $10,576.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,104.14 | ▲ +62.73 after sell → book $10,574.41; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,337.35 | ▼ -27.47 after sell → book $10,571.56; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,690.02 | ▲ +94.17 after sell → book $10,569.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,039.02 | ▲ +102.43 after sell → book $10,567.21; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,320.18 | ▲ +10.11 after sell → book $10,557.78; vs 09:30 mark -9.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,555.75 | ▲ +77.23 after sell → book $10,555.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.75 | ▲ close $10,781.93 vs 09:30 $10,580.85 (session +265.42) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.75 | ▲ 09:30 equity $11,161.11 vs yday $10,781.93 (+379.18) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,338.32 | ▲ +7.81 after sell → book $11,159.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,595.40 | ▼ -52.34 after sell → book $11,156.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,895.55 | ▲ +0.34 after sell → book $11,154.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.33 | $2.38 | $+254.88 | $5,466.12 | ▲ +254.88 after sell → book $11,152.43; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.40 | $6.99 | $-51.26 | $6,740.73 | ▼ -51.26 after sell → book $11,145.44; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.88 | $8.93 | $-51.90 | $8,015.83 | ▼ -51.90 after sell → book $11,136.50; vs 09:30 mark -8.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.75 | $2.08 | $-25.47 | $9,306.26 | ▼ -25.47 after sell → book $11,134.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 999 | $1.83 | $13.07 | $+483.54 | $11,121.36 | ▲ +483.54 after sell → book $11,121.36; vs 09:30 mark -13.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,121.36 | ▲ close $11,121.36 vs 09:30 $11,161.11 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,121.36 | ▲ 09:30 equity $11,121.36 vs yday $11,121.36 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,740.54 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+13.0; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.98 | $2.37 | — | $8,354.69 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+1.2; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $7,006.45 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+7.4; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.35 | $2.49 | — | $5,617.86 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 281 | $4.94 | $3.62 | — | $4,226.10 | — | baseline list, no extra gate; list flatten; ret5=+7.1; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,943.19 | — | baseline list, no extra gate; list flatten; ret5=+6.0; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 191 | $7.25 | $2.56 | — | $1,555.88 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1390.17 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3883 | $0.36 | $25.55 | — | $140.21 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-15.6; leftover $1390.17 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.21 | ▲ close $11,335.31 vs 09:30 $11,121.36 (session +256.77) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.21 | ▼ 09:30 equity $11,334.01 vs yday $11,335.31 (-1.30) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 191 | $8.29 | $2.61 | $+193.47 | $1,721.00 | ▲ +193.47 after sell → book $11,331.40; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3883 | $0.35 | $26.01 | $-70.98 | $3,065.68 | ▼ -70.98 after sell → book $11,305.39; vs 09:30 mark -26.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 49 | $31.21 | $2.14 | — | $1,534.26 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1532.84 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 137 | $11.12 | $2.40 | — | $8.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1532.84 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.41 | ▼ close $11,260.59 vs 09:30 $11,334.01 (session -40.26) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.41 | ▲ 09:30 equity $11,300.78 vs yday $11,260.59 (+40.19) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.63 | $2.40 | $-48.87 | $1,345.40 | ▼ -48.87 after sell → book $11,298.39; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,709.52 | ▲ +15.89 after sell → book $11,296.31; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 166 | $8.49 | $2.53 | $+18.23 | $4,116.33 | ▲ +18.23 after sell → book $11,293.78; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 281 | $5.07 | $3.68 | $+29.22 | $5,537.32 | ▲ +29.22 after sell → book $11,290.10; vs 09:30 mark -3.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,809.13 | ▼ -11.10 after sell → book $11,288.08; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $5,480.96 | — | baseline list, no extra gate; list flatten; ret5=+3.1; leftover $1361.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 94 | $14.42 | $2.27 | — | $4,123.21 | — | baseline list, no extra gate; list flatten; ret5=+7.1; leftover $1361.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 523 | $2.60 | $6.75 | — | $2,756.66 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+13.0; leftover $1361.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 104 | $12.98 | $2.30 | — | $1,404.44 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1361.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 140 | $9.68 | $2.41 | — | $46.83 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1361.83 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.83 | ▲ close $11,344.22 vs 09:30 $11,300.78 (session +71.96) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.83 | ▼ 09:30 equity $11,328.83 vs yday $11,344.22 (-15.39) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 49 | $30.53 | $2.16 | $-37.62 | $1,540.64 | ▼ -37.62 after sell → book $11,326.67; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 137 | $11.27 | $2.44 | $+15.71 | $3,082.20 | ▲ +15.71 after sell → book $11,324.24; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 104 | $13.05 | $2.33 | $+2.65 | $4,437.07 | ▲ +2.65 after sell → book $11,321.91; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 140 | $9.88 | $2.44 | $+23.15 | $5,817.82 | ▲ +23.15 after sell → book $11,319.46; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 44 | $32.90 | $2.12 | — | $4,368.10 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1454.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 92 | $15.66 | $2.27 | — | $2,925.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1454.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 18 | $79.42 | $2.04 | — | $1,493.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1454.46 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 438 | $3.32 | $5.65 | — | $33.70 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+6.4; leftover $1454.46 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.70 | ▼ close $10,988.25 vs 09:30 $11,328.83 (session -319.13) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.70 | ▲ 09:30 equity $11,015.84 vs yday $10,988.25 (+27.59) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-9.57 | $1,404.96 | ▼ -9.57 after sell → book $11,013.66; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,746.85 | ▲ +13.73 after sell → book $11,011.55; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 94 | $14.54 | $2.30 | $+6.71 | $4,111.31 | ▲ +6.71 after sell → book $11,009.25; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 523 | $2.58 | $6.84 | $-24.05 | $5,453.81 | ▼ -24.05 after sell → book $11,002.41; vs 09:30 mark -6.84 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 44 | $31.15 | $2.14 | $-81.27 | $6,822.26 | ▼ -81.27 after sell → book $11,000.26; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 92 | $14.44 | $2.29 | $-116.80 | $8,148.45 | ▼ -116.80 after sell → book $10,997.97; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 18 | $80.44 | $2.07 | $+14.25 | $9,594.31 | ▲ +14.25 after sell → book $10,995.91; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 438 | $3.20 | $5.73 | $-63.94 | $10,990.17 | ▼ -63.94 after sell → book $10,990.17; vs 09:30 mark -5.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,990.17 | ▲ close $10,990.17 vs 09:30 $11,015.84 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,990.17 | ▲ 09:30 equity $10,990.17 vs yday $10,990.17 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,990.17 | ▲ close $10,990.17 vs 09:30 $10,990.17 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,990.17 | ▲ 09:30 equity $10,990.17 vs yday $10,990.17 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,990.17 | ▲ close $10,990.17 vs 09:30 $10,990.17 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,990.17 | ▲ 09:30 equity $10,990.17 vs yday $10,990.17 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,666.11 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $8,290.26 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 378 | $3.63 | $4.88 | — | $6,913.24 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 171 | $8.03 | $2.50 | — | $5,537.61 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,211.09 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,849.24 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,533.72 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1373.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $173.11 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1373.77 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.11 | ▼ close $10,726.24 vs 09:30 $10,990.17 (session -243.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.11 | ▲ 09:30 equity $10,729.64 vs yday $10,726.24 (+3.40) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $41.50 | $2.11 | $-49.95 | $1,499.01 | ▼ -49.95 after sell → book $10,727.54; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 171 | $7.91 | $2.54 | $-25.57 | $2,849.07 | ▼ -25.57 after sell → book $10,724.99; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,147.33 | ▼ -28.26 after sell → book $10,722.95; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,465.05 | ▼ -44.13 after sell → book $10,720.67; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,845.60 | ▲ +65.02 after sell → book $10,718.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,107.75 | ▼ -98.45 after sell → book $10,716.38; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 536 | $2.52 | $6.91 | — | $6,750.12 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1351.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 201 | $6.71 | $2.60 | — | $5,398.81 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1351.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 711 | $1.90 | $9.17 | — | $4,038.74 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1351.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 282 | $4.78 | $3.64 | — | $2,687.14 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1351.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 849 | $1.59 | $10.95 | — | $1,326.28 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1351.29 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 117 | $11.31 | $2.34 | — | $0.66 | — | baseline list, no extra gate; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1351.29 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.66 | ▼ close $10,626.24 vs 09:30 $10,729.64 (session -54.53) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.66 | ▼ 09:30 equity $10,582.41 vs yday $10,626.24 (-43.83) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,356.33 | ▲ +31.60 after sell → book $10,580.33; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 378 | $3.43 | $4.95 | $-85.43 | $2,647.92 | ▼ -85.43 after sell → book $10,575.38; vs 09:30 mark -4.95 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 536 | $2.38 | $7.01 | $-88.97 | $3,916.59 | ▼ -88.97 after sell → book $10,568.37; vs 09:30 mark -7.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 201 | $6.57 | $2.64 | $-33.38 | $5,234.51 | ▼ -33.38 after sell → book $10,565.72; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 711 | $2.00 | $9.30 | $+52.63 | $6,647.21 | ▲ +52.63 after sell → book $10,556.42; vs 09:30 mark -9.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 282 | $4.30 | $3.69 | $-142.69 | $7,856.12 | ▼ -142.69 after sell → book $10,552.73; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 849 | $1.63 | $11.10 | $+11.90 | $9,228.88 | ▲ +11.90 after sell → book $10,541.62; vs 09:30 mark -11.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 117 | $11.22 | $2.37 | $-15.24 | $10,539.25 | ▼ -15.24 after sell → book $10,539.25; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,539.25 | ▲ close $10,539.25 vs 09:30 $10,582.41 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,539.25 | ▲ 09:30 equity $10,539.25 vs yday $10,539.25 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,539.25 | ▲ close $10,539.25 vs 09:30 $10,539.25 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,539.25 | ▲ 09:30 equity $10,539.25 vs yday $10,539.25 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,539.25 | ▲ close $10,539.25 vs 09:30 $10,539.25 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,539.25 | ▲ 09:30 equity $10,539.25 vs yday $10,539.25 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,234.62 | — | baseline list, no extra gate; list flatten; 🔵; ret5=-1.1; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 482 | $2.73 | $6.22 | — | $7,912.55 | — | baseline list, no extra gate; list flatten; 🔵; ret5=-3.0; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,669.50 | — | baseline list, no extra gate; list flatten; ret5=+8.3; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $5,352.04 | — | baseline list, no extra gate; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,087.79 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+4.7; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,795.66 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+19.6; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 645 | $2.04 | $8.32 | — | $1,471.54 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1317.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 277 | $4.75 | $3.57 | — | $152.22 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1317.41 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.22 | ▼ close $10,478.51 vs 09:30 $10,539.25 (session -32.31) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.22 | ▼ 09:30 equity $10,165.77 vs yday $10,478.51 (-312.74) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 80 | $16.03 | $2.25 | $-24.48 | $1,432.36 | ▼ -24.48 after sell → book $10,163.51; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 482 | $2.75 | $6.31 | $-0.48 | $2,753.96 | ▼ -0.48 after sell → book $10,157.20; vs 09:30 mark -6.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $3,990.94 | ▼ -6.08 after sell → book $10,155.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 8 | $141.42 | $2.03 | $-188.13 | $5,120.26 | ▼ -188.13 after sell → book $10,153.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $6,319.47 | ▼ -92.92 after sell → book $10,151.06; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 645 | $2.01 | $8.44 | $-36.11 | $7,607.49 | ▼ -36.11 after sell → book $10,142.63; vs 09:30 mark -8.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 277 | $4.82 | $3.63 | $+12.19 | $8,939.00 | ▲ +12.19 after sell → book $10,139.00; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,939.00 | ▼ close $10,112.12 vs 09:30 $10,165.77 (session -26.88) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,939.00 | ▲ 09:30 equity $10,147.96 vs yday $10,112.12 (+35.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $10,145.92 | ▼ -57.33 after sell → book $10,145.92; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,145.92 | ▲ close $10,145.92 vs 09:30 $10,147.96 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,145.92 | ▲ 09:30 equity $10,145.92 vs yday $10,145.92 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $9,060.36 | — | baseline list, no extra gate; list flatten; ret5=+4.0; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,824.40 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+7.2; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 88 | $14.31 | $2.25 | — | $6,562.87 | — | baseline list, no extra gate; list flatten; ret5=+4.8; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,321.14 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+2.9; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 68 | $18.61 | $2.19 | — | $4,053.46 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 69 | $18.21 | $2.20 | — | $2,794.77 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-19.1; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $1,554.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1268.24 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 216 | $5.87 | $2.79 | — | $283.80 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1268.24 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.80 | ▲ close $10,330.21 vs 09:30 $10,145.92 (session +201.90) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.80 | ▲ 09:30 equity $10,500.13 vs yday $10,330.21 (+169.92) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,374.38 | ▲ +5.02 after sell → book $10,498.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,595.36 | ▼ -14.98 after sell → book $10,496.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 88 | $14.33 | $2.28 | $-2.77 | $3,854.13 | ▼ -2.77 after sell → book $10,493.78; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $5,098.79 | ▲ +2.94 after sell → book $10,491.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 68 | $22.46 | $2.22 | $+257.39 | $6,623.86 | ▲ +257.39 after sell → book $10,489.45; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 69 | $19.59 | $2.22 | $+90.80 | $7,973.35 | ▲ +90.80 after sell → book $10,487.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $9,279.88 | ▲ +66.27 after sell → book $10,485.16; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 216 | $5.58 | $2.83 | $-68.26 | $10,482.33 | ▼ -68.26 after sell → book $10,482.33; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $9,311.07 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+11.7; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $8,097.62 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,914.73 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+17.7; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 127 | $10.25 | $2.37 | — | $5,610.61 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 172 | $7.59 | $2.51 | — | $4,302.62 | — | baseline list, no extra gate; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 37 | $34.93 | $2.10 | — | $3,008.11 | — | baseline list, no extra gate; list flatten; ret5=+1.6; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7707 | $0.17 | $36.22 | — | $1,661.70 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1310.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 82 | $15.87 | $2.24 | — | $358.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1310.29 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $358.12 | ▲ close $10,507.75 vs 09:30 $10,500.13 (session +76.89) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $358.12 | ▲ 09:30 equity $10,715.40 vs yday $10,507.75 (+207.65) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,601.74 | ▲ +72.37 after sell → book $10,713.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,864.03 | ▲ +48.83 after sell → book $10,711.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $4,034.00 | ▼ -12.93 after sell → book $10,709.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 127 | $10.12 | $2.40 | $-21.28 | $5,316.83 | ▼ -21.28 after sell → book $10,706.90; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 172 | $7.98 | $2.55 | $+62.03 | $6,686.85 | ▲ +62.03 after sell → book $10,704.36; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 37 | $34.52 | $2.12 | $-19.39 | $7,961.97 | ▼ -19.39 after sell → book $10,702.24; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7707 | $0.17 | $37.51 | $-73.74 | $9,234.65 | ▼ -73.74 after sell → book $10,664.73; vs 09:30 mark -37.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 82 | $17.44 | $2.26 | $+124.24 | $10,662.46 | ▲ +124.24 after sell → book $10,662.46; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 12 | $108.55 | $2.03 | — | $9,357.84 | — | baseline list, no extra gate; list flatten; ⚪; ret5=+21.3; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $8,169.54 | — | baseline list, no extra gate; list flatten,ohlc_hot; ret5=+16.1; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $6,910.41 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $5,590.69 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $4,313.65 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 38 | $34.44 | $2.10 | — | $3,002.83 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1374 | $0.97 | $17.45 | — | $1,652.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1332.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 640 | $2.08 | $8.26 | — | $313.14 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1332.81 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.14 | ▼ close $10,445.19 vs 09:30 $10,715.40 (session -179.39) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.14 | ▲ 09:30 equity $10,583.03 vs yday $10,445.19 (+137.84) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 12 | $107.57 | $2.05 | $-15.83 | $1,601.93 | ▼ -15.83 after sell → book $10,580.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 2 | $586.77 | $2.02 | $-16.77 | $2,773.46 | ▼ -16.77 after sell → book $10,578.97; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $4,031.43 | ▼ -1.16 after sell → book $10,576.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $5,410.90 | ▲ +59.74 after sell → book $10,574.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $6,651.30 | ▼ -36.64 after sell → book $10,572.86; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 38 | $33.00 | $2.12 | $-58.95 | $7,903.17 | ▼ -58.95 after sell → book $10,570.73; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1374 | $0.94 | $17.28 | $-75.95 | $9,177.46 | ▼ -75.95 after sell → book $10,553.46; vs 09:30 mark -17.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 640 | $2.15 | $8.37 | $+28.17 | $10,545.08 | ▲ +28.17 after sell → book $10,545.08; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,280.11 | — | baseline list, no extra gate; list flatten; ret5=+6.5; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $8,119.51 | — | baseline list, no extra gate; list flatten; ret5=-5.8; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,873.86 | — | baseline list, no extra gate; list flatten; ret5=+7.6; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 168 | $7.84 | $2.49 | — | $5,554.24 | — | baseline list, no extra gate; list flatten; ret5=+13.6; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 126 | $10.43 | $2.37 | — | $4,237.70 | — | baseline list, no extra gate; list flatten; ret5=+19.2; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 97 | $13.47 | $2.28 | — | $2,928.83 | — | baseline list, no extra gate; list flatten; ret5=+3.6; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 329 | $4.00 | $4.24 | — | $1,608.58 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1318.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 141 | $9.31 | $2.41 | — | $293.46 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1318.14 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.46 | ▼ close $10,226.74 vs 09:30 $10,583.03 (session -298.50) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.46 | ▲ 09:30 equity $10,261.67 vs yday $10,226.74 (+34.93) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 126 | $10.18 | $2.40 | $-36.27 | $1,573.74 | ▼ -36.27 after sell → book $10,259.27; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 329 | $3.51 | $4.31 | $-169.76 | $2,724.22 | ▼ -169.76 after sell → book $10,254.96; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 3 | $93.97 | $2.00 | — | $2,440.31 | — | baseline list, no extra gate; list flatten; ret5=-0.6; leftover $340.53 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 587 | $0.58 | $5.17 | — | $2,094.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $340.53 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,094.69 | ▼ close $10,218.33 vs 09:30 $10,261.67 (session -29.47) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,094.69 | ▲ 09:30 equity $10,273.76 vs yday $10,218.33 (+55.43) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $3,202.67 | ▼ -52.62 after sell → book $10,271.74; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 97 | $12.26 | $2.31 | $-121.96 | $4,389.58 | ▼ -121.96 after sell → book $10,269.43; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 141 | $9.50 | $2.45 | $+21.93 | $5,726.63 | ▲ +21.93 after sell → book $10,266.99; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 3 | $93.97 | $2.02 | $-4.02 | $6,006.52 | ▼ -4.02 after sell → book $10,264.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 587 | $0.57 | $5.24 | $-13.34 | $6,338.80 | ▼ -13.34 after sell → book $10,259.72; vs 09:30 mark -5.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $5,168.28 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1267.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 45 | $27.79 | $2.12 | — | $3,915.61 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1267.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 129 | $9.81 | $2.38 | — | $2,647.74 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1267.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 62 | $20.25 | $2.18 | — | $1,390.07 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1267.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 61 | $20.65 | $2.17 | — | $128.24 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1267.76 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.24 | ▼ close $9,967.12 vs 09:30 $10,273.76 (session -281.73) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.24 | ▼ 09:30 equity $9,911.18 vs yday $9,967.12 (-55.94) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,437.81 | ▲ +44.59 after sell → book $9,909.15; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,663.21 | ▼ -20.25 after sell → book $9,907.10; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 168 | $7.38 | $2.53 | $-82.31 | $3,900.52 | ▼ -82.31 after sell → book $9,904.57; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $5,020.68 | ▼ -50.36 after sell → book $9,902.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 45 | $26.22 | $2.15 | $-74.92 | $6,198.43 | ▼ -74.92 after sell → book $9,900.38; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 129 | $9.67 | $2.41 | $-22.85 | $7,443.45 | ▼ -22.85 after sell → book $9,897.97; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 62 | $19.40 | $2.20 | $-57.07 | $8,644.06 | ▼ -57.07 after sell → book $9,895.78; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 61 | $20.52 | $2.19 | $-12.30 | $9,893.58 | ▼ -12.30 after sell → book $9,893.58; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,893.58 | ▲ close $9,893.58 vs 09:30 $9,911.18 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,779.94 | ▲ 09:30 equity $8,779.94 vs yday $8,779.94 (+0.00) | 09:30 open · cash $8,779.94 · no holdings · equity $8,779.94 vs prior close $8,779.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $7,974.08 | — | baseline list, no extra gate; list flatten; ret5=+0.8; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $6,933.82 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 53 | $20.61 | $2.15 | — | $5,839.34 | — | baseline list, no extra gate; list flatten; 🔵; ret5=+9.1; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $4,758.99 | — | baseline list, no extra gate; list flatten; ret5=+4.7; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 143 | $7.65 | $2.42 | — | $3,662.62 | — | baseline list, no extra gate; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 41 | $26.27 | $2.11 | — | $2,583.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $1,492.53 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1097.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 121 | $9.05 | $2.35 | — | $395.12 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-27.1; leftover $1097.49 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $395.12 | ▲ close $8,778.92 vs 09:30 $8,779.94 (session +16.13) | 16:00 close · cash $395.12 · equity $8,778.92 vs 09:30 $8,779.94 (-1.02; session marks +16.13) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×53 09:30 $20.61 → close $20.08 -28.09; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×143 09:30 $7.65 → close $7.60 -7.15; WRBY×41 09:30 $26.27 → close $26.71 +18.04; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×121 09:30 $9.05 → close $9.36 +37.51 | — |

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
