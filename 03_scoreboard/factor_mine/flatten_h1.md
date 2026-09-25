# Factor mine action — `flatten_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-14.59%** ($8,542) · signal-only (no cash/fees) was -9.51%. Starts YES **0/30**. Fills 195 · skips 78 · realized $-357.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,642.91.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | — |
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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.9; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-8.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $1267.99 | — |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $1269.49 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | — |
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
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 77 | $23.77 | $2.22 | — | $9,288.85 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1853.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 168 | $10.98 | $2.49 | — | $7,441.71 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $1853.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 30 | $61.19 | $2.08 | — | $5,603.93 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $1853.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 221 | $8.35 | $2.85 | — | $3,755.73 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $1853.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 375 | $4.94 | $4.84 | — | $1,898.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $1853.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $426.97 | $2.00 | — | $188.51 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $1853.56 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.51 | ▲ close $11,202.79 vs 09:30 $11,121.36 (session +97.92) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.51 | ▲ 09:30 equity $11,205.36 vs yday $11,202.79 (+2.57) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.51 | ▼ close $11,165.02 vs 09:30 $11,205.36 (session -40.34) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.51 | ▼ 09:30 equity $11,161.33 vs yday $11,165.02 (-3.69) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 168 | $10.63 | $2.54 | $-63.83 | $1,971.82 | ▼ -63.83 after sell → book $11,158.80; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 30 | $62.10 | $2.10 | $+23.12 | $3,832.71 | ▲ +23.12 after sell → book $11,156.69; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 221 | $8.49 | $2.90 | $+25.19 | $5,706.10 | ▲ +25.19 after sell → book $11,153.79; vs 09:30 mark -2.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 375 | $5.07 | $4.92 | $+39.00 | $7,602.44 | ▲ +39.00 after sell → book $11,148.88; vs 09:30 mark -4.91 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 4 | $424.61 | $2.03 | $-13.47 | $9,298.85 | ▼ -13.47 after sell → book $11,146.85; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 74 | $41.44 | $2.21 | — | $6,230.08 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $3099.62 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 214 | $14.42 | $2.76 | — | $3,141.44 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $3099.62 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1192 | $2.60 | $15.38 | — | $26.86 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $3099.62 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.86 | ▲ close $11,213.30 vs 09:30 $11,161.33 (session +86.80) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.86 | ▲ 09:30 equity $11,285.15 vs yday $11,213.30 (+71.85) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.86 | ▼ close $11,009.76 vs 09:30 $11,285.15 (session -275.39) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.86 | ▲ 09:30 equity $11,145.14 vs yday $11,009.76 (+135.38) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 77 | $23.68 | $2.25 | $-11.40 | $1,847.97 | ▼ -11.40 after sell → book $11,142.89; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 74 | $42.00 | $2.25 | $+36.98 | $4,953.72 | ▲ +36.98 after sell → book $11,140.64; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 214 | $14.54 | $2.82 | $+20.10 | $8,062.46 | ▲ +20.10 after sell → book $11,137.82; vs 09:30 mark -2.82 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 1192 | $2.58 | $15.60 | $-54.82 | $11,122.22 | ▼ -54.82 after sell → book $11,122.22; vs 09:30 mark -15.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,122.22 | ▲ close $11,122.22 vs 09:30 $11,145.14 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,122.22 | ▲ 09:30 equity $11,122.22 vs yday $11,122.22 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,122.22 | ▲ close $11,122.22 vs 09:30 $11,122.22 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,122.22 | ▲ 09:30 equity $11,122.22 vs yday $11,122.22 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,122.22 | ▲ close $11,122.22 vs 09:30 $11,122.22 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,122.22 | ▲ 09:30 equity $11,122.22 vs yday $11,122.22 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 42 | $52.88 | $2.12 | — | $8,899.15 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2224.44 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 51 | $42.93 | $2.14 | — | $6,707.57 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2224.44 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 612 | $3.63 | $7.89 | — | $4,478.12 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2224.44 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 277 | $8.03 | $3.57 | — | $2,250.24 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2224.44 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 16 | $132.45 | $2.04 | — | $129.00 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2224.44 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.00 | ▼ close $10,897.48 vs 09:30 $11,122.22 (session -206.98) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.00 | ▼ 09:30 equity $10,819.83 vs yday $10,897.48 (-77.65) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 51 | $41.50 | $2.17 | $-77.24 | $2,243.33 | ▼ -77.24 after sell → book $10,817.66; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 277 | $7.91 | $3.64 | $-40.45 | $4,430.76 | ▼ -40.45 after sell → book $10,814.02; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 16 | $130.03 | $2.06 | $-42.82 | $6,509.18 | ▼ -42.82 after sell → book $10,811.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 430 | $2.52 | $5.55 | — | $5,420.03 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1084.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 161 | $6.71 | $2.47 | — | $4,337.25 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1084.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 570 | $1.90 | $7.35 | — | $3,246.89 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1084.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 226 | $4.78 | $2.92 | — | $2,163.70 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1084.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 682 | $1.59 | $8.80 | — | $1,070.52 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1084.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 94 | $11.31 | $2.27 | — | $5.11 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1084.86 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.11 | ▼ close $10,730.90 vs 09:30 $10,819.83 (session -51.70) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.11 | ▲ 09:30 equity $10,744.60 vs yday $10,730.90 (+13.70) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 42 | $54.31 | $2.14 | $+55.80 | $2,283.98 | ▲ +55.80 after sell → book $10,742.45; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 612 | $3.43 | $8.01 | $-138.31 | $4,375.13 | ▼ -138.31 after sell → book $10,734.44; vs 09:30 mark -8.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 430 | $2.38 | $5.63 | $-71.38 | $5,392.90 | ▼ -71.38 after sell → book $10,728.81; vs 09:30 mark -5.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 161 | $6.57 | $2.51 | $-27.52 | $6,448.16 | ▼ -27.52 after sell → book $10,726.30; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 570 | $2.00 | $7.46 | $+42.19 | $7,580.71 | ▲ +42.19 after sell → book $10,718.85; vs 09:30 mark -7.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 226 | $4.30 | $2.96 | $-114.36 | $8,549.54 | ▼ -114.36 after sell → book $10,715.88; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 682 | $1.63 | $8.92 | $+9.56 | $9,652.28 | ▲ +9.56 after sell → book $10,706.96; vs 09:30 mark -8.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 94 | $11.22 | $2.30 | $-13.03 | $10,704.66 | ▼ -13.03 after sell → book $10,704.66; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,704.66 | ▲ close $10,704.66 vs 09:30 $10,744.60 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,704.66 | ▲ 09:30 equity $10,704.66 vs yday $10,704.66 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,704.66 | ▲ close $10,704.66 vs 09:30 $10,704.66 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,704.66 | ▲ 09:30 equity $10,704.66 vs yday $10,704.66 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,704.66 | ▲ close $10,704.66 vs 09:30 $10,704.66 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,704.66 | ▲ 09:30 equity $10,704.66 vs yday $10,704.66 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 109 | $16.28 | $2.32 | — | $8,927.83 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1784.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 653 | $2.73 | $8.42 | — | $7,136.71 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1784.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,479.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1784.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $3,833.66 | — | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1784.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 11 | $157.78 | $2.02 | — | $2,096.06 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1784.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 31 | $56.09 | $2.08 | — | $355.18 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1784.11 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $355.18 | ▼ close $10,653.11 vs 09:30 $10,704.66 (session -32.67) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $355.18 | ▼ 09:30 equity $10,236.80 vs yday $10,653.11 (-416.31) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 109 | $16.03 | $2.35 | $-31.92 | $2,100.10 | ▼ -31.92 after sell → book $10,234.45; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 653 | $2.75 | $8.55 | $-0.65 | $3,890.57 | ▼ -0.65 after sell → book $10,225.90; vs 09:30 mark -8.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 8 | $206.50 | $2.04 | $-6.77 | $5,540.54 | ▼ -6.77 after sell → book $10,223.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 10 | $141.42 | $2.04 | $-234.16 | $6,952.69 | ▼ -234.16 after sell → book $10,221.82; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 31 | $52.23 | $2.11 | $-123.85 | $8,569.72 | ▼ -123.85 after sell → book $10,219.72; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,569.72 | ▼ close $10,182.76 vs 09:30 $10,236.80 (session -36.96) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,569.72 | ▲ 09:30 equity $10,232.04 vs yday $10,182.76 (+49.28) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 11 | $151.12 | $2.05 | $-77.33 | $10,229.99 | ▼ -77.33 after sell → book $10,229.99; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,229.99 | ▲ close $10,229.99 vs 09:30 $10,232.04 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,229.99 | ▲ 09:30 equity $10,229.99 vs yday $10,229.99 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 9 | $270.89 | $2.02 | — | $7,789.96 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $2557.50 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 33 | $77.12 | $2.09 | — | $5,242.92 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $2557.50 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 178 | $14.31 | $2.52 | — | $2,693.21 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $2557.50 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 70 | $36.46 | $2.20 | — | $138.81 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $2557.50 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.81 | ▼ close $10,124.91 vs 09:30 $10,229.99 (session -96.25) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.81 | ▲ 09:30 equity $10,237.32 vs yday $10,124.91 (+112.41) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 9 | $273.15 | $2.05 | $+16.28 | $2,595.12 | ▲ +16.28 after sell → book $10,235.27; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 33 | $76.44 | $2.12 | $-26.65 | $5,115.52 | ▼ -26.65 after sell → book $10,233.16; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 178 | $14.33 | $2.57 | $-1.54 | $7,663.68 | ▼ -1.54 after sell → book $10,230.58; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 70 | $36.67 | $2.23 | $+10.27 | $10,228.35 | ▲ +10.27 after sell → book $10,228.35; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 7 | $233.85 | $2.01 | — | $8,589.39 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; leftover $1704.72 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 11 | $151.43 | $2.02 | — | $6,921.64 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; leftover $1704.72 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 11 | $147.61 | $2.02 | — | $5,295.90 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; leftover $1704.72 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 166 | $10.25 | $2.49 | — | $3,591.91 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $1704.72 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 224 | $7.59 | $2.89 | — | $1,888.87 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $1704.72 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 48 | $34.93 | $2.13 | — | $210.09 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; leftover $1704.72 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.09 | ▲ close $10,336.14 vs 09:30 $10,237.32 (session +121.36) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.09 | ▲ 09:30 equity $10,428.34 vs yday $10,336.14 (+92.20) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 7 | $249.13 | $2.04 | $+102.91 | $1,951.97 | ▲ +102.91 after sell → book $10,426.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 11 | $158.04 | $2.05 | $+68.64 | $3,688.36 | ▲ +68.64 after sell → book $10,424.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 11 | $146.50 | $2.05 | $-16.28 | $5,297.81 | ▼ -16.28 after sell → book $10,422.21; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 166 | $10.12 | $2.53 | $-26.60 | $6,975.20 | ▼ -26.60 after sell → book $10,419.68; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 224 | $7.98 | $2.94 | $+81.53 | $8,759.78 | ▲ +81.53 after sell → book $10,416.74; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 48 | $34.52 | $2.16 | $-23.97 | $10,414.59 | ▼ -23.97 after sell → book $10,414.59; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 15 | $108.55 | $2.04 | — | $8,784.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1735.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $7,596.00 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1735.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 8 | $209.52 | $2.01 | — | $5,917.83 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1735.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 7 | $219.62 | $2.01 | — | $4,378.48 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1735.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 20 | $85.00 | $2.05 | — | $2,676.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1735.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 50 | $34.44 | $2.14 | — | $952.29 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1735.76 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $952.29 | ▼ close $10,230.12 vs 09:30 $10,428.34 (session -172.22) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $952.29 | ▲ 09:30 equity $10,337.73 vs yday $10,230.12 (+107.61) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 15 | $107.57 | $2.06 | $-18.79 | $2,563.78 | ▼ -18.79 after sell → book $10,335.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 2 | $586.77 | $2.02 | $-16.77 | $3,735.31 | ▼ -16.77 after sell → book $10,333.66; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 8 | $210.00 | $2.04 | $-0.21 | $5,413.27 | ▼ -0.21 after sell → book $10,331.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 7 | $230.25 | $2.03 | $+70.37 | $7,022.98 | ▲ +70.37 after sell → book $10,329.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 20 | $82.83 | $2.07 | $-47.52 | $8,677.51 | ▼ -47.52 after sell → book $10,327.51; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 50 | $33.00 | $2.16 | $-76.30 | $10,325.35 | ▼ -76.30 after sell → book $10,325.35; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 9 | $157.87 | $2.02 | — | $8,902.50 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $1475.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $7,741.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; leftover $1475.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 16 | $88.83 | $2.04 | — | $6,318.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $1475.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 188 | $7.84 | $2.55 | — | $4,842.11 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.6; leftover $1475.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 141 | $10.43 | $2.41 | — | $3,369.07 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+19.2; leftover $1475.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 109 | $13.47 | $2.32 | — | $1,898.52 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $1475.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 368 | $4.00 | $4.75 | — | $421.77 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $1475.05 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $421.77 | ▼ close $10,003.00 vs 09:30 $10,337.73 (session -304.26) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $421.77 | ▲ 09:30 equity $10,042.07 vs yday $10,003.00 (+39.07) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 141 | $10.18 | $2.45 | $-40.11 | $1,854.71 | ▼ -40.11 after sell → book $10,039.63; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 368 | $3.51 | $4.82 | $-189.89 | $3,141.57 | ▼ -189.89 after sell → book $10,034.81; vs 09:30 mark -4.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 5 | $93.97 | $2.00 | — | $2,669.71 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $523.59 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,669.71 | ▼ close $10,032.60 vs 09:30 $10,042.07 (session -0.20) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,669.71 | ▼ 09:30 equity $10,011.36 vs yday $10,032.60 (-21.24) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $3,777.69 | ▼ -52.62 after sell → book $10,009.34; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 109 | $12.26 | $2.35 | $-136.55 | $5,111.69 | ▼ -136.55 after sell → book $10,007.00; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 5 | $93.97 | $2.02 | $-4.03 | $5,579.51 | ▼ -4.03 after sell → book $10,004.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $4,525.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $1115.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 40 | $27.79 | $2.11 | — | $3,412.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $1115.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 113 | $9.81 | $2.33 | — | $2,301.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $1115.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 55 | $20.25 | $2.15 | — | $1,185.37 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $1115.90 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 54 | $20.65 | $2.15 | — | $68.12 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $1115.90 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.12 | ▼ close $9,715.22 vs 09:30 $10,011.36 (session -278.99) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.12 | ▼ 09:30 equity $9,660.48 vs yday $9,715.22 (-54.74) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 9 | $163.95 | $2.04 | $+50.66 | $1,541.63 | ▲ +50.66 after sell → book $9,658.44; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 16 | $87.67 | $2.06 | $-22.58 | $2,942.37 | ▼ -22.58 after sell → book $9,656.38; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 188 | $7.38 | $2.60 | $-91.63 | $4,327.21 | ▼ -91.63 after sell → book $9,653.78; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 9 | $112.22 | $2.04 | $-45.72 | $5,335.16 | ▼ -45.72 after sell → book $9,651.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 40 | $26.22 | $2.13 | $-67.04 | $6,381.83 | ▼ -67.04 after sell → book $9,649.62; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 113 | $9.67 | $2.36 | $-20.51 | $7,472.18 | ▼ -20.51 after sell → book $9,647.26; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 55 | $19.40 | $2.17 | $-51.08 | $8,537.00 | ▼ -51.08 after sell → book $9,645.08; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 54 | $20.52 | $2.17 | $-11.34 | $9,642.91 | ▼ -11.34 after sell → book $9,642.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,642.91 | ▲ close $9,642.91 vs 09:30 $9,660.48 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,663.08 | ▲ 09:30 equity $8,663.08 vs yday $8,663.08 (+0.00) | 09:30 open · cash $8,663.08 · no holdings · equity $8,663.08 vs prior close $8,663.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 2 | $803.87 | $2.00 | — | $7,053.34 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.8; leftover $1732.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 15 | $115.36 | $2.04 | — | $5,320.91 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.1; leftover $1732.62 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 84 | $20.61 | $2.24 | — | $3,587.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.1; leftover $1732.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 44 | $38.51 | $2.12 | — | $1,890.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $1732.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 226 | $7.65 | $2.92 | — | $159.05 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $1732.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.05 | ▼ close $8,541.51 vs 09:30 $8,663.08 (session -110.26) | 16:00 close · cash $159.05 · equity $8,541.51 vs 09:30 $8,663.08 (-121.57; session marks -110.26) · 5 name(s) marked open→close (per-name table). REGN×2 09:30 $803.87 → close $788.04 -31.66; HALO×15 09:30 $115.36 → close $113.90 -21.90; OMER×84 09:30 $20.61 → close $20.08 -44.52; BLFS×44 09:30 $38.51 → close $38.49 -0.88; MRVI×226 09:30 $7.65 → close $7.60 -11.30 | — |

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
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
