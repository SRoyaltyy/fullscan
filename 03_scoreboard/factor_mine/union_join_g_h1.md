# Factor mine action — `union_join_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_g, no 🚨

Cash book **-12.04%** ($8,796) · signal-only (no cash/fees) was -3.42%. Starts YES **0/30**. Fills 262 · skips 104 · realized $-96.65.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera (do several factors agree?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
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
- **Gate** `join=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,903.34.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | — |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-7.2; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 250 | $5.07 | $3.23 | — | $189.31 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-4.7; leftover $1269.49 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.31 | ▲ close $10,137.18 vs 09:30 $10,196.20 (session +4.10) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.31 | ▼ 09:30 equity $10,059.24 vs yday $10,137.18 (-77.94) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,483.22 | ▲ +44.98 after sell → book $10,057.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.51 | ▲ +38.11 after sell → book $10,055.12; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,917.06 | ▲ +33.34 after sell → book $10,053.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,077.32 | ▼ -111.43 after sell → book $10,048.99; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,357.35 | ▲ +8.58 after sell → book $10,046.52; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.45 | ▲ +36.52 after sell → book $10,044.46; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,874.34 | ▼ -60.99 after sell → book $10,039.34; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 250 | $4.66 | $3.28 | $-109.00 | $10,036.07 | ▼ -109.00 after sell → book $10,036.07; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,059.24 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,036.07 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,780.34 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,595.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,354.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 217 | $5.77 | $2.80 | — | $5,099.13 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,860.26 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,613.68 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 716 | $1.75 | $9.24 | — | $1,351.45 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $193.11 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1254.51 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.11 | ▲ close $10,244.74 vs 09:30 $10,036.07 (session +233.39) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.11 | ▲ 09:30 equity $10,512.85 vs yday $10,244.74 (+268.11) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,526.82 | ▲ +77.98 after sell → book $10,510.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,769.13 | ▲ +57.15 after sell → book $10,508.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,071.94 | ▲ +61.64 after sell → book $10,506.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 217 | $5.67 | $2.85 | $-27.34 | $5,299.48 | ▼ -27.34 after sell → book $10,503.57; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,630.99 | ▲ +92.64 after sell → book $10,501.37; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,980.00 | ▲ +102.43 after sell → book $10,499.24; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 716 | $1.79 | $9.37 | $+10.04 | $9,252.27 | ▲ +10.04 after sell → book $10,489.87; vs 09:30 mark -9.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,487.84 | ▲ +77.23 after sell → book $10,487.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,291.52 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,982.10 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,682.29 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,377.74 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $4,061.80 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 679 | $1.93 | $8.76 | — | $2,742.57 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,486.40 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 993 | $1.32 | $12.81 | — | $162.83 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1310.98 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.83 | ▲ close $10,710.24 vs 09:30 $10,512.85 (session +261.45) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.83 | ▲ 09:30 equity $11,088.50 vs yday $10,710.24 (+378.26) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,365.89 | ▲ +6.74 after sell → book $11,086.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,622.97 | ▼ -52.34 after sell → book $11,084.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,923.12 | ▲ +0.34 after sell → book $11,082.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.33 | $2.37 | $+252.69 | $5,480.36 | ▲ +252.69 after sell → book $11,079.82; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 530 | $2.40 | $6.94 | $-50.87 | $6,745.42 | ▼ -50.87 after sell → book $11,072.88; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 679 | $1.88 | $8.88 | $-51.59 | $8,013.06 | ▼ -51.59 after sell → book $11,064.00; vs 09:30 mark -8.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,244.74 | ▼ -24.50 after sell → book $11,061.93; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 993 | $1.83 | $12.99 | $+480.63 | $11,048.94 | ▲ +480.63 after sell → book $11,048.94; vs 09:30 mark -12.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,048.94 | ▲ close $11,048.94 vs 09:30 $11,088.50 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,048.94 | ▲ 09:30 equity $11,048.94 vs yday $11,048.94 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,668.12 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.0; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.98 | $2.37 | — | $8,293.25 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+1.2; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,945.01 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+7.4; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 165 | $8.35 | $2.48 | — | $5,564.78 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 279 | $4.94 | $3.60 | — | $4,182.92 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.1; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,900.01 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.0; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 847 | $1.63 | $10.93 | — | $1,508.47 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 885 | $1.56 | $11.42 | — | $116.46 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1381.12 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.46 | ▲ close $11,240.84 vs 09:30 $11,048.94 (session +228.91) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.46 | ▼ 09:30 equity $11,229.24 vs yday $11,240.84 (-11.60) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `OCUL` | 125 | $10.79 | $2.40 | $-28.51 | $1,462.81 | ▼ -28.51 after sell → book $11,226.85; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-28.77 | $2,782.28 | ▼ -28.77 after sell → book $11,224.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 165 | $8.60 | $2.52 | $+36.24 | $4,198.75 | ▲ +36.24 after sell → book $11,222.25; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RZLT` | 279 | $5.01 | $3.66 | $+12.27 | $5,592.88 | ▲ +12.27 after sell → book $11,218.59; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $6,873.37 | ▼ -2.43 after sell → book $11,216.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 847 | $1.75 | $11.08 | $+83.87 | $8,348.77 | ▲ +83.87 after sell → book $11,205.49; vs 09:30 mark -11.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 885 | $1.60 | $11.57 | $+12.41 | $9,753.20 | ▲ +12.41 after sell → book $11,193.92; vs 09:30 mark -11.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 141 | $9.83 | $2.41 | — | $8,364.75 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1393.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 79 | $17.51 | $2.23 | — | $6,979.24 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1393.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 6 | $213.94 | $2.01 | — | $5,693.59 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1393.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 83 | $16.77 | $2.24 | — | $4,299.44 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1393.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 50 | $27.59 | $2.14 | — | $2,917.80 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; ret5=+2.0; leftover $1393.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 633 | $2.20 | $8.17 | — | $1,517.03 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=+17.8; leftover $1393.31 | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2389 | $0.58 | $21.09 | — | $103.15 | — | union ∩ join_g, no 🚨; gate join=good; list yday_mover; 🔵; ret5=-27.5; leftover $1393.31 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.15 | ▲ close $11,318.31 vs 09:30 $11,229.24 (session +164.68) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.15 | ▼ 09:30 equity $11,304.54 vs yday $11,318.31 (-13.77) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 79 | $18.43 | $2.25 | $+68.20 | $1,556.87 | ▲ +68.20 after sell → book $11,302.29; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 633 | $2.19 | $8.28 | $-22.78 | $2,934.86 | ▼ -22.78 after sell → book $11,294.01; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2389 | $0.53 | $20.24 | $-167.95 | $4,180.79 | ▼ -167.95 after sell → book $11,273.77; vs 09:30 mark -20.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $2,811.18 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+3.1; leftover $1393.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.42 | $2.28 | — | $1,424.59 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.1; leftover $1393.60 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 535 | $2.60 | $6.90 | — | $26.68 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ret5=+13.0; leftover $1393.60 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.68 | ▼ close $11,140.31 vs 09:30 $11,304.54 (session -122.19) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.68 | ▲ 09:30 equity $11,193.24 vs yday $11,140.31 (+52.93) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 58 | $23.95 | $2.19 | $+6.09 | $1,413.60 | ▲ +6.09 after sell → book $11,191.06; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 141 | $9.88 | $2.45 | $+2.19 | $2,804.23 | ▲ +2.19 after sell → book $11,188.61; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BE` | 6 | $215.71 | $2.03 | $+6.55 | $4,096.43 | ▲ +6.55 after sell → book $11,186.58; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MAIR` | 50 | $27.36 | $2.16 | $-15.80 | $5,462.27 | ▼ -15.80 after sell → book $11,184.42; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 33 | $41.74 | $2.11 | $+5.70 | $6,837.58 | ▲ +5.70 after sell → book $11,182.31; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 96 | $14.63 | $2.31 | $+15.58 | $8,239.76 | ▲ +15.58 after sell → book $11,180.01; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 535 | $2.68 | $7.00 | $+28.90 | $9,666.56 | ▲ +28.90 after sell → book $11,173.01; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $8,315.54 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1380.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 88 | $15.66 | $2.25 | — | $6,935.21 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1380.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $5,583.03 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1380.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $4,319.82 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1380.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $3,003.18 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1380.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,725.32 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1380.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 151 | $9.13 | $2.44 | — | $344.25 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=+20.0; leftover $1380.94 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $344.25 | ▼ close $10,827.21 vs 09:30 $11,193.24 (session -330.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $344.25 | ▼ 09:30 equity $10,794.58 vs yday $10,827.21 (-32.63) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 83 | $17.70 | $2.26 | $+72.69 | $1,811.08 | ▲ +72.69 after sell → book $10,792.31; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $3,086.10 | ▼ -76.00 after sell → book $10,790.18; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 88 | $14.44 | $2.28 | $-111.89 | $4,354.54 | ▼ -111.89 after sell → book $10,787.90; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $5,719.96 | ▲ +13.24 after sell → book $10,785.84; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $6,953.18 | ▼ -29.98 after sell → book $10,783.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $8,283.41 | ▲ +13.59 after sell → book $10,781.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $9,472.08 | ▼ -89.19 after sell → book $10,779.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 151 | $8.66 | $2.48 | $-75.89 | $10,777.26 | ▼ -75.89 after sell → book $10,777.26; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,777.26 | ▲ close $10,777.26 vs 09:30 $10,794.58 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,777.26 | ▲ 09:30 equity $10,777.26 vs yday $10,777.26 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,777.26 | ▲ close $10,777.26 vs 09:30 $10,777.26 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,777.26 | ▲ 09:30 equity $10,777.26 vs yday $10,777.26 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,777.26 | ▲ close $10,777.26 vs 09:30 $10,777.26 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,777.26 | ▲ 09:30 equity $10,777.26 vs yday $10,777.26 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,453.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,120.28 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 371 | $3.63 | $4.79 | — | $6,768.77 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 167 | $8.03 | $2.49 | — | $5,425.26 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,098.74 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.45 | $2.25 | — | $2,752.34 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,436.82 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1347.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.77 | $2.23 | — | $92.99 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1347.16 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.99 | ▼ close $10,517.47 vs 09:30 $10,777.26 (session -239.84) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.99 | ▲ 09:30 equity $10,521.55 vs yday $10,517.47 (+4.08) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,377.39 | ▼ -48.52 after sell → book $10,519.45; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 167 | $7.91 | $2.53 | $-25.06 | $2,695.83 | ▼ -25.06 after sell → book $10,516.92; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $3,994.09 | ▼ -28.26 after sell → book $10,514.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.00 | $2.28 | $-43.68 | $5,296.81 | ▼ -43.68 after sell → book $10,512.60; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,677.35 | ▲ +65.02 after sell → book $10,510.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 80 | $15.61 | $2.25 | $-97.28 | $7,923.90 | ▼ -97.28 after sell → book $10,508.31; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 524 | $2.52 | $6.76 | — | $6,596.66 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1320.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 196 | $6.71 | $2.58 | — | $5,278.92 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1320.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 695 | $1.90 | $8.97 | — | $3,949.46 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1320.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 276 | $4.78 | $3.56 | — | $2,626.62 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1320.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 830 | $1.59 | $10.71 | — | $1,296.21 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1320.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 114 | $11.31 | $2.33 | — | $4.54 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1320.65 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.54 | ▼ close $10,419.73 vs 09:30 $10,521.55 (session -53.68) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.54 | ▼ 09:30 equity $10,378.44 vs yday $10,419.73 (-41.29) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,360.20 | ▲ +31.60 after sell → book $10,376.35; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 371 | $3.43 | $4.86 | $-83.84 | $2,627.87 | ▼ -83.84 after sell → book $10,371.49; vs 09:30 mark -4.86 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 524 | $2.38 | $6.86 | $-86.98 | $3,868.14 | ▼ -86.98 after sell → book $10,364.64; vs 09:30 mark -6.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 196 | $6.57 | $2.62 | $-32.64 | $5,153.24 | ▼ -32.64 after sell → book $10,362.02; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 695 | $2.00 | $9.09 | $+51.44 | $6,534.15 | ▲ +51.44 after sell → book $10,352.93; vs 09:30 mark -9.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 276 | $4.30 | $3.62 | $-139.66 | $7,717.33 | ▼ -139.66 after sell → book $10,349.31; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 830 | $1.63 | $10.86 | $+11.64 | $9,059.37 | ▲ +11.64 after sell → book $10,338.45; vs 09:30 mark -10.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 114 | $11.22 | $2.36 | $-14.95 | $10,336.09 | ▼ -14.95 after sell → book $10,336.09; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,336.09 | ▲ close $10,336.09 vs 09:30 $10,378.44 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,336.09 | ▲ 09:30 equity $10,336.09 vs yday $10,336.09 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,336.09 | ▲ close $10,336.09 vs 09:30 $10,336.09 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,336.09 | ▲ 09:30 equity $10,336.09 vs yday $10,336.09 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,336.09 | ▲ close $10,336.09 vs 09:30 $10,336.09 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,336.09 | ▲ 09:30 equity $10,336.09 vs yday $10,336.09 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 79 | $16.28 | $2.23 | — | $9,047.75 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=-1.1; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 473 | $2.73 | $6.10 | — | $7,750.35 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=-3.0; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,507.31 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+8.3; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,354.28 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,090.03 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+4.7; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,797.90 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+19.6; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 272 | $4.75 | $3.51 | — | $1,502.39 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1292.01 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 54 | $23.63 | $2.15 | — | $224.22 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ret5=-6.3; leftover $1292.01 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.22 | ▼ close $10,228.99 vs 09:30 $10,336.09 (session -85.02) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.22 | ▼ 09:30 equity $9,987.78 vs yday $10,228.99 (-241.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 79 | $16.03 | $2.25 | $-24.23 | $1,488.34 | ▼ -24.23 after sell → book $9,985.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 473 | $2.75 | $6.19 | $-0.47 | $2,785.27 | ▼ -0.47 after sell → book $9,979.34; vs 09:30 mark -6.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $4,022.24 | ▼ -6.08 after sell → book $9,977.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $5,010.15 | ▼ -165.11 after sell → book $9,975.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $6,209.36 | ▼ -92.92 after sell → book $9,973.20; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 272 | $4.82 | $3.56 | $+11.97 | $7,516.83 | ▲ +11.97 after sell → book $9,969.63; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 54 | $23.20 | $2.17 | $-27.54 | $8,767.46 | ▼ -27.54 after sell → book $9,967.46; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,767.46 | ▼ close $9,940.58 vs 09:30 $9,987.78 (session -26.88) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,767.46 | ▲ 09:30 equity $9,976.42 vs yday $9,940.58 (+35.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $9,974.39 | ▼ -57.33 after sell → book $9,974.39; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,974.39 | ▲ close $9,974.39 vs 09:30 $9,976.42 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,974.39 | ▲ 09:30 equity $9,974.39 vs yday $9,974.39 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,888.82 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.0; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,652.87 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 87 | $14.31 | $2.25 | — | $6,405.65 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.8; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,163.91 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+2.9; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 66 | $18.61 | $2.19 | — | $3,933.47 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 68 | $18.21 | $2.19 | — | $2,692.99 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; ret5=-19.1; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $1,452.73 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1246.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 212 | $5.87 | $2.73 | — | $205.55 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1246.80 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.55 | ▲ close $10,152.17 vs 09:30 $9,974.39 (session +195.33) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.55 | ▲ 09:30 equity $10,320.72 vs yday $10,152.17 (+168.55) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,296.13 | ▲ +5.02 after sell → book $10,318.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,517.11 | ▼ -14.98 after sell → book $10,316.64; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 87 | $14.33 | $2.28 | $-2.79 | $3,761.55 | ▼ -2.79 after sell → book $10,314.37; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $5,006.22 | ▲ +2.94 after sell → book $10,312.26; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 66 | $22.46 | $2.21 | $+249.70 | $6,486.36 | ▲ +249.70 after sell → book $10,310.04; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 68 | $19.59 | $2.22 | $+89.43 | $7,816.27 | ▲ +89.43 after sell → book $10,307.83; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $9,122.80 | ▲ +66.27 after sell → book $10,305.76; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 212 | $5.58 | $2.78 | $-66.99 | $10,302.98 | ▼ -66.99 after sell → book $10,302.98; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $9,131.73 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $7,918.28 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,735.38 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ret5=+17.7; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 125 | $10.25 | $2.37 | — | $5,451.77 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 169 | $7.59 | $2.50 | — | $4,166.56 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 36 | $34.93 | $2.10 | — | $2,906.98 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+1.6; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 18 | $67.91 | $2.04 | — | $1,682.56 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $1287.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 49 | $25.95 | $2.14 | — | $408.87 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1287.87 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $408.87 | ▲ close $10,395.46 vs 09:30 $10,320.72 (session +109.65) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $408.87 | ▲ 09:30 equity $10,483.00 vs yday $10,395.46 (+87.54) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,652.50 | ▲ +72.37 after sell → book $10,480.98; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,914.78 | ▲ +48.83 after sell → book $10,478.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $4,084.75 | ▼ -12.93 after sell → book $10,476.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 125 | $10.12 | $2.40 | $-21.01 | $5,347.35 | ▼ -21.01 after sell → book $10,474.51; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 169 | $7.98 | $2.54 | $+60.88 | $6,693.44 | ▲ +60.88 after sell → book $10,471.98; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 36 | $34.52 | $2.12 | $-18.98 | $7,934.04 | ▼ -18.98 after sell → book $10,469.86; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 18 | $69.72 | $2.06 | $+28.47 | $9,186.93 | ▲ +28.47 after sell → book $10,467.79; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 49 | $26.14 | $2.16 | $+5.02 | $10,465.64 | ▲ +5.02 after sell → book $10,465.64; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 12 | $108.55 | $2.03 | — | $9,161.01 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+21.3; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $7,901.88 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $6,801.78 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $5,524.74 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 37 | $34.44 | $2.10 | — | $4,248.36 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 331 | $3.95 | $4.27 | — | $2,936.64 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 92 | $14.07 | $2.27 | — | $1,639.94 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1308.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 224 | $5.83 | $2.89 | — | $331.13 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1308.20 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $331.13 | ▼ close $10,312.44 vs 09:30 $10,483.00 (session -133.60) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $331.13 | ▲ 09:30 equity $10,493.40 vs yday $10,312.44 (+180.96) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 12 | $107.57 | $2.05 | $-15.83 | $1,619.92 | ▼ -15.83 after sell → book $10,491.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $2,877.89 | ▼ -1.16 after sell → book $10,489.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $4,027.12 | ▲ +49.12 after sell → book $10,487.30; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $5,267.51 | ▼ -36.64 after sell → book $10,485.24; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 37 | $33.00 | $2.12 | $-57.50 | $6,486.39 | ▼ -57.50 after sell → book $10,483.12; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 331 | $3.87 | $4.34 | $-35.09 | $7,763.03 | ▼ -35.09 after sell → book $10,478.79; vs 09:30 mark -4.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 92 | $13.90 | $2.29 | $-20.20 | $9,039.53 | ▼ -20.20 after sell → book $10,476.49; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 224 | $6.42 | $2.94 | $+125.21 | $10,473.56 | ▲ +125.21 after sell → book $10,473.56; vs 09:30 mark -2.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,208.58 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.5; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $8,047.98 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-5.8; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,802.33 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+7.6; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 140 | $9.31 | $2.41 | — | $5,496.52 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 97 | $13.47 | $2.28 | — | $4,187.16 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1179 | $1.11 | $15.21 | — | $2,863.27 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 131 | $9.99 | $2.38 | — | $1,552.19 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1309.19 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 717 | $1.82 | $9.25 | — | $234.42 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1309.19 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.42 | ▼ close $10,318.51 vs 09:30 $10,493.40 (session -117.46) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.42 | ▼ 09:30 equity $10,291.41 vs yday $10,318.51 (-27.10) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1179 | $1.05 | $15.41 | $-101.36 | $1,456.95 | ▼ -101.36 after sell → book $10,276.00; vs 09:30 mark -15.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 131 | $9.91 | $2.42 | $-15.28 | $2,752.75 | ▼ -15.28 after sell → book $10,273.58; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 717 | $1.79 | $9.38 | $-40.14 | $4,030.38 | ▼ -40.14 after sell → book $10,264.20; vs 09:30 mark -9.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 5 | $93.97 | $2.00 | — | $3,558.53 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-0.6; leftover $503.80 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 868 | $0.58 | $7.64 | — | $3,047.45 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $503.80 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,047.45 | ▼ close $10,210.96 vs 09:30 $10,291.41 (session -43.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,047.45 | ▲ 09:30 equity $10,287.20 vs yday $10,210.96 (+76.24) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $4,155.43 | ▼ -52.62 after sell → book $10,285.18; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 140 | $9.50 | $2.44 | $+21.75 | $5,482.99 | ▲ +21.75 after sell → book $10,282.74; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 97 | $12.84 | $2.31 | $-66.18 | $6,726.16 | ▼ -66.18 after sell → book $10,280.43; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 5 | $93.97 | $2.02 | $-4.03 | $7,193.99 | ▼ -4.03 after sell → book $10,278.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 868 | $0.57 | $7.75 | $-19.73 | $7,685.34 | ▼ -19.73 after sell → book $10,270.66; vs 09:30 mark -7.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,514.82 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1280.89 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 46 | $27.79 | $2.13 | — | $5,234.35 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1280.89 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 161 | $7.95 | $2.47 | — | $3,951.93 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1280.89 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 130 | $9.81 | $2.38 | — | $2,674.25 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1280.89 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 63 | $20.25 | $2.18 | — | $1,396.32 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1280.89 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 62 | $20.65 | $2.18 | — | $113.84 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1280.89 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.84 | ▼ close $9,976.98 vs 09:30 $10,287.20 (session -280.32) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.84 | ▼ 09:30 equity $9,920.93 vs yday $9,976.98 (-56.05) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,423.41 | ▲ +44.59 after sell → book $9,918.90; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,648.80 | ▼ -20.25 after sell → book $9,916.84; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $3,768.96 | ▼ -50.36 after sell → book $9,914.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 46 | $26.22 | $2.15 | $-76.50 | $4,972.94 | ▼ -76.50 after sell → book $9,912.66; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 161 | $7.38 | $2.51 | $-96.75 | $6,158.61 | ▼ -96.75 after sell → book $9,910.15; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 130 | $9.67 | $2.41 | $-22.99 | $7,413.30 | ▼ -22.99 after sell → book $9,907.74; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 63 | $19.40 | $2.20 | $-57.93 | $8,633.30 | ▼ -57.93 after sell → book $9,905.54; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 62 | $20.52 | $2.20 | $-12.43 | $9,903.34 | ▼ -12.43 after sell → book $9,903.34; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,903.34 | ▲ close $9,903.34 vs 09:30 $9,920.93 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,974.14 | ▲ 09:30 equity $8,974.14 vs yday $8,974.14 (+0.00) | 09:30 open · cash $8,974.14 · no holdings · equity $8,974.14 vs prior close $8,974.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $8,168.28 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+0.8; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,128.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 54 | $20.61 | $2.15 | — | $6,012.93 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+9.1; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 29 | $38.51 | $2.08 | — | $4,894.06 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.7; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 146 | $7.65 | $2.43 | — | $3,774.73 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 42 | $26.27 | $2.12 | — | $2,669.28 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $1,578.37 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 47 | $23.58 | $2.13 | — | $467.98 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $1121.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $467.98 | ▼ close $8,796.44 vs 09:30 $8,974.14 (session -160.76) | 16:00 close · cash $467.98 · equity $8,796.44 vs 09:30 $8,974.14 (-177.70; session marks -160.76) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×54 09:30 $20.61 → close $20.08 -28.62; BLFS×29 09:30 $38.51 → close $38.49 -0.58; MRVI×146 09:30 $7.65 → close $7.60 -7.30; WRBY×42 09:30 $26.27 → close $26.71 +18.48; TXG×13 09:30 $83.76 → close $85.71 +25.35; BRVE×47 09:30 $23.58 → close $20.62 -139.12 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CDW` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TDTH` | hard_red | hard-red S=-7.66 sit; no new buys |
