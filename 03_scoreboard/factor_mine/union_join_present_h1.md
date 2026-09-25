# Factor mine action — `union_join_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_present, no 🚨

Cash book **-10.75%** ($8,925) · signal-only (no cash/fees) was -0.36%. Starts YES **0/30**. Fills 250 · skips 106 · realized $-101.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera printed something (any color, not blank).
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
- **Gate** `join_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,898.20.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | — |
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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-7.2; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 250 | $5.07 | $3.23 | — | $189.31 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-4.7; leftover $1269.49 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,780.34 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,595.19 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,354.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 217 | $5.77 | $2.80 | — | $5,099.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,860.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,613.68 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 716 | $1.75 | $9.24 | — | $1,351.45 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1254.51 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $193.11 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1254.51 | — |
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
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,291.52 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,982.10 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,682.29 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,377.74 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $4,061.80 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 679 | $1.93 | $8.76 | — | $2,742.57 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,486.40 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1310.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 993 | $1.32 | $12.81 | — | $162.83 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1310.98 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,668.12 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+13.0; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.98 | $2.37 | — | $8,293.25 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+1.2; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $6,945.01 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+7.4; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 165 | $8.35 | $2.48 | — | $5,564.78 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 279 | $4.94 | $3.60 | — | $4,182.92 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+7.1; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,900.01 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+6.0; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 190 | $7.25 | $2.56 | — | $1,519.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1381.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3857 | $0.36 | $25.38 | — | $113.77 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; ret5=-15.6; leftover $1381.12 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.77 | ▲ close $11,261.91 vs 09:30 $11,048.94 (session +255.58) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.77 | ▼ 09:30 equity $11,260.69 vs yday $11,261.91 (-1.22) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 190 | $8.29 | $2.60 | $+192.44 | $1,686.26 | ▲ +192.44 after sell → book $11,258.08; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3857 | $0.35 | $25.84 | $-70.50 | $3,021.95 | ▼ -70.50 after sell → book $11,232.25; vs 09:30 mark -25.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 48 | $31.21 | $2.13 | — | $1,521.73 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1510.97 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 135 | $11.12 | $2.40 | — | $18.14 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1510.97 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.14 | ▼ close $11,187.78 vs 09:30 $11,260.69 (session -39.94) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.14 | ▲ 09:30 equity $11,227.42 vs yday $11,187.78 (+39.64) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 125 | $10.63 | $2.40 | $-48.51 | $1,344.49 | ▼ -48.51 after sell → book $11,225.02; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,708.61 | ▲ +15.89 after sell → book $11,222.94; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 165 | $8.49 | $2.52 | $+18.09 | $4,106.94 | ▲ +18.09 after sell → book $11,220.42; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 279 | $5.07 | $3.66 | $+29.01 | $5,517.81 | ▲ +29.01 after sell → book $11,216.76; vs 09:30 mark -3.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,789.62 | ▼ -11.10 after sell → book $11,214.74; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $5,461.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+3.1; leftover $1357.92 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 94 | $14.42 | $2.27 | — | $4,103.71 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+7.1; leftover $1357.92 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 522 | $2.60 | $6.73 | — | $2,739.77 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; ret5=+13.0; leftover $1357.92 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 104 | $12.98 | $2.30 | — | $1,387.55 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1357.92 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 140 | $9.68 | $2.41 | — | $29.94 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1357.92 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.94 | ▲ close $11,270.83 vs 09:30 $11,227.42 (session +71.89) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.94 | ▼ 09:30 equity $11,256.19 vs yday $11,270.83 (-14.64) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 48 | $30.53 | $2.16 | $-36.93 | $1,493.22 | ▼ -36.93 after sell → book $11,254.03; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 135 | $11.27 | $2.43 | $+15.43 | $3,012.24 | ▲ +15.43 after sell → book $11,251.60; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 104 | $13.05 | $2.33 | $+2.65 | $4,367.11 | ▲ +2.65 after sell → book $11,249.27; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 140 | $9.88 | $2.44 | $+23.15 | $5,747.87 | ▲ +23.15 after sell → book $11,246.83; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $4,331.05 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1436.97 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 91 | $15.66 | $2.26 | — | $2,903.73 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1436.97 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 18 | $79.42 | $2.04 | — | $1,472.12 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1436.97 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $208.92 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1436.97 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.92 | ▼ close $10,929.41 vs 09:30 $11,256.19 (session -308.99) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.92 | ▲ 09:30 equity $10,976.54 vs yday $10,929.41 (+47.13) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-9.57 | $1,580.17 | ▼ -9.57 after sell → book $10,974.35; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,922.07 | ▲ +13.73 after sell → book $10,972.25; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 94 | $14.54 | $2.30 | $+6.71 | $4,286.53 | ▲ +6.71 after sell → book $10,969.95; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 522 | $2.58 | $6.83 | $-24.01 | $5,626.46 | ▼ -24.01 after sell → book $10,963.12; vs 09:30 mark -6.83 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 43 | $31.15 | $2.14 | $-79.51 | $6,963.77 | ▼ -79.51 after sell → book $10,960.98; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 91 | $14.44 | $2.29 | $-115.57 | $8,275.52 | ▼ -115.57 after sell → book $10,958.69; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 18 | $80.44 | $2.07 | $+14.25 | $9,721.37 | ▲ +14.25 after sell → book $10,956.62; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $10,954.60 | ▼ -29.98 after sell → book $10,954.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,954.60 | ▲ close $10,954.60 vs 09:30 $10,976.54 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,954.60 | ▲ 09:30 equity $10,954.60 vs yday $10,954.60 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,954.60 | ▲ close $10,954.60 vs 09:30 $10,954.60 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,954.60 | ▲ 09:30 equity $10,954.60 vs yday $10,954.60 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,954.60 | ▲ close $10,954.60 vs 09:30 $10,954.60 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,954.60 | ▲ 09:30 equity $10,954.60 vs yday $10,954.60 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,630.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,297.62 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 377 | $3.63 | $4.86 | — | $6,924.25 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 170 | $8.03 | $2.50 | — | $5,556.65 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,230.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,868.27 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,552.75 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1369.32 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $192.15 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1369.32 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.15 | ▼ close $10,691.96 vs 09:30 $10,954.60 (session -242.60) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.15 | ▲ 09:30 equity $10,695.81 vs yday $10,691.96 (+3.85) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,476.55 | ▼ -48.52 after sell → book $10,693.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 170 | $7.91 | $2.54 | $-25.44 | $2,818.71 | ▼ -25.44 after sell → book $10,691.17; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,116.97 | ▼ -28.26 after sell → book $10,689.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,434.69 | ▼ -44.13 after sell → book $10,686.85; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,815.23 | ▲ +65.02 after sell → book $10,684.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,077.38 | ▼ -98.45 after sell → book $10,682.55; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 534 | $2.52 | $6.89 | — | $6,724.81 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1346.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 200 | $6.71 | $2.59 | — | $5,380.22 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1346.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 708 | $1.90 | $9.13 | — | $4,025.89 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1346.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 281 | $4.78 | $3.62 | — | $2,679.09 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1346.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 846 | $1.59 | $10.91 | — | $1,323.03 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1346.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 116 | $11.31 | $2.34 | — | $8.73 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1346.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.73 | ▼ close $10,592.63 vs 09:30 $10,695.81 (session -54.43) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.73 | ▼ 09:30 equity $10,549.31 vs yday $10,592.63 (-43.32) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,364.40 | ▲ +31.60 after sell → book $10,547.23; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 377 | $3.43 | $4.94 | $-85.20 | $2,652.57 | ▼ -85.20 after sell → book $10,542.29; vs 09:30 mark -4.94 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 534 | $2.38 | $6.99 | $-88.64 | $3,916.50 | ▼ -88.64 after sell → book $10,535.30; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 200 | $6.57 | $2.63 | $-33.22 | $5,227.87 | ▼ -33.22 after sell → book $10,532.67; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 708 | $2.00 | $9.26 | $+52.40 | $6,634.61 | ▲ +52.40 after sell → book $10,523.41; vs 09:30 mark -9.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 281 | $4.30 | $3.68 | $-142.19 | $7,839.23 | ▼ -142.19 after sell → book $10,519.73; vs 09:30 mark -3.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 846 | $1.63 | $11.06 | $+11.86 | $9,207.14 | ▲ +11.86 after sell → book $10,508.66; vs 09:30 mark -11.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 116 | $11.22 | $2.37 | $-15.15 | $10,506.29 | ▼ -15.15 after sell → book $10,506.29; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,506.29 | ▲ close $10,506.29 vs 09:30 $10,549.31 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,506.29 | ▲ 09:30 equity $10,506.29 vs yday $10,506.29 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,506.29 | ▲ close $10,506.29 vs 09:30 $10,506.29 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,506.29 | ▲ 09:30 equity $10,506.29 vs yday $10,506.29 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,506.29 | ▲ close $10,506.29 vs 09:30 $10,506.29 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,506.29 | ▲ 09:30 equity $10,506.29 vs yday $10,506.29 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,201.66 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=-1.1; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 481 | $2.73 | $6.20 | — | $7,882.33 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=-3.0; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,639.28 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+8.3; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $5,486.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,222.01 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+4.7; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,929.88 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+19.6; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 643 | $2.04 | $8.29 | — | $1,609.86 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1313.29 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 276 | $4.75 | $3.56 | — | $295.30 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1313.29 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.30 | ▼ close $10,459.78 vs 09:30 $10,506.29 (session -18.13) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.30 | ▼ 09:30 equity $10,155.84 vs yday $10,459.78 (-303.94) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 80 | $16.03 | $2.25 | $-24.48 | $1,575.45 | ▼ -24.48 after sell → book $10,153.58; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 481 | $2.75 | $6.30 | $-0.48 | $2,894.31 | ▼ -0.48 after sell → book $10,147.29; vs 09:30 mark -6.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $4,131.28 | ▼ -6.08 after sell → book $10,145.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $5,119.19 | ▼ -165.11 after sell → book $10,143.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $6,318.40 | ▼ -92.92 after sell → book $10,141.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 643 | $2.01 | $8.41 | $-36.00 | $7,602.42 | ▼ -36.00 after sell → book $10,132.74; vs 09:30 mark -8.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 276 | $4.82 | $3.62 | $+12.14 | $8,929.12 | ▲ +12.14 after sell → book $10,129.12; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,929.12 | ▼ close $10,102.24 vs 09:30 $10,155.84 (session -26.88) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,929.12 | ▲ 09:30 equity $10,138.08 vs yday $10,102.24 (+35.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $10,136.05 | ▼ -57.33 after sell → book $10,136.05; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,136.05 | ▲ close $10,136.05 vs 09:30 $10,138.08 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,136.05 | ▲ 09:30 equity $10,136.05 vs yday $10,136.05 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $9,050.49 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+4.0; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $7,814.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 88 | $14.31 | $2.25 | — | $6,552.99 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+4.8; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 34 | $36.46 | $2.09 | — | $5,311.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+2.9; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 68 | $18.61 | $2.19 | — | $4,043.59 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 69 | $18.21 | $2.20 | — | $2,784.90 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; ret5=-19.1; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 18 | $68.79 | $2.04 | — | $1,544.64 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1267.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 215 | $5.87 | $2.77 | — | $279.81 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1267.01 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.81 | ▲ close $10,320.68 vs 09:30 $10,136.05 (session +202.23) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.81 | ▲ 09:30 equity $10,490.56 vs yday $10,320.68 (+169.88) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 4 | $273.15 | $2.02 | $+5.02 | $1,370.39 | ▲ +5.02 after sell → book $10,488.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $2,591.37 | ▼ -14.98 after sell → book $10,486.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 88 | $14.33 | $2.28 | $-2.77 | $3,850.13 | ▼ -2.77 after sell → book $10,484.20; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 34 | $36.67 | $2.11 | $+2.94 | $5,094.80 | ▲ +2.94 after sell → book $10,482.09; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 68 | $22.46 | $2.22 | $+257.39 | $6,619.87 | ▲ +257.39 after sell → book $10,479.88; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 69 | $19.59 | $2.22 | $+90.80 | $7,969.36 | ▲ +90.80 after sell → book $10,477.66; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 18 | $72.70 | $2.06 | $+66.27 | $9,275.89 | ▲ +66.27 after sell → book $10,475.59; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 215 | $5.58 | $2.82 | $-67.94 | $10,472.77 | ▼ -67.94 after sell → book $10,472.77; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $9,301.52 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 8 | $151.43 | $2.01 | — | $8,088.06 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,905.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 127 | $10.25 | $2.37 | — | $5,601.05 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 172 | $7.59 | $2.51 | — | $4,293.06 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 37 | $34.93 | $2.10 | — | $2,998.55 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+1.6; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 7700 | $0.17 | $36.19 | — | $1,653.36 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1309.10 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 82 | $15.87 | $2.24 | — | $349.79 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1309.10 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.79 | ▲ close $10,498.30 vs 09:30 $10,490.56 (session +76.96) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.79 | ▲ 09:30 equity $10,705.88 vs yday $10,498.30 (+207.58) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $1,593.41 | ▲ +72.37 after sell → book $10,703.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 8 | $158.04 | $2.03 | $+48.83 | $2,855.70 | ▲ +48.83 after sell → book $10,701.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $4,025.66 | ▼ -12.93 after sell → book $10,699.78; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 127 | $10.12 | $2.40 | $-21.28 | $5,308.50 | ▼ -21.28 after sell → book $10,697.38; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 172 | $7.98 | $2.55 | $+62.03 | $6,678.51 | ▲ +62.03 after sell → book $10,694.83; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 37 | $34.52 | $2.12 | $-19.39 | $7,953.63 | ▼ -19.39 after sell → book $10,692.71; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 7700 | $0.17 | $37.48 | $-73.67 | $9,225.15 | ▼ -73.67 after sell → book $10,655.23; vs 09:30 mark -37.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 82 | $17.44 | $2.26 | $+124.24 | $10,652.97 | ▲ +124.24 after sell → book $10,652.97; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 12 | $108.55 | $2.03 | — | $9,348.35 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+21.3; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $8,089.22 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $6,769.49 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $5,492.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 38 | $34.44 | $2.10 | — | $4,181.63 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1372 | $0.97 | $17.42 | — | $2,833.37 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 337 | $3.95 | $4.35 | — | $1,497.87 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1331.62 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 94 | $14.07 | $2.27 | — | $173.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1331.62 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.02 | ▼ close $10,368.86 vs 09:30 $10,705.88 (session -249.89) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.02 | ▲ 09:30 equity $10,502.28 vs yday $10,368.86 (+133.42) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 12 | $107.57 | $2.05 | $-15.83 | $1,461.81 | ▼ -15.83 after sell → book $10,500.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $2,719.78 | ▼ -1.16 after sell → book $10,498.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $4,099.25 | ▲ +59.74 after sell → book $10,496.17; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $5,339.65 | ▼ -36.64 after sell → book $10,494.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 38 | $33.00 | $2.12 | $-58.95 | $6,591.53 | ▼ -58.95 after sell → book $10,492.00; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1372 | $0.94 | $17.25 | $-75.84 | $7,863.95 | ▼ -75.84 after sell → book $10,474.74; vs 09:30 mark -17.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 337 | $3.87 | $4.41 | $-35.72 | $9,163.73 | ▼ -35.72 after sell → book $10,470.33; vs 09:30 mark -4.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 94 | $13.90 | $2.30 | $-20.55 | $10,468.03 | ▼ -20.55 after sell → book $10,468.03; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,203.06 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+6.5; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $8,042.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-5.8; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $6,796.81 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+7.6; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 140 | $9.31 | $2.41 | — | $5,491.00 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 97 | $13.47 | $2.28 | — | $4,181.64 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1178 | $1.11 | $15.20 | — | $2,858.87 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 130 | $9.99 | $2.38 | — | $1,557.79 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1308.50 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 716 | $1.82 | $9.24 | — | $241.85 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1308.50 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.85 | ▼ close $10,313.09 vs 09:30 $10,502.28 (session -117.39) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $241.85 | ▼ 09:30 equity $10,286.09 vs yday $10,313.09 (-27.00) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 1178 | $1.05 | $15.40 | $-101.28 | $1,463.35 | ▼ -101.28 after sell → book $10,270.69; vs 09:30 mark -15.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 130 | $9.91 | $2.41 | $-15.19 | $2,749.24 | ▼ -15.19 after sell → book $10,268.28; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 716 | $1.79 | $9.37 | $-40.08 | $4,025.09 | ▼ -40.08 after sell → book $10,258.91; vs 09:30 mark -9.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 5 | $93.97 | $2.00 | — | $3,553.24 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-0.6; leftover $503.14 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 867 | $0.58 | $7.63 | — | $3,042.75 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $503.14 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,042.75 | ▼ close $10,205.73 vs 09:30 $10,286.09 (session -43.55) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,042.75 | ▲ 09:30 equity $10,281.92 vs yday $10,205.73 (+76.19) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 3 | $370.00 | $2.02 | $-52.62 | $4,150.73 | ▼ -52.62 after sell → book $10,279.90; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 140 | $9.50 | $2.44 | $+21.75 | $5,478.28 | ▲ +21.75 after sell → book $10,277.46; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 97 | $12.84 | $2.31 | $-66.18 | $6,721.46 | ▼ -66.18 after sell → book $10,275.15; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 5 | $93.97 | $2.02 | $-4.03 | $7,189.28 | ▼ -4.03 after sell → book $10,273.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 867 | $0.57 | $7.74 | $-19.70 | $7,680.07 | ▼ -19.70 after sell → book $10,265.39; vs 09:30 mark -7.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,509.55 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1280.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 46 | $27.79 | $2.13 | — | $5,229.08 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1280.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 161 | $7.95 | $2.47 | — | $3,946.65 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1280.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 130 | $9.81 | $2.38 | — | $2,668.97 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1280.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 63 | $20.25 | $2.18 | — | $1,391.05 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1280.01 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 61 | $20.65 | $2.17 | — | $129.22 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1280.01 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.22 | ▼ close $9,971.62 vs 09:30 $10,281.92 (session -280.41) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.22 | ▼ 09:30 equity $9,915.79 vs yday $9,971.62 (-55.83) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,438.79 | ▲ +44.59 after sell → book $9,913.76; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,664.19 | ▼ -20.25 after sell → book $9,911.71; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 10 | $112.22 | $2.04 | $-50.36 | $3,784.35 | ▼ -50.36 after sell → book $9,909.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 46 | $26.22 | $2.15 | $-76.50 | $4,988.32 | ▼ -76.50 after sell → book $9,907.52; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 161 | $7.38 | $2.51 | $-96.75 | $6,173.99 | ▼ -96.75 after sell → book $9,905.01; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 130 | $9.67 | $2.41 | $-22.99 | $7,428.68 | ▼ -22.99 after sell → book $9,902.60; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 63 | $19.40 | $2.20 | $-57.93 | $8,648.68 | ▼ -57.93 after sell → book $9,900.40; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 61 | $20.52 | $2.19 | $-12.30 | $9,898.20 | ▼ -12.30 after sell → book $9,898.20; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,898.20 | ▲ close $9,898.20 vs 09:30 $9,915.79 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,925.18 | ▲ 09:30 equity $8,925.18 vs yday $8,925.18 (+0.00) | 09:30 open · cash $8,925.18 · no holdings · equity $8,925.18 vs prior close $8,925.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $8,119.32 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+0.8; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,079.06 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 54 | $20.61 | $2.15 | — | $5,963.97 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+9.1; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 28 | $38.51 | $2.07 | — | $4,883.61 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+4.7; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 145 | $7.65 | $2.42 | — | $3,771.94 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 42 | $26.27 | $2.12 | — | $2,666.48 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 13 | $83.76 | $2.03 | — | $1,575.57 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1115.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 123 | $9.05 | $2.36 | — | $460.06 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer; ret5=-27.1; leftover $1115.65 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $460.06 | ▲ close $8,924.57 vs 09:30 $8,925.18 (session +16.56) | 16:00 close · cash $460.06 · equity $8,924.57 vs 09:30 $8,925.18 (-0.61; session marks +16.56) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×54 09:30 $20.61 → close $20.08 -28.62; BLFS×28 09:30 $38.51 → close $38.49 -0.56; MRVI×145 09:30 $7.65 → close $7.60 -7.25; WRBY×42 09:30 $26.27 → close $26.71 +18.48; TXG×13 09:30 $83.76 → close $85.71 +25.35; AEHL×123 09:30 $9.05 → close $9.36 +38.13 | — |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
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
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
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
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
