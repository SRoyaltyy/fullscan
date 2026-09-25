# Factor mine action — `flatten_white_yday_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · flatten looker: 0 red + yesterday up

Cash book **-5.14%** ($9,486) · signal-only (no cash/fees) was +1.97%. Starts YES **12/30**. Fills 55 · skips 119 · realized $-62.52.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: yesterday's session was up (prior close-to-close Change% > 0, or last finished bar green if the % is missing).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,yday_up=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $77.13.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $5,061.02 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $2,566.17 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $83.49 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.49 | ▲ close $10,279.02 vs 09:30 $10,000.00 (session +288.17) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.49 | ▼ 09:30 equity $10,260.41 vs yday $10,279.02 (-18.61) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $68.54 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $16.70 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 11 | $1.50 | $0.20 | — | $51.84 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $16.70 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 17 | $0.94 | $0.21 | — | $35.71 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $16.70 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▲ close $10,311.91 vs 09:30 $10,260.41 (session +52.06) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▲ 09:30 equity $10,356.19 vs yday $10,311.91 (+44.28) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▼ close $10,287.63 vs 09:30 $10,356.19 (session -68.56) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▼ 09:30 equity $10,126.20 vs yday $10,287.63 (-161.43) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▼ close $10,126.07 vs 09:30 $10,126.20 (session -0.12) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▲ 09:30 equity $10,209.60 vs yday $10,126.07 (+83.53) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▲ close $10,380.79 vs 09:30 $10,209.60 (session +171.19) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▼ 09:30 equity $10,324.41 vs yday $10,380.79 (-56.38) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 41 | $58.64 | $2.14 | $-51.82 | $2,437.80 | ▼ -51.82 after sell → book $10,322.27; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 54 | $42.46 | $2.18 | $-194.41 | $4,728.46 | ▼ -194.41 after sell → book $10,320.08; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 213 | $13.84 | $2.81 | $+450.27 | $7,673.58 | ▲ +450.27 after sell → book $10,317.28; vs 09:30 mark -2.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 49 | $53.06 | $2.17 | $+115.10 | $10,271.35 | ▲ +115.10 after sell → book $10,315.11; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,995.07 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,718.90 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,436.42 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $5,152.62 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,874.49 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,598.28 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 733 | $1.75 | $9.46 | — | $1,306.07 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1283.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $147.74 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1283.92 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.74 | ▲ close $10,530.37 vs 09:30 $10,324.41 (session +240.28) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.74 | ▲ 09:30 equity $10,806.62 vs yday $10,530.37 (+276.25) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 1 | $11.73 | $0.14 | $-3.36 | $159.33 | ▼ -3.36 after sell → book $10,806.48; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 11 | $1.66 | $0.24 | $+1.33 | $177.35 | ▲ +1.33 after sell → book $10,806.24; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 17 | $0.87 | $0.22 | $-1.62 | $191.87 | ▼ -1.62 after sell → book $10,806.02; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $174.50 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $31.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $152.01 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $31.98 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 24 | $1.32 | $0.39 | — | $119.94 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $31.98 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.94 | ▲ close $10,807.31 vs 09:30 $10,806.62 (session +2.08) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.94 | ▲ 09:30 equity $10,920.70 vs yday $10,807.31 (+113.39) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.94 | ▼ close $10,886.15 vs 09:30 $10,920.70 (session -34.55) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.94 | ▼ 09:30 equity $10,714.12 vs yday $10,886.15 (-172.03) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 14 | $8.35 | $1.21 | — | $1.83 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $119.94 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.83 | ▲ close $11,145.89 vs 09:30 $10,714.12 (session +432.98) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.83 | ▼ 09:30 equity $10,944.81 vs yday $11,145.89 (-201.08) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.83 | ▼ close $10,790.99 vs 09:30 $10,944.81 (session -153.82) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.83 | ▲ 09:30 equity $10,818.04 vs yday $10,790.99 (+27.05) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 62 | $20.93 | $2.20 | $+19.19 | $1,297.29 | ▲ +19.19 after sell → book $10,815.84; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,632.52 | ▲ +59.06 after sell → book $10,813.79; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 62 | $21.31 | $2.20 | $+36.55 | $3,951.54 | ▲ +36.55 after sell → book $10,811.59; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 222 | $5.49 | $2.91 | $-67.93 | $5,167.41 | ▼ -67.93 after sell → book $10,808.68; vs 09:30 mark -2.91 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 65 | $21.47 | $2.21 | $+115.21 | $6,560.75 | ▲ +115.21 after sell → book $10,806.47; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,948.37 | ▲ +111.41 after sell → book $10,804.33; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 733 | $1.91 | $9.59 | $+98.24 | $9,338.82 | ▲ +98.24 after sell → book $10,794.75; vs 09:30 mark -9.58 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $155.89 | $2.03 | $+86.75 | $10,583.90 | ▲ +86.75 after sell → book $10,792.71; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,583.90 | ▲ close $10,794.42 vs 09:30 $10,818.04 (session +1.71) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,583.90 | ▼ 09:30 equity $10,790.80 vs yday $10,794.42 (-3.62) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $10,600.15 | ▼ -1.12 after sell → book $10,790.61; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $10,630.68 | ▲ +8.04 after sell → book $10,790.28; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 24 | $1.82 | $0.53 | $+11.08 | $10,673.83 | ▲ +11.08 after sell → book $10,789.75; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.83 | ▲ close $10,790.03 vs 09:30 $10,790.80 (session +0.28) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.83 | ▼ 09:30 equity $10,789.47 vs yday $10,790.03 (-0.56) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.83 | ▲ close $10,789.47 vs 09:30 $10,789.47 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.83 | ▼ 09:30 equity $10,789.33 vs yday $10,789.47 (-0.14) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 14 | $8.25 | $1.22 | $-3.83 | $10,788.11 | ▼ -3.83 after sell → book $10,788.11; vs 09:30 mark -1.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,788.11 | ▲ close $10,788.11 vs 09:30 $10,789.33 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,788.11 | ▲ 09:30 equity $10,788.11 vs yday $10,788.11 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,788.11 | ▲ close $10,788.11 vs 09:30 $10,788.11 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,788.11 | ▲ 09:30 equity $10,788.11 vs yday $10,788.11 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 50 | $42.93 | $2.14 | — | $8,639.47 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2157.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 16 | $132.45 | $2.04 | — | $6,518.24 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2157.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 268 | $8.03 | $3.46 | — | $4,362.74 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2157.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 40 | $52.88 | $2.11 | — | $2,245.43 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2157.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 594 | $3.63 | $7.66 | — | $81.55 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2157.62 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $10,568.79 vs 09:30 $10,788.11 (session -201.92) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▼ 09:30 equity $10,493.35 vs yday $10,568.79 (-75.44) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▲ close $10,597.15 vs 09:30 $10,493.35 (session +103.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▲ 09:30 equity $10,654.97 vs yday $10,597.15 (+57.82) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $10,475.35 vs 09:30 $10,654.97 (session -179.62) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▼ 09:30 equity $10,415.77 vs yday $10,475.35 (-59.58) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $10,121.81 vs 09:30 $10,415.77 (session -293.96) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▼ 09:30 equity $10,014.05 vs yday $10,121.81 (-107.76) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $9,859.71 vs 09:30 $10,014.05 (session -154.34) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▲ 09:30 equity $9,955.13 vs yday $9,859.71 (+95.42) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 50 | $41.30 | $2.17 | $-85.81 | $2,144.38 | ▼ -85.81 after sell → book $9,952.96; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 16 | $122.40 | $2.06 | $-164.90 | $4,100.72 | ▼ -164.90 after sell → book $9,950.90; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 268 | $7.70 | $3.52 | $-95.42 | $6,160.80 | ▼ -95.42 after sell → book $9,947.38; vs 09:30 mark -3.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 40 | $53.53 | $2.14 | $+21.75 | $8,299.86 | ▲ +21.75 after sell → book $9,945.24; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 594 | $2.77 | $7.77 | $-526.28 | $9,937.47 | ▼ -526.28 after sell → book $9,937.47; vs 09:30 mark -7.77 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,955.13 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 96 | $34.44 | $2.28 | — | $6,628.95 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $3312.49 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 38 | $85.00 | $2.10 | — | $3,396.84 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $3312.49 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 30 | $108.55 | $2.08 | — | $138.26 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $3312.49 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.26 | ▼ close $9,684.78 vs 09:30 $9,937.47 (session -246.22) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.26 | ▼ 09:30 equity $9,680.90 vs yday $9,684.78 (-3.88) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.26 | ▲ close $10,219.96 vs 09:30 $9,680.90 (session +539.06) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.26 | ▲ 09:30 equity $10,219.96 vs yday $10,219.96 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.26 | ▲ close $10,219.96 vs 09:30 $10,219.96 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.26 | ▼ 09:30 equity $10,193.84 vs yday $10,219.96 (-26.12) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $117.40 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $27.65 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 2 | $9.81 | $0.20 | — | $97.58 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $27.65 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 1 | $20.25 | $0.21 | — | $77.13 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $27.65 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.13 | ▼ close $10,186.31 vs 09:30 $10,193.84 (session -6.92) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.13 | ▼ 09:30 equity $10,094.66 vs yday $10,186.31 (-91.65) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.13 | ▼ close $10,042.04 vs 09:30 $10,094.66 (session -52.62) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.46 | ▲ 09:30 equity $9,525.26 vs yday $9,503.15 (+22.11) | 09:30 open · cash $46.46 (unchanged overnight, no fees) · equity $9,525.26 vs prior close $9,503.15 (+22.11) · 13 name(s) re-marked at the open (per-name table). A×7 yday $172.84 → 09:30 $171.98 -6.02; ADMA×119 yday $9.52 → 09:30 $9.52 +0.00; ARQT×42 yday $26.27 → 09:30 $26.27 +0.00; CYPH×2 yday $4.08 → 09:30 $4.00 -0.15; DXCM×13 yday $87.47 → 09:30 $87.47 +0.00; ECO×1 yday $78.22 → 09:30 $78.22 +0.00; FIVN×3 yday $36.66 → 09:30 $36.66 +0.00; FTRE×57 yday $20.02 → 09:30 $20.02 +0.00; HALO×10 yday $115.22 → 09:30 $115.36 +1.40; IOVA×1 yday $10.80 → 09:30 $10.80 +0.00; OMER×56 yday $20.13 → 09:30 $20.61 +26.88; PGEN×147 yday $7.70 → 09:30 $7.70 +0.00; RBRK×1 yday $113.80 → 09:30 $113.80 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 6 | $7.65 | $0.48 | — | $0.08 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $46.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.08 | ▼ close $9,486.10 vs 09:30 $9,525.26 (session -38.68) | 16:00 close · cash $0.08 · equity $9,486.10 vs 09:30 $9,525.26 (-39.16; session marks -38.68) · 14 name(s) marked open→close (per-name table). A×7 09:30 $171.98 → close $172.79 +5.67; ADMA×119 09:30 $9.52 → close $9.52 +0.00; ARQT×42 09:30 $26.27 → close $26.27 +0.00; CYPH×2 09:30 $4.00 → close $4.12 +0.23; DXCM×13 09:30 $87.47 → close $87.47 +0.00; ECO×1 09:30 $78.22 → close $78.22 +0.00; FIVN×3 09:30 $36.66 → close $36.66 -0.00; FTRE×57 09:30 $20.02 → close $20.02 +0.00; HALO×10 09:30 $115.36 → close $113.90 -14.60; IOVA×1 09:30 $10.80 → close $10.80 +0.00; OMER×56 09:30 $20.61 → close $20.08 -29.68; PGEN×147 09:30 $7.70 → close $7.70 -0.00; RBRK×1 09:30 $113.80 → close $113.80 +0.00; MRVI×6 09:30 $7.65 → close $7.60 -0.30 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 16.70 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 16.70 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 31.98 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 31.98 < 1 share @ 216.30 |
| 2026-08-21 | `FUTU` | cash | leftover split 31.98 < 1 share @ 115.18 |
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
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 81.55 < 1 share @ 263.36 |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `A` | cash | leftover split 27.65 < 1 share @ 166.54 |
| 2026-09-23 | `ARQT` | cash | leftover split 27.65 < 1 share @ 27.79 |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 96 | 2026-09-18 @ $34.44 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $3312.49 |
| `ECO` | 38 | 2026-09-18 @ $85.00 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $3312.49 |
| `RBRK` | 30 | 2026-09-18 @ $108.55 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $3312.49 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $27.65 |
| `ADMA` | 2 | 2026-09-23 @ $9.81 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $27.65 |
| `FTRE` | 1 | 2026-09-23 @ $20.25 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $27.65 |
