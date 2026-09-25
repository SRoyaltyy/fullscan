# Factor mine action — `flatten_h3_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **-21.83%** ($7,817) · signal-only (no cash/fees) was -10.41%. Starts YES **2/30**. Fills 138 · skips 214 · realized $-900.04.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Give about 40% of leftover cash to the first name; split the rest.
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,732.49.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $4000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $857.14 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.57 | ▲ close $10,123.05 vs 09:30 $10,000.00 (session +149.49) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.52 | ▲ close $10,395.68 vs 09:30 $10,109.78 (session +286.33) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.52 | ▼ 09:30 equity $10,380.01 vs yday $10,395.68 (-15.67) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $49.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.67 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.59 | ▲ close $10,388.13 vs 09:30 $10,380.01 (session +8.96) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▼ 09:30 equity $10,277.67 vs yday $10,388.13 (-110.46) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 66 | $60.00 | $2.23 | $+8.78 | $3,999.36 | ▲ +8.78 after sell → book $10,275.44; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 18 | $43.56 | $2.06 | $-47.67 | $4,781.37 | ▼ -47.67 after sell → book $10,273.37; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 16 | $51.77 | $2.06 | $+14.25 | $5,607.64 | ▲ +14.25 after sell → book $10,271.32; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 17 | $49.28 | $2.06 | $-11.24 | $6,443.34 | ▼ -11.24 after sell → book $10,269.26; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 73 | $12.66 | $2.23 | $+65.64 | $7,365.28 | ▲ +65.64 after sell → book $10,267.02; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 28 | $27.85 | $2.09 | $-57.09 | $8,142.99 | ▼ -57.09 after sell → book $10,264.93; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1058 | $1.14 | $13.83 | $+323.56 | $9,335.28 | ▲ +323.56 after sell → book $10,251.10; vs 09:30 mark -13.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 36 | $22.16 | $2.12 | $-46.34 | $10,130.92 | ▼ -46.34 after sell → book $10,248.98; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,130.92 | ▼ close $10,247.92 vs 09:30 $10,277.67 (session -1.06) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,130.92 | ▲ 09:30 equity $10,248.88 vs yday $10,247.92 (+0.96) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,139.72 | ▼ -0.31 after sell → book $10,248.77; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 14 | $0.88 | $0.19 | $-1.16 | $10,151.85 | ▼ -1.16 after sell → book $10,248.58; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 9 | $1.42 | $0.17 | $-1.06 | $10,164.46 | ▼ -1.06 after sell → book $10,248.41; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.46 | ▲ close $10,248.76 vs 09:30 $10,248.88 (session +0.35) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.46 | ▲ 09:30 equity $10,249.23 vs yday $10,248.76 (+0.47) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $10,212.96 | ▲ +1.86 after sell → book $10,248.71; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $10,220.70 | ▼ -0.45 after sell → book $10,248.61; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,228.94 | ▼ -0.30 after sell → book $10,248.50; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 3 | $3.20 | $0.12 | $-0.35 | $10,238.42 | ▼ -0.35 after sell → book $10,248.38; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 2 | $4.98 | $0.13 | $+0.11 | $10,248.25 | ▲ +0.11 after sell → book $10,248.25; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,156.21 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4099.30 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,335.11 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,465.69 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,586.21 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,720.36 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,859.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $975.80 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $106.56 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $878.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.56 | ▲ close $10,491.02 vs 09:30 $10,249.23 (session +264.60) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.56 | ▲ 09:30 equity $10,790.88 vs yday $10,491.02 (+299.86) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $99.06 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $9.13 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $91.25 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $9.13 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 6 | $1.32 | $0.10 | — | $83.24 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $9.13 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.24 | ▼ close $10,661.83 vs 09:30 $10,790.88 (session -128.78) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.24 | ▲ 09:30 equity $10,768.68 vs yday $10,661.83 (+106.85) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.24 | ▼ close $10,671.78 vs 09:30 $10,768.68 (session -96.90) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.24 | ▼ 09:30 equity $10,475.50 vs yday $10,671.78 (-196.28) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 199 | $20.32 | $2.65 | $-51.01 | $4,124.26 | ▼ -51.01 after sell → book $10,472.84; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 9 | $95.86 | $2.04 | $+39.60 | $4,984.97 | ▲ +39.60 after sell → book $10,470.81; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 42 | $20.47 | $2.14 | $-11.81 | $5,842.57 | ▼ -11.81 after sell → book $10,468.67; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 152 | $5.53 | $2.48 | $-41.41 | $6,680.65 | ▼ -41.41 after sell → book $10,466.19; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 44 | $21.21 | $2.14 | $+65.26 | $7,611.75 | ▲ +65.26 after sell → book $10,464.05; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 29 | $32.32 | $2.10 | $+73.84 | $8,546.93 | ▲ +73.84 after sell → book $10,461.95; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 501 | $1.90 | $6.56 | $+62.13 | $9,492.27 | ▲ +62.13 after sell → book $10,455.39; vs 09:30 mark -6.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 6 | $156.51 | $2.03 | $+67.78 | $10,429.31 | ▲ +67.78 after sell → book $10,453.37; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 175 | $23.77 | $2.52 | — | $6,267.04 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $4171.72 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 113 | $10.98 | $2.33 | — | $5,023.97 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $1251.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $3,798.12 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $1251.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 149 | $8.35 | $2.44 | — | $2,551.54 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $1251.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 253 | $4.94 | $3.26 | — | $1,298.45 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $1251.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $442.52 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $1251.52 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $442.52 | ▲ close $10,566.26 vs 09:30 $10,475.50 (session +127.48) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $442.52 | ▲ 09:30 equity $10,639.07 vs yday $10,566.26 (+72.81) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $449.64 | ▼ -0.36 after sell → book $10,638.96; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $457.65 | ▲ +0.20 after sell → book $10,638.85; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 6 | $1.60 | $0.13 | $+1.45 | $467.12 | ▲ +1.45 after sell → book $10,638.72; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $467.12 | ▼ close $10,527.68 vs 09:30 $10,639.07 (session -111.04) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $467.12 | ▼ 09:30 equity $10,507.25 vs yday $10,527.68 (-20.43) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 4 | $41.44 | $1.67 | — | $299.69 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $186.85 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 9 | $14.42 | $1.32 | — | $168.58 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $140.14 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 53 | $2.60 | $1.54 | — | $29.25 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $140.14 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.25 | ▼ close $10,401.18 vs 09:30 $10,507.25 (session -101.54) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.25 | ▲ 09:30 equity $10,444.77 vs yday $10,401.18 (+43.59) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 113 | $10.97 | $2.36 | $-5.82 | $1,266.50 | ▼ -5.82 after sell → book $10,442.41; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $60.52 | $2.07 | $-17.52 | $2,474.83 | ▼ -17.52 after sell → book $10,440.34; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 149 | $8.28 | $2.47 | $-15.34 | $3,706.08 | ▼ -15.34 after sell → book $10,437.87; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 253 | $4.95 | $3.32 | $-4.05 | $4,955.11 | ▼ -4.05 after sell → book $10,434.55; vs 09:30 mark -3.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $5,800.61 | ▼ -10.43 after sell → book $10,432.53; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,800.61 | ▼ close $10,360.21 vs 09:30 $10,444.77 (session -72.32) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,800.61 | ▲ 09:30 equity $10,380.21 vs yday $10,360.21 (+20.00) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 175 | $23.68 | $2.58 | $-20.84 | $9,942.04 | ▼ -20.84 after sell → book $10,377.64; vs 09:30 mark -2.57 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,942.04 | ▲ close $10,378.70 vs 09:30 $10,380.21 (session +1.06) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,942.04 | ▲ 09:30 equity $10,393.69 vs yday $10,378.70 (+14.99) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 4 | $41.94 | $1.71 | $-1.38 | $10,108.09 | ▼ -1.38 after sell → book $10,391.98; vs 09:30 mark -1.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 9 | $15.82 | $1.47 | $+9.80 | $10,249.00 | ▲ +9.80 after sell → book $10,390.51; vs 09:30 mark -1.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 53 | $2.67 | $1.59 | $+0.58 | $10,388.91 | ▲ +0.58 after sell → book $10,388.91; vs 09:30 mark -1.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,388.91 | ▲ close $10,388.91 vs 09:30 $10,393.69 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,388.91 | ▲ 09:30 equity $10,388.91 vs yday $10,388.91 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,388.91 | ▲ close $10,388.91 vs 09:30 $10,388.91 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,388.91 | ▲ 09:30 equity $10,388.91 vs yday $10,388.91 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 78 | $52.88 | $2.22 | — | $6,262.05 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $4155.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 36 | $42.93 | $2.10 | — | $4,714.47 | — | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $1558.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 429 | $3.63 | $5.53 | — | $3,151.67 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $1558.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 194 | $8.03 | $2.57 | — | $1,591.27 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $1558.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 11 | $132.45 | $2.02 | — | $132.30 | — | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $1558.34 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.30 | ▼ close $10,209.11 vs 09:30 $10,388.91 (session -165.35) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.30 | ▼ 09:30 equity $10,133.85 vs yday $10,209.11 (-75.26) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 21 | $2.52 | $0.59 | — | $78.79 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $52.92 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $65.23 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $15.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 8 | $1.90 | $0.18 | — | $49.85 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $15.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $4.78 | $0.15 | — | $35.36 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $15.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 9 | $1.59 | $0.17 | — | $20.88 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $15.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $9.45 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $15.88 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.45 | ▲ close $10,181.02 vs 09:30 $10,133.85 (session +48.51) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.45 | ▲ 09:30 equity $10,358.51 vs yday $10,181.02 (+177.49) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.45 | ▼ close $10,201.52 vs 09:30 $10,358.51 (session -156.99) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.45 | ▼ 09:30 equity $10,130.86 vs yday $10,201.52 (-70.66) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 78 | $53.16 | $2.27 | $+17.35 | $4,153.66 | ▲ +17.35 after sell → book $10,128.59; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 36 | $42.01 | $2.12 | $-37.34 | $5,663.90 | ▼ -37.34 after sell → book $10,126.47; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 429 | $3.28 | $5.62 | $-161.30 | $7,065.41 | ▼ -161.30 after sell → book $10,120.86; vs 09:30 mark -5.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 194 | $8.01 | $2.62 | $-9.07 | $8,616.73 | ▼ -9.07 after sell → book $10,118.24; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 11 | $125.77 | $2.04 | $-77.55 | $9,998.16 | ▼ -77.55 after sell → book $10,116.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,998.16 | ▼ close $10,109.62 vs 09:30 $10,130.86 (session -6.58) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,998.16 | ▼ 09:30 equity $10,107.74 vs yday $10,109.62 (-1.88) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 21 | $2.22 | $0.55 | $-7.44 | $10,044.23 | ▼ -7.44 after sell → book $10,107.19; vs 09:30 mark -0.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $10,056.30 | ▼ -1.49 after sell → book $10,107.05; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 8 | $1.83 | $0.19 | $-0.93 | $10,070.75 | ▼ -0.93 after sell → book $10,106.86; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 3 | $3.92 | $0.15 | $-2.87 | $10,082.37 | ▼ -2.87 after sell → book $10,106.71; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 9 | $1.53 | $0.18 | $-0.89 | $10,095.95 | ▼ -0.89 after sell → book $10,106.52; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $10,106.40 | ▼ -0.98 after sell → book $10,106.40; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.40 | ▲ close $10,106.40 vs 09:30 $10,107.74 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.40 | ▲ 09:30 equity $10,106.40 vs yday $10,106.40 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 248 | $16.28 | $3.20 | — | $6,065.76 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $4042.56 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 444 | $2.73 | $5.73 | — | $4,847.91 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1212.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $3,811.70 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1212.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $2,658.68 | — | 40% to #1, rest split; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1212.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $1,552.21 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1212.77 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 21 | $56.09 | $2.05 | — | $372.27 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1212.77 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.27 | ▼ close $10,026.73 vs 09:30 $10,106.40 (session -62.66) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.27 | ▼ 09:30 equity $9,740.20 vs yday $10,026.73 (-286.53) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.27 | ▼ close $9,728.09 vs 09:30 $9,740.20 (session -12.11) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.27 | ▲ 09:30 equity $9,765.32 vs yday $9,728.09 (+37.23) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.27 | ▼ close $9,593.22 vs 09:30 $9,765.32 (session -172.10) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.27 | ▲ 09:30 equity $9,653.66 vs yday $9,593.22 (+60.44) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 248 | $16.16 | $3.27 | $-36.23 | $4,376.68 | ▼ -36.23 after sell → book $9,650.39; vs 09:30 mark -3.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 444 | $2.72 | $5.81 | $-15.98 | $5,578.54 | ▼ -15.98 after sell → book $9,644.57; vs 09:30 mark -5.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $6,550.72 | ▼ -64.03 after sell → book $9,642.55; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $7,528.90 | ▼ -174.84 after sell → book $9,640.52; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $8,561.40 | ▼ -73.97 after sell → book $9,638.49; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 21 | $51.29 | $2.07 | $-104.93 | $9,636.41 | ▼ -104.93 after sell → book $9,636.41; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 14 | $270.89 | $2.03 | — | $5,841.92 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $3854.57 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 24 | $77.12 | $2.06 | — | $3,988.98 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $1927.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 134 | $14.31 | $2.39 | — | $2,069.05 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $1927.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 52 | $36.46 | $2.15 | — | $170.98 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $1927.28 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $170.98 | ▼ close $9,541.74 vs 09:30 $9,653.66 (session -86.04) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $170.98 | ▲ 09:30 equity $9,656.70 vs yday $9,541.74 (+114.96) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $150.27 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $20.52 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $134.93 | — | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $20.52 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.93 | ▼ close $9,597.77 vs 09:30 $9,656.70 (session -58.56) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.93 | ▼ 09:30 equity $9,579.83 vs yday $9,597.77 (-17.94) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.93 | ▼ close $9,464.53 vs 09:30 $9,579.83 (session -115.30) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.93 | ▲ 09:30 equity $9,474.09 vs yday $9,464.53 (+9.56) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 14 | $266.76 | $2.07 | $-61.92 | $3,867.50 | ▼ -61.92 after sell → book $9,472.02; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 24 | $76.27 | $2.09 | $-24.55 | $5,695.90 | ▼ -24.55 after sell → book $9,469.94; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 134 | $13.65 | $2.43 | $-93.26 | $7,522.57 | ▼ -93.26 after sell → book $9,467.51; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 52 | $36.70 | $2.17 | $+8.16 | $9,428.80 | ▲ +8.16 after sell → book $9,465.34; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 23 | $157.87 | $2.06 | — | $5,795.73 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $3771.52 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $4,635.13 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; leftover $1414.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 15 | $88.83 | $2.04 | — | $3,300.64 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $1414.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 104 | $13.47 | $2.30 | — | $1,897.46 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $1414.32 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 353 | $4.00 | $4.55 | — | $480.91 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $1414.32 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $480.91 | ▼ close $9,275.12 vs 09:30 $9,474.09 (session -177.27) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $480.91 | ▲ 09:30 equity $9,313.93 vs yday $9,275.12 (+38.81) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $501.04 | ▼ -0.58 after sell → book $9,313.70; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.04 | ▲ close $9,391.36 vs 09:30 $9,313.93 (session +77.66) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.04 | ▲ 09:30 equity $9,423.36 vs yday $9,391.36 (+32.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $383.02 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $200.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 2 | $27.79 | $0.56 | — | $326.87 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $75.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 7 | $9.81 | $0.71 | — | $257.50 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $75.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 3 | $20.25 | $0.62 | — | $196.13 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $75.16 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 3 | $20.65 | $0.63 | — | $133.55 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $75.16 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.55 | ▼ close $9,241.29 vs 09:30 $9,423.36 (session -178.38) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.55 | ▼ 09:30 equity $9,097.90 vs yday $9,241.29 (-143.39) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $148.14 | ▼ -0.75 after sell → book $9,097.72; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 23 | $163.95 | $2.10 | $+135.68 | $3,916.89 | ▲ +135.68 after sell → book $9,095.62; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $5,038.49 | ▼ -39.00 after sell → book $9,093.60; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 15 | $87.67 | $2.06 | $-21.42 | $6,351.56 | ▼ -21.42 after sell → book $9,091.55; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 104 | $11.42 | $2.33 | $-217.83 | $7,536.91 | ▼ -217.83 after sell → book $9,089.22; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 353 | $3.40 | $4.62 | $-220.98 | $8,732.49 | ▼ -220.98 after sell → book $9,084.60; vs 09:30 mark -4.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,732.49 | ▲ close $9,087.34 vs 09:30 $9,097.90 (session +2.74) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,729.24 | ▲ 09:30 equity $7,928.06 vs yday $7,928.06 (-0.00) | 09:30 open · cash $7,729.24 (unchanged overnight, no fees) · equity $7,928.06 vs prior close $7,928.06 (-0.00) · 5 name(s) re-marked at the open (per-name table). ADMA×1 yday $9.52 → 09:30 $9.52 +0.00; DLO×2 yday $13.88 → 09:30 $13.88 +0.00; PACS×3 yday $41.46 → 09:30 $41.46 +0.00; PGEN×1 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 3 | $803.87 | $2.00 | — | $5,315.63 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.8; leftover $3091.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 10 | $115.36 | $2.02 | — | $4,160.01 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.1; leftover $1159.39 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 56 | $20.61 | $2.16 | — | $3,003.69 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.1; leftover $1159.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 30 | $38.51 | $2.08 | — | $1,846.31 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $1159.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 151 | $7.65 | $2.44 | — | $688.72 | — | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $1159.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $688.72 | ▼ close $7,817.44 vs 09:30 $7,928.06 (session -99.92) | 16:00 close · cash $688.72 · equity $7,817.44 vs 09:30 $7,928.06 (-110.62; session marks -99.92) · 10 name(s) marked open→close (per-name table). ADMA×1 09:30 $9.52 → close $9.52 +0.00; DLO×2 09:30 $13.88 → close $13.88 +0.00; PACS×3 09:30 $41.46 → close $41.46 -0.00; PGEN×1 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; REGN×3 09:30 $803.87 → close $788.04 -47.49; HALO×10 09:30 $115.36 → close $113.90 -14.60; OMER×56 09:30 $20.61 → close $20.08 -29.68; BLFS×30 09:30 $38.51 → close $38.49 -0.60; MRVI×151 09:30 $7.65 → close $7.60 -7.55 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 64.23 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
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
| 2026-08-17 | `EOG` | cash | leftover split 10.67 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.67 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.67 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 42.62 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.13 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.13 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.13 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 9.13 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 68.39 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 20.52 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 20.52 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 20.52 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 53.97 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 16.19 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 16.19 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 16.19 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 16.19 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 16.19 < 1 share @ 34.44 |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 60.12 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HALO` | 1 | 2026-09-23 @ $116.85 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $200.41 |
| `ARQT` | 2 | 2026-09-23 @ $27.79 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $75.16 |
| `ADMA` | 7 | 2026-09-23 @ $9.81 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $75.16 |
| `FTRE` | 3 | 2026-09-23 @ $20.25 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $75.16 |
| `OMER` | 3 | 2026-09-23 @ $20.65 | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $75.16 |
