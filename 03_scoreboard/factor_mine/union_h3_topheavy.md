# Factor mine action — `union_h3_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **-24.02%** ($7,598) · signal-only (no cash/fees) was -4.92%. Starts YES **0/30**. Fills 245 · skips 294 · realized $-410.95.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,437.78.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $4000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+12.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+6.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.2; leftover $857.14 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+19.7; leftover $857.14 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.57 | ▲ close $10,123.05 vs 09:30 $10,000.00 (session +149.49) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $13.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.52 | ▲ close $10,395.68 vs 09:30 $10,109.78 (session +286.33) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.52 | ▼ 09:30 equity $10,380.01 vs yday $10,395.68 (-15.67) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $49.81 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $10.67 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $10.67 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,156.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4099.30 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,335.11 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,465.69 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,586.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,720.36 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,859.02 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $975.80 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $878.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $106.56 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $878.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.56 | ▲ close $10,491.02 vs 09:30 $10,249.23 (session +264.60) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.56 | ▲ 09:30 equity $10,790.88 vs yday $10,491.02 (+299.86) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $99.06 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $9.13 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $91.25 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $9.13 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 6 | $1.32 | $0.10 | — | $83.24 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $9.13 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 175 | $23.77 | $2.52 | — | $6,267.04 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $4171.72 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 81 | $10.98 | $2.23 | — | $5,375.43 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+1.2; leftover $893.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 14 | $61.19 | $2.03 | — | $4,516.74 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+7.4; leftover $893.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 107 | $8.35 | $2.31 | — | $3,620.98 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.0; leftover $893.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 180 | $4.94 | $2.53 | — | $2,729.25 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $893.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $1,873.31 | — | 40% to #1, rest split; list flatten; ret5=+6.0; leftover $893.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 123 | $7.25 | $2.36 | — | $979.20 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $893.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 2497 | $0.36 | $16.43 | — | $68.84 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-15.6; leftover $893.94 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.84 | ▲ close $10,656.36 vs 09:30 $10,475.50 (session +235.40) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.84 | ▲ 09:30 equity $10,733.88 vs yday $10,656.36 (+77.52) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $75.97 | ▼ -0.36 after sell → book $10,733.77; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $83.98 | ▲ +0.20 after sell → book $10,733.66; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 6 | $1.60 | $0.13 | $+1.45 | $93.45 | ▲ +1.45 after sell → book $10,733.53; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 1 | $31.21 | $0.32 | — | $61.92 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $37.38 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 5 | $11.12 | $0.57 | — | $5.75 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $56.07 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.75 | ▲ close $10,832.23 vs 09:30 $10,733.88 (session +99.59) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.75 | ▼ 09:30 equity $10,804.01 vs yday $10,832.23 (-28.22) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.75 | ▼ close $10,762.82 vs 09:30 $10,804.01 (session -41.19) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.75 | ▼ 09:30 equity $10,752.40 vs yday $10,762.82 (-10.42) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 81 | $10.97 | $2.26 | $-5.30 | $892.06 | ▼ -5.30 after sell → book $10,750.15; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 14 | $60.52 | $2.05 | $-13.46 | $1,737.29 | ▼ -13.46 after sell → book $10,748.10; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 107 | $8.28 | $2.34 | $-12.14 | $2,620.91 | ▼ -12.14 after sell → book $10,745.76; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 180 | $4.95 | $2.57 | $-3.30 | $3,509.34 | ▼ -3.30 after sell → book $10,743.19; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $423.76 | $2.02 | $-10.43 | $4,354.85 | ▼ -10.43 after sell → book $10,741.17; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 123 | $9.73 | $2.39 | $+300.29 | $5,549.25 | ▲ +300.29 after sell → book $10,738.78; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 2497 | $0.36 | $17.03 | $-15.98 | $6,443.62 | ▼ -15.98 after sell → book $10,721.75; vs 09:30 mark -17.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 61 | $41.74 | $2.17 | — | $3,895.31 | — | 40% to #1, rest split; list flatten; ret5=+2.4; leftover $2577.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 44 | $14.63 | $2.12 | — | $3,249.47 | — | 40% to #1, rest split; list flatten; ret5=+5.8; leftover $644.36 | — |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 240 | $2.68 | $3.10 | — | $2,603.17 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+16.3; leftover $644.36 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 19 | $32.90 | $2.05 | — | $1,976.02 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $644.36 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 41 | $15.66 | $2.11 | — | $1,331.85 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $644.36 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 8 | $79.42 | $2.01 | — | $694.48 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $644.36 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 194 | $3.32 | $2.57 | — | $47.83 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=+6.4; leftover $644.36 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.83 | ▼ close $10,495.16 vs 09:30 $10,752.40 (session -210.46) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.83 | ▲ 09:30 equity $10,545.04 vs yday $10,495.16 (+49.88) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 175 | $23.68 | $2.58 | $-20.84 | $4,189.25 | ▼ -20.84 after sell → book $10,542.46; vs 09:30 mark -2.58 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 1 | $29.94 | $0.32 | $-1.91 | $4,218.87 | ▼ -1.91 after sell → book $10,542.14; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 5 | $10.82 | $0.58 | $-2.65 | $4,272.39 | ▼ -2.65 after sell → book $10,541.56; vs 09:30 mark -0.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,272.39 | ▲ close $10,571.15 vs 09:30 $10,545.04 (session +29.59) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,272.39 | ▲ 09:30 equity $10,639.41 vs yday $10,571.15 (+68.26) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,272.39 | ▲ close $10,657.95 vs 09:30 $10,639.41 (session +18.54) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,272.39 | ▼ 09:30 equity $10,615.61 vs yday $10,657.95 (-42.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 61 | $42.10 | $2.20 | $+17.58 | $6,838.29 | ▲ +17.58 after sell → book $10,613.41; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 44 | $15.70 | $2.14 | $+42.82 | $7,526.94 | ▲ +42.82 after sell → book $10,611.26; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SLI` | 240 | $2.49 | $3.15 | $-51.84 | $8,121.40 | ▼ -51.84 after sell → book $10,608.12; vs 09:30 mark -3.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 19 | $32.42 | $2.07 | $-13.23 | $8,735.31 | ▼ -13.23 after sell → book $10,606.05; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 41 | $13.92 | $2.13 | $-75.59 | $9,303.90 | ▼ -75.59 after sell → book $10,603.92; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 8 | $78.84 | $2.03 | $-8.69 | $9,932.58 | ▼ -8.69 after sell → book $10,601.88; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 194 | $3.45 | $2.61 | $+20.03 | $10,599.27 | ▲ +20.03 after sell → book $10,599.27; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,599.27 | ▲ close $10,599.27 vs 09:30 $10,615.61 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,599.27 | ▲ 09:30 equity $10,599.27 vs yday $10,599.27 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 80 | $52.88 | $2.23 | — | $6,366.64 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4239.71 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 21 | $42.93 | $2.05 | — | $5,463.06 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $908.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 250 | $3.63 | $3.23 | — | $4,552.33 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $908.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 113 | $8.03 | $2.33 | — | $3,642.61 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $908.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $2,845.91 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $908.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 58 | $15.45 | $2.16 | — | $1,947.64 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $908.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 6 | $145.94 | $2.01 | — | $1,069.96 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $908.51 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 54 | $16.77 | $2.15 | — | $162.23 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $908.51 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.23 | ▼ close $10,394.17 vs 09:30 $10,599.27 (session -186.93) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.23 | ▼ 09:30 equity $10,369.80 vs yday $10,394.17 (-24.37) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 25 | $2.52 | $0.70 | — | $98.53 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $64.89 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $84.97 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $19.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 10 | $1.90 | $0.22 | — | $65.75 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $19.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $4.78 | $0.20 | — | $46.42 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $19.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 12 | $1.59 | $0.23 | — | $27.12 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $19.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $15.69 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $19.47 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.69 | ▼ close $10,355.92 vs 09:30 $10,369.80 (session -12.27) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.69 | ▲ 09:30 equity $10,552.67 vs yday $10,355.92 (+196.75) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.69 | ▼ close $10,396.99 vs 09:30 $10,552.67 (session -155.68) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.69 | ▼ 09:30 equity $10,327.09 vs yday $10,396.99 (-69.90) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 80 | $53.16 | $2.28 | $+17.89 | $4,266.21 | ▲ +17.89 after sell → book $10,324.81; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 21 | $42.01 | $2.07 | $-23.45 | $5,146.35 | ▼ -23.45 after sell → book $10,322.74; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 113 | $8.01 | $2.36 | $-6.95 | $6,049.12 | ▼ -6.95 after sell → book $10,320.38; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 6 | $125.77 | $2.03 | $-44.12 | $6,801.71 | ▼ -44.12 after sell → book $10,318.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 58 | $15.16 | $2.18 | $-21.17 | $7,678.81 | ▼ -21.17 after sell → book $10,316.17; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 6 | $140.29 | $2.03 | $-37.94 | $8,518.55 | ▼ -37.94 after sell → book $10,314.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 54 | $15.46 | $2.17 | $-75.06 | $9,351.22 | ▼ -75.06 after sell → book $10,311.97; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,351.22 | ▼ close $10,211.59 vs 09:30 $10,327.09 (session -100.38) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,351.22 | ▼ 09:30 equity $10,194.36 vs yday $10,211.59 (-17.23) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 250 | $2.85 | $3.28 | $-201.50 | $10,060.44 | ▼ -201.50 after sell → book $10,191.08; vs 09:30 mark -3.28 | dropped from list after 4 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 25 | $2.22 | $0.65 | $-8.85 | $10,115.29 | ▼ -8.85 after sell → book $10,190.43; vs 09:30 mark -0.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $10,127.37 | ▼ -1.49 after sell → book $10,190.28; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 10 | $1.83 | $0.23 | $-1.15 | $10,145.43 | ▼ -1.15 after sell → book $10,190.05; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 4 | $3.92 | $0.19 | $-3.82 | $10,160.93 | ▼ -3.82 after sell → book $10,189.86; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 12 | $1.53 | $0.24 | $-1.19 | $10,179.05 | ▼ -1.19 after sell → book $10,189.62; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $10,189.49 | ▼ -0.98 after sell → book $10,189.49; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.49 | ▲ close $10,189.49 vs 09:30 $10,194.36 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.49 | ▲ 09:30 equity $10,189.49 vs yday $10,189.49 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 250 | $16.28 | $3.23 | — | $6,116.27 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-1.1; leftover $4075.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 319 | $2.73 | $4.12 | — | $5,241.28 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-3.0; leftover $873.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 4 | $206.84 | $2.00 | — | $4,411.92 | — | 40% to #1, rest split; list flatten; ret5=+8.3; leftover $873.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 5 | $164.43 | $2.00 | — | $3,587.77 | — | 40% to #1, rest split; list flatten,earn_react; ⚪; ret5=+4.9; leftover $873.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 5 | $157.78 | $2.00 | — | $2,796.86 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+4.7; leftover $873.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 15 | $56.09 | $2.04 | — | $1,953.48 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+19.6; leftover $873.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 428 | $2.04 | $5.52 | — | $1,074.83 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $873.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 183 | $4.75 | $2.54 | — | $203.05 | — | 40% to #1, rest split; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $873.39 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.05 | ▼ close $10,112.00 vs 09:30 $10,189.49 (session -54.05) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.05 | ▼ 09:30 equity $9,898.28 vs yday $10,112.00 (-213.72) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.05 | ▲ close $9,938.37 vs 09:30 $9,898.28 (session +40.09) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.05 | ▲ 09:30 equity $9,945.67 vs yday $9,938.37 (+7.30) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.05 | ▼ close $9,722.04 vs 09:30 $9,945.67 (session -223.63) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.05 | ▲ 09:30 equity $9,773.05 vs yday $9,722.04 (+51.01) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 250 | $16.16 | $3.30 | $-36.52 | $4,239.75 | ▼ -36.52 after sell → book $9,769.75; vs 09:30 mark -3.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 319 | $2.72 | $4.18 | $-11.48 | $5,103.25 | ▼ -11.48 after sell → book $9,765.57; vs 09:30 mark -4.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 4 | $194.84 | $2.02 | $-52.02 | $5,880.59 | ▼ -52.02 after sell → book $9,763.55; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 5 | $140.03 | $2.02 | $-126.03 | $6,578.71 | ▼ -126.03 after sell → book $9,761.52; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 5 | $147.79 | $2.02 | $-53.98 | $7,315.64 | ▼ -53.98 after sell → book $9,759.50; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 15 | $51.29 | $2.06 | $-76.09 | $8,082.93 | ▼ -76.09 after sell → book $9,757.44; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 428 | $1.89 | $5.60 | $-75.32 | $8,886.25 | ▼ -75.32 after sell → book $9,751.84; vs 09:30 mark -5.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 183 | $4.73 | $2.58 | $-8.78 | $9,749.26 | ▼ -8.78 after sell → book $9,749.26; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 14 | $270.89 | $2.03 | — | $5,954.77 | — | 40% to #1, rest split; list flatten; ret5=+4.0; leftover $3899.70 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 10 | $77.12 | $2.02 | — | $5,181.55 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+7.2; leftover $835.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 58 | $14.31 | $2.16 | — | $4,349.40 | — | 40% to #1, rest split; list flatten; ret5=+4.8; leftover $835.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 22 | $36.46 | $2.06 | — | $3,545.23 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+2.9; leftover $835.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 44 | $18.61 | $2.12 | — | $2,724.27 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $835.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 45 | $18.21 | $2.12 | — | $1,902.69 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-19.1; leftover $835.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 12 | $68.79 | $2.03 | — | $1,075.19 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $835.65 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 142 | $5.87 | $2.42 | — | $239.23 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $835.65 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.23 | ▲ close $9,839.70 vs 09:30 $9,773.05 (session +107.40) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $239.23 | ▲ 09:30 equity $10,000.16 vs yday $9,839.70 (+160.46) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $218.52 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $20.51 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $203.18 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $20.51 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 120 | $0.17 | $0.56 | — | $182.22 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $20.51 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $166.18 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $20.51 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.18 | ▼ close $9,998.12 vs 09:30 $10,000.16 (session -0.94) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.18 | ▲ 09:30 equity $10,017.40 vs yday $9,998.12 (+19.28) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 14 | $0.97 | $0.18 | — | $152.43 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $14.24 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 6 | $2.08 | $0.14 | — | $139.80 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $14.24 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.80 | ▼ close $9,930.66 vs 09:30 $10,017.40 (session -86.42) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.80 | ▲ 09:30 equity $9,980.23 vs yday $9,930.66 (+49.57) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 14 | $266.76 | $2.07 | $-61.92 | $3,872.37 | ▼ -61.92 after sell → book $9,978.16; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 10 | $76.27 | $2.04 | $-12.56 | $4,633.03 | ▼ -12.56 after sell → book $9,976.12; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 58 | $13.65 | $2.18 | $-42.63 | $5,422.55 | ▼ -42.63 after sell → book $9,973.94; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 22 | $36.70 | $2.08 | $+1.15 | $6,227.87 | ▲ +1.15 after sell → book $9,971.86; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 44 | $22.11 | $2.14 | $+149.74 | $7,198.57 | ▲ +149.74 after sell → book $9,969.72; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 45 | $20.55 | $2.15 | $+101.03 | $8,121.18 | ▲ +101.03 after sell → book $9,967.58; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 12 | $79.08 | $2.05 | $+119.41 | $9,068.09 | ▲ +119.41 after sell → book $9,965.53; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 142 | $5.62 | $2.45 | $-40.37 | $9,863.68 | ▼ -40.37 after sell → book $9,963.08; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 24 | $157.87 | $2.06 | — | $6,072.74 | — | 40% to #1, rest split; list flatten; ret5=+6.5; leftover $3945.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 3 | $386.20 | $2.00 | — | $4,912.14 | — | 40% to #1, rest split; list flatten; ret5=-5.8; leftover $1183.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 13 | $88.83 | $2.03 | — | $3,755.32 | — | 40% to #1, rest split; list flatten; ret5=+7.6; leftover $1183.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 87 | $13.47 | $2.25 | — | $2,581.18 | — | 40% to #1, rest split; list flatten; ret5=+3.6; leftover $1183.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 295 | $4.00 | $3.81 | — | $1,397.37 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $1183.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 127 | $9.31 | $2.37 | — | $212.63 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1183.64 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.63 | ▼ close $9,792.57 vs 09:30 $9,980.23 (session -155.99) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.63 | ▲ 09:30 equity $9,825.00 vs yday $9,792.57 (+32.43) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $232.76 | ▼ -0.58 after sell → book $9,824.77; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 120 | $0.16 | $0.58 | $-2.35 | $251.38 | ▼ -2.35 after sell → book $9,824.19; vs 09:30 mark -0.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 37 | $0.58 | $0.33 | — | $229.59 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $21.55 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.59 | ▲ close $9,886.91 vs 09:30 $9,825.00 (session +63.05) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $229.59 | ▲ 09:30 equity $9,979.77 vs yday $9,886.91 (+92.86) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 1 | $17.10 | $0.19 | $+0.87 | $246.50 | ▲ +0.87 after sell → book $9,979.58; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 14 | $0.89 | $0.19 | $-1.48 | $258.77 | ▼ -1.48 after sell → book $9,979.39; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 6 | $2.16 | $0.17 | $+0.17 | $271.57 | ▲ +0.17 after sell → book $9,979.22; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $243.50 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+7.0; leftover $40.74 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 4 | $9.81 | $0.40 | — | $203.85 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.0; leftover $40.74 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 2 | $20.25 | $0.41 | — | $162.94 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+15.0; leftover $40.74 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 1 | $20.65 | $0.21 | — | $142.08 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $40.74 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.08 | ▼ close $9,740.26 vs 09:30 $9,979.77 (session -237.66) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.08 | ▼ 09:30 equity $9,596.14 vs yday $9,740.26 (-144.12) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $156.67 | ▼ -0.75 after sell → book $9,595.97; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 24 | $163.95 | $2.10 | $+141.75 | $4,089.36 | ▲ +141.75 after sell → book $9,593.86; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 3 | $374.54 | $2.02 | $-39.00 | $5,210.96 | ▼ -39.00 after sell → book $9,591.84; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 13 | $87.67 | $2.05 | $-19.09 | $6,348.69 | ▼ -19.09 after sell → book $9,589.80; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 87 | $11.42 | $2.28 | $-182.88 | $7,339.96 | ▼ -182.88 after sell → book $9,587.52; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 295 | $3.40 | $3.86 | $-184.67 | $8,339.09 | ▼ -184.67 after sell → book $9,583.66; vs 09:30 mark -3.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 127 | $8.67 | $2.40 | $-86.05 | $9,437.78 | ▼ -86.05 after sell → book $9,581.25; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,437.78 | ▲ close $9,581.98 vs 09:30 $9,596.14 (session +0.73) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,891.33 | ▲ 09:30 equity $8,105.25 vs yday $8,105.25 (-0.00) | 09:30 open · cash $7,891.33 (unchanged overnight, no fees) · equity $8,105.25 vs prior close $8,105.25 (-0.00) · 6 name(s) re-marked at the open (per-name table). ADMA×1 yday $9.52 → 09:30 $9.52 +0.00; DEFT×48 yday $0.53 → 09:30 $0.53 +0.00; DLO×1 yday $13.88 → 09:30 $13.88 +0.00; FJET×14 yday $1.80 → 09:30 $1.80 +0.00; PACS×3 yday $41.46 → 09:30 $41.46 +0.00; PGEN×2 yday $7.70 → 09:30 $7.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 3 | $803.87 | $2.00 | — | $5,477.72 | — | 40% to #1, rest split; list flatten; ret5=+0.8; leftover $3156.53 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 5 | $115.36 | $2.00 | — | $4,898.92 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.1; leftover $676.40 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 32 | $20.61 | $2.09 | — | $4,237.31 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+9.1; leftover $676.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 17 | $38.51 | $2.04 | — | $3,580.60 | — | 40% to #1, rest split; list flatten; ret5=+4.7; leftover $676.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 88 | $7.65 | $2.25 | — | $2,905.14 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.2; leftover $676.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 25 | $26.27 | $2.06 | — | $2,246.33 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $676.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 8 | $83.76 | $2.01 | — | $1,574.24 | — | 40% to #1, rest split; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $676.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 74 | $9.05 | $2.21 | — | $902.32 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-27.1; leftover $676.40 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $902.32 | ▼ close $8,061.62 vs 09:30 $8,105.25 (session -26.95) | 16:00 close · cash $902.32 · equity $8,061.62 vs 09:30 $8,105.25 (-43.63; session marks -26.95) · 14 name(s) marked open→close (per-name table). ADMA×1 09:30 $9.52 → close $9.52 +0.00; DEFT×48 09:30 $0.53 → close $0.53 +0.00; DLO×1 09:30 $13.88 → close $13.88 +0.00; FJET×14 09:30 $1.80 → close $1.80 -0.00; PACS×3 09:30 $41.46 → close $41.46 -0.00; PGEN×2 09:30 $7.70 → close $7.70 -0.00; REGN×3 09:30 $803.87 → close $788.04 -47.49; HALO×5 09:30 $115.36 → close $113.90 -7.30; OMER×32 09:30 $20.61 → close $20.08 -16.96; BLFS×17 09:30 $38.51 → close $38.49 -0.34; MRVI×88 09:30 $7.65 → close $7.60 -4.40; WRBY×25 09:30 $26.27 → close $26.71 +11.00; TXG×8 09:30 $83.76 → close $85.71 +15.60; AEHL×74 09:30 $9.05 → close $9.36 +22.94 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $902.32 | ▼ 09:30 equity $7,943.94 vs yday $8,061.62 (-117.68) | 09:30 open · cash $902.32 (unchanged overnight, no fees) · equity $7,943.94 vs prior close $8,061.62 (-117.68) · 14 name(s) re-marked at the open (per-name table). ADMA×1 yday $9.52 → 09:30 $9.38 -0.14; AEHL×74 yday $9.36 → 09:30 $9.01 -25.90; BLFS×17 yday $38.49 → 09:30 $38.13 -6.12; DEFT×48 yday $0.53 → 09:30 $0.51 -1.26; DLO×1 yday $13.88 → 09:30 $13.91 +0.03; FJET×14 yday $1.80 → 09:30 $1.79 -0.14; HALO×5 yday $113.90 → 09:30 $113.34 -2.80; MRVI×88 yday $7.60 → 09:30 $7.49 -9.68; OMER×32 yday $20.08 → 09:30 $19.83 -8.00; PACS×3 yday $41.46 → 09:30 $41.27 -0.57; PGEN×2 yday $7.70 → 09:30 $7.61 -0.18; REGN×3 yday $788.04 → 09:30 $771.84 -48.61; TXG×8 yday $85.71 → 09:30 $86.14 +3.44; WRBY×25 yday $26.71 → 09:30 $26.00 -17.75 | — |
| 2026-09-28 09:30 ET | **SELL** | `ADMA` | 1 | $9.38 | $0.12 | $-0.65 | $911.58 | ▼ -0.65 after sell → book $7,943.82; vs 09:30 mark -0.12 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DEFT` | 48 | $0.51 | $0.41 | $-4.88 | $935.45 | ▼ -4.88 after sell → book $7,943.41; vs 09:30 mark -0.41 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DLO` | 1 | $13.91 | $0.16 | $-0.90 | $949.20 | ▼ -0.90 after sell → book $7,943.25; vs 09:30 mark -0.16 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FJET` | 14 | $1.79 | $0.31 | $-3.86 | $973.95 | ▼ -3.86 after sell → book $7,942.94; vs 09:30 mark -0.31 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `PACS` | 3 | $41.27 | $1.27 | $-4.51 | $1,096.49 | ▼ -4.51 after sell → book $7,941.67; vs 09:30 mark -1.27 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `PGEN` | 2 | $7.61 | $0.18 | $-1.02 | $1,111.53 | ▼ -1.02 after sell → book $7,941.49; vs 09:30 mark -0.18 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,111.53 | ▼ close $7,825.00 vs 09:30 $7,943.94 (session -116.49) | 16:00 close · cash $1,111.53 · equity $7,825.00 vs 09:30 $7,943.94 (-118.94; session marks -116.49) · 8 name(s) marked open→close (per-name table). AEHL×74 09:30 $9.01 → close $8.23 -57.72; BLFS×17 09:30 $38.13 → close $39.15 +17.34; HALO×5 09:30 $113.34 → close $112.89 -2.25; MRVI×88 09:30 $7.49 → close $7.63 +12.32; OMER×32 09:30 $19.83 → close $19.43 -12.80; REGN×3 09:30 $771.84 → close $752.25 -58.76; TXG×8 09:30 $86.14 → close $88.50 +18.88; WRBY×25 09:30 $26.00 → close $24.66 -33.50 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,111.53 | ▼ 09:30 equity $7,799.55 vs yday $7,825.00 (-25.45) | 09:30 open · cash $1,111.53 (unchanged overnight, no fees) · equity $7,799.55 vs prior close $7,825.00 (-25.45) · 8 name(s) re-marked at the open (per-name table). AEHL×74 yday $8.23 → 09:30 $8.26 +1.85; BLFS×17 yday $39.15 → 09:30 $39.04 -1.87; HALO×5 yday $112.89 → 09:30 $112.89 +0.00; MRVI×88 yday $7.63 → 09:30 $7.52 -9.68; OMER×32 yday $19.43 → 09:30 $19.26 -5.44; REGN×3 yday $752.25 → 09:30 $745.85 -19.20; TXG×8 yday $88.50 → 09:30 $88.83 +2.64; WRBY×25 yday $24.66 → 09:30 $24.91 +6.25 | — |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 2 | $184.05 | $2.00 | — | $741.43 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.7; leftover $444.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 1 | $57.39 | $0.58 | — | $683.47 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+2.6; leftover $95.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 3 | $30.40 | $0.92 | — | $591.35 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+1.9; leftover $95.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 5 | $17.76 | $0.90 | — | $501.64 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+9.8; leftover $95.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 1 | $50.25 | $0.51 | — | $450.89 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+6.5; leftover $95.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 2 | $45.06 | $0.91 | — | $359.86 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.8; leftover $95.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SFIX` | 38 | $2.47 | $1.05 | — | $264.95 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-13.3; leftover $95.27 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ANNX` | 23 | $3.99 | $0.99 | — | $172.19 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=-5.0; leftover $95.27 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.19 | ▲ close $7,914.41 vs 09:30 $7,799.55 (session +122.71) | 16:00 close · cash $172.19 · equity $7,914.41 vs 09:30 $7,799.55 (+114.86; session marks +122.71) · 16 name(s) marked open→close (per-name table). AEHL×74 09:30 $8.26 → close $8.59 +24.79; BLFS×17 09:30 $39.04 → close $39.22 +3.06; HALO×5 09:30 $112.89 → close $111.56 -6.65; MRVI×88 09:30 $7.52 → close $7.65 +11.44; OMER×32 09:30 $19.26 → close $19.25 -0.32; REGN×3 09:30 $745.85 → close $750.51 +13.98; TXG×8 09:30 $88.83 → close $90.04 +9.68; WRBY×25 09:30 $24.91 → close $27.47 +64.00; SN×2 09:30 $184.05 → close $182.44 -3.22; DT×1 09:30 $57.39 → close $57.53 +0.14; TOST×3 09:30 $30.40 → close $30.46 +0.18; SONO×5 09:30 $17.76 → close $17.85 +0.45; PDFS×1 09:30 $50.25 → close $49.77 -0.48; SHOO×2 09:30 $45.06 → close $45.14 +0.16; SFIX×38 09:30 $2.47 → close $2.53 +2.28; ANNX×23 09:30 $3.99 → close $4.13 +3.22 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.19 | ▲ 09:30 equity $7,914.41 vs yday $7,914.41 (-0.00) | 09:30 open · cash $172.19 (unchanged overnight, no fees) · equity $7,914.41 vs prior close $7,914.41 (-0.00) · 16 name(s) re-marked at the open (per-name table). AEHL×74 yday $8.59 → 09:30 $8.59 +0.00; ANNX×23 yday $4.13 → 09:30 $4.13 +0.00; BLFS×17 yday $39.22 → 09:30 $39.22 +0.00; DT×1 yday $57.53 → 09:30 $57.53 +0.00; HALO×5 yday $111.56 → 09:30 $111.56 +0.00; MRVI×88 yday $7.65 → 09:30 $7.65 +0.00; OMER×32 yday $19.25 → 09:30 $19.25 +0.00; PDFS×1 yday $49.77 → 09:30 $49.77 +0.00; REGN×3 yday $750.51 → 09:30 $750.51 +0.00; SFIX×38 yday $2.53 → 09:30 $2.53 +0.00; SHOO×2 yday $45.14 → 09:30 $45.14 +0.00; SN×2 yday $182.44 → 09:30 $182.44 +0.00; SONO×5 yday $17.85 → 09:30 $17.85 +0.00; TOST×3 yday $30.46 → 09:30 $30.46 +0.00; TXG×8 yday $90.04 → 09:30 $90.04 +0.00; WRBY×25 yday $27.47 → 09:30 $27.47 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.19 | ▲ close $7,914.41 vs 09:30 $7,914.41 (session +0.00) | 16:00 close · cash $172.19 · equity $7,914.41 vs 09:30 $7,914.41 (-0.00; session marks +0.00) · 16 name(s) marked open→close (per-name table). AEHL×74 09:30 $8.59 → close $8.59 +0.00; ANNX×23 09:30 $4.13 → close $4.13 +0.00; BLFS×17 09:30 $39.22 → close $39.22 +0.00; DT×1 09:30 $57.53 → close $57.53 +0.00; HALO×5 09:30 $111.56 → close $111.56 +0.00; MRVI×88 09:30 $7.65 → close $7.65 +0.00; OMER×32 09:30 $19.25 → close $19.25 +0.00; PDFS×1 09:30 $49.77 → close $49.77 +0.00; REGN×3 09:30 $750.51 → close $750.51 +0.00; SFIX×38 09:30 $2.53 → close $2.53 +0.00; SHOO×2 09:30 $45.14 → close $45.14 +0.00; SN×2 09:30 $182.44 → close $182.44 +0.00; SONO×5 09:30 $17.85 → close $17.85 +0.00; TOST×3 09:30 $30.46 → close $30.46 +0.00; TXG×8 09:30 $90.04 → close $90.04 +0.00; WRBY×25 09:30 $27.47 → close $27.47 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.19 | ▲ 09:30 equity $8,024.83 vs yday $7,914.41 (+110.42) | 09:30 open · cash $172.19 (unchanged overnight, no fees) · equity $8,024.83 vs prior close $7,914.41 (+110.42) · 16 name(s) re-marked at the open (per-name table). AEHL×74 yday $8.59 → 09:30 $9.74 +85.10; ANNX×23 yday $4.13 → 09:30 $4.18 +1.15; BLFS×17 yday $39.22 → 09:30 $38.90 -5.44; DT×1 yday $57.53 → 09:30 $59.04 +1.51; HALO×5 yday $111.56 → 09:30 $109.93 -8.15; MRVI×88 yday $7.65 → 09:30 $7.67 +1.76; OMER×32 yday $19.25 → 09:30 $18.87 -12.16; PDFS×1 yday $49.77 → 09:30 $51.41 +1.64; REGN×3 yday $750.51 → 09:30 $766.53 +48.06; SFIX×38 yday $2.53 → 09:30 $2.55 +0.76; SHOO×2 yday $45.14 → 09:30 $44.46 -1.36; SN×2 yday $182.44 → 09:30 $182.44 +0.00; SONO×5 yday $17.85 → 09:30 $18.09 +1.20; TOST×3 yday $30.46 → 09:30 $29.05 -4.23; TXG×8 yday $90.04 → 09:30 $89.30 -5.92; WRBY×25 yday $27.47 → 09:30 $27.73 +6.50 | — |
| 2026-10-01 09:30 ET | **SELL** | `AEHL` | 74 | $9.74 | $2.23 | $+46.61 | $890.72 | ▲ +46.61 after sell → book $8,022.60; vs 09:30 mark -2.23 | exit unpriced hold on first bar after 4 sess | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `ANNX` | 23 | $4.18 | $1.05 | $+2.33 | $985.81 | ▲ +2.33 after sell → book $8,021.55; vs 09:30 mark -1.05 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `BLFS` | 17 | $38.90 | $2.06 | $+2.53 | $1,645.04 | ▲ +2.53 after sell → book $8,019.48; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DT` | 1 | $59.04 | $0.61 | $+0.46 | $1,703.47 | ▲ +0.46 after sell → book $8,018.87; vs 09:30 mark -0.61 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `HALO` | 5 | $109.93 | $2.02 | $-31.18 | $2,251.10 | ▼ -31.18 after sell → book $8,016.85; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MRVI` | 88 | $7.67 | $2.28 | $-2.77 | $2,923.78 | ▼ -2.77 after sell → book $8,014.57; vs 09:30 mark -2.28 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `OMER` | 32 | $18.87 | $2.11 | $-59.87 | $3,525.51 | ▼ -59.87 after sell → book $8,012.46; vs 09:30 mark -2.11 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 1 | $51.41 | $0.54 | $+0.12 | $3,576.38 | ▲ +0.12 after sell → book $8,011.92; vs 09:30 mark -0.54 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `REGN` | 3 | $766.53 | $2.03 | $-116.05 | $5,873.95 | ▼ -116.05 after sell → book $8,009.90; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SFIX` | 38 | $2.55 | $1.10 | $+0.88 | $5,969.74 | ▲ +0.88 after sell → book $8,008.79; vs 09:30 mark -1.11 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SHOO` | 2 | $44.46 | $0.92 | $-3.02 | $6,057.75 | ▼ -3.02 after sell → book $8,007.88; vs 09:30 mark -0.91 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 5 | $18.09 | $0.94 | $-0.19 | $6,147.26 | ▼ -0.19 after sell → book $8,006.94; vs 09:30 mark -0.94 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TOST` | 3 | $29.05 | $0.90 | $-5.87 | $6,233.51 | ▼ -5.87 after sell → book $8,006.04; vs 09:30 mark -0.90 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `TXG` | 8 | $89.30 | $2.03 | $+40.27 | $6,945.87 | ▲ +40.27 after sell → book $8,004.00; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `WRBY` | 25 | $27.73 | $2.08 | $+32.35 | $7,637.04 | ▲ +32.35 after sell → book $8,001.92; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `IT` | 15 | $196.19 | $2.04 | — | $4,692.15 | — | 40% to #1, rest split; list flatten; ret5=+4.5; leftover $3054.82 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 8 | $92.93 | $2.01 | — | $3,946.70 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+0.2; leftover $763.70 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 19 | $38.99 | $2.05 | — | $3,203.84 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-2.4; leftover $763.70 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 53 | $14.27 | $2.15 | — | $2,445.38 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $763.70 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 35 | $21.28 | $2.10 | — | $1,698.49 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.7; leftover $763.70 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 688 | $1.11 | $8.88 | — | $925.93 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; leftover $763.70 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IVA` | 226 | $3.38 | $2.92 | — | $160.27 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=+7.0; leftover $763.70 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.27 | ▼ close $7,978.39 vs 09:30 $8,024.83 (session -1.40) | 16:00 close · cash $160.27 · equity $7,978.39 vs 09:30 $8,024.83 (-46.44; session marks -1.40) · 8 name(s) marked open→close (per-name table). SN×2 09:30 $182.44 → close $182.44 +0.00; IT×15 09:30 $196.19 → close $192.80 -50.85; KSPI×8 09:30 $92.93 → close $92.02 -7.28; IOT×19 09:30 $38.99 → close $40.04 +19.95; AVPT×53 09:30 $14.27 → close $14.08 -10.07; RELY×35 09:30 $21.28 → close $21.48 +7.00; TLSA×688 09:30 $1.11 → close $1.14 +20.64; IVA×226 09:30 $3.38 → close $3.46 +19.21 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.27 | ▲ 09:30 equity $8,044.26 vs yday $7,978.39 (+65.87) | 09:30 open · cash $160.27 (unchanged overnight, no fees) · equity $8,044.26 vs prior close $7,978.39 (+65.87) · 8 name(s) re-marked at the open (per-name table). AVPT×53 yday $14.08 → 09:30 $14.22 +7.42; IOT×19 yday $40.04 → 09:30 $40.41 +7.03; IT×15 yday $192.80 → 09:30 $192.74 -0.90; IVA×226 yday $3.46 → 09:30 $3.57 +24.86; KSPI×8 yday $92.02 → 09:30 $92.05 +0.24; RELY×35 yday $21.48 → 09:30 $21.80 +11.20; SN×2 yday $182.44 → 09:30 $183.57 +2.26; TLSA×688 yday $1.14 → 09:30 $1.16 +13.76 | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 2 | $183.57 | $2.02 | $-4.97 | $525.39 | ▼ -4.97 after sell → book $8,042.24; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 1 | $27.63 | $0.28 | — | $497.48 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.6; leftover $45.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `BLFS` | 1 | $37.02 | $0.37 | — | $460.09 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-4.7; leftover $45.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `QNC` | 26 | $1.69 | $0.52 | — | $415.63 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-7.9; leftover $45.03 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $415.63 | ▼ close $7,909.76 vs 09:30 $8,044.26 (session -131.31) | 16:00 close · cash $415.63 · equity $7,909.76 vs 09:30 $8,044.26 (-134.50; session marks -131.31) · 10 name(s) marked open→close (per-name table). AVPT×53 09:30 $14.22 → close $14.07 -7.95; IOT×19 09:30 $40.41 → close $41.12 +13.49; IT×15 09:30 $192.74 → close $184.80 -119.10; IVA×226 09:30 $3.57 → close $3.56 -2.26; KSPI×8 09:30 $92.05 → close $94.05 +16.00; RELY×35 09:30 $21.80 → close $21.77 -1.05; TLSA×688 09:30 $1.16 → close $1.12 -27.52; WRBY×1 09:30 $27.63 → close $27.03 -0.60; BLFS×1 09:30 $37.02 → close $37.30 +0.28; QNC×26 09:30 $1.69 → close $1.59 -2.60 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $415.63 | ▲ 09:30 equity $7,916.89 vs yday $7,909.76 (+7.13) | 09:30 open · cash $415.63 (unchanged overnight, no fees) · equity $7,916.89 vs prior close $7,909.76 (+7.13) · 10 name(s) re-marked at the open (per-name table). AVPT×53 yday $14.07 → 09:30 $13.91 -8.48; BLFS×1 yday $37.30 → 09:30 $37.16 -0.14; IOT×19 yday $41.12 → 09:30 $42.00 +16.72; IT×15 yday $184.80 → 09:30 $185.07 +4.05; IVA×226 yday $3.56 → 09:30 $3.52 -9.04; KSPI×8 yday $94.05 → 09:30 $94.50 +3.60; QNC×26 yday $1.59 → 09:30 $1.62 +0.78; RELY×35 yday $21.77 → 09:30 $21.76 -0.35; TLSA×688 yday $1.12 → 09:30 $1.12 +0.00; WRBY×1 yday $27.03 → 09:30 $27.02 -0.01 | — |
| 2026-10-05 09:30 ET | **BUY** | `COP` | 1 | $127.00 | $1.27 | — | $287.36 | — | 40% to #1, rest split; list flatten; ret5=-0.4; leftover $166.25 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SM` | 1 | $35.27 | $0.36 | — | $251.73 | — | 40% to #1, rest split; list flatten; ret5=+3.9; leftover $35.63 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `GPRK` | 3 | $10.87 | $0.34 | — | $218.79 | — | 40% to #1, rest split; list flatten; ret5=-3.1; leftover $35.63 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 3 | $10.26 | $0.32 | — | $187.69 | — | 40% to #1, rest split; list flatten; ret5=-2.7; leftover $35.63 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.69 | ▲ close $7,999.50 vs 09:30 $7,916.89 (session +84.89) | 16:00 close · cash $187.69 · equity $7,999.50 vs 09:30 $7,916.89 (+82.61; session marks +84.89) · 14 name(s) marked open→close (per-name table). AVPT×53 09:30 $13.91 → close $14.66 +39.75; BLFS×1 09:30 $37.16 → close $38.61 +1.45; IOT×19 09:30 $42.00 → close $42.39 +7.41; IT×15 09:30 $185.07 → close $187.76 +40.35; IVA×226 09:30 $3.52 → close $3.48 -9.04; KSPI×8 09:30 $94.50 → close $93.98 -4.16; QNC×26 09:30 $1.62 → close $1.64 +0.52; RELY×35 09:30 $21.76 → close $22.91 +40.25; TLSA×688 09:30 $1.12 → close $1.07 -34.40; WRBY×1 09:30 $27.02 → close $26.65 -0.37; COP×1 09:30 $127.00 → close $128.40 +1.40; SM×1 09:30 $35.27 → close $35.05 -0.22; GPRK×3 09:30 $10.87 → close $11.28 +1.23; OBE×3 09:30 $10.26 → close $10.50 +0.72 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.69 | ▲ 09:30 equity $8,051.12 vs yday $7,999.50 (+51.62) | 09:30 open · cash $187.69 (unchanged overnight, no fees) · equity $8,051.12 vs prior close $7,999.50 (+51.62) · 14 name(s) re-marked at the open (per-name table). AVPT×53 yday $14.66 → 09:30 $14.77 +5.83; BLFS×1 yday $38.61 → 09:30 $38.61 +0.00; COP×1 yday $128.40 → 09:30 $128.19 -0.21; GPRK×3 yday $11.28 → 09:30 $11.32 +0.12; IOT×19 yday $42.39 → 09:30 $42.70 +5.89; IT×15 yday $187.76 → 09:30 $188.47 +10.65; IVA×226 yday $3.48 → 09:30 $3.53 +11.30; KSPI×8 yday $93.98 → 09:30 $94.50 +4.16; OBE×3 yday $10.50 → 09:30 $10.44 -0.18; QNC×26 yday $1.64 → 09:30 $1.63 -0.26; RELY×35 yday $22.91 → 09:30 $23.12 +7.35; SM×1 yday $35.05 → 09:30 $34.90 -0.15; TLSA×688 yday $1.07 → 09:30 $1.08 +6.88; WRBY×1 yday $26.65 → 09:30 $26.89 +0.24 | — |
| 2026-10-06 09:30 ET | **SELL** | `IOT` | 19 | $42.70 | $2.07 | $+66.38 | $996.92 | ▲ +66.38 after sell → book $8,049.05; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `IT` | 15 | $188.47 | $2.07 | $-119.90 | $3,821.91 | ▼ -119.90 after sell → book $8,046.99; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `IVA` | 226 | $3.53 | $2.96 | $+29.15 | $4,616.72 | ▲ +29.15 after sell → book $8,044.02; vs 09:30 mark -2.97 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `KSPI` | 8 | $94.50 | $2.03 | $+8.51 | $5,370.69 | ▲ +8.51 after sell → book $8,041.99; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `RELY` | 35 | $23.12 | $2.12 | $+60.19 | $6,177.77 | ▲ +60.19 after sell → book $8,039.87; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `TLSA` | 688 | $1.08 | $9.00 | $-38.51 | $6,911.81 | ▼ -38.51 after sell → book $8,030.87; vs 09:30 mark -9.00 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **BUY** | `CDNA` | 39 | $70.89 | $2.11 | — | $4,145.00 | — | 40% to #1, rest split; list flatten,ohlc_hot; ret5=+8.1; leftover $2764.73 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `SN` | 3 | $183.47 | $2.00 | — | $3,592.59 | — | 40% to #1, rest split; list flatten; ret5=+0.4; leftover $691.18 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TOST` | 22 | $30.07 | $2.06 | — | $2,928.99 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-1.1; leftover $691.18 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `NTAP` | 3 | $224.80 | $2.00 | — | $2,252.59 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+9.5; leftover $691.18 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PDFS` | 12 | $56.89 | $2.03 | — | $1,567.89 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+12.7; leftover $691.18 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `EBS` | 96 | $7.16 | $2.28 | — | $878.25 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.5; leftover $691.18 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 47 | $14.70 | $2.13 | — | $185.22 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=+1.4; leftover $691.18 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.22 | ▼ close $7,620.64 vs 09:30 $8,051.12 (session -395.64) | 16:00 close · cash $185.22 · equity $7,620.64 vs 09:30 $8,051.12 (-430.48; session marks -395.64) · 15 name(s) marked open→close (per-name table). AVPT×53 09:30 $14.77 → close $14.59 -9.54; BLFS×1 09:30 $38.61 → close $38.61 +0.00; COP×1 09:30 $128.19 → close $129.35 +1.16; GPRK×3 09:30 $11.32 → close $11.44 +0.36; OBE×3 09:30 $10.44 → close $10.62 +0.54; QNC×26 09:30 $1.63 → close $1.63 +0.00; SM×1 09:30 $34.90 → close $35.10 +0.20; WRBY×1 09:30 $26.89 → close $26.23 -0.66; CDNA×39 09:30 $70.89 → close $63.79 -276.90; SN×3 09:30 $183.47 → close $184.72 +3.75; TOST×22 09:30 $30.07 → close $30.25 +3.96; NTAP×3 09:30 $224.80 → close $228.45 +10.95; PDFS×12 09:30 $56.89 → close $54.44 -29.40; EBS×96 09:30 $7.16 → close $6.95 -20.16; ARCT×47 09:30 $14.70 → close $13.00 -79.90 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.22 | ▼ 09:30 equity $7,500.26 vs yday $7,620.64 (-120.38) | 09:30 open · cash $185.22 (unchanged overnight, no fees) · equity $7,500.26 vs prior close $7,620.64 (-120.38) · 15 name(s) re-marked at the open (per-name table). ARCT×47 yday $13.00 → 09:30 $12.71 -13.63; AVPT×53 yday $14.59 → 09:30 $14.51 -4.24; BLFS×1 yday $38.61 → 09:30 $38.61 +0.00; CDNA×39 yday $63.79 → 09:30 $61.38 -93.91; COP×1 yday $129.35 → 09:30 $131.29 +1.94; EBS×96 yday $6.95 → 09:30 $7.05 +9.60; GPRK×3 yday $11.44 → 09:30 $11.50 +0.18; NTAP×3 yday $228.45 → 09:30 $232.00 +10.65; OBE×3 yday $10.62 → 09:30 $10.72 +0.30; PDFS×12 yday $54.44 → 09:30 $52.54 -22.80; QNC×26 yday $1.63 → 09:30 $1.60 -0.78; SM×1 yday $35.10 → 09:30 $35.46 +0.36; SN×3 yday $184.72 → 09:30 $183.00 -5.16; TOST×22 yday $30.25 → 09:30 $30.13 -2.64; WRBY×1 yday $26.23 → 09:30 $25.98 -0.25 | — |
| 2026-10-07 09:30 ET | **SELL** | `AVPT` | 53 | $14.51 | $2.17 | $+8.40 | $952.08 | ▲ +8.40 after sell → book $7,498.09; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-10-07 09:30 ET | **SELL** | `QNC` | 26 | $1.60 | $0.51 | $-3.37 | $993.17 | ▼ -3.37 after sell → book $7,497.57; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **SELL** | `WRBY` | 1 | $25.98 | $0.28 | $-2.21 | $1,018.86 | ▼ -2.21 after sell → book $7,497.29; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,018.86 | ▲ close $7,598.04 vs 09:30 $7,500.26 (session +100.74) | 16:00 close · cash $1,018.86 · equity $7,598.04 vs 09:30 $7,500.26 (+97.78; session marks +100.74) · 12 name(s) marked open→close (per-name table). ARCT×47 09:30 $12.71 → close $12.77 +2.58; BLFS×1 09:30 $38.61 → close $38.61 +0.00; CDNA×39 09:30 $61.38 → close $63.04 +64.66; COP×1 09:30 $131.29 → close $129.84 -1.45; EBS×96 09:30 $7.05 → close $7.07 +1.92; GPRK×3 09:30 $11.50 → close $11.11 -1.17; NTAP×3 09:30 $232.00 → close $235.77 +11.31; OBE×3 09:30 $10.72 → close $10.63 -0.27; PDFS×12 09:30 $52.54 → close $53.17 +7.56; SM×1 09:30 $35.46 → close $35.25 -0.21; SN×3 09:30 $183.00 → close $185.19 +6.57; TOST×22 09:30 $30.13 → close $30.55 +9.24 | — |

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
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 2.30 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 0.86 < 1 share @ 14.42 |
| 2026-08-27 | `SLI` | cash | leftover split 0.86 < 1 share @ 2.60 |
| 2026-08-27 | `KURA` | cash | leftover split 0.86 < 1 share @ 12.98 |
| 2026-08-27 | `ABX` | cash | leftover split 0.86 < 1 share @ 9.68 |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 95.69 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 20.51 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 20.51 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 20.51 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 66.47 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 14.24 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 14.24 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 14.24 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 14.24 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 14.24 < 1 share @ 34.44 |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 21.55 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 108.63 < 1 share @ 116.85 |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 37 | 2026-09-22 @ $0.58 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $21.55 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+7.0; leftover $40.74 |
| `ADMA` | 4 | 2026-09-23 @ $9.81 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+4.0; leftover $40.74 |
| `FTRE` | 2 | 2026-09-23 @ $20.25 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+15.0; leftover $40.74 |
| `OMER` | 1 | 2026-09-23 @ $20.65 | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $40.74 |
