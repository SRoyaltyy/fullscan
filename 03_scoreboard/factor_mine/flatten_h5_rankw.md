# Factor mine action — `flatten_h5_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **-16.49%** ($8,351) · signal-only (no cash/fees) was +4.47%. Starts YES **4/30**. Fills 118 · skips 337 · realized $+263.83.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $231.03.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $2222.22 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1944.44 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1388.89 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $555.56 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $277.78 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.05 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +139.38) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $118.95 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $10.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $112.30 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $7.11 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $109.27 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $3.56 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.27 | ▲ close $10,260.71 vs 09:30 $10,103.42 (session +157.50) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.27 | ▲ 09:30 equity $10,281.18 vs yday $10,260.71 (+20.47) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $96.99 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $15.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $88.44 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $12.14 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $85.16 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $6.07 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.16 | ▲ close $10,290.17 vs 09:30 $10,281.18 (session +9.25) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▼ 09:30 equity $10,157.73 vs yday $10,290.17 (-132.44) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.16 | ▲ close $10,194.81 vs 09:30 $10,157.73 (session +37.08) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▲ 09:30 equity $10,296.33 vs yday $10,194.81 (+101.52) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.16 | ▲ close $10,540.20 vs 09:30 $10,296.33 (session +243.87) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▼ 09:30 equity $10,477.31 vs yday $10,540.20 (-62.89) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 37 | $58.64 | $2.13 | $-47.15 | $2,252.71 | ▼ -47.15 after sell → book $10,475.18; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 42 | $42.46 | $2.14 | $-152.10 | $4,033.89 | ▼ -152.10 after sell → book $10,473.04; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 32 | $53.06 | $2.11 | $+73.78 | $5,729.70 | ▲ +73.78 after sell → book $10,470.93; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 27 | $51.65 | $2.09 | $+48.49 | $7,122.16 | ▲ +48.49 after sell → book $10,468.84; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $8,420.82 | ▲ +196.59 after sell → book $10,466.54; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $9,277.21 | ▲ +21.59 after sell → book $10,464.45; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 685 | $1.30 | $8.96 | $+319.09 | $10,158.75 | ▲ +319.09 after sell → book $10,455.49; vs 09:30 mark -8.96 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 11 | $23.11 | $2.04 | $-6.49 | $10,410.92 | ▼ -6.49 after sell → book $10,453.44; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 112 | $20.55 | $2.33 | — | $8,106.99 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $2313.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 22 | $91.01 | $2.06 | — | $6,102.72 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $2024.35 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 84 | $20.65 | $2.24 | — | $4,365.87 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1735.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 250 | $5.77 | $3.23 | — | $2,920.15 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1445.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,779.44 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1156.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $918.10 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $867.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 330 | $1.75 | $4.26 | — | $336.34 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $578.38 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $45.26 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $289.19 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.26 | ▲ close $10,666.78 vs 09:30 $10,477.31 (session +233.68) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.26 | ▲ 09:30 equity $10,954.91 vs yday $10,666.78 (+288.13) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $56.82 | ▲ +2.46 after sell → book $10,954.77; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $62.79 | ▼ -0.68 after sell → book $10,954.67; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 2 | $1.66 | $0.06 | $+0.22 | $66.05 | ▲ +0.22 after sell → book $10,954.61; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 2 | $2.47 | $0.06 | — | $61.06 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $7.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 2 | $1.93 | $0.04 | — | $57.15 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $5.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1 | $1.32 | $0.02 | — | $55.82 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $1.83 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.82 | ▼ close $10,857.48 vs 09:30 $10,954.91 (session -97.02) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.82 | ▲ 09:30 equity $10,958.31 vs yday $10,857.48 (+100.83) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 3 | $4.62 | $0.17 | $+1.43 | $69.52 | ▲ +1.43 after sell → book $10,958.14; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $78.67 | ▲ +0.60 after sell → book $10,958.03; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $82.11 | ▲ +0.17 after sell → book $10,957.97; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.11 | ▼ close $10,864.13 vs 09:30 $10,958.31 (session -93.84) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.11 | ▼ 09:30 equity $10,686.43 vs yday $10,864.13 (-177.70) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $71.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $19.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $62.58 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $11.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $4.94 | $0.05 | — | $57.59 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $7.82 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.59 | ▲ close $11,081.10 vs 09:30 $10,686.43 (session +394.92) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.59 | ▼ 09:30 equity $10,869.39 vs yday $11,081.10 (-211.71) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.59 | ▼ close $10,811.60 vs 09:30 $10,869.39 (session -57.79) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.59 | ▲ 09:30 equity $10,825.11 vs yday $10,811.60 (+13.51) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 112 | $20.93 | $2.36 | $+37.87 | $2,399.38 | ▲ +37.87 after sell → book $10,822.74; vs 09:30 mark -2.37 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 22 | $95.52 | $2.08 | $+95.08 | $4,498.74 | ▲ +95.08 after sell → book $10,820.66; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 84 | $21.31 | $2.27 | $+50.93 | $6,286.51 | ▲ +50.93 after sell → book $10,818.39; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 250 | $5.49 | $3.28 | $-76.50 | $7,655.73 | ▼ -76.50 after sell → book $10,815.11; vs 09:30 mark -3.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 58 | $21.47 | $2.18 | $+102.37 | $8,898.81 | ▲ +102.37 after sell → book $10,812.93; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 29 | $32.32 | $2.10 | $+73.84 | $9,833.99 | ▲ +73.84 after sell → book $10,810.83; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 330 | $1.91 | $4.32 | $+44.22 | $10,459.97 | ▲ +44.22 after sell → book $10,806.51; vs 09:30 mark -4.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 2 | $155.89 | $2.02 | $+18.69 | $10,769.74 | ▲ +18.69 after sell → book $10,804.50; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 103 | $41.44 | $2.30 | — | $6,499.12 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $4307.89 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 224 | $14.42 | $2.89 | — | $3,266.15 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $3230.92 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 89 | $24.00 | $2.26 | — | $1,127.89 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.7; leftover $2153.95 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 414 | $2.60 | $5.34 | — | $46.15 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $1076.97 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.15 | ▲ close $10,852.39 vs 09:30 $10,825.11 (session +60.68) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.15 | ▲ 09:30 equity $10,898.40 vs yday $10,852.39 (+46.01) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 2 | $2.35 | $0.07 | $-0.37 | $50.78 | ▼ -0.37 after sell → book $10,898.33; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 2 | $2.06 | $0.07 | $+0.15 | $54.83 | ▲ +0.15 after sell → book $10,898.26; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 1 | $1.82 | $0.04 | $+0.44 | $56.61 | ▲ +0.44 after sell → book $10,898.22; vs 09:30 mark -0.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.61 | ▼ close $10,707.33 vs 09:30 $10,898.40 (session -190.89) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.61 | ▲ 09:30 equity $10,838.42 vs yday $10,707.33 (+131.09) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.61 | ▼ close $10,820.39 vs 09:30 $10,838.42 (session -18.03) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.61 | ▲ 09:30 equity $11,179.46 vs yday $10,820.39 (+359.07) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $66.90 | ▼ -0.80 after sell → book $11,179.33; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $75.04 | ▼ -0.29 after sell → book $11,179.22; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.64 | $0.07 | $-0.42 | $79.62 | ▼ -0.42 after sell → book $11,179.16; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.62 | ▲ close $11,271.58 vs 09:30 $11,179.46 (session +92.42) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.62 | ▼ 09:30 equity $11,161.88 vs yday $11,271.58 (-109.70) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.62 | ▲ close $11,228.18 vs 09:30 $11,161.88 (session +66.30) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.62 | ▲ 09:30 equity $11,266.25 vs yday $11,228.18 (+38.07) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 103 | $42.43 | $2.35 | $+97.32 | $4,447.55 | ▲ +97.32 after sell → book $11,263.89; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 224 | $15.45 | $2.95 | $+224.88 | $7,905.40 | ▲ +224.88 after sell → book $11,260.94; vs 09:30 mark -2.95 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 89 | $26.12 | $2.29 | $+184.13 | $10,227.79 | ▲ +184.13 after sell → book $11,258.65; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 414 | $2.49 | $5.42 | $-56.30 | $11,253.23 | ▼ -56.30 after sell → book $11,253.23; vs 09:30 mark -5.42 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 70 | $52.88 | $2.20 | — | $7,549.43 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $3751.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 69 | $42.93 | $2.20 | — | $4,585.06 | — | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $3000.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 620 | $3.63 | $8.00 | — | $2,326.47 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2250.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 186 | $8.03 | $2.55 | — | $830.34 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $1500.43 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $132.45 | $2.00 | — | $166.08 | — | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $750.22 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.08 | ▼ close $11,021.65 vs 09:30 $11,266.25 (session -214.63) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.08 | ▼ 09:30 equity $10,938.29 vs yday $11,021.65 (-83.36) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 18 | $2.52 | $0.51 | — | $120.21 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $47.45 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 5 | $6.71 | $0.35 | — | $86.31 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $39.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 16 | $1.90 | $0.35 | — | $55.56 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $31.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $4.78 | $0.20 | — | $36.24 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $23.73 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 9 | $1.59 | $0.17 | — | $21.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $15.82 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.76 | ▲ close $11,012.75 vs 09:30 $10,938.29 (session +76.04) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.76 | ▲ 09:30 equity $11,169.12 vs yday $11,012.75 (+156.37) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.76 | ▼ close $10,990.13 vs 09:30 $11,169.12 (session -178.99) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.76 | ▼ 09:30 equity $10,932.42 vs yday $10,990.13 (-57.71) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.76 | ▼ close $10,637.03 vs 09:30 $10,932.42 (session -295.39) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.76 | ▼ 09:30 equity $10,511.76 vs yday $10,637.03 (-125.27) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.76 | ▼ close $10,410.96 vs 09:30 $10,511.76 (session -100.80) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.76 | ▲ 09:30 equity $10,507.11 vs yday $10,410.96 (+96.15) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 70 | $53.53 | $2.24 | $+41.06 | $3,766.62 | ▲ +41.06 after sell → book $10,504.87; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 69 | $41.30 | $2.23 | $-116.90 | $6,614.09 | ▼ -116.90 after sell → book $10,502.64; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 620 | $2.77 | $8.11 | $-549.31 | $8,323.37 | ▼ -549.31 after sell → book $10,494.52; vs 09:30 mark -8.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 186 | $7.70 | $2.59 | $-66.52 | $9,752.98 | ▼ -66.52 after sell → book $10,491.93; vs 09:30 mark -2.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 5 | $122.40 | $2.02 | $-54.28 | $10,362.96 | ▼ -54.28 after sell → book $10,489.91; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 181 | $16.28 | $2.53 | — | $7,413.74 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $2960.84 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 903 | $2.73 | $11.65 | — | $4,936.90 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $2467.37 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 9 | $206.84 | $2.02 | — | $3,073.33 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1973.90 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $1,591.44 | — | rank-weighted leftover; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1480.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $642.75 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $986.95 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 8 | $56.09 | $2.01 | — | $192.02 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $493.47 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.02 | ▼ close $10,388.70 vs 09:30 $10,507.11 (session -78.97) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.02 | ▼ 09:30 equity $10,156.95 vs yday $10,388.70 (-231.75) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 18 | $2.15 | $0.46 | $-7.63 | $230.26 | ▼ -7.63 after sell → book $10,156.49; vs 09:30 mark -0.46 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 5 | $5.93 | $0.33 | $-4.58 | $259.58 | ▼ -4.58 after sell → book $10,156.16; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 16 | $1.72 | $0.34 | $-3.65 | $286.67 | ▼ -3.65 after sell → book $10,155.82; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 4 | $4.13 | $0.20 | $-3.00 | $303.00 | ▼ -3.00 after sell → book $10,155.62; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 9 | $1.59 | $0.19 | $-0.36 | $317.12 | ▼ -0.36 after sell → book $10,155.43; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.12 | ▼ close $10,108.62 vs 09:30 $10,156.95 (session -46.81) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.12 | ▲ 09:30 equity $10,132.96 vs yday $10,108.62 (+24.34) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.12 | ▼ close $9,951.98 vs 09:30 $10,132.96 (session -180.98) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.12 | ▲ 09:30 equity $10,009.13 vs yday $9,951.98 (+57.15) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 1 | $77.12 | $0.77 | — | $239.22 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $95.13 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 4 | $14.31 | $0.58 | — | $181.40 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $63.42 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.40 | ▲ close $10,148.59 vs 09:30 $10,009.13 (session +140.82) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.40 | ▲ 09:30 equity $10,333.70 vs yday $10,148.59 (+185.11) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $160.69 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $25.91 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $145.35 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $17.28 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.35 | ▼ close $10,290.48 vs 09:30 $10,333.70 (session -42.85) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.35 | ▲ 09:30 equity $10,295.91 vs yday $10,290.48 (+5.43) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 181 | $16.93 | $2.59 | $+112.53 | $3,207.09 | ▲ +112.53 after sell → book $10,293.32; vs 09:30 mark -2.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 903 | $2.68 | $11.82 | $-68.62 | $5,615.31 | ▼ -68.62 after sell → book $10,281.50; vs 09:30 mark -11.82 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 9 | $197.76 | $2.04 | $-85.78 | $7,393.11 | ▼ -85.78 after sell → book $10,279.46; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 9 | $150.47 | $2.04 | $-129.69 | $8,745.30 | ▼ -129.69 after sell → book $10,277.42; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 6 | $152.71 | $2.03 | $-34.46 | $9,659.54 | ▼ -34.46 after sell → book $10,275.40; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 8 | $55.80 | $2.03 | $-6.37 | $10,103.90 | ▼ -6.37 after sell → book $10,273.36; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 26 | $108.55 | $2.07 | — | $7,279.53 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $2886.83 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 4 | $593.15 | $2.00 | — | $4,904.93 | — | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $2405.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 9 | $209.52 | $2.02 | — | $3,017.24 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1924.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $1,697.51 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1443.41 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 11 | $85.00 | $2.02 | — | $760.48 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $962.28 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 13 | $34.44 | $2.03 | — | $310.74 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $481.14 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.74 | ▼ close $10,084.02 vs 09:30 $10,295.91 (session -177.20) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.74 | ▲ 09:30 equity $10,233.68 vs yday $10,084.02 (+149.66) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 3 | $13.47 | $0.41 | — | $269.91 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $41.43 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 5 | $4.00 | $0.21 | — | $249.70 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $20.72 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.70 | ▲ close $10,284.45 vs 09:30 $10,233.68 (session +51.40) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $249.70 | ▼ 09:30 equity $10,262.94 vs yday $10,284.45 (-21.51) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.70 | ▼ close $10,181.44 vs 09:30 $10,262.94 (session -81.50) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $249.70 | ▲ 09:30 equity $10,426.23 vs yday $10,181.44 (+244.79) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 1 | $73.61 | $0.76 | $-5.04 | $322.55 | ▼ -5.04 after sell → book $10,425.47; vs 09:30 mark -0.76 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 4 | $13.12 | $0.56 | $-5.90 | $374.47 | ▼ -5.90 after sell → book $10,424.92; vs 09:30 mark -0.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 1 | $89.50 | $0.90 | — | $284.07 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.3; leftover $93.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 1 | $27.79 | $0.28 | — | $256.00 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $53.50 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 4 | $9.81 | $0.40 | — | $216.36 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $40.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 1 | $20.25 | $0.21 | — | $195.90 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $26.75 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.90 | ▲ close $10,502.94 vs 09:30 $10,426.23 (session +79.81) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.90 | ▼ 09:30 equity $10,312.85 vs yday $10,502.94 (-190.09) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 2 | $10.39 | $0.23 | $-0.16 | $216.45 | ▼ -0.16 after sell → book $10,312.62; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $231.03 | ▼ -0.75 after sell → book $10,312.44; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $231.03 | ▲ close $10,335.11 vs 09:30 $10,312.85 (session +22.68) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $325.08 | ▼ 09:30 equity $8,369.25 vs yday $8,369.67 (-0.42) | 09:30 open · cash $325.08 (unchanged overnight, no fees) · equity $8,369.25 vs prior close $8,369.67 (-0.42) · 11 name(s) re-marked at the open (per-name table). A×10 yday $172.84 → 09:30 $171.98 -8.60; ADMA×91 yday $9.52 → 09:30 $9.52 +0.00; ARQT×42 yday $26.27 → 09:30 $26.27 +0.00; CYPH×3 yday $4.08 → 09:30 $4.00 -0.22; DLO×1 yday $13.88 → 09:30 $13.88 +0.00; DXCM×23 yday $87.47 → 09:30 $87.47 +0.00; FTRE×29 yday $20.02 → 09:30 $20.02 +0.00; HALO×12 yday $115.22 → 09:30 $115.36 +1.68; MGTX×2 yday $11.05 → 09:30 $11.05 +0.00; OMER×14 yday $20.13 → 09:30 $20.61 +6.72; PACS×1 yday $41.46 → 09:30 $41.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 2 | $38.51 | $0.78 | — | $247.28 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $108.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 7 | $7.65 | $0.56 | — | $193.18 | — | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $54.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.18 | ▼ close $8,351.03 vs 09:30 $8,369.25 (session -16.88) | 16:00 close · cash $193.18 · equity $8,351.03 vs 09:30 $8,369.25 (-18.22; session marks -16.88) · 13 name(s) marked open→close (per-name table). A×10 09:30 $171.98 → close $172.79 +8.10; ADMA×91 09:30 $9.52 → close $9.52 +0.00; ARQT×42 09:30 $26.27 → close $26.27 +0.00; CYPH×3 09:30 $4.00 → close $4.12 +0.35; DLO×1 09:30 $13.88 → close $13.88 +0.00; DXCM×23 09:30 $87.47 → close $87.47 +0.00; FTRE×29 09:30 $20.02 → close $20.02 +0.00; HALO×12 09:30 $115.36 → close $113.90 -17.52; MGTX×2 09:30 $11.05 → close $11.05 +0.00; OMER×14 09:30 $20.61 → close $20.08 -7.42; PACS×1 09:30 $41.46 → close $41.46 -0.00; BLFS×2 09:30 $38.51 → close $38.49 -0.04; MRVI×7 09:30 $7.65 → close $7.60 -0.35 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
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
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
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
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 14.68 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 12.84 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 11.01 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.17 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 3.67 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 23.46 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 15.64 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 3.91 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 28.79 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 19.20 < 1 share @ 24.84 |
| 2026-08-26 | `INSP` | cash | leftover split 9.60 < 1 share @ 60.07 |
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
| 2026-09-04 | `VIR` | cash | leftover split 7.91 < 1 share @ 11.31 |
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
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-16 | `IQV` | cash | leftover split 126.85 < 1 share @ 270.89 |
| 2026-09-16 | `BLFS` | cash | leftover split 31.71 < 1 share @ 36.46 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 51.83 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 43.19 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 34.55 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 8.64 < 1 share @ 34.93 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 103.58 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 82.86 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 62.15 < 1 share @ 88.83 |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 35.67 < 1 share @ 93.97 |
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
| 2026-09-23 | `A` | cash | leftover split 80.24 < 1 share @ 166.54 |
| 2026-09-23 | `HALO` | cash | leftover split 66.87 < 1 share @ 116.85 |
| 2026-09-23 | `OMER` | cash | leftover split 13.37 < 1 share @ 20.65 |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 26 | 2026-09-18 @ $108.55 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $2886.83 |
| `DELL` | 4 | 2026-09-18 @ $593.15 | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $2405.69 |
| `GNRC` | 9 | 2026-09-18 @ $209.52 | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1924.55 |
| `VICR` | 6 | 2026-09-18 @ $219.62 | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1443.41 |
| `ECO` | 11 | 2026-09-18 @ $85.00 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $962.28 |
| `FIVN` | 13 | 2026-09-18 @ $34.44 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $481.14 |
| `MGTX` | 3 | 2026-09-21 @ $13.47 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $41.43 |
| `CYPH` | 5 | 2026-09-21 @ $4.00 | rank-weighted leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $20.72 |
| `DXCM` | 1 | 2026-09-23 @ $89.50 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.3; leftover $93.62 |
| `ARQT` | 1 | 2026-09-23 @ $27.79 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $53.50 |
| `ADMA` | 4 | 2026-09-23 @ $9.81 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $40.12 |
| `FTRE` | 1 | 2026-09-23 @ $20.25 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $26.75 |
