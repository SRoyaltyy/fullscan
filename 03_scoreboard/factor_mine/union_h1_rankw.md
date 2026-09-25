# Factor mine action — `union_h1_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **-11.05%** ($8,895) · signal-only (no cash/fees) was -0.80%. Starts YES **0/30**. Fills 248 · skips 107 · realized $-24.70.

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
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,975.22.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $2222.22 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=+12.3; leftover $1944.44 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1388.89 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $833.33 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.2; leftover $555.56 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; ⚪; ret5=+19.7; leftover $277.78 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.05 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +139.38) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 37 | $59.65 | $2.13 | $-9.78 | $2,332.97 | ▼ -9.78 after sell → book $10,101.29; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 42 | $44.09 | $2.14 | $-83.64 | $4,182.61 | ▼ -83.64 after sell → book $10,099.15; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,949.78 | ▲ +145.14 after sell → book $10,097.04; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 27 | $47.27 | $2.09 | $-69.77 | $7,223.98 | ▼ -69.77 after sell → book $10,094.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $8,387.28 | ▲ +61.23 after sell → book $10,092.65; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $9,201.39 | ▼ -20.69 after sell → book $10,090.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 685 | $0.93 | $8.55 | $+66.05 | $9,829.89 | ▲ +66.05 after sell → book $10,082.01; vs 09:30 mark -8.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 11 | $22.92 | $2.04 | $-8.58 | $10,079.97 | ▼ -8.58 after sell → book $10,079.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 6 | $359.83 | $2.01 | — | $7,918.98 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.9; leftover $2239.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 13 | $146.90 | $2.03 | — | $6,007.25 | — | rank-weighted leftover; list flatten; 🔵; ret5=+3.6; leftover $1959.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 13 | $120.00 | $2.03 | — | $4,445.22 | — | rank-weighted leftover; list flatten; 🔵; ret5=+0.6; leftover $1679.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 4 | $330.91 | $2.00 | — | $3,119.58 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1400.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 19 | $57.61 | $2.05 | — | $2,022.94 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1120.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 93 | $9.01 | $2.27 | — | $1,182.74 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $840.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 597 | $0.94 | $7.38 | — | $615.97 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $560.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 186 | $1.50 | $2.55 | — | $334.42 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $280.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $334.42 | ▲ close $10,164.79 vs 09:30 $10,103.42 (session +107.14) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $334.42 | ▲ 09:30 equity $10,221.16 vs yday $10,164.79 (+56.37) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 6 | $367.88 | $2.04 | $+44.26 | $2,539.66 | ▲ +44.26 after sell → book $10,219.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 13 | $149.37 | $2.05 | $+28.03 | $4,479.42 | ▲ +28.03 after sell → book $10,217.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 13 | $127.40 | $2.05 | $+92.12 | $6,133.57 | ▲ +92.12 after sell → book $10,215.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 4 | $336.94 | $2.02 | $+20.10 | $7,479.31 | ▲ +20.10 after sell → book $10,212.99; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 19 | $55.37 | $2.07 | $-46.67 | $8,529.27 | ▼ -46.67 after sell → book $10,210.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 93 | $9.22 | $2.29 | $+14.97 | $9,384.43 | ▲ +14.97 after sell → book $10,208.63; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 597 | $0.91 | $7.31 | $-32.61 | $9,918.60 | ▼ -32.61 after sell → book $10,201.32; vs 09:30 mark -7.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 186 | $1.52 | $2.59 | $-1.42 | $10,198.73 | ▼ -1.42 after sell → book $10,198.73; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 49 | $46.18 | $2.14 | — | $7,933.77 | — | rank-weighted leftover; list flatten; 🔵; ret5=+6.7; leftover $2266.38 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 13 | $142.77 | $2.03 | — | $6,075.73 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.8; leftover $1983.09 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $4,452.12 | — | rank-weighted leftover; list flatten; 🔵; ret5=+8.3; leftover $1699.79 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 349 | $4.05 | $4.50 | — | $3,034.17 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1416.49 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 133 | $8.46 | $2.39 | — | $1,906.60 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1133.19 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,089.72 | — | rank-weighted leftover; list flatten; ret5=-7.2; leftover $849.89 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 174 | $3.24 | $2.51 | — | $523.45 | — | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $566.60 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 58 | $4.81 | $2.16 | — | $242.31 | — | rank-weighted leftover; list flatten; ⚪; ret5=-11.4; leftover $283.30 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.31 | ▲ close $10,276.25 vs 09:30 $10,221.16 (session +97.28) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.31 | ▲ 09:30 equity $10,278.66 vs yday $10,276.25 (+2.41) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 49 | $48.00 | $2.17 | $+84.88 | $2,592.14 | ▲ +84.88 after sell → book $10,276.49; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 13 | $148.04 | $2.05 | $+64.43 | $4,514.60 | ▲ +64.43 after sell → book $10,274.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $6,184.01 | ▲ +45.79 after sell → book $10,272.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 349 | $3.72 | $4.57 | $-124.24 | $7,477.72 | ▼ -124.24 after sell → book $10,267.83; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 133 | $8.55 | $2.42 | $+7.16 | $8,612.45 | ▲ +7.16 after sell → book $10,265.41; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $9,451.37 | ▲ +22.05 after sell → book $10,263.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 174 | $3.11 | $2.55 | $-27.68 | $9,989.96 | ▼ -27.68 after sell → book $10,260.82; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 58 | $4.67 | $2.18 | $-12.47 | $10,258.63 | ▼ -12.47 after sell → book $10,258.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,258.63 | ▲ close $10,258.63 vs 09:30 $10,278.66 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,258.63 | ▲ 09:30 equity $10,258.63 vs yday $10,258.63 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,258.63 | ▲ close $10,258.63 vs 09:30 $10,258.63 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,258.63 | ▲ 09:30 equity $10,258.63 vs yday $10,258.63 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 110 | $20.55 | $2.32 | — | $7,995.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2279.70 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $6,082.55 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1994.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 82 | $20.65 | $2.24 | — | $4,387.02 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1709.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 246 | $5.77 | $3.17 | — | $2,964.42 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1424.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,823.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1139.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $992.00 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $854.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 325 | $1.75 | $4.19 | — | $419.06 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $569.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $273.07 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $284.96 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.07 | ▲ close $10,459.48 vs 09:30 $10,258.63 (session +220.51) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.07 | ▲ 09:30 equity $10,735.58 vs yday $10,459.48 (+276.10) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 110 | $21.90 | $2.36 | $+143.82 | $2,679.72 | ▲ +143.82 after sell → book $10,733.23; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $4,687.76 | ▲ +94.78 after sell → book $10,731.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 82 | $21.75 | $2.26 | $+85.70 | $6,468.99 | ▲ +85.70 after sell → book $10,728.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 246 | $5.67 | $3.23 | $-31.00 | $7,860.59 | ▼ -31.00 after sell → book $10,725.66; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 58 | $21.17 | $2.18 | $+84.97 | $9,086.26 | ▲ +84.97 after sell → book $10,723.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $9,984.93 | ▲ +66.95 after sell → book $10,721.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 325 | $1.79 | $4.26 | $+4.55 | $10,562.42 | ▲ +4.55 after sell → book $10,717.12; vs 09:30 mark -4.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 1 | $154.70 | $1.57 | $+7.14 | $10,715.55 | ▲ +7.14 after sell → book $10,715.55; vs 09:30 mark -1.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 19 | $119.43 | $2.05 | — | $8,444.34 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $2381.23 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 121 | $17.20 | $2.35 | — | $6,360.78 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $2083.58 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 8 | $216.30 | $2.01 | — | $4,628.37 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1785.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 133 | $11.13 | $2.39 | — | $3,145.69 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1488.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 482 | $2.47 | $6.22 | — | $1,948.93 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1190.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 462 | $1.93 | $5.96 | — | $1,051.31 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $892.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 9 | $59.72 | $2.02 | — | $511.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $595.31 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 225 | $1.32 | $2.90 | — | $211.91 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $297.65 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.91 | ▲ close $10,923.01 vs 09:30 $10,735.58 (session +233.36) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.91 | ▲ 09:30 equity $10,981.56 vs yday $10,923.01 (+58.55) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 19 | $120.51 | $2.08 | $+16.40 | $2,499.53 | ▲ +16.40 after sell → book $10,979.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 121 | $16.57 | $2.39 | $-80.97 | $4,502.11 | ▼ -80.97 after sell → book $10,977.10; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 8 | $217.03 | $2.04 | $+1.79 | $6,236.31 | ▲ +1.79 after sell → book $10,975.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 133 | $13.33 | $2.43 | $+287.79 | $8,006.77 | ▲ +287.79 after sell → book $10,972.63; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 482 | $2.40 | $6.31 | $-46.27 | $9,157.27 | ▼ -46.27 after sell → book $10,966.33; vs 09:30 mark -6.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 462 | $1.88 | $6.05 | $-35.11 | $10,019.78 | ▼ -35.11 after sell → book $10,960.28; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 9 | $58.75 | $2.04 | $-12.78 | $10,546.49 | ▼ -12.78 after sell → book $10,958.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 225 | $1.83 | $2.95 | $+108.90 | $10,955.29 | ▲ +108.90 after sell → book $10,955.29; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,955.29 | ▲ close $10,955.29 vs 09:30 $10,981.56 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,955.29 | ▲ 09:30 equity $10,955.29 vs yday $10,955.29 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 102 | $23.77 | $2.30 | — | $8,528.46 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.0; leftover $2434.51 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 194 | $10.98 | $2.57 | — | $6,395.77 | — | rank-weighted leftover; list flatten; 🔵; ret5=+1.2; leftover $2130.20 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.19 | $2.08 | — | $4,619.18 | — | rank-weighted leftover; list flatten; 🔵; ret5=+7.4; leftover $1825.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 182 | $8.35 | $2.54 | — | $3,096.94 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1521.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 246 | $4.94 | $3.17 | — | $1,878.53 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $1217.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $1,022.59 | — | rank-weighted leftover; list flatten; ret5=+6.0; leftover $912.94 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 83 | $7.25 | $2.24 | — | $418.60 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $608.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 850 | $0.36 | $5.59 | — | $108.71 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-15.6; leftover $304.31 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.71 | ▲ close $11,102.87 vs 09:30 $10,955.29 (session +170.06) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.71 | ▲ 09:30 equity $11,118.46 vs yday $11,102.87 (+15.59) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 83 | $8.29 | $2.26 | $+81.82 | $794.52 | ▲ +81.82 after sell → book $11,116.20; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 850 | $0.35 | $5.70 | $-15.54 | $1,088.87 | ▼ -15.54 after sell → book $11,110.50; vs 09:30 mark -5.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 23 | $31.21 | $2.06 | — | $368.98 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $725.91 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 32 | $11.12 | $2.09 | — | $11.05 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $362.96 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.05 | ▼ close $11,048.87 vs 09:30 $11,118.46 (session -57.48) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.05 | ▼ 09:30 equity $11,040.60 vs yday $11,048.87 (-8.27) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 194 | $10.63 | $2.62 | $-73.09 | $2,070.65 | ▼ -73.09 after sell → book $11,037.98; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 29 | $62.10 | $2.10 | $+22.21 | $3,869.45 | ▲ +22.21 after sell → book $11,035.88; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 182 | $8.49 | $2.58 | $+20.37 | $5,412.05 | ▲ +20.37 after sell → book $11,033.30; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 246 | $5.07 | $3.22 | $+25.58 | $6,656.05 | ▲ +25.58 after sell → book $11,030.08; vs 09:30 mark -3.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-8.73 | $7,503.25 | ▼ -8.73 after sell → book $11,028.06; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 60 | $41.44 | $2.17 | — | $5,014.68 | — | rank-weighted leftover; list flatten; ret5=+3.1; leftover $2501.08 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 138 | $14.42 | $2.40 | — | $3,022.32 | — | rank-weighted leftover; list flatten; ret5=+7.1; leftover $2000.87 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 577 | $2.60 | $7.44 | — | $1,514.67 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+13.0; leftover $1500.65 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 77 | $12.98 | $2.22 | — | $512.99 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1000.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 51 | $9.68 | $2.14 | — | $17.17 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $500.22 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.17 | ▲ close $11,074.88 vs 09:30 $11,040.60 (session +63.20) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.17 | ▲ 09:30 equity $11,101.33 vs yday $11,074.88 (+26.45) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 23 | $30.53 | $2.08 | $-19.78 | $717.28 | ▼ -19.78 after sell → book $11,099.25; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 32 | $11.27 | $2.11 | $+0.61 | $1,075.81 | ▲ +0.61 after sell → book $11,097.14; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 77 | $13.05 | $2.24 | $+0.93 | $2,078.42 | ▲ +0.93 after sell → book $11,094.90; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 51 | $9.88 | $2.16 | $+5.89 | $2,580.14 | ▲ +5.89 after sell → book $11,092.74; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 31 | $32.90 | $2.08 | — | $1,558.15 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1032.06 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 49 | $15.66 | $2.14 | — | $788.68 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $774.04 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 6 | $79.42 | $2.01 | — | $310.15 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $516.03 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 77 | $3.32 | $2.22 | — | $52.29 | — | rank-weighted leftover; list probable,yday_gainer; ret5=+6.4; leftover $258.01 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.29 | ▼ close $10,805.51 vs 09:30 $11,101.33 (session -278.78) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.29 | ▲ 09:30 equity $10,885.08 vs yday $10,805.51 (+79.57) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 102 | $23.68 | $2.33 | $-13.81 | $2,465.32 | ▼ -13.81 after sell → book $10,882.75; vs 09:30 mark -2.33 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 60 | $42.00 | $2.20 | $+29.23 | $4,983.12 | ▲ +29.23 after sell → book $10,880.55; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 138 | $14.54 | $2.44 | $+11.71 | $6,987.19 | ▲ +11.71 after sell → book $10,878.10; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 577 | $2.58 | $7.55 | $-26.53 | $8,468.30 | ▼ -26.53 after sell → book $10,870.55; vs 09:30 mark -7.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 31 | $31.15 | $2.10 | $-58.44 | $9,431.85 | ▼ -58.44 after sell → book $10,868.45; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 49 | $14.44 | $2.16 | $-64.07 | $10,137.25 | ▼ -64.07 after sell → book $10,866.29; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 6 | $80.44 | $2.03 | $+2.08 | $10,617.86 | ▲ +2.08 after sell → book $10,864.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 77 | $3.20 | $2.24 | $-13.70 | $10,862.02 | ▼ -13.70 after sell → book $10,862.02; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,862.02 | ▲ close $10,862.02 vs 09:30 $10,885.08 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,862.02 | ▲ 09:30 equity $10,862.02 vs yday $10,862.02 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,862.02 | ▲ close $10,862.02 vs 09:30 $10,862.02 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,862.02 | ▲ 09:30 equity $10,862.02 vs yday $10,862.02 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,862.02 | ▲ close $10,862.02 vs 09:30 $10,862.02 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,862.02 | ▲ 09:30 equity $10,862.02 vs yday $10,862.02 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 45 | $52.88 | $2.12 | — | $8,480.30 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2413.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 49 | $42.93 | $2.14 | — | $6,374.59 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2112.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 498 | $3.63 | $6.42 | — | $4,560.42 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1810.34 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 187 | $8.03 | $2.55 | — | $3,056.26 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1508.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $1,862.20 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1206.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 58 | $15.45 | $2.16 | — | $963.93 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $905.17 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 4 | $145.94 | $2.00 | — | $378.15 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $603.45 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 17 | $16.77 | $2.04 | — | $91.02 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $301.72 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.02 | ▼ close $10,630.93 vs 09:30 $10,862.02 (session -209.63) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.02 | ▼ 09:30 equity $10,588.24 vs yday $10,630.93 (-42.69) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 49 | $41.50 | $2.16 | $-74.37 | $2,122.36 | ▼ -74.37 after sell → book $10,586.08; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 187 | $7.91 | $2.59 | $-27.58 | $3,598.93 | ▼ -27.58 after sell → book $10,583.48; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $4,767.16 | ▼ -25.83 after sell → book $10,581.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 58 | $15.00 | $2.18 | $-30.45 | $5,634.98 | ▼ -30.45 after sell → book $10,579.26; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 4 | $153.62 | $2.02 | $+26.68 | $6,247.44 | ▲ +26.68 after sell → book $10,577.24; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 17 | $15.61 | $2.06 | $-23.82 | $6,510.75 | ▼ -23.82 after sell → book $10,575.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 738 | $2.52 | $9.52 | — | $4,641.47 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1860.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 231 | $6.71 | $2.98 | — | $3,088.48 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1550.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 652 | $1.90 | $8.41 | — | $1,841.27 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1240.14 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 194 | $4.78 | $2.57 | — | $911.38 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $930.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 389 | $1.59 | $5.02 | — | $287.85 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $620.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 25 | $11.31 | $2.06 | — | $3.03 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $310.04 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.03 | ▼ close $10,466.50 vs 09:30 $10,588.24 (session -78.12) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.03 | ▲ 09:30 equity $10,482.00 vs yday $10,466.50 (+15.50) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 45 | $54.31 | $2.15 | $+60.07 | $2,444.83 | ▲ +60.07 after sell → book $10,479.85; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 498 | $3.43 | $6.52 | $-112.54 | $4,146.45 | ▼ -112.54 after sell → book $10,473.33; vs 09:30 mark -6.52 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 738 | $2.38 | $9.66 | $-122.50 | $5,893.23 | ▼ -122.50 after sell → book $10,463.67; vs 09:30 mark -9.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 231 | $6.57 | $3.03 | $-38.35 | $7,407.87 | ▼ -38.35 after sell → book $10,460.64; vs 09:30 mark -3.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 652 | $2.00 | $8.53 | $+48.26 | $8,703.34 | ▲ +48.26 after sell → book $10,452.11; vs 09:30 mark -8.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 194 | $4.30 | $2.61 | $-98.31 | $9,534.93 | ▼ -98.31 after sell → book $10,449.50; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 389 | $1.63 | $5.09 | $+5.45 | $10,163.90 | ▲ +5.45 after sell → book $10,444.40; vs 09:30 mark -5.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 25 | $11.22 | $2.08 | $-6.40 | $10,442.32 | ▼ -6.40 after sell → book $10,442.32; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.32 | ▲ close $10,442.32 vs 09:30 $10,482.00 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.32 | ▲ 09:30 equity $10,442.32 vs yday $10,442.32 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.32 | ▲ close $10,442.32 vs 09:30 $10,442.32 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.32 | ▲ 09:30 equity $10,442.32 vs yday $10,442.32 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.32 | ▲ close $10,442.32 vs 09:30 $10,442.32 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.32 | ▲ 09:30 equity $10,442.32 vs yday $10,442.32 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 142 | $16.28 | $2.42 | — | $8,128.14 | — | rank-weighted leftover; list flatten; 🔵; ret5=-1.1; leftover $2320.52 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 743 | $2.73 | $9.58 | — | $6,090.17 | — | rank-weighted leftover; list flatten; 🔵; ret5=-3.0; leftover $2030.45 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $4,433.43 | — | rank-weighted leftover; list flatten; ret5=+8.3; leftover $1740.39 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $3,115.98 | — | rank-weighted leftover; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1450.32 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $2,009.51 | — | rank-weighted leftover; list flatten; 🔵; ret5=+4.7; leftover $1160.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 15 | $56.09 | $2.04 | — | $1,166.12 | — | rank-weighted leftover; list flatten; 🔵; ret5=+19.6; leftover $870.19 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 284 | $2.04 | $3.66 | — | $583.10 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $580.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 61 | $4.75 | $2.17 | — | $291.18 | — | rank-weighted leftover; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $290.06 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.18 | ▼ close $10,364.01 vs 09:30 $10,442.32 (session -52.40) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.18 | ▼ 09:30 equity $10,096.07 vs yday $10,364.01 (-267.94) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 142 | $16.03 | $2.46 | $-40.37 | $2,564.98 | ▼ -40.37 after sell → book $10,093.61; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 743 | $2.75 | $9.72 | $-0.73 | $4,602.22 | ▼ -0.73 after sell → book $10,083.89; vs 09:30 mark -9.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 8 | $206.50 | $2.04 | $-6.77 | $6,252.18 | ▼ -6.77 after sell → book $10,081.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 8 | $141.42 | $2.03 | $-188.13 | $7,381.51 | ▼ -188.13 after sell → book $10,079.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 15 | $52.23 | $2.06 | $-61.99 | $8,162.90 | ▼ -61.99 after sell → book $10,077.76; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 284 | $2.01 | $3.72 | $-15.90 | $8,730.02 | ▼ -15.90 after sell → book $10,074.04; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 61 | $4.82 | $2.19 | $-0.10 | $9,021.85 | ▼ -0.10 after sell → book $10,071.85; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,021.85 | ▼ close $10,048.33 vs 09:30 $10,096.07 (session -23.52) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,021.85 | ▲ 09:30 equity $10,079.69 vs yday $10,048.33 (+31.36) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 7 | $151.12 | $2.03 | $-50.66 | $10,077.66 | ▼ -50.66 after sell → book $10,077.66; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,077.66 | ▲ close $10,077.66 vs 09:30 $10,079.69 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,077.66 | ▲ 09:30 equity $10,077.66 vs yday $10,077.66 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 8 | $270.89 | $2.01 | — | $7,908.52 | — | rank-weighted leftover; list flatten; ret5=+4.0; leftover $2239.48 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 25 | $77.12 | $2.06 | — | $5,978.46 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+7.2; leftover $1959.54 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 117 | $14.31 | $2.34 | — | $4,301.85 | — | rank-weighted leftover; list flatten; ret5=+4.8; leftover $1679.61 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 38 | $36.46 | $2.10 | — | $2,914.26 | — | rank-weighted leftover; list flatten; 🔵; ret5=+2.9; leftover $1399.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 60 | $18.61 | $2.17 | — | $1,795.49 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $1119.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 46 | $18.21 | $2.13 | — | $955.71 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-19.1; leftover $839.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 8 | $68.79 | $2.01 | — | $403.37 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $559.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 47 | $5.87 | $2.13 | — | $125.35 | — | rank-weighted leftover; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $279.93 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.35 | ▲ close $10,236.41 vs 09:30 $10,077.66 (session +175.72) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.35 | ▲ 09:30 equity $10,384.22 vs yday $10,236.41 (+147.81) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 8 | $273.15 | $2.04 | $+14.02 | $2,308.51 | ▲ +14.02 after sell → book $10,382.18; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 25 | $76.44 | $2.09 | $-21.16 | $4,217.42 | ▼ -21.16 after sell → book $10,380.09; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 117 | $14.33 | $2.37 | $-2.37 | $5,891.66 | ▼ -2.37 after sell → book $10,377.72; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 38 | $36.67 | $2.13 | $+3.75 | $7,282.99 | ▲ +3.75 after sell → book $10,375.59; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 60 | $22.46 | $2.19 | $+226.64 | $8,628.40 | ▲ +226.64 after sell → book $10,373.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 46 | $19.59 | $2.15 | $+59.20 | $9,527.39 | ▲ +59.20 after sell → book $10,371.25; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 8 | $72.70 | $2.03 | $+27.23 | $10,106.96 | ▲ +27.23 after sell → book $10,369.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 47 | $5.58 | $2.15 | $-17.91 | $10,367.07 | ▼ -17.91 after sell → book $10,367.07; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 9 | $233.85 | $2.02 | — | $8,260.40 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+11.7; leftover $2303.79 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 13 | $151.43 | $2.03 | — | $6,289.78 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $2015.82 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 11 | $147.61 | $2.02 | — | $4,664.05 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+17.7; leftover $1727.84 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 140 | $10.25 | $2.41 | — | $3,226.64 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1439.87 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 151 | $7.59 | $2.44 | — | $2,078.11 | — | rank-weighted leftover; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1151.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 24 | $34.93 | $2.06 | — | $1,237.72 | — | rank-weighted leftover; list flatten; ret5=+1.6; leftover $863.92 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 3387 | $0.17 | $15.92 | — | $646.01 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $575.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 18 | $15.87 | $2.04 | — | $358.31 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $287.97 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $358.31 | ▲ close $10,463.77 vs 09:30 $10,384.22 (session +127.65) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $358.31 | ▲ 09:30 equity $10,606.47 vs yday $10,463.77 (+142.70) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 9 | $249.13 | $2.04 | $+133.46 | $2,598.44 | ▲ +133.46 after sell → book $10,604.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 13 | $158.04 | $2.06 | $+81.85 | $4,650.90 | ▲ +81.85 after sell → book $10,602.37; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 11 | $146.50 | $2.05 | $-16.28 | $6,260.35 | ▼ -16.28 after sell → book $10,600.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 140 | $10.12 | $2.44 | $-23.05 | $7,674.71 | ▼ -23.05 after sell → book $10,597.88; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 151 | $7.98 | $2.48 | $+53.97 | $8,877.21 | ▲ +53.97 after sell → book $10,595.40; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 24 | $34.52 | $2.08 | $-13.98 | $9,703.61 | ▼ -13.98 after sell → book $10,593.32; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 3387 | $0.17 | $16.49 | $-32.41 | $10,262.91 | ▼ -32.41 after sell → book $10,576.83; vs 09:30 mark -16.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 18 | $17.44 | $2.06 | $+24.15 | $10,574.76 | ▲ +24.15 after sell → book $10,574.76; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 21 | $108.55 | $2.05 | — | $8,293.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=+21.3; leftover $2349.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 3 | $593.15 | $2.00 | — | $6,511.71 | — | rank-weighted leftover; list flatten,ohlc_hot; ret5=+16.1; leftover $2056.20 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 8 | $209.52 | $2.01 | — | $4,833.54 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1762.46 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $3,513.81 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1468.72 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 13 | $85.00 | $2.03 | — | $2,406.78 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1174.97 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 25 | $34.44 | $2.06 | — | $1,543.72 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $881.23 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 605 | $0.97 | $7.68 | — | $949.18 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $587.49 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 141 | $2.08 | $2.41 | — | $653.49 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $293.74 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $653.49 | ▼ close $10,364.22 vs 09:30 $10,606.47 (session -188.28) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $653.49 | ▲ 09:30 equity $10,507.91 vs yday $10,364.22 (+143.69) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 21 | $107.57 | $2.08 | $-24.71 | $2,910.38 | ▼ -24.71 after sell → book $10,505.83; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 3 | $586.77 | $2.02 | $-23.16 | $4,668.67 | ▼ -23.16 after sell → book $10,503.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 8 | $210.00 | $2.04 | $-0.21 | $6,346.63 | ▼ -0.21 after sell → book $10,501.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $7,726.10 | ▲ +59.74 after sell → book $10,499.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 13 | $82.83 | $2.05 | $-32.29 | $8,800.84 | ▼ -32.29 after sell → book $10,497.69; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 25 | $33.00 | $2.08 | $-40.15 | $9,623.76 | ▼ -40.15 after sell → book $10,495.61; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 605 | $0.94 | $7.61 | $-33.45 | $10,184.84 | ▼ -33.45 after sell → book $10,487.99; vs 09:30 mark -7.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 141 | $2.15 | $2.45 | $+5.01 | $10,485.55 | ▲ +5.01 after sell → book $10,485.55; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 14 | $157.87 | $2.03 | — | $8,273.33 | — | rank-weighted leftover; list flatten; ret5=+6.5; leftover $2330.12 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 5 | $386.20 | $2.00 | — | $6,340.33 | — | rank-weighted leftover; list flatten; ret5=-5.8; leftover $2038.86 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 19 | $88.83 | $2.05 | — | $4,650.51 | — | rank-weighted leftover; list flatten; ret5=+7.6; leftover $1747.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 185 | $7.84 | $2.54 | — | $3,197.57 | — | rank-weighted leftover; list flatten; ret5=+13.6; leftover $1456.33 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 111 | $10.43 | $2.32 | — | $2,037.51 | — | rank-weighted leftover; list flatten; ret5=+19.2; leftover $1165.06 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 64 | $13.47 | $2.18 | — | $1,173.25 | — | rank-weighted leftover; list flatten; ret5=+3.6; leftover $873.80 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 145 | $4.00 | $2.42 | — | $590.83 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $582.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 31 | $9.31 | $2.08 | — | $300.13 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $291.27 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $300.13 | ▼ close $10,323.47 vs 09:30 $10,507.91 (session -144.43) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $300.13 | ▲ 09:30 equity $10,338.31 vs yday $10,323.47 (+14.84) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 111 | $10.18 | $2.35 | $-32.42 | $1,427.76 | ▼ -32.42 after sell → book $10,335.96; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 145 | $3.51 | $2.46 | $-75.93 | $1,934.25 | ▼ -75.93 after sell → book $10,333.50; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 2 | $93.97 | $1.89 | — | $1,744.43 | — | rank-weighted leftover; list flatten; ret5=-0.6; leftover $268.65 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 92 | $0.58 | $0.81 | — | $1,690.26 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $53.73 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,690.26 | ▼ close $10,326.13 vs 09:30 $10,338.31 (session -4.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,690.26 | ▲ 09:30 equity $10,363.05 vs yday $10,326.13 (+36.92) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 5 | $370.00 | $2.03 | $-85.03 | $3,538.23 | ▼ -85.03 after sell → book $10,361.02; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 64 | $12.26 | $2.20 | $-81.82 | $4,320.67 | ▼ -81.82 after sell → book $10,358.82; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 31 | $9.50 | $2.10 | $+1.70 | $4,613.06 | ▲ +1.70 after sell → book $10,356.71; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 2 | $93.97 | $1.91 | $-3.79 | $4,799.10 | ▼ -3.79 after sell → book $10,354.81; vs 09:30 mark -1.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 92 | $0.57 | $0.83 | $-2.10 | $4,851.17 | ▼ -2.10 after sell → book $10,353.98; vs 09:30 mark -0.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 13 | $116.85 | $2.03 | — | $3,330.09 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1617.06 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 46 | $27.79 | $2.13 | — | $2,049.62 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1293.64 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 98 | $9.81 | $2.28 | — | $1,085.96 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $970.23 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 31 | $20.25 | $2.08 | — | $456.12 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $646.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 15 | $20.65 | $2.04 | — | $144.34 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $323.41 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.34 | ▼ close $10,044.11 vs 09:30 $10,363.05 (session -299.31) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.34 | ▼ 09:30 equity $9,992.60 vs yday $10,044.11 (-51.51) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 14 | $163.95 | $2.06 | $+81.03 | $2,437.58 | ▲ +81.03 after sell → book $9,990.54; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 19 | $87.67 | $2.07 | $-26.06 | $4,101.33 | ▼ -26.06 after sell → book $9,988.47; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 185 | $7.38 | $2.59 | $-90.23 | $5,464.05 | ▼ -90.23 after sell → book $9,985.89; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 13 | $112.22 | $2.05 | $-64.27 | $6,920.86 | ▼ -64.27 after sell → book $9,983.84; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 46 | $26.22 | $2.15 | $-76.50 | $8,124.83 | ▼ -76.50 after sell → book $9,981.69; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 98 | $9.67 | $2.31 | $-18.31 | $9,070.18 | ▼ -18.31 after sell → book $9,979.38; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 31 | $19.40 | $2.10 | $-30.54 | $9,669.47 | ▼ -30.54 after sell → book $9,977.27; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 15 | $20.52 | $2.06 | $-6.04 | $9,975.22 | ▼ -6.04 after sell → book $9,975.22; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,975.22 | ▲ close $9,975.22 vs 09:30 $9,992.60 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,980.07 | ▲ 09:30 equity $8,980.07 vs yday $8,980.07 (+0.00) | 09:30 open · cash $8,980.07 · no holdings · equity $8,980.07 vs prior close $8,980.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 2 | $803.87 | $2.00 | — | $7,370.33 | — | rank-weighted leftover; list flatten; ret5=+0.8; leftover $1995.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 15 | $115.36 | $2.04 | — | $5,637.90 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1746.12 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 72 | $20.61 | $2.21 | — | $4,151.77 | — | rank-weighted leftover; list flatten; 🔵; ret5=+9.1; leftover $1496.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 32 | $38.51 | $2.09 | — | $2,917.37 | — | rank-weighted leftover; list flatten; ret5=+4.7; leftover $1247.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 130 | $7.65 | $2.38 | — | $1,920.49 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+5.2; leftover $997.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 28 | $26.27 | $2.07 | — | $1,182.85 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $748.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 5 | $83.76 | $2.00 | — | $762.05 | — | rank-weighted leftover; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $498.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 27 | $9.05 | $2.07 | — | $515.63 | — | rank-weighted leftover; list probable,yday_gainer; ret5=-27.1; leftover $249.45 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $515.63 | ▼ close $8,894.80 vs 09:30 $8,980.07 (session -68.42) | 16:00 close · cash $515.63 · equity $8,894.80 vs 09:30 $8,980.07 (-85.27; session marks -68.42) · 8 name(s) marked open→close (per-name table). REGN×2 09:30 $803.87 → close $788.04 -31.66; HALO×15 09:30 $115.36 → close $113.90 -21.90; OMER×72 09:30 $20.61 → close $20.08 -38.16; BLFS×32 09:30 $38.51 → close $38.49 -0.64; MRVI×130 09:30 $7.65 → close $7.60 -6.50; WRBY×28 09:30 $26.27 → close $26.71 +12.32; TXG×5 09:30 $83.76 → close $85.71 +9.75; AEHL×27 09:30 $9.05 → close $9.36 +8.37 | — |

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
