# Factor mine action — `short_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **+2.35%** ($10,235) · signal-only (no cash/fees) was -2.93%. Starts YES **19/30**. Fills 251 · skips 107 · realized $-717.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,282.27.

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
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $11,240.38 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $12,478.21 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 42 | $29.74 | $2.17 | — | $13,725.12 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `VOR` | 56 | $22.01 | $2.21 | — | $14,955.47 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,955.47 | ▼ close $9,934.23 vs 09:30 $10,000.00 (session -56.90) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,955.47 | ▼ 09:30 equity $9,928.54 vs yday $9,934.23 (-5.69) | — | — |
| 2026-08-14 09:30 ET | **COVER** | `TGTX` | 25 | $47.27 | $2.06 | $+56.57 | $13,771.65 | ▲ +56.57 after sell → book $9,926.47; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `SLS` | 106 | $12.40 | $2.31 | $-78.88 | $12,454.95 | ▼ -78.88 after sell → book $9,924.17; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `HIMS` | 42 | $29.15 | $2.12 | $+20.49 | $11,228.53 | ▲ +20.49 after sell → book $9,922.05; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `VOR` | 56 | $23.33 | $2.16 | $-78.29 | $9,919.89 | ▼ -78.29 after sell → book $9,919.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `TLN` | 1 | $359.83 | $2.02 | — | $10,277.70 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $10,875.66 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $11,486.11 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 109 | $5.64 | $2.36 | — | $12,098.50 | — | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $12,703.05 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $13,320.64 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $13,914.82 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $619.99 | — |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $14,532.11 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $619.99 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,532.11 | ▼ close $9,873.39 vs 09:30 $9,928.54 (session -29.22) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,532.11 | ▼ 09:30 equity $9,870.07 vs yday $9,873.39 (-3.32) | — | — |
| 2026-08-17 09:30 ET | **COVER** | `TLN` | 1 | $367.88 | $1.99 | $-12.07 | $14,162.24 | ▼ -12.07 after sell → book $9,868.08; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `NRG` | 5 | $127.40 | $2.00 | $-41.05 | $13,523.24 | ▼ -41.05 after sell → book $9,866.08; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MARA` | 68 | $9.22 | $2.19 | $-18.71 | $12,894.08 | ▼ -18.71 after sell → book $9,863.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 109 | $5.50 | $2.32 | $+10.58 | $12,292.27 | ▲ +10.58 after sell → book $9,861.57; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 31 | $19.57 | $2.08 | $-4.20 | $11,683.51 | ▼ -4.20 after sell → book $9,859.48; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $11,099.94 | ▲ +34.02 after sell → book $9,857.26; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `BIRK` | 15 | $39.48 | $2.04 | $-0.06 | $10,505.71 | ▼ -0.06 after sell → book $9,855.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `HLIT` | 47 | $13.84 | $2.13 | $-35.32 | $9,853.10 | ▼ -35.32 after sell → book $9,853.10; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 152 | $4.05 | $2.50 | — | $10,466.20 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `TGB` | 72 | $8.46 | $2.24 | — | $11,073.07 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ELF` | 6 | $90.54 | $2.04 | — | $11,614.27 | — | last bar red; gate last_red=True; list flatten; ret5=-7.2; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `DNN` | 190 | $3.24 | $2.62 | — | $12,227.25 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 128 | $4.81 | $2.42 | — | $12,840.51 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-11.4; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 89 | $6.87 | $2.30 | — | $13,449.64 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 47 | $12.83 | $2.17 | — | $14,050.48 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $615.82 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 39 | $15.40 | $2.14 | — | $14,648.94 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $615.82 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,648.94 | ▲ close $9,890.28 vs 09:30 $9,870.07 (session +55.62) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,648.94 | ▲ 09:30 equity $9,961.79 vs yday $9,890.28 (+71.51) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `TMC` | 152 | $3.72 | $2.45 | $+45.22 | $14,081.05 | ▲ +45.22 after sell → book $9,959.34; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `TGB` | 72 | $8.55 | $2.21 | $-10.93 | $13,463.25 | ▼ -10.93 after sell → book $9,957.14; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ELF` | 6 | $93.44 | $2.01 | $-21.45 | $12,900.60 | ▼ -21.45 after sell → book $9,955.13; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `DNN` | 190 | $3.11 | $2.56 | $+19.52 | $12,307.14 | ▲ +19.52 after sell → book $9,952.57; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HNST` | 128 | $4.67 | $2.37 | $+13.12 | $11,707.01 | ▲ +13.12 after sell → book $9,950.20; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 89 | $7.50 | $2.26 | $-60.63 | $11,037.25 | ▼ -60.63 after sell → book $9,947.94; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 47 | $11.12 | $2.13 | $+76.07 | $10,512.48 | ▲ +76.07 after sell → book $9,945.81; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NU` | 39 | $14.53 | $2.11 | $+29.68 | $9,943.70 | ▲ +29.68 after sell → book $9,943.70; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,943.70 | ▲ close $9,943.70 vs 09:30 $9,961.79 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,943.70 | ▲ 09:30 equity $9,943.70 vs yday $9,943.70 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,943.70 | ▲ close $9,943.70 vs 09:30 $9,943.70 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,943.70 | ▲ 09:30 equity $9,943.70 vs yday $9,943.70 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `BHP` | 6 | $91.01 | $2.04 | — | $10,487.72 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 83 | $7.44 | $2.28 | — | $11,102.96 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `CRCL` | 7 | $82.99 | $2.05 | — | $11,681.84 | — | last bar red; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $12,300.33 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 140 | $4.43 | $2.46 | — | $12,918.07 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2071 | $0.30 | $12.80 | — | $13,526.57 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1755 | $0.35 | $11.80 | — | $14,136.04 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $621.48 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,743.03 | — | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $621.48 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,743.03 | ▼ close $9,846.06 vs 09:30 $9,943.70 (session -60.04) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,743.03 | ▼ 09:30 equity $9,778.16 vs yday $9,846.06 (-67.90) | — | — |
| 2026-08-21 09:30 ET | **COVER** | `BHP` | 6 | $95.72 | $2.01 | $-32.31 | $14,166.70 | ▼ -32.31 after sell → book $9,776.15; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `MRVI` | 83 | $8.28 | $2.24 | $-74.24 | $13,477.22 | ▼ -74.24 after sell → book $9,773.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `CRCL` | 7 | $87.98 | $2.01 | $-38.99 | $12,859.35 | ▼ -38.99 after sell → book $9,771.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 29 | $21.54 | $2.08 | $-8.25 | $12,232.61 | ▼ -8.25 after sell → book $9,769.82; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 140 | $4.68 | $2.41 | $-39.87 | $11,575.00 | ▼ -39.87 after sell → book $9,767.41; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `DVLT` | 2071 | $0.31 | $12.63 | $-46.14 | $10,920.36 | ▼ -46.14 after sell → book $9,754.78; vs 09:30 mark -12.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SAFX` | 1755 | $0.35 | $11.41 | $-16.18 | $10,294.70 | ▼ -16.18 after sell → book $9,743.37; vs 09:30 mark -11.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $9,741.34 | ▲ +53.63 after sell → book $9,741.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 246 | $2.47 | $3.24 | — | $10,345.72 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 315 | $1.93 | $4.14 | — | $10,949.53 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 10 | $59.72 | $2.06 | — | $11,544.67 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 5 | $115.18 | $2.04 | — | $12,118.53 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 18 | $33.36 | $2.08 | — | $12,716.93 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 356 | $1.71 | $4.68 | — | $13,321.01 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2070 | $0.29 | $12.67 | — | $13,916.93 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $608.83 | — |
| 2026-08-21 09:30 ET | **SHORT** | `PRQR` | 267 | $2.28 | $3.52 | — | $14,522.17 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $608.83 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,522.17 | ▼ close $9,556.16 vs 09:30 $9,778.16 (session -150.76) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,522.17 | ▼ 09:30 equity $9,516.61 vs yday $9,556.16 (-39.55) | — | — |
| 2026-08-24 09:30 ET | **COVER** | `AUTL` | 246 | $2.40 | $3.17 | $+10.81 | $13,928.60 | ▲ +10.81 after sell → book $9,513.44; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 315 | $1.88 | $4.06 | $+7.54 | $13,332.34 | ▲ +7.54 after sell → book $9,509.38; vs 09:30 mark -4.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRSP` | 10 | $58.75 | $2.02 | $+5.62 | $12,742.82 | ▲ +5.62 after sell → book $9,507.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `FUTU` | 5 | $121.00 | $2.00 | $-33.15 | $12,135.81 | ▼ -33.15 after sell → book $9,505.35; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 18 | $32.82 | $2.04 | $+5.60 | $11,543.01 | ▲ +5.60 after sell → book $9,503.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ENHA` | 356 | $1.74 | $4.59 | $-19.95 | $10,918.98 | ▼ -19.95 after sell → book $9,498.72; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CAN` | 2070 | $0.38 | $14.14 | $-211.03 | $10,112.03 | ▼ -211.03 after sell → book $9,484.58; vs 09:30 mark -14.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `PRQR` | 267 | $2.35 | $3.44 | $-25.65 | $9,481.13 | ▼ -25.65 after sell → book $9,481.13; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,481.13 | ▲ close $9,481.13 vs 09:30 $9,516.61 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,481.13 | ▲ 09:30 equity $9,481.13 vs yday $9,481.13 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **SHORT** | `MOS` | 24 | $23.77 | $2.10 | — | $10,049.52 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 53 | $10.98 | $2.18 | — | $10,629.27 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INSP` | 9 | $61.19 | $2.05 | — | $11,177.93 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `RZLT` | 119 | $4.94 | $2.39 | — | $11,763.40 | — | last bar red; gate last_red=True; list flatten; ret5=+7.1; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `HCA` | 1 | $426.97 | $2.02 | — | $12,188.34 | — | last bar red; gate last_red=True; list flatten; ret5=+6.0; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 81 | $7.25 | $2.27 | — | $12,773.32 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 155 | $3.80 | $2.51 | — | $13,359.81 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $592.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 379 | $1.56 | $4.98 | — | $13,946.07 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $592.57 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,946.07 | ▼ close $9,333.42 vs 09:30 $9,481.13 (session -127.20) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,946.07 | ▲ 09:30 equity $9,341.41 vs yday $9,333.42 (+7.99) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `MOS` | 24 | $24.84 | $2.06 | $-29.84 | $13,347.85 | ▼ -29.84 after sell → book $9,339.35; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `RZLT` | 119 | $5.01 | $2.35 | $-13.07 | $12,749.31 | ▼ -13.07 after sell → book $9,337.00; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `HCA` | 1 | $427.50 | $1.99 | $-4.55 | $12,319.82 | ▼ -4.55 after sell → book $9,335.01; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CAPR` | 81 | $8.29 | $2.23 | $-88.75 | $11,646.10 | ▼ -88.75 after sell → book $9,332.77; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `PUSA` | 155 | $3.83 | $2.46 | $-10.39 | $11,049.22 | ▼ -10.39 after sell → book $9,330.32; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 379 | $1.60 | $4.89 | $-25.03 | $10,437.93 | ▼ -25.03 after sell → book $9,325.43; vs 09:30 mark -4.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `FLNC` | 69 | $11.12 | $2.24 | — | $11,202.97 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $777.12 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 44 | $17.51 | $2.16 | — | $11,971.25 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $777.12 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AXTI` | 11 | $65.34 | $2.06 | — | $12,687.92 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $777.12 | — |
| 2026-08-26 09:30 ET | **SHORT** | `INDP` | 712 | $1.09 | $9.33 | — | $13,454.67 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $777.12 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NVTS` | 61 | $12.60 | $2.21 | — | $14,221.06 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $777.12 | — |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 16 | $46.96 | $2.08 | — | $14,970.34 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $777.12 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,970.34 | ▼ close $9,215.12 vs 09:30 $9,341.41 (session -90.22) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,970.34 | ▼ 09:30 equity $9,101.05 vs yday $9,215.12 (-114.07) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `OCUL` | 53 | $10.63 | $2.15 | $+14.22 | $14,404.80 | ▲ +14.22 after sell → book $9,098.90; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INSP` | 9 | $62.10 | $2.02 | $-12.26 | $13,843.88 | ▼ -12.26 after sell → book $9,096.88; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AVEX` | 44 | $18.43 | $2.12 | $-44.77 | $13,030.84 | ▼ -44.77 after sell → book $9,094.76; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INDP` | 712 | $1.13 | $9.18 | $-47.00 | $12,217.09 | ▼ -47.00 after sell → book $9,085.57; vs 09:30 mark -9.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NVTS` | 61 | $13.18 | $2.17 | $-39.77 | $11,410.94 | ▼ -39.77 after sell → book $9,083.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `IRDM` | 16 | $47.46 | $2.04 | $-12.12 | $10,649.54 | ▼ -12.12 after sell → book $9,081.36; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `MOS` | 31 | $24.00 | $2.12 | — | $11,391.42 | — | last bar red; gate last_red=True; list flatten; ret5=+8.7; leftover $756.78 | — |
| 2026-08-27 09:30 ET | **SHORT** | `KURA` | 58 | $12.98 | $2.20 | — | $12,142.05 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $756.78 | — |
| 2026-08-27 09:30 ET | **SHORT** | `AVBP` | 24 | $30.79 | $2.10 | — | $12,878.91 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $756.78 | — |
| 2026-08-27 09:30 ET | **SHORT** | `ABX` | 78 | $9.68 | $2.27 | — | $13,631.68 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $756.78 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SENS` | 81 | $9.33 | $2.28 | — | $14,385.14 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+2.5; leftover $756.78 | — |
| 2026-08-27 09:30 ET | **SHORT** | `ACRS` | 123 | $6.15 | $2.41 | — | $15,139.18 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-3.9; leftover $756.78 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,139.18 | ▲ close $9,083.57 vs 09:30 $9,101.05 (session +15.59) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,139.18 | ▲ 09:30 equity $9,129.76 vs yday $9,083.57 (+46.19) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `FLNC` | 69 | $11.27 | $2.20 | $-14.79 | $14,359.35 | ▼ -14.79 after sell → book $9,127.56; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AXTI` | 11 | $65.29 | $2.02 | $-3.54 | $13,639.14 | ▼ -3.54 after sell → book $9,125.54; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `KURA` | 58 | $13.05 | $2.16 | $-8.43 | $12,880.07 | ▼ -8.43 after sell → book $9,123.37; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVBP` | 24 | $30.53 | $2.06 | $+2.08 | $12,145.29 | ▲ +2.08 after sell → book $9,121.31; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `ABX` | 78 | $9.88 | $2.22 | $-20.09 | $11,372.43 | ▼ -20.09 after sell → book $9,119.09; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `SENS` | 81 | $9.39 | $2.23 | $-9.37 | $10,609.60 | ▼ -9.37 after sell → book $9,116.85; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `ACRS` | 123 | $6.10 | $2.36 | $+1.38 | $9,856.95 | ▲ +1.38 after sell → book $9,114.50; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 19 | $32.90 | $2.08 | — | $10,479.96 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $651.04 | — |
| 2026-08-28 09:30 ET | **SHORT** | `GRRR` | 41 | $15.66 | $2.15 | — | $11,119.87 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $651.04 | — |
| 2026-08-28 09:30 ET | **SHORT** | `URBN` | 8 | $79.42 | $2.05 | — | $11,753.18 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $651.04 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1783 | $0.36 | $12.18 | — | $12,391.79 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+7.6; leftover $651.04 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 2 | $252.24 | $2.03 | — | $12,894.24 | — | last bar red; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $651.04 | — |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 121 | $5.38 | $2.40 | — | $13,542.82 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+6.5; leftover $651.04 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 40 | $15.88 | $2.15 | — | $14,175.88 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $651.04 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,175.88 | ▲ close $9,202.81 vs 09:30 $9,129.76 (session +113.36) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,175.88 | ▲ 09:30 equity $9,206.67 vs yday $9,202.81 (+3.86) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `MOS` | 31 | $23.68 | $2.08 | $+5.71 | $13,439.71 | ▲ +5.71 after sell → book $9,204.59; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SEDG` | 19 | $31.15 | $2.05 | $+29.12 | $12,845.82 | ▲ +29.12 after sell → book $9,202.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `GRRR` | 41 | $14.44 | $2.11 | $+45.76 | $12,251.66 | ▲ +45.76 after sell → book $9,200.43; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `URBN` | 8 | $80.44 | $2.01 | $-12.23 | $11,606.13 | ▼ -12.23 after sell → book $9,198.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1783 | $0.36 | $11.80 | $-18.64 | $10,948.88 | ▼ -18.64 after sell → book $9,186.61; vs 09:30 mark -11.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 2 | $247.05 | $2.00 | $+6.35 | $10,452.78 | ▲ +6.35 after sell → book $9,184.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 121 | $5.37 | $2.35 | $-3.54 | $9,800.66 | ▼ -3.54 after sell → book $9,182.26; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BHVN` | 40 | $15.46 | $2.11 | $+12.54 | $9,180.15 | ▲ +12.54 after sell → book $9,180.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,180.15 | ▲ close $9,180.15 vs 09:30 $9,206.67 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,180.15 | ▲ 09:30 equity $9,180.15 vs yday $9,180.15 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,180.15 | ▲ close $9,180.15 vs 09:30 $9,180.15 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,180.15 | ▲ 09:30 equity $9,180.15 vs yday $9,180.15 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,180.15 | ▲ close $9,180.15 vs 09:30 $9,180.15 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,180.15 | ▲ 09:30 equity $9,180.15 vs yday $9,180.15 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `CRK` | 37 | $15.45 | $2.14 | — | $9,749.66 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 3 | $145.94 | $2.03 | — | $10,185.47 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $55.42 | $2.06 | — | $10,737.61 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1521 | $0.38 | $10.58 | — | $11,300.45 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.28 | $2.12 | — | $11,865.02 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 882 | $0.65 | $8.55 | — | $12,429.76 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 44 | $12.83 | $2.16 | — | $12,992.13 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $573.76 | — |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 35 | $15.95 | $2.13 | — | $13,548.25 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $573.76 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,548.25 | ▼ close $9,137.25 vs 09:30 $9,180.15 (session -11.15) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,548.25 | ▼ 09:30 equity $9,124.84 vs yday $9,137.25 (-12.41) | — | — |
| 2026-09-04 09:30 ET | **COVER** | `CRK` | 37 | $15.00 | $2.10 | $+12.41 | $12,991.15 | ▲ +12.41 after sell → book $9,122.74; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MRNA` | 3 | $153.62 | $2.00 | $-27.05 | $12,528.29 | ▼ -27.05 after sell → book $9,120.74; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `EIX` | 10 | $55.79 | $2.02 | $-7.78 | $11,968.37 | ▼ -7.78 after sell → book $9,118.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SAFX` | 1521 | $0.38 | $10.31 | $-22.41 | $11,383.12 | ▼ -22.41 after sell → book $9,108.41; vs 09:30 mark -10.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $17.27 | $2.08 | $+27.11 | $10,845.66 | ▲ +27.11 after sell → book $9,106.32; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DEFT` | 882 | $0.69 | $8.73 | $-52.56 | $10,228.35 | ▼ -52.56 after sell → book $9,097.59; vs 09:30 mark -8.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `GMRS` | 44 | $13.29 | $2.12 | $-24.52 | $9,641.47 | ▼ -24.52 after sell → book $9,095.47; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `KLRA` | 35 | $15.60 | $2.10 | $+8.02 | $9,093.37 | ▲ +8.02 after sell → book $9,093.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `CABA` | 164 | $3.46 | $2.53 | — | $9,658.28 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ALEC` | 225 | $2.52 | $2.97 | — | $10,222.31 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `BHC` | 84 | $6.71 | $2.28 | — | $10,783.67 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `BMEA` | 299 | $1.90 | $3.93 | — | $11,347.84 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 118 | $4.78 | $2.39 | — | $11,909.49 | — | last bar red; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 357 | $1.59 | $4.69 | — | $12,472.43 | — | last bar red; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `VIR` | 50 | $11.31 | $2.18 | — | $13,035.76 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $568.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ATRC` | 10 | $52.03 | $2.05 | — | $13,554.00 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $568.34 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,554.00 | ▲ close $9,092.54 vs 09:30 $9,124.84 (session +22.19) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,554.00 | ▲ 09:30 equity $9,112.69 vs yday $9,092.54 (+20.15) | — | — |
| 2026-09-08 09:30 ET | **COVER** | `CABA` | 164 | $3.43 | $2.48 | $-0.10 | $12,989.00 | ▼ -0.10 after sell → book $9,110.21; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ALEC` | 225 | $2.38 | $2.90 | $+25.63 | $12,450.60 | ▲ +25.63 after sell → book $9,107.31; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BHC` | 84 | $6.57 | $2.24 | $+7.24 | $11,896.48 | ▲ +7.24 after sell → book $9,105.07; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BMEA` | 299 | $2.00 | $3.86 | $-37.69 | $11,294.62 | ▼ -37.69 after sell → book $9,101.21; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OABI` | 118 | $4.30 | $2.34 | $+51.91 | $10,784.87 | ▲ +51.91 after sell → book $9,098.86; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 357 | $1.63 | $4.61 | $-23.58 | $10,198.36 | ▼ -23.58 after sell → book $9,094.26; vs 09:30 mark -4.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `VIR` | 50 | $11.22 | $2.14 | $+0.18 | $9,635.22 | ▲ +0.18 after sell → book $9,092.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ATRC` | 10 | $54.31 | $2.02 | $-26.87 | $9,090.10 | ▼ -26.87 after sell → book $9,090.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,090.10 | ▲ close $9,090.10 vs 09:30 $9,112.69 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,090.10 | ▲ 09:30 equity $9,090.10 vs yday $9,090.10 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,090.10 | ▲ close $9,090.10 vs 09:30 $9,090.10 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,090.10 | ▲ 09:30 equity $9,090.10 vs yday $9,090.10 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,090.10 | ▲ close $9,090.10 vs 09:30 $9,090.10 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,090.10 | ▲ 09:30 equity $9,090.10 vs yday $9,090.10 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `AUPH` | 34 | $16.28 | $2.13 | — | $9,641.49 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=-1.1; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `OVID` | 208 | $2.73 | $2.74 | — | $10,206.59 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=-3.0; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `ORCL` | 3 | $164.43 | $2.03 | — | $10,697.85 | — | last bar red; gate last_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `NVT` | 3 | $157.78 | $2.03 | — | $11,169.15 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+4.7; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `NAVN` | 27 | $20.61 | $2.11 | — | $11,723.52 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-24.7; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `SLBT` | 271 | $2.09 | $3.57 | — | $12,286.34 | — | last bar red; gate last_red=True; list yday_mover; ret5=-36.6; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BHVN` | 43 | $13.03 | $2.15 | — | $12,844.48 | — | last bar red; gate last_red=True; list yday_mover; ret5=-18.5; leftover $568.13 | — |
| 2026-09-11 09:30 ET | **SHORT** | `AEO` | 38 | $14.71 | $2.14 | — | $13,401.32 | — | last bar red; gate last_red=True; list yday_mover; ret5=-12.8; leftover $568.13 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,401.32 | ▲ close $9,113.33 vs 09:30 $9,090.10 (session +42.13) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,401.32 | ▲ 09:30 equity $9,189.22 vs yday $9,113.33 (+75.89) | — | — |
| 2026-09-14 09:30 ET | **COVER** | `AUPH` | 34 | $16.03 | $2.09 | $+4.28 | $12,854.21 | ▲ +4.28 after sell → book $9,187.13; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `OVID` | 208 | $2.75 | $2.68 | $-10.63 | $12,278.48 | ▼ -10.63 after sell → book $9,184.44; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `ORCL` | 3 | $141.42 | $2.00 | $+65.00 | $11,852.22 | ▲ +65.00 after sell → book $9,182.44; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `NVT` | 3 | $150.00 | $2.00 | $+19.31 | $11,400.22 | ▲ +19.31 after sell → book $9,180.44; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `NAVN` | 27 | $21.10 | $2.07 | $-17.41 | $10,828.45 | ▼ -17.41 after sell → book $9,178.37; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `SLBT` | 271 | $2.02 | $3.50 | $+11.91 | $10,277.54 | ▲ +11.91 after sell → book $9,174.88; vs 09:30 mark -3.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `BHVN` | 43 | $12.52 | $2.12 | $+17.66 | $9,737.06 | ▲ +17.66 after sell → book $9,172.76; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `AEO` | 38 | $14.85 | $2.10 | $-9.56 | $9,170.65 | ▼ -9.56 after sell → book $9,170.65; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,170.65 | ▲ close $9,170.65 vs 09:30 $9,189.22 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,170.65 | ▲ 09:30 equity $9,170.65 vs yday $9,170.65 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,170.65 | ▲ close $9,170.65 vs 09:30 $9,170.65 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,170.65 | ▲ 09:30 equity $9,170.65 vs yday $9,170.65 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 40 | $14.31 | $2.15 | — | $9,740.91 | — | last bar red; gate last_red=True; list flatten; ret5=+4.8; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `WAY` | 21 | $26.27 | $2.09 | — | $10,290.49 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `SWRD` | 286 | $2.00 | $3.76 | — | $10,858.73 | — | last bar red; gate last_red=True; list yday_mover; ret5=+0.0; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `ALHC` | 55 | $10.30 | $2.19 | — | $11,423.04 | — | last bar red; gate last_red=True; list yday_mover; ret5=-23.0; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `PLAY` | 83 | $6.86 | $2.28 | — | $11,990.14 | — | last bar red; gate last_red=True; list yday_mover; ret5=-22.4; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `DVLT` | 3582 | $0.16 | $17.10 | — | $12,546.16 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.8; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `USDE` | 94 | $6.06 | $2.31 | — | $13,113.49 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-19.6; leftover $573.17 | — |
| 2026-09-16 09:30 ET | **SHORT** | `NMRA` | 679 | $0.84 | $7.91 | — | $13,678.66 | — | last bar red; gate last_red=True; list yday_mover; ret5=-33.5; leftover $573.17 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,678.66 | ▲ close $9,231.35 vs 09:30 $9,170.65 (session +100.48) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,678.66 | ▲ 09:30 equity $9,247.35 vs yday $9,231.35 (+16.00) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `AVAH` | 40 | $14.33 | $2.11 | $-5.06 | $13,103.35 | ▼ -5.06 after sell → book $9,245.24; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `WAY` | 21 | $26.51 | $2.05 | $-9.18 | $12,544.59 | ▼ -9.18 after sell → book $9,243.19; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `SWRD` | 286 | $1.72 | $3.69 | $+72.63 | $12,048.98 | ▲ +72.63 after sell → book $9,239.50; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `PLAY` | 83 | $6.96 | $2.24 | $-12.82 | $11,469.06 | ▼ -12.82 after sell → book $9,237.26; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `DVLT` | 3582 | $0.17 | $16.84 | $-69.75 | $10,843.28 | ▼ -69.75 after sell → book $9,220.42; vs 09:30 mark -16.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `USDE` | 94 | $6.61 | $2.27 | $-56.29 | $10,219.67 | ▼ -56.29 after sell → book $9,218.15; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `NMRA` | 679 | $0.78 | $7.33 | $+28.22 | $9,682.72 | ▲ +28.22 after sell → book $9,210.82; vs 09:30 mark -7.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `CYPH` | 245 | $2.67 | $3.23 | — | $10,334.86 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-0.4; leftover $657.92 | — |
| 2026-09-17 09:30 ET | **SHORT** | `MRLN` | 289 | $2.27 | $3.80 | — | $10,987.09 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.6; leftover $657.92 | — |
| 2026-09-17 09:30 ET | **SHORT** | `PALI` | 375 | $1.75 | $4.93 | — | $11,638.41 | — | last bar red; gate last_red=True; list yday_mover; ret5=-17.6; leftover $657.92 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BAK` | 371 | $1.77 | $4.88 | — | $12,290.20 | — | last bar red; gate last_red=True; list yday_mover; ret5=-10.2; leftover $657.92 | — |
| 2026-09-17 09:30 ET | **SHORT** | `JBHT` | 2 | $238.60 | $2.03 | — | $12,765.38 | — | last bar red; gate last_red=True; list yday_mover; ret5=-11.6; leftover $657.92 | — |
| 2026-09-17 09:30 ET | **SHORT** | `INDP` | 199 | $3.30 | $2.65 | — | $13,419.43 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=+54.3; leftover $657.92 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BTGO` | 100 | $6.56 | $2.33 | — | $14,073.09 | — | last bar red; gate last_red=True; list yday_mover; ret5=-14.6; leftover $657.92 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,073.09 | ▼ close $9,013.84 vs 09:30 $9,247.35 (session -173.13) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,073.09 | ▲ 09:30 equity $9,033.47 vs yday $9,013.84 (+19.63) | — | — |
| 2026-09-18 09:30 ET | **COVER** | `ALHC` | 55 | $8.68 | $2.15 | $+84.75 | $13,593.54 | ▲ +84.75 after sell → book $9,031.31; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `CYPH` | 245 | $3.04 | $3.16 | $-94.59 | $12,846.80 | ▼ -94.59 after sell → book $9,028.15; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `MRLN` | 289 | $2.07 | $3.73 | $+50.27 | $12,244.84 | ▲ +50.27 after sell → book $9,024.42; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `PALI` | 375 | $1.68 | $4.84 | $+16.48 | $11,610.01 | ▲ +16.48 after sell → book $9,019.59; vs 09:30 mark -4.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BAK` | 371 | $1.77 | $4.79 | $-9.66 | $10,948.55 | ▼ -9.66 after sell → book $9,014.80; vs 09:30 mark -4.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `JBHT` | 2 | $236.80 | $2.00 | $-0.43 | $10,472.96 | ▼ -0.43 after sell → book $9,012.81; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `INDP` | 199 | $3.85 | $2.59 | $-114.68 | $9,704.22 | ▼ -114.68 after sell → book $9,010.22; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BTGO` | 100 | $6.94 | $2.29 | $-42.62 | $9,007.93 | ▼ -42.62 after sell → book $9,007.93; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `GNRC` | 10 | $209.52 | $2.10 | — | $11,101.02 | — | last bar red; gate last_red=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $2251.98 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 65 | $34.44 | $2.28 | — | $13,337.35 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $2251.98 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,337.35 | ▲ close $9,152.40 vs 09:30 $9,033.47 (session +148.85) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,337.35 | ▼ 09:30 equity $9,092.35 vs yday $9,152.40 (-60.05) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `GNRC` | 10 | $210.00 | $2.02 | $-8.92 | $11,235.33 | ▼ -8.92 after sell → book $9,090.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `FIVN` | 65 | $33.00 | $2.19 | $+89.14 | $9,088.14 | ▲ +89.14 after sell → book $9,088.14; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `PGEN` | 72 | $7.84 | $2.24 | — | $9,650.38 | — | last bar red; gate last_red=True; list flatten; ret5=+13.6; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 68 | $8.26 | $2.23 | — | $10,209.83 | — | last bar red; gate last_red=True; list yday_mover; ret5=+7.7; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `XENE` | 14 | $40.00 | $2.07 | — | $10,767.76 | — | last bar red; gate last_red=True; list yday_mover; ret5=-32.2; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SION` | 94 | $6.00 | $2.31 | — | $11,329.45 | — | last bar red; gate last_red=True; list yday_mover; ret5=-24.1; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `KDK` | 166 | $3.42 | $2.54 | — | $11,894.63 | — | last bar red; gate last_red=True; list yday_mover; ret5=-12.3; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ABVX` | 5 | $105.72 | $2.04 | — | $12,421.19 | — | last bar red; gate last_red=True; list overnight; ret5=-11.5; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `MLKN` | 27 | $20.85 | $2.11 | — | $12,982.03 | — | last bar red; gate last_red=True; list overnight; ret5=-2.5; leftover $568.01 | — |
| 2026-09-21 09:30 ET | **SHORT** | `THO` | 8 | $68.39 | $2.05 | — | $13,527.10 | — | last bar red; gate last_red=True; list overnight; ret5=-7.0; leftover $568.01 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,527.10 | ▲ close $9,224.05 vs 09:30 $9,092.35 (session +153.50) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,527.10 | ▲ 09:30 equity $9,224.99 vs yday $9,224.05 (+0.94) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `SION` | 94 | $5.99 | $2.27 | $-3.65 | $12,961.77 | ▼ -3.65 after sell → book $9,222.72; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `DEFT` | 993 | $0.58 | $8.93 | — | $13,528.78 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $576.42 | — |
| 2026-09-22 09:30 ET | **SHORT** | `GLND` | 196 | $2.94 | $2.64 | — | $14,102.39 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+136.1; leftover $576.42 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USDE` | 44 | $12.99 | $2.16 | — | $14,671.79 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+69.4; leftover $576.42 | — |
| 2026-09-22 09:30 ET | **SHORT** | `MX` | 181 | $3.18 | $2.59 | — | $15,244.78 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $576.42 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,244.78 | ▲ close $9,312.52 vs 09:30 $9,224.99 (session +106.11) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,244.78 | ▼ 09:30 equity $9,215.99 vs yday $9,312.52 (-96.53) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `XENE` | 14 | $39.51 | $2.03 | $+2.76 | $14,689.61 | ▲ +2.76 after sell → book $9,213.96; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `KDK` | 166 | $3.19 | $2.49 | $+33.15 | $14,157.58 | ▲ +33.15 after sell → book $9,211.47; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `ABVX` | 5 | $98.30 | $2.00 | $+33.06 | $13,664.07 | ▲ +33.06 after sell → book $9,209.47; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `MLKN` | 27 | $19.76 | $2.07 | $+25.25 | $13,128.48 | ▲ +25.25 after sell → book $9,207.40; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `THO` | 8 | $71.41 | $2.01 | $-28.22 | $12,555.19 | ▼ -28.22 after sell → book $9,205.38; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `DEFT` | 993 | $0.57 | $8.69 | $-12.65 | $11,975.53 | ▼ -12.65 after sell → book $9,196.70; vs 09:30 mark -8.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `GLND` | 196 | $2.70 | $2.58 | $+41.83 | $11,443.75 | ▲ +41.83 after sell → book $9,194.12; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 44 | $13.22 | $2.12 | $-14.40 | $10,859.95 | ▼ -14.40 after sell → book $9,192.00; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `MX` | 181 | $3.19 | $2.53 | $-6.93 | $10,280.02 | ▼ -6.93 after sell → book $9,189.46; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 6 | $116.85 | $2.05 | — | $10,979.08 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $765.79 | — |
| 2026-09-23 09:30 ET | **SHORT** | `FTRE` | 37 | $20.25 | $2.14 | — | $11,726.18 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $765.79 | — |
| 2026-09-23 09:30 ET | **SHORT** | `MAZE` | 27 | $28.30 | $2.11 | — | $12,488.17 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $765.79 | — |
| 2026-09-23 09:30 ET | **SHORT** | `BLLN` | 6 | $116.00 | $2.05 | — | $13,182.13 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $765.79 | — |
| 2026-09-23 09:30 ET | **SHORT** | `VICR` | 2 | $266.50 | $2.03 | — | $13,713.09 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $765.79 | — |
| 2026-09-23 09:30 ET | **SHORT** | `DNA` | 83 | $9.13 | $2.28 | — | $14,468.60 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $765.79 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,468.60 | ▲ close $9,252.99 vs 09:30 $9,215.99 (session +76.19) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,468.60 | ▲ 09:30 equity $9,299.09 vs yday $9,252.99 (+46.10) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `PGEN` | 72 | $7.38 | $2.21 | $+28.67 | $13,935.03 | ▲ +28.67 after sell → book $9,296.88; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AEHL` | 68 | $8.21 | $2.19 | $-1.02 | $13,374.56 | ▼ -1.02 after sell → book $9,294.69; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `HALO` | 6 | $112.22 | $2.01 | $+23.72 | $12,699.23 | ▲ +23.72 after sell → book $9,292.68; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `FTRE` | 37 | $19.40 | $2.10 | $+27.21 | $11,979.33 | ▲ +27.21 after sell → book $9,290.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `MAZE` | 27 | $28.15 | $2.07 | $-0.13 | $11,217.21 | ▼ -0.13 after sell → book $9,288.51; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `BLLN` | 6 | $112.33 | $2.01 | $+17.96 | $10,541.22 | ▲ +17.96 after sell → book $9,286.50; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `VICR` | 2 | $274.61 | $2.00 | $-20.25 | $9,990.01 | ▼ -20.25 after sell → book $9,284.51; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `DNA` | 83 | $8.50 | $2.24 | $+47.77 | $9,282.27 | ▲ +47.77 after sell → book $9,282.27; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,282.27 | ▲ close $9,282.27 vs 09:30 $9,299.09 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,098.41 | ▲ 09:30 equity $10,098.41 vs yday $10,098.41 (+0.00) | 09:30 open · cash $10,098.41 · no holdings · equity $10,098.41 vs prior close $10,098.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **SHORT** | `OMER` | 30 | $20.61 | $2.12 | — | $10,714.59 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+9.1; leftover $631.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `NEOV` | 264 | $2.39 | $3.48 | — | $11,342.08 | — | last bar red; gate last_red=True; list yday_mover; ret5=-31.4; leftover $631.15 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SFIX` | 286 | $2.20 | $3.76 | — | $11,967.51 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-24.1; leftover $631.15 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `LRMR` | 190 | $3.32 | $2.62 | — | $12,595.69 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-12.6; leftover $631.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `ACAD` | 28 | $22.21 | $2.11 | — | $13,215.46 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-19.2; leftover $631.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SGMT` | 69 | $9.11 | $2.24 | — | $13,841.82 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $631.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SMWB` | 84 | $7.50 | $2.28 | — | $14,469.53 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-9.7; leftover $631.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,469.53 | ▲ close $10,235.07 vs 09:30 $10,098.41 (session +155.27) | 16:00 close · cash $14,469.53 · equity $10,235.07 vs 09:30 $10,098.41 (+136.66; session marks +155.27) · 7 name(s) marked open→close (per-name table). OMER×30 09:30 $20.61 → close $20.08 +15.90; NEOV×264 09:30 $2.39 → close $2.19 +52.80; SFIX×286 09:30 $2.20 → close $2.15 +12.87; LRMR×190 09:30 $3.32 → close $3.08 +46.55; ACAD×28 09:30 $22.21 → close $20.68 +42.84; SGMT×69 09:30 $9.11 → close $9.24 -8.97; SMWB×84 09:30 $7.50 → close $7.58 -6.72 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AEM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XLAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ON` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SYNA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CDW` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ADBT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `XHLD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARQQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AEHL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `XENE` | no_price | no 09:30 open — carry |
| 2026-09-22 | `KDK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
