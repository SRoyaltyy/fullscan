# Factor mine action — `flatten_live_h1_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **-2.65%** ($9,735) · signal-only (no cash/fees) was +4.45%. Starts YES **6/30**. Fills 48 · skips 0 · realized $+403.06.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Live flatten gate: new buys only when flatten_robust would actually send 09:30 tickets.

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If the live flatten gate is HOLD / io that morning, buy nobody new.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,403.05.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 108 | $20.55 | $2.31 | — | $7,778.29 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $2222.22 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $5,865.02 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1944.44 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 80 | $20.65 | $2.23 | — | $4,210.79 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1666.67 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 240 | $5.77 | $3.10 | — | $2,822.90 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1388.89 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $1,721.46 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1111.11 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $889.75 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $833.33 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 317 | $1.75 | $4.09 | — | $330.91 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $555.56 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $184.92 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $277.78 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $184.92 | ▲ close $10,198.31 vs 09:30 $10,000.00 (session +217.77) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.92 | ▲ 09:30 equity $10,469.45 vs yday $10,198.31 (+271.14) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 108 | $21.90 | $2.35 | $+141.14 | $2,547.77 | ▲ +141.14 after sell → book $10,467.10; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $4,555.81 | ▲ +94.78 after sell → book $10,465.02; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 80 | $21.75 | $2.26 | $+83.51 | $6,293.55 | ▲ +83.51 after sell → book $10,462.76; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 240 | $5.67 | $3.15 | $-30.24 | $7,651.20 | ▼ -30.24 after sell → book $10,459.61; vs 09:30 mark -3.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $8,834.55 | ▲ +81.90 after sell → book $10,457.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $9,733.21 | ▲ +66.95 after sell → book $10,455.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 317 | $1.79 | $4.15 | $+4.44 | $10,296.49 | ▲ +4.44 after sell → book $10,451.19; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 1 | $154.70 | $1.57 | $+7.14 | $10,449.62 | ▲ +7.14 after sell → book $10,449.62; vs 09:30 mark -1.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 19 | $119.43 | $2.05 | — | $8,178.40 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $2322.14 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 118 | $17.20 | $2.34 | — | $6,146.46 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $2031.87 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 8 | $216.30 | $2.01 | — | $4,414.04 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $1741.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 130 | $11.13 | $2.38 | — | $2,964.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $1451.34 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 470 | $2.47 | $6.06 | — | $1,797.80 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $1161.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 451 | $1.93 | $5.82 | — | $921.55 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $870.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 9 | $59.72 | $2.02 | — | $382.06 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $580.53 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 219 | $1.32 | $2.83 | — | $90.15 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $290.27 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.15 | ▲ close $10,653.05 vs 09:30 $10,469.45 (session +228.94) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.15 | ▲ 09:30 equity $10,709.64 vs yday $10,653.05 (+56.59) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 19 | $120.51 | $2.08 | $+16.40 | $2,377.77 | ▲ +16.40 after sell → book $10,707.57; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 118 | $16.57 | $2.38 | $-79.06 | $4,330.65 | ▼ -79.06 after sell → book $10,705.19; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 8 | $217.03 | $2.04 | $+1.79 | $6,064.85 | ▲ +1.79 after sell → book $10,703.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 130 | $13.33 | $2.42 | $+281.20 | $7,795.33 | ▲ +281.20 after sell → book $10,700.73; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 470 | $2.40 | $6.15 | $-45.11 | $8,917.18 | ▼ -45.11 after sell → book $10,694.58; vs 09:30 mark -6.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 451 | $1.88 | $5.90 | $-34.27 | $9,759.16 | ▼ -34.27 after sell → book $10,688.68; vs 09:30 mark -5.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 9 | $58.75 | $2.04 | $-12.78 | $10,285.87 | ▼ -12.78 after sell → book $10,686.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 219 | $1.83 | $2.87 | $+105.99 | $10,683.77 | ▲ +105.99 after sell → book $10,683.77; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,709.64 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 686 | $3.46 | $8.85 | — | $8,301.36 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $2374.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 824 | $2.52 | $10.63 | — | $6,214.25 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $2077.40 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 265 | $6.71 | $3.42 | — | $4,432.68 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1780.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 780 | $1.90 | $10.06 | — | $2,940.62 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1483.86 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 248 | $4.78 | $3.20 | — | $1,751.98 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1187.09 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 559 | $1.59 | $7.21 | — | $855.96 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $890.31 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 52 | $11.31 | $2.15 | — | $265.70 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $593.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 5 | $52.03 | $2.00 | — | $3.54 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $296.77 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.54 | ▼ close $10,573.02 vs 09:30 $10,683.77 (session -63.23) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.54 | ▼ 09:30 equity $10,451.25 vs yday $10,573.02 (-121.77) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 686 | $3.43 | $8.98 | $-38.41 | $2,347.54 | ▼ -38.41 after sell → book $10,442.27; vs 09:30 mark -8.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 824 | $2.38 | $10.78 | $-136.77 | $4,297.88 | ▼ -136.77 after sell → book $10,431.49; vs 09:30 mark -10.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 265 | $6.57 | $3.48 | $-43.99 | $6,035.45 | ▼ -43.99 after sell → book $10,428.01; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 780 | $2.00 | $10.20 | $+57.73 | $7,585.25 | ▲ +57.73 after sell → book $10,417.81; vs 09:30 mark -10.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 248 | $4.30 | $3.25 | $-125.49 | $8,648.40 | ▼ -125.49 after sell → book $10,414.56; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 559 | $1.63 | $7.31 | $+7.83 | $9,552.25 | ▲ +7.83 after sell → book $10,407.24; vs 09:30 mark -7.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 52 | $11.22 | $2.17 | $-8.99 | $10,133.53 | ▼ -8.99 after sell → book $10,405.08; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 5 | $54.31 | $2.02 | $+7.37 | $10,403.05 | ▲ +7.37 after sell → book $10,403.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,451.25 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,403.05 | ▲ 09:30 equity $10,403.05 vs yday $10,403.05 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,403.05 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,735.28 | ▲ 09:30 equity $9,735.28 vs yday $9,735.28 (+0.00) | 09:30 open · cash $9,735.28 · no holdings · equity $9,735.28 vs prior close $9,735.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,735.28 | ▲ close $9,735.28 vs 09:30 $9,735.28 (session +0.00) | 16:00 close · cash $9,735.28 · no lots left · equity $9,735.28. | — |
