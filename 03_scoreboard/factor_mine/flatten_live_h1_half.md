# Factor mine action — `flatten_live_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **-0.93%** ($9,907) · signal-only (no cash/fees) was +4.45%. Starts YES **7/30**. Fills 48 · skips 0 · realized $+373.73.

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
- Only spend half of leftover cash; the rest stays cash.
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
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,373.74.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,381.42 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,833.35 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,211.77 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,586.30 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,975.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,351.40 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,722.05 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $625.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,141.88 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $625.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,141.88 | ▲ close $10,095.50 vs 09:30 $10,000.00 (session +114.73) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,141.88 | ▲ 09:30 equity $10,227.73 vs yday $10,095.50 (+132.23) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,796.78 | ▲ +36.32 after sell → book $10,225.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,369.08 | ▲ +24.22 after sell → book $10,223.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,019.48 | ▲ +28.82 after sell → book $10,221.51; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,629.49 | ▼ -15.46 after sell → book $10,219.16; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,283.66 | ▲ +43.55 after sell → book $10,217.06; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,957.16 | ▲ +49.21 after sell → book $10,214.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,591.51 | ▲ +5.00 after sell → book $10,210.31; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,208.29 | ▲ +36.62 after sell → book $10,208.29; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,609.14 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,970.64 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,536.04 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,899.47 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,258.88 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,617.72 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,018.50 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $638.02 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,374.71 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $638.02 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,374.71 | ▲ close $10,312.07 vs 09:30 $10,227.73 (session +127.88) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,374.71 | ▲ 09:30 equity $10,495.21 vs yday $10,312.07 (+183.14) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $5,975.24 | ▲ +1.37 after sell → book $10,493.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.57 | $2.12 | $-27.53 | $6,586.21 | ▼ -27.53 after sell → book $10,491.07; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,018.25 | ▼ -2.55 after sell → book $10,489.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.33 | $2.18 | $+121.06 | $7,775.88 | ▲ +121.06 after sell → book $10,486.87; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.40 | $3.38 | $-24.77 | $8,391.70 | ▼ -24.77 after sell → book $10,483.49; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.88 | $4.32 | $-25.08 | $9,007.78 | ▼ -25.08 after sell → book $10,479.17; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.75 | $2.04 | $-13.76 | $9,593.24 | ▼ -13.76 after sell → book $10,477.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,470.81 | ▲ +233.78 after sell → book $10,470.81; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,495.21 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,470.81 | ▲ close $10,470.81 vs 09:30 $10,470.81 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,470.81 | ▲ 09:30 equity $10,470.81 vs yday $10,470.81 (-0.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 189 | $3.46 | $2.56 | — | $9,814.31 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 259 | $2.52 | $3.34 | — | $9,158.29 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 97 | $6.71 | $2.28 | — | $8,505.14 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 344 | $1.90 | $4.44 | — | $7,847.10 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 136 | $4.78 | $2.40 | — | $7,194.62 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 411 | $1.59 | $5.30 | — | $6,535.83 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 57 | $11.31 | $2.16 | — | $5,889.00 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $654.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 12 | $52.03 | $2.03 | — | $5,262.61 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $654.43 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,262.61 | ▼ close $10,420.33 vs 09:30 $10,470.81 (session -25.98) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,262.61 | ▼ 09:30 equity $10,398.58 vs yday $10,420.33 (-21.75) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 189 | $3.43 | $2.60 | $-10.83 | $5,908.28 | ▼ -10.83 after sell → book $10,395.98; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 259 | $2.38 | $3.39 | $-43.00 | $6,521.31 | ▼ -43.00 after sell → book $10,392.59; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 97 | $6.57 | $2.31 | $-18.17 | $7,156.29 | ▼ -18.17 after sell → book $10,390.28; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 344 | $2.00 | $4.50 | $+25.46 | $7,839.79 | ▲ +25.46 after sell → book $10,385.78; vs 09:30 mark -4.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 136 | $4.30 | $2.43 | $-70.11 | $8,422.16 | ▼ -70.11 after sell → book $10,383.35; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 411 | $1.63 | $5.38 | $+5.76 | $9,086.71 | ▲ +5.76 after sell → book $10,377.97; vs 09:30 mark -5.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 57 | $11.22 | $2.18 | $-9.47 | $9,724.07 | ▼ -9.47 after sell → book $10,375.79; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 12 | $54.31 | $2.05 | $+23.29 | $10,373.74 | ▲ +23.29 after sell → book $10,373.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,398.58 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.74 | ▲ 09:30 equity $10,373.74 vs yday $10,373.74 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.74 | ▲ close $10,373.74 vs 09:30 $10,373.74 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.34 | ▲ 09:30 equity $9,907.34 vs yday $9,907.34 (+0.00) | 09:30 open · cash $9,907.34 · no holdings · equity $9,907.34 vs prior close $9,907.34 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.34 | ▲ close $9,907.34 vs 09:30 $9,907.34 (session +0.00) | 16:00 close · cash $9,907.34 · no lots left · equity $9,907.34. | — |
