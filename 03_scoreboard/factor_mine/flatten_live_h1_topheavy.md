# Factor mine action — `flatten_live_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **-1.75%** ($9,825) · signal-only (no cash/fees) was +4.45%. Starts YES **7/30**. Fills 48 · skips 0 · realized $+718.61.

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
- Give about 40% of leftover cash to the first name; split the rest.
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
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,718.60.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 194 | $20.55 | $2.57 | — | $6,010.73 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4000.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,189.62 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $857.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 41 | $20.65 | $2.11 | — | $4,340.86 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $857.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 148 | $5.77 | $2.43 | — | $3,484.46 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $857.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 43 | $19.63 | $2.12 | — | $2,638.25 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $857.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $1,806.54 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $857.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 489 | $1.75 | $6.31 | — | $944.48 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $857.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $219.78 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $857.14 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.78 | ▲ close $10,231.72 vs 09:30 $10,000.00 (session +253.36) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.78 | ▲ 09:30 equity $10,520.65 vs yday $10,231.72 (+288.93) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 194 | $21.90 | $2.64 | $+256.69 | $4,465.74 | ▲ +256.69 after sell → book $10,518.01; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,325.18 | ▲ +38.34 after sell → book $10,515.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 41 | $21.75 | $2.13 | $+40.85 | $6,214.80 | ▲ +40.85 after sell → book $10,513.84; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 148 | $5.67 | $2.47 | $-19.70 | $7,051.49 | ▼ -19.70 after sell → book $10,511.37; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 43 | $21.17 | $2.14 | $+61.96 | $7,959.66 | ▲ +61.96 after sell → book $10,509.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $8,858.33 | ▲ +66.95 after sell → book $10,507.14; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 489 | $1.79 | $6.40 | $+6.85 | $9,727.24 | ▲ +6.85 after sell → book $10,500.74; vs 09:30 mark -6.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 5 | $154.70 | $2.02 | $+46.77 | $10,498.71 | ▲ +46.77 after sell → book $10,498.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 35 | $119.43 | $2.10 | — | $6,316.57 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $4199.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 52 | $17.20 | $2.15 | — | $5,420.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $899.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,552.82 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $899.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 80 | $11.13 | $2.23 | — | $3,660.19 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $899.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 364 | $2.47 | $4.70 | — | $2,756.42 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $899.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 466 | $1.93 | $6.01 | — | $1,851.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $899.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $953.19 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $899.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 681 | $1.32 | $8.78 | — | $45.48 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $899.89 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.48 | ▲ close $10,697.74 vs 09:30 $10,520.65 (session +229.03) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.48 | ▲ 09:30 equity $10,936.65 vs yday $10,697.74 (+238.91) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 35 | $120.51 | $2.14 | $+33.57 | $4,261.20 | ▲ +33.57 after sell → book $10,934.52; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 52 | $16.57 | $2.17 | $-37.07 | $5,120.67 | ▼ -37.07 after sell → book $10,932.35; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $5,986.77 | ▼ -1.10 after sell → book $10,930.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 80 | $13.33 | $2.25 | $+171.52 | $7,050.91 | ▲ +171.52 after sell → book $10,928.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 364 | $2.40 | $4.77 | $-34.94 | $7,919.75 | ▼ -34.94 after sell → book $10,923.31; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 466 | $1.88 | $6.10 | $-35.41 | $8,789.73 | ▼ -35.41 after sell → book $10,917.21; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.75 | $2.06 | $-18.64 | $9,668.92 | ▼ -18.64 after sell → book $10,915.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 681 | $1.83 | $8.91 | $+329.62 | $10,906.25 | ▲ +329.62 after sell → book $10,906.25; vs 09:30 mark -8.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,936.65 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 1260 | $3.46 | $16.25 | — | $6,530.39 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $4362.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 370 | $2.52 | $4.77 | — | $5,593.22 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $934.82 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 139 | $6.71 | $2.41 | — | $4,658.12 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $934.82 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 492 | $1.90 | $6.35 | — | $3,716.98 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $934.82 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 195 | $4.78 | $2.58 | — | $2,782.30 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $934.82 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 587 | $1.59 | $7.57 | — | $1,841.40 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $934.82 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 82 | $11.31 | $2.24 | — | $911.74 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $934.82 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 17 | $52.03 | $2.04 | — | $25.19 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $934.82 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.19 | ▼ close $10,834.63 vs 09:30 $10,906.25 (session -27.41) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.19 | ▼ 09:30 equity $10,763.44 vs yday $10,834.63 (-71.19) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 1260 | $3.43 | $16.50 | $-70.55 | $4,330.49 | ▼ -70.55 after sell → book $10,746.94; vs 09:30 mark -16.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 370 | $2.38 | $4.84 | $-61.42 | $5,206.25 | ▼ -61.42 after sell → book $10,742.10; vs 09:30 mark -4.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 139 | $6.57 | $2.44 | $-24.31 | $6,117.04 | ▼ -24.31 after sell → book $10,739.66; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 492 | $2.00 | $6.44 | $+36.41 | $7,094.60 | ▲ +36.41 after sell → book $10,733.22; vs 09:30 mark -6.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 195 | $4.30 | $2.62 | $-98.79 | $7,930.48 | ▼ -98.79 after sell → book $10,730.60; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 587 | $1.63 | $7.68 | $+8.23 | $8,879.61 | ▲ +8.23 after sell → book $10,722.92; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 82 | $11.22 | $2.26 | $-11.88 | $9,797.39 | ▼ -11.88 after sell → book $10,720.66; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 17 | $54.31 | $2.06 | $+34.66 | $10,718.60 | ▲ +34.66 after sell → book $10,718.60; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,763.44 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,825.11 | ▲ 09:30 equity $9,825.11 vs yday $9,825.11 (+0.00) | 09:30 open · cash $9,825.11 · no holdings · equity $9,825.11 vs prior close $9,825.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,825.11 | ▲ close $9,825.11 vs 09:30 $9,825.11 (session +0.00) | 16:00 close · cash $9,825.11 · no lots left · equity $9,825.11. | — |
