# Factor mine action — `flatten_live_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · 09:30 tickets only when flatten_robust gate fires (mover)

Cash book **-9.67%** ($9,033) · signal-only (no cash/fees) was +5.13%. Starts YES **0/30**. Fills 42 · skips 87 · realized $-550.56.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,449.44.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $23.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $23.36 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▲ close $10,474.93 vs 09:30 $10,475.50 (session +0.63) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,582.83 vs yday $10,474.93 (+107.90) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▼ close $10,549.90 vs 09:30 $10,582.83 (session -32.93) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▼ 09:30 equity $10,384.32 vs yday $10,549.90 (-165.58) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▲ close $10,803.37 vs 09:30 $10,384.32 (session +419.05) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▼ 09:30 equity $10,608.36 vs yday $10,803.37 (-195.01) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▼ close $10,460.93 vs 09:30 $10,608.36 (session -147.43) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,484.99 vs yday $10,460.93 (+24.06) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 60 | $20.93 | $2.19 | $+18.44 | $1,332.03 | ▲ +18.44 after sell → book $10,482.80; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 13 | $95.52 | $2.05 | $+54.55 | $2,571.74 | ▲ +54.55 after sell → book $10,480.75; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 60 | $21.31 | $2.19 | $+35.24 | $3,848.15 | ▲ +35.24 after sell → book $10,478.56; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 216 | $5.49 | $2.83 | $-66.10 | $5,031.16 | ▼ -66.10 after sell → book $10,475.73; vs 09:30 mark -2.83 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 63 | $21.47 | $2.20 | $+111.54 | $6,381.57 | ▲ +111.54 after sell → book $10,473.53; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 42 | $32.32 | $2.14 | $+108.73 | $7,736.87 | ▲ +108.73 after sell → book $10,471.39; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 714 | $1.91 | $9.34 | $+95.69 | $9,091.27 | ▲ +95.69 after sell → book $10,462.05; vs 09:30 mark -9.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $155.89 | $2.03 | $+86.75 | $10,336.36 | ▲ +86.75 after sell → book $10,460.02; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,336.36 | ▲ close $10,463.26 vs 09:30 $10,484.99 (session +3.25) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,336.36 | ▼ 09:30 equity $10,460.47 vs yday $10,463.26 (-2.79) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $10,352.61 | ▼ -1.12 after sell → book $10,460.28; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $10,383.13 | ▲ +8.04 after sell → book $10,459.94; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.35 | $0.26 | $-1.59 | $10,404.03 | ▼ -1.59 after sell → book $10,459.69; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.06 | $0.30 | $+0.99 | $10,428.44 | ▲ +0.99 after sell → book $10,459.38; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 17 | $1.82 | $0.38 | $+7.84 | $10,459.00 | ▲ +7.84 after sell → book $10,459.00; vs 09:30 mark -0.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,460.47 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.00 | ▲ close $10,459.00 vs 09:30 $10,459.00 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.00 | ▲ 09:30 equity $10,459.00 vs yday $10,459.00 (+0.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 377 | $3.46 | $4.86 | — | $9,149.72 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 518 | $2.52 | $6.68 | — | $7,837.68 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 194 | $6.71 | $2.57 | — | $6,533.36 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 688 | $1.90 | $8.88 | — | $5,217.29 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 273 | $4.78 | $3.52 | — | $3,908.83 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 822 | $1.59 | $10.60 | — | $2,591.24 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 115 | $11.31 | $2.33 | — | $1,288.26 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1307.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 24 | $52.03 | $2.06 | — | $37.48 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $1307.38 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $10,365.15 vs 09:30 $10,459.00 (session -52.34) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $10,321.51 vs yday $10,365.15 (-43.64) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $10,164.71 vs 09:30 $10,321.51 (session -156.80) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $10,119.47 vs yday $10,164.71 (-45.24) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $9,652.65 vs 09:30 $10,119.47 (session -466.82) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $9,505.62 vs yday $9,652.65 (-147.03) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $9,317.03 vs 09:30 $9,505.62 (session -188.60) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▲ 09:30 equity $9,431.27 vs yday $9,317.03 (+114.24) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▲ close $9,441.08 vs 09:30 $9,431.27 (session +9.81) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▲ 09:30 equity $9,491.54 vs yday $9,441.08 (+50.46) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CABA` | 377 | $2.72 | $4.94 | $-288.78 | $1,057.98 | ▼ -288.78 after sell → book $9,486.60; vs 09:30 mark -4.94 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 518 | $2.15 | $6.78 | $-205.12 | $2,164.90 | ▼ -205.12 after sell → book $9,479.82; vs 09:30 mark -6.78 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 194 | $5.93 | $2.61 | $-156.51 | $3,312.71 | ▼ -156.51 after sell → book $9,477.21; vs 09:30 mark -2.61 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 688 | $1.72 | $9.00 | $-145.15 | $4,483.63 | ▼ -145.15 after sell → book $9,468.21; vs 09:30 mark -9.00 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 273 | $4.13 | $3.58 | $-184.55 | $5,607.54 | ▼ -184.55 after sell → book $9,464.63; vs 09:30 mark -3.58 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 822 | $1.59 | $10.75 | $-21.35 | $6,903.77 | ▼ -21.35 after sell → book $9,453.88; vs 09:30 mark -10.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 115 | $10.73 | $2.36 | $-71.40 | $8,135.36 | ▼ -71.40 after sell → book $9,451.52; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ATRC` | 24 | $54.84 | $2.08 | $+63.30 | $9,449.44 | ▲ +63.30 after sell → book $9,449.44; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,491.54 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.44 | ▲ 09:30 equity $9,449.44 vs yday $9,449.44 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.44 | ▲ close $9,449.44 vs 09:30 $9,449.44 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,033.41 | ▲ 09:30 equity $9,033.41 vs yday $9,033.41 (+0.00) | 09:30 open · cash $9,033.41 · no holdings · equity $9,033.41 vs prior close $9,033.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,033.41 | ▲ close $9,033.41 vs 09:30 $9,033.41 (session +0.00) | 16:00 close · cash $9,033.41 · no lots left · equity $9,033.41. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
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
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
