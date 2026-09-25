# Factor mine action — `flatten_h3_sizeup`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `sizeup` · S≥+5: 1.35× leftover

Cash book **-22.46%** ($7,754) · signal-only (no cash/fees) was -10.41%. Starts YES **1/30**. Fills 137 · skips 216 · realized $-1149.04.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On a strong morning (S ≥ +5), spend 1.35× leftover — still capped by cash.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `sizeup`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,386.97.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,525.15 vs 09:30 $10,414.78 (session +110.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.37 | ▼ -0.12 after sell → book $10,389.73; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.40 | ▼ -69.50 after sell → book $10,387.64; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.80 | ▲ +23.38 after sell → book $10,385.56; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.71 | ▼ -14.65 after sell → book $10,383.47; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.34 | ▲ +97.12 after sell → book $10,381.14; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.90 | ▼ -83.63 after sell → book $10,379.00; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.75 | ▲ +471.89 after sell → book $10,358.83; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,309.06 | ▼ -66.33 after sell → book $10,356.66; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.06 | ▼ close $10,355.74 vs 09:30 $10,391.80 (session -0.92) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.06 | ▲ 09:30 equity $10,355.88 vs yday $10,355.74 (+0.14) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.85 | ▼ -0.31 after sell → book $10,355.76; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,329.12 | ▼ -1.08 after sell → book $10,355.59; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.32 | ▼ -0.94 after sell → book $10,355.43; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,340.32 | ▲ close $10,355.75 vs 09:30 $10,355.88 (session +0.32) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.32 | ▼ 09:30 equity $10,355.62 vs yday $10,355.75 (-0.13) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,344.18 | ▼ -0.24 after sell → book $10,355.56; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.49 | ▼ -0.24 after sell → book $10,355.47; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 1 | $4.98 | $0.07 | $+0.05 | $10,355.40 | ▲ +0.05 after sell → book $10,355.40; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.98 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.64 | ▲ close $10,569.98 vs 09:30 $10,355.62 (session +239.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $26.21 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.04 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $26.21 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▲ close $10,844.89 vs 09:30 $10,845.87 (session +0.29) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,956.63 vs yday $10,844.89 (+111.74) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▼ close $10,921.87 vs 09:30 $10,956.63 (session -34.76) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▼ 09:30 equity $10,751.25 vs yday $10,921.87 (-170.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,351.68 | ▼ -18.63 after sell → book $10,749.05; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,691.67 | ▲ +63.82 after sell → book $10,747.00; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.47 | $2.20 | $-15.53 | $3,958.61 | ▼ -15.53 after sell → book $10,744.80; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,194.39 | ▼ -59.59 after sell → book $10,741.86; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $6,570.84 | ▲ +98.31 after sell → book $10,739.66; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,958.46 | ▲ +111.41 after sell → book $10,737.52; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.90 | $9.67 | $+91.65 | $9,352.89 | ▲ +91.65 after sell → book $10,727.85; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $10,602.94 | ▲ +91.71 after sell → book $10,725.82; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 74 | $23.77 | $2.21 | — | $8,841.74 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1767.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 160 | $10.98 | $2.47 | — | $7,082.47 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $1767.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 28 | $61.19 | $2.07 | — | $5,367.08 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $1767.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 211 | $8.35 | $2.72 | — | $3,602.51 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $1767.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 357 | $4.94 | $4.61 | — | $1,834.32 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $1767.16 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $426.97 | $2.00 | — | $124.44 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $1767.16 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.44 | ▲ close $10,810.14 vs 09:30 $10,751.25 (session +100.41) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.44 | ▲ 09:30 equity $10,812.32 vs yday $10,810.14 (+2.18) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $140.85 | ▼ -0.96 after sell → book $10,812.13; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $171.22 | ▲ +7.88 after sell → book $10,811.80; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $195.03 | ▼ -1.17 after sell → book $10,811.51; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $221.09 | ▲ +0.69 after sell → book $10,811.18; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $251.11 | ▲ +4.63 after sell → book $10,810.80; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.11 | ▼ close $10,770.76 vs 09:30 $10,812.32 (session -40.04) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.11 | ▼ 09:30 equity $10,766.53 vs yday $10,770.76 (-4.23) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 2 | $41.44 | $0.83 | — | $167.40 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $83.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 5 | $14.42 | $0.74 | — | $94.56 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $83.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 32 | $2.60 | $0.93 | — | $10.43 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $83.70 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.43 | ▼ close $10,652.32 vs 09:30 $10,766.53 (session -111.71) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.43 | ▲ 09:30 equity $10,684.15 vs yday $10,652.32 (+31.83) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 160 | $10.97 | $2.51 | $-6.58 | $1,763.12 | ▼ -6.58 after sell → book $10,681.64; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 28 | $60.52 | $2.10 | $-22.93 | $3,455.59 | ▼ -22.93 after sell → book $10,679.55; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 211 | $8.28 | $2.77 | $-20.26 | $5,199.90 | ▼ -20.26 after sell → book $10,676.78; vs 09:30 mark -2.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 357 | $4.95 | $4.68 | $-5.71 | $6,962.37 | ▼ -5.71 after sell → book $10,672.10; vs 09:30 mark -4.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 4 | $423.76 | $2.03 | $-16.87 | $8,655.38 | ▼ -16.87 after sell → book $10,670.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,655.38 | ▼ close $10,637.75 vs 09:30 $10,684.15 (session -32.32) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,655.38 | ▲ 09:30 equity $10,646.96 vs yday $10,637.75 (+9.21) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 74 | $23.68 | $2.24 | $-11.11 | $10,405.46 | ▼ -11.11 after sell → book $10,644.72; vs 09:30 mark -2.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,405.46 | ▲ close $10,645.69 vs 09:30 $10,646.96 (session +0.97) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,405.46 | ▲ 09:30 equity $10,653.88 vs yday $10,645.69 (+8.19) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 2 | $41.94 | $0.86 | $-0.70 | $10,488.48 | ▼ -0.70 after sell → book $10,653.02; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 5 | $15.82 | $0.83 | $+5.44 | $10,566.75 | ▲ +5.44 after sell → book $10,652.19; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 32 | $2.67 | $0.97 | $+0.34 | $10,651.22 | ▲ +0.34 after sell → book $10,651.22; vs 09:30 mark -0.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,651.22 | ▲ close $10,651.22 vs 09:30 $10,653.88 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,651.22 | ▲ 09:30 equity $10,651.22 vs yday $10,651.22 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,651.22 | ▲ close $10,651.22 vs 09:30 $10,651.22 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,651.22 | ▲ 09:30 equity $10,651.22 vs yday $10,651.22 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 40 | $52.88 | $2.11 | — | $8,533.91 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2130.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 49 | $42.93 | $2.14 | — | $6,428.20 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2130.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 586 | $3.63 | $7.56 | — | $4,293.46 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2130.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 265 | $8.03 | $3.42 | — | $2,162.10 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2130.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 16 | $132.45 | $2.04 | — | $40.86 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2130.24 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.86 | ▼ close $10,434.46 vs 09:30 $10,651.22 (session -199.50) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.86 | ▼ 09:30 equity $10,359.75 vs yday $10,434.46 (-74.71) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 2 | $2.52 | $0.06 | — | $35.76 | — | S≥+5: 1.35× leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $6.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $28.98 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $6.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 3 | $1.90 | $0.07 | — | $23.22 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $6.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 1 | $4.78 | $0.05 | — | $18.39 | — | S≥+5: 1.35× leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $6.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 4 | $1.59 | $0.08 | — | $11.95 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $6.81 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.95 | ▲ close $10,461.40 vs 09:30 $10,359.75 (session +101.97) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.95 | ▲ 09:30 equity $10,519.28 vs yday $10,461.40 (+57.88) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.95 | ▼ close $10,341.04 vs 09:30 $10,519.28 (session -178.24) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.95 | ▼ 09:30 equity $10,281.56 vs yday $10,341.04 (-59.48) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 40 | $53.16 | $2.14 | $+6.95 | $2,136.21 | ▲ +6.95 after sell → book $10,279.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 49 | $42.01 | $2.16 | $-49.38 | $4,192.54 | ▼ -49.38 after sell → book $10,277.26; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 586 | $3.28 | $7.67 | $-220.33 | $6,106.95 | ▼ -220.33 after sell → book $10,269.59; vs 09:30 mark -7.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 265 | $8.01 | $3.48 | $-12.20 | $8,226.12 | ▼ -12.20 after sell → book $10,266.11; vs 09:30 mark -3.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 16 | $125.77 | $2.06 | $-110.98 | $10,236.37 | ▼ -110.98 after sell → book $10,264.04; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,236.37 | ▼ close $10,262.78 vs 09:30 $10,281.56 (session -1.26) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,236.37 | ▼ 09:30 equity $10,262.46 vs yday $10,262.78 (-0.32) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 2 | $2.22 | $0.07 | $-0.73 | $10,240.74 | ▼ -0.73 after sell → book $10,262.38; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $10,246.77 | ▼ -0.75 after sell → book $10,262.30; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 3 | $1.83 | $0.08 | $-0.36 | $10,252.17 | ▼ -0.36 after sell → book $10,262.22; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 1 | $3.92 | $0.06 | $-0.97 | $10,256.03 | ▼ -0.97 after sell → book $10,262.15; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 4 | $1.53 | $0.09 | $-0.41 | $10,262.06 | ▼ -0.41 after sell → book $10,262.06; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,262.06 | ▲ close $10,262.06 vs 09:30 $10,262.46 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,262.06 | ▲ 09:30 equity $10,262.06 vs yday $10,262.06 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 105 | $16.28 | $2.31 | — | $8,550.36 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1710.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 626 | $2.73 | $8.08 | — | $6,833.30 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1710.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,176.57 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1710.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $3,530.25 | — | S≥+5: 1.35× leftover; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1710.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 10 | $157.78 | $2.02 | — | $1,950.43 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1710.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 30 | $56.09 | $2.08 | — | $265.65 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1710.34 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $265.65 | ▼ close $10,207.09 vs 09:30 $10,262.06 (session -36.46) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $265.65 | ▼ 09:30 equity $9,806.53 vs yday $10,207.09 (-400.56) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $265.65 | ▼ close $9,685.66 vs 09:30 $9,806.53 (session -120.87) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $265.65 | ▲ 09:30 equity $9,755.84 vs yday $9,685.66 (+70.18) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $265.65 | ▼ close $9,559.24 vs 09:30 $9,755.84 (session -196.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $265.65 | ▲ 09:30 equity $9,640.79 vs yday $9,559.24 (+81.55) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 105 | $16.16 | $2.34 | $-17.24 | $1,960.11 | ▼ -17.24 after sell → book $9,638.45; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 626 | $2.72 | $8.19 | $-22.53 | $3,654.64 | ▼ -22.53 after sell → book $9,630.26; vs 09:30 mark -8.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 8 | $194.84 | $2.04 | $-100.05 | $5,211.32 | ▼ -100.05 after sell → book $9,628.22; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 10 | $140.03 | $2.04 | $-248.06 | $6,609.58 | ▼ -248.06 after sell → book $9,626.18; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 10 | $147.79 | $2.04 | $-103.96 | $8,085.44 | ▼ -103.96 after sell → book $9,624.14; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 30 | $51.29 | $2.10 | $-148.18 | $9,622.04 | ▼ -148.18 after sell → book $9,622.04; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 8 | $270.89 | $2.01 | — | $7,452.90 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $2405.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 31 | $77.12 | $2.08 | — | $5,060.10 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $2405.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 168 | $14.31 | $2.49 | — | $2,653.53 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $2405.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 65 | $36.46 | $2.19 | — | $281.44 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $2405.51 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $281.44 | ▼ close $9,524.01 vs 09:30 $9,640.79 (session -89.25) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $281.44 | ▲ 09:30 equity $9,627.27 vs yday $9,524.01 (+103.26) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 4 | $10.25 | $0.42 | — | $240.02 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $46.91 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 6 | $7.59 | $0.47 | — | $194.00 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $46.91 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $158.72 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; leftover $46.91 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.72 | ▼ close $9,589.48 vs 09:30 $9,627.27 (session -36.54) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.72 | ▼ 09:30 equity $9,567.31 vs yday $9,589.48 (-22.17) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.72 | ▼ close $9,449.43 vs 09:30 $9,567.31 (session -117.88) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.72 | ▲ 09:30 equity $9,459.10 vs yday $9,449.43 (+9.67) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 8 | $266.76 | $2.04 | $-37.10 | $2,290.76 | ▼ -37.10 after sell → book $9,457.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 31 | $76.27 | $2.11 | $-30.54 | $4,653.02 | ▼ -30.54 after sell → book $9,454.95; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 168 | $13.65 | $2.54 | $-115.91 | $6,943.68 | ▼ -115.91 after sell → book $9,452.41; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 65 | $36.70 | $2.21 | $+11.20 | $9,326.96 | ▲ +11.20 after sell → book $9,450.19; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 11 | $157.87 | $2.02 | — | $7,588.37 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $1865.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 4 | $386.20 | $2.00 | — | $6,041.57 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; leftover $1865.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 20 | $88.83 | $2.05 | — | $4,262.92 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $1865.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 138 | $13.47 | $2.40 | — | $2,401.66 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $1865.39 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 466 | $4.00 | $6.01 | — | $531.64 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $1865.39 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $531.64 | ▼ close $9,121.34 vs 09:30 $9,459.10 (session -314.36) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $531.64 | ▲ 09:30 equity $9,172.56 vs yday $9,121.34 (+51.22) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 4 | $10.18 | $0.44 | $-1.14 | $571.92 | ▼ -1.14 after sell → book $9,172.12; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 1 | $93.97 | $0.94 | — | $477.01 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $95.32 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $477.01 | ▲ close $9,273.66 vs 09:30 $9,172.56 (session +102.48) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $477.01 | ▼ 09:30 equity $9,227.40 vs yday $9,273.66 (-46.26) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 1 | $34.78 | $0.37 | $-0.87 | $511.42 | ▼ -0.87 after sell → book $9,227.03; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 3 | $27.79 | $0.84 | — | $427.21 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $102.28 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 10 | $9.81 | $1.01 | — | $328.10 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $102.28 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 5 | $20.25 | $1.03 | — | $225.82 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $102.28 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 4 | $20.65 | $0.84 | — | $142.38 | — | S≥+5: 1.35× leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $102.28 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.38 | ▼ close $9,013.65 vs 09:30 $9,227.40 (session -209.66) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.38 | ▼ 09:30 equity $8,850.78 vs yday $9,013.65 (-162.87) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 6 | $7.38 | $0.48 | $-2.21 | $186.18 | ▼ -2.21 after sell → book $8,850.30; vs 09:30 mark -0.48 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 11 | $163.95 | $2.05 | $+62.81 | $1,987.58 | ▲ +62.81 after sell → book $8,848.25; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 4 | $374.54 | $2.02 | $-50.67 | $3,483.72 | ▼ -50.67 after sell → book $8,846.23; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 20 | $87.67 | $2.07 | $-27.22 | $5,235.15 | ▼ -27.22 after sell → book $8,844.16; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 138 | $11.42 | $2.44 | $-287.74 | $6,808.67 | ▼ -287.74 after sell → book $8,841.72; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 466 | $3.40 | $6.10 | $-291.71 | $8,386.97 | ▼ -291.71 after sell → book $8,835.62; vs 09:30 mark -6.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,386.97 | ▼ close $8,835.42 vs 09:30 $8,850.78 (session -0.20) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,553.35 | ▲ 09:30 equity $7,832.29 vs yday $7,831.81 (+0.48) | 09:30 open · cash $7,553.35 (unchanged overnight, no fees) · equity $7,832.29 vs prior close $7,831.81 (+0.48) · 9 name(s) re-marked at the open (per-name table). ADMA×3 yday $9.52 → 09:30 $9.52 +0.00; ARQT×1 yday $26.27 → 09:30 $26.27 +0.00; DLO×3 yday $13.88 → 09:30 $13.88 +0.00; FTRE×1 yday $20.02 → 09:30 $20.02 +0.00; MKC×1 yday $47.82 → 09:30 $47.82 +0.00; OMER×1 yday $20.13 → 09:30 $20.61 +0.48; PACS×1 yday $41.46 → 09:30 $41.46 +0.00; PGEN×3 yday $7.70 → 09:30 $7.70 +0.00; TDC×1 yday $29.46 → 09:30 $29.46 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 2 | $803.87 | $2.00 | — | $5,943.61 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.8; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 16 | $115.36 | $2.04 | — | $4,095.82 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.1; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 49 | $38.51 | $2.14 | — | $2,206.69 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 246 | $7.65 | $3.17 | — | $321.62 | — | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $1888.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.62 | ▼ close $7,754.12 vs 09:30 $7,832.29 (session -68.83) | 16:00 close · cash $321.62 · equity $7,754.12 vs 09:30 $7,832.29 (-78.17; session marks -68.83) · 13 name(s) marked open→close (per-name table). ADMA×3 09:30 $9.52 → close $9.52 +0.00; ARQT×1 09:30 $26.27 → close $26.27 +0.00; DLO×3 09:30 $13.88 → close $13.88 +0.00; FTRE×1 09:30 $20.02 → close $20.02 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; OMER×1 09:30 $20.61 → close $20.08 -0.53; PACS×1 09:30 $41.46 → close $41.46 -0.00; PGEN×3 09:30 $7.70 → close $7.70 -0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; REGN×2 09:30 $803.87 → close $788.04 -31.66; HALO×16 09:30 $115.36 → close $113.90 -23.36; BLFS×49 09:30 $38.51 → close $38.49 -0.98; MRVI×246 09:30 $7.65 → close $7.60 -12.30 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
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
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `VIR` | cash | leftover split 6.81 < 1 share @ 11.31 |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `ILMN` | cash | leftover split 46.91 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 46.91 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 46.91 < 1 share @ 147.61 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 26.45 < 1 share @ 108.55 |
| 2026-09-18 | `DELL` | cash | leftover split 26.45 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 26.45 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 26.45 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 26.45 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 26.45 < 1 share @ 34.44 |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `HALO` | cash | leftover split 102.28 < 1 share @ 116.85 |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| `USFD` | 1 | 2026-09-22 @ $93.97 | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $95.32 |
| `ARQT` | 3 | 2026-09-23 @ $27.79 | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $102.28 |
| `ADMA` | 10 | 2026-09-23 @ $9.81 | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $102.28 |
| `FTRE` | 5 | 2026-09-23 @ $20.25 | S≥+5: 1.35× leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $102.28 |
| `OMER` | 4 | 2026-09-23 @ $20.65 | S≥+5: 1.35× leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $102.28 |
