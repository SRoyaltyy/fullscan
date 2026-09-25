# Factor mine action — `flatten_h5_s8`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · flatten hold 5, stop −8% at 09:30 even inside hold

Cash book **-13.16%** ($8,684) · signal-only (no cash/fees) was +4.47%. Starts YES **3/30**. Fills 135 · skips 322 · realized $+309.24.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- Stop-loss: sell at the next 09:30 if that open is 8% worse than our fill, even inside the minimum hold.
- The hold timer still applies if take-profit and stop-loss do not fire.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $667.69.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $12.19 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $12.19 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $7.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $7.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,525.15 vs 09:30 $10,414.78 (session +110.53) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 1 | $3.72 | $0.06 | $-0.43 | $52.10 | ▼ -0.43 after sell → book $10,391.74; vs 09:30 mark -0.06 | stop-loss after 1 sess | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.10 | ▲ close $10,572.11 vs 09:30 $10,391.80 (session +180.37) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.10 | ▲ 09:30 equity $10,709.86 vs yday $10,572.11 (+137.75) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `IREN` | 27 | $41.41 | $2.09 | $-127.42 | $1,168.21 | ▼ -127.42 after sell → book $10,707.76; vs 09:30 mark -2.10 | stop-loss after 4 sess | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,168.21 | ▲ close $10,990.24 vs 09:30 $10,709.86 (session +282.48) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,168.21 | ▼ 09:30 equity $10,935.75 vs yday $10,990.24 (-54.49) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $2,338.94 | ▼ -27.32 after sell → book $10,933.68; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,610.30 | ▲ +54.34 after sell → book $10,931.60; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,899.47 | ▲ +44.60 after sell → book $10,929.51; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,364.17 | ▲ +222.19 after sell → book $10,927.17; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,649.75 | ▲ +34.39 after sell → book $10,925.04; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,635.48 | ▲ +718.77 after sell → book $10,904.86; vs 09:30 mark -20.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,858.14 | ▼ -15.98 after sell → book $10,902.69; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,499.65 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,223.48 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,879.04 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,520.06 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,163.39 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,827.92 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 775 | $1.75 | $10.00 | — | $1,461.67 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1357.27 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $158.79 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1357.27 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▲ close $11,133.06 vs 09:30 $10,935.75 (session +256.15) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▲ 09:30 equity $11,426.09 vs yday $11,133.06 (+293.03) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $170.35 | ▲ +2.46 after sell → book $11,425.95; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $181.45 | ▼ -1.24 after sell → book $11,425.77; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $194.56 | ▲ +0.96 after sell → book $11,425.60; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $177.18 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $24.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $154.69 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $24.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $132.21 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $24.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $108.79 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $24.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $84.73 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $24.32 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.73 | ▲ close $11,425.24 vs 09:30 $11,426.09 (session +0.86) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.73 | ▲ 09:30 equity $11,542.87 vs yday $11,425.24 (+117.63) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $91.64 | ▲ +0.35 after sell → book $11,542.78; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $96.62 | ▲ +0.12 after sell → book $11,542.71; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.62 | ▼ close $11,507.00 vs 09:30 $11,542.87 (session -35.71) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.62 | ▼ 09:30 equity $11,325.91 vs yday $11,507.00 (-181.09) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $85.52 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $16.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $77.09 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $16.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $4.94 | $0.16 | — | $62.11 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $16.10 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.11 | ▲ close $11,782.80 vs 09:30 $11,325.91 (session +457.25) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.11 | ▼ 09:30 equity $11,570.00 vs yday $11,782.80 (-212.80) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.11 | ▼ close $11,408.63 vs 09:30 $11,570.00 (session -161.37) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.11 | ▲ 09:30 equity $11,434.90 vs yday $11,408.63 (+26.27) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.93 | $2.21 | $+20.68 | $1,441.28 | ▲ +20.68 after sell → book $11,432.69; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,776.51 | ▲ +59.06 after sell → book $11,430.64; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,159.45 | ▲ +38.51 after sell → book $11,428.43; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.49 | $3.08 | $-71.91 | $5,446.52 | ▼ -71.91 after sell → book $11,425.35; vs 09:30 mark -3.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.47 | $2.22 | $+122.54 | $6,925.73 | ▲ +122.54 after sell → book $11,423.13; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,377.98 | ▲ +116.78 after sell → book $11,420.98; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 775 | $1.91 | $10.14 | $+103.86 | $9,848.09 | ▲ +103.86 after sell → book $11,410.84; vs 09:30 mark -10.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,249.07 | ▲ +98.09 after sell → book $11,408.81; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 67 | $41.44 | $2.19 | — | $8,470.39 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $2812.27 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 195 | $14.42 | $2.58 | — | $5,655.92 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $2812.27 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 117 | $24.00 | $2.34 | — | $2,845.58 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.7; leftover $2812.27 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1081 | $2.60 | $13.94 | — | $21.03 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $2812.27 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.03 | ▲ close $11,458.44 vs 09:30 $11,434.90 (session +70.69) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.03 | ▲ 09:30 equity $11,529.72 vs yday $11,458.44 (+71.28) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $37.29 | ▼ -1.12 after sell → book $11,529.54; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $67.81 | ▲ +8.04 after sell → book $11,529.20; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.35 | $0.26 | $-1.59 | $88.70 | ▼ -1.59 after sell → book $11,528.94; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.06 | $0.30 | $+0.99 | $113.12 | ▲ +0.99 after sell → book $11,528.64; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.82 | $0.40 | $+8.31 | $145.48 | ▲ +8.31 after sell → book $11,528.24; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.48 | ▼ close $11,260.12 vs 09:30 $11,529.72 (session -268.12) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.48 | ▲ 09:30 equity $11,386.83 vs yday $11,260.12 (+126.71) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.48 | ▲ close $11,468.90 vs 09:30 $11,386.83 (session +82.07) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.48 | ▲ 09:30 equity $11,760.20 vs yday $11,468.90 (+291.30) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $155.77 | ▼ -0.80 after sell → book $11,760.07; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $163.92 | ▼ -0.29 after sell → book $11,759.97; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.64 | $0.17 | $-1.23 | $177.67 | ▼ -1.23 after sell → book $11,759.80; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.67 | ▼ close $11,733.32 vs 09:30 $11,760.20 (session -26.48) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.67 | ▼ 09:30 equity $11,641.46 vs yday $11,733.32 (-91.86) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.67 | ▲ close $11,675.60 vs 09:30 $11,641.46 (session +34.14) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.67 | ▲ 09:30 equity $11,780.96 vs yday $11,675.60 (+105.36) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 67 | $42.43 | $2.22 | $+61.91 | $3,018.25 | ▲ +61.91 after sell → book $11,778.73; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 195 | $15.45 | $2.63 | $+195.64 | $6,028.37 | ▲ +195.64 after sell → book $11,776.10; vs 09:30 mark -2.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 117 | $26.12 | $2.38 | $+243.31 | $9,082.03 | ▲ +243.31 after sell → book $11,773.72; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1081 | $2.49 | $14.15 | $-147.00 | $11,759.57 | ▼ -147.00 after sell → book $11,759.57; vs 09:30 mark -14.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 44 | $52.88 | $2.12 | — | $9,430.73 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2351.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 54 | $42.93 | $2.15 | — | $7,110.36 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2351.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 647 | $3.63 | $8.35 | — | $4,753.40 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2351.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 292 | $8.03 | $3.77 | — | $2,404.87 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2351.91 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $132.45 | $2.04 | — | $151.18 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2351.91 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.18 | ▼ close $11,522.29 vs 09:30 $11,780.96 (session -218.85) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.18 | ▼ 09:30 equity $11,440.35 vs yday $11,522.29 (-81.94) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 9 | $2.52 | $0.25 | — | $128.25 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $25.20 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 3 | $6.71 | $0.21 | — | $107.91 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $25.20 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 13 | $1.90 | $0.29 | — | $82.92 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $25.20 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $4.78 | $0.25 | — | $58.77 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $25.20 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 15 | $1.59 | $0.28 | — | $34.63 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $25.20 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 2 | $11.31 | $0.23 | — | $11.78 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $25.20 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.78 | ▲ close $11,550.62 vs 09:30 $11,440.35 (session +111.79) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.78 | ▲ 09:30 equity $11,613.85 vs yday $11,550.62 (+63.23) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 5 | $4.30 | $0.25 | $-2.90 | $33.03 | ▼ -2.90 after sell → book $11,613.60; vs 09:30 mark -0.25 | stop-loss after 1 sess | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.03 | ▼ close $11,417.16 vs 09:30 $11,613.85 (session -196.44) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.03 | ▼ 09:30 equity $11,352.15 vs yday $11,417.16 (-65.01) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 647 | $3.28 | $8.47 | $-243.27 | $2,146.72 | ▼ -243.27 after sell → book $11,343.68; vs 09:30 mark -8.47 | stop-loss after 3 sess | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,146.72 | ▼ close $11,259.07 vs 09:30 $11,352.15 (session -84.61) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,146.72 | ▼ 09:30 equity $11,179.40 vs yday $11,259.07 (-79.67) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 9 | $2.22 | $0.25 | $-3.20 | $2,166.45 | ▼ -3.20 after sell → book $11,179.15; vs 09:30 mark -0.25 | stop-loss after 3 sess | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 3 | $6.11 | $0.21 | $-2.22 | $2,184.57 | ▼ -2.22 after sell → book $11,178.94; vs 09:30 mark -0.21 | stop-loss after 3 sess | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,184.57 | ▼ close $11,080.84 vs 09:30 $11,179.40 (session -98.10) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,184.57 | ▲ 09:30 equity $11,165.97 vs yday $11,080.84 (+85.13) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 44 | $53.53 | $2.15 | $+24.33 | $4,537.74 | ▲ +24.33 after sell → book $11,163.82; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 54 | $41.30 | $2.18 | $-92.35 | $6,765.76 | ▼ -92.35 after sell → book $11,161.64; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 292 | $7.70 | $3.83 | $-103.96 | $9,010.33 | ▼ -103.96 after sell → book $11,157.81; vs 09:30 mark -3.83 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 17 | $122.40 | $2.07 | $-174.96 | $11,089.06 | ▼ -174.96 after sell → book $11,155.74; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 113 | $16.28 | $2.33 | — | $9,247.09 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1848.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 676 | $2.73 | $8.72 | — | $7,392.89 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1848.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,736.16 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1848.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $3,925.40 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1848.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 11 | $157.78 | $2.02 | — | $2,187.80 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1848.18 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 32 | $56.09 | $2.09 | — | $390.84 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1848.18 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $390.84 | ▼ close $11,089.81 vs 09:30 $11,165.97 (session -46.74) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $390.84 | ▼ 09:30 equity $10,661.19 vs yday $11,089.81 (-428.62) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 13 | $1.72 | $0.28 | $-2.97 | $412.85 | ▼ -2.97 after sell → book $10,660.91; vs 09:30 mark -0.28 | stop-loss after 5 sess | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 15 | $1.59 | $0.30 | $-0.59 | $436.39 | ▼ -0.59 after sell → book $10,660.60; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 2 | $10.73 | $0.24 | $-1.63 | $457.61 | ▼ -1.63 after sell → book $10,660.36; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 11 | $141.42 | $2.05 | $-257.18 | $2,011.19 | ▼ -257.18 after sell → book $10,658.32; vs 09:30 mark -2.04 | stop-loss after 1 sess | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,011.19 | ▼ close $10,498.16 vs 09:30 $10,661.19 (session -160.16) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,011.19 | ▲ 09:30 equity $10,587.34 vs yday $10,498.16 (+89.18) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `COHU` | 32 | $51.07 | $2.11 | $-164.84 | $3,643.32 | ▼ -164.84 after sell → book $10,585.23; vs 09:30 mark -2.11 | stop-loss after 2 sess | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,643.32 | ▼ close $10,445.65 vs 09:30 $10,587.34 (session -139.58) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,643.32 | ▲ 09:30 equity $10,492.53 vs yday $10,445.65 (+46.88) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 3 | $270.89 | $2.00 | — | $2,828.65 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $910.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 11 | $77.12 | $2.02 | — | $1,978.31 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $910.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 63 | $14.31 | $2.18 | — | $1,074.60 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $910.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 24 | $36.46 | $2.06 | — | $197.50 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $910.83 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $197.50 | ▲ close $10,520.87 vs 09:30 $10,492.53 (session +36.60) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $197.50 | ▲ 09:30 equity $10,694.66 vs yday $10,520.87 (+173.79) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 3 | $10.25 | $0.32 | — | $166.43 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $32.92 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 4 | $7.59 | $0.32 | — | $135.75 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $32.92 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.75 | ▼ close $10,600.49 vs 09:30 $10,694.66 (session -93.53) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.75 | ▲ 09:30 equity $10,604.52 vs yday $10,600.49 (+4.03) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 113 | $16.93 | $2.36 | $+68.76 | $2,046.48 | ▲ +68.76 after sell → book $10,602.16; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 676 | $2.68 | $8.85 | $-51.37 | $3,849.31 | ▼ -51.37 after sell → book $10,593.31; vs 09:30 mark -8.85 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 8 | $197.76 | $2.04 | $-76.69 | $5,429.36 | ▼ -76.69 after sell → book $10,591.28; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 11 | $152.71 | $2.05 | $-59.84 | $7,107.12 | ▼ -59.84 after sell → book $10,589.23; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 10 | $108.55 | $2.02 | — | $6,019.60 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1184.52 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $5,424.46 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1184.52 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $4,374.85 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1184.52 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $3,274.75 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1184.52 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 13 | $85.00 | $2.03 | — | $2,167.72 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1184.52 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 34 | $34.44 | $2.09 | — | $994.67 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1184.52 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $994.67 | ▼ close $10,427.86 vs 09:30 $10,604.52 (session -149.23) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $994.67 | ▲ 09:30 equity $10,499.83 vs yday $10,427.86 (+71.97) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $835.22 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $198.93 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 2 | $88.83 | $1.78 | — | $655.77 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $198.93 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 14 | $13.47 | $1.93 | — | $465.27 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $198.93 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 49 | $4.00 | $2.11 | — | $267.16 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $198.93 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.16 | ▲ close $10,548.32 vs 09:30 $10,499.83 (session +55.89) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.16 | ▼ 09:30 equity $10,548.17 vs yday $10,548.32 (-0.15) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 49 | $3.51 | $1.89 | $-28.00 | $437.26 | ▼ -28.00 after sell → book $10,546.28; vs 09:30 mark -1.89 | stop-loss after 1 sess | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $437.26 | ▼ close $10,526.78 vs 09:30 $10,548.17 (session -19.50) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $437.26 | ▲ 09:30 equity $10,748.49 vs yday $10,526.78 (+221.71) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 3 | $270.66 | $2.02 | $-4.71 | $1,247.22 | ▼ -4.71 after sell → book $10,746.47; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 11 | $73.61 | $2.04 | $-42.68 | $2,054.89 | ▼ -42.68 after sell → book $10,744.43; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 63 | $13.12 | $2.20 | $-79.35 | $2,879.25 | ▼ -79.35 after sell → book $10,742.23; vs 09:30 mark -2.20 | stop-loss after 5 sess | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 24 | $38.04 | $2.08 | $+33.78 | $3,790.13 | ▲ +33.78 after sell → book $10,740.15; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ECO` | 13 | $77.55 | $2.05 | $-100.93 | $4,796.23 | ▼ -100.93 after sell → book $10,738.10; vs 09:30 mark -2.05 | stop-loss after 3 sess | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 14 | $12.26 | $1.78 | $-20.65 | $4,966.09 | ▼ -20.65 after sell → book $10,736.32; vs 09:30 mark -1.78 | stop-loss after 2 sess | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 8 | $116.85 | $2.01 | — | $4,029.28 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $993.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 35 | $27.79 | $2.10 | — | $3,054.53 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $993.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 101 | $9.81 | $2.29 | — | $2,061.43 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $993.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 49 | $20.25 | $2.14 | — | $1,067.04 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $993.22 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 48 | $20.65 | $2.13 | — | $73.71 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $993.22 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.71 | ▼ close $10,628.89 vs 09:30 $10,748.49 (session -96.76) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.71 | ▼ 09:30 equity $10,506.35 vs yday $10,628.89 (-122.54) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 3 | $10.39 | $0.34 | $-0.24 | $104.54 | ▼ -0.24 after sell → book $10,506.01; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 4 | $7.38 | $0.33 | $-1.48 | $133.73 | ▼ -1.48 after sell → book $10,505.68; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DELL` | 1 | $535.97 | $2.01 | $-61.19 | $667.69 | ▼ -61.19 after sell → book $10,503.67; vs 09:30 mark -2.01 | stop-loss after 4 sess | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $667.69 | ▲ close $10,520.41 vs 09:30 $10,506.35 (session +16.74) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $221.18 | ▲ 09:30 equity $8,718.67 vs yday $8,699.19 (+19.48) | 09:30 open · cash $221.18 (unchanged overnight, no fees) · equity $8,718.67 vs prior close $8,699.19 (+19.48) · 17 name(s) re-marked at the open (per-name table). A×6 yday $172.84 → 09:30 $171.98 -5.16; ADMA×103 yday $9.52 → 09:30 $9.52 +0.00; ARQT×36 yday $26.27 → 09:30 $26.27 +0.00; DLO×8 yday $13.88 → 09:30 $13.88 +0.00; DXCM×1 yday $87.47 → 09:30 $87.47 +0.00; EL×1 yday $95.37 → 09:30 $95.37 +0.00; FIVN×13 yday $36.66 → 09:30 $36.66 +0.00; FTRE×50 yday $20.02 → 09:30 $20.02 +0.00; GNRC×2 yday $198.05 → 09:30 $198.05 +0.00; HALO×8 yday $115.22 → 09:30 $115.36 +1.12; MKC×2 yday $47.82 → 09:30 $47.82 +0.00; OMER×49 yday $20.13 → 09:30 $20.61 +23.52; PACS×3 yday $41.46 → 09:30 $41.46 +0.00; RBRK×4 yday $113.80 → 09:30 $113.80 +0.00; TDC×4 yday $29.46 → 09:30 $29.46 +0.00; USFD×1 yday $93.82 → 09:30 $93.82 +0.00; VICR×2 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 1 | $38.51 | $0.39 | — | $182.28 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $73.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 9 | $7.65 | $0.72 | — | $112.72 | — | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $73.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.72 | ▼ close $8,684.31 vs 09:30 $8,718.67 (session -33.26) | 16:00 close · cash $112.72 · equity $8,684.31 vs 09:30 $8,718.67 (-34.36; session marks -33.26) · 19 name(s) marked open→close (per-name table). A×6 09:30 $171.98 → close $172.79 +4.86; ADMA×103 09:30 $9.52 → close $9.52 +0.00; ARQT×36 09:30 $26.27 → close $26.27 +0.00; DLO×8 09:30 $13.88 → close $13.88 +0.00; DXCM×1 09:30 $87.47 → close $87.47 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FIVN×13 09:30 $36.66 → close $36.66 -0.00; FTRE×50 09:30 $20.02 → close $20.02 +0.00; GNRC×2 09:30 $198.05 → close $198.05 +0.00; HALO×8 09:30 $115.36 → close $113.90 -11.68; MKC×2 09:30 $47.82 → close $47.82 -0.00; OMER×49 09:30 $20.61 → close $20.08 -25.97; PACS×3 09:30 $41.46 → close $41.46 -0.00; RBRK×4 09:30 $113.80 → close $113.80 +0.00; TDC×4 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; VICR×2 09:30 $276.06 → close $276.06 -0.00; BLFS×1 09:30 $38.51 → close $38.49 -0.02; MRVI×9 09:30 $7.65 → close $7.60 -0.45 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
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
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
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
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 24.32 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 24.32 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 24.32 < 1 share @ 59.72 |
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
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 16.10 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 16.10 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 16.10 < 1 share @ 426.97 |
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
| 2026-08-26 | `HCA` | cash | leftover split 20.70 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 20.70 < 1 share @ 24.84 |
| 2026-08-26 | `INSP` | cash | leftover split 20.70 < 1 share @ 60.07 |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `OVID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 32.92 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 32.92 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 32.92 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 32.92 < 1 share @ 34.93 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `IQV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `HUM` | cash | leftover split 198.93 < 1 share @ 386.20 |
| 2026-09-22 | `IQV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 72.88 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 10 | 2026-09-18 @ $108.55 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1184.52 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1184.52 |
| `VICR` | 5 | 2026-09-18 @ $219.62 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1184.52 |
| `FIVN` | 34 | 2026-09-18 @ $34.44 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1184.52 |
| `A` | 1 | 2026-09-21 @ $157.87 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $198.93 |
| `DXCM` | 2 | 2026-09-21 @ $88.83 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $198.93 |
| `HALO` | 8 | 2026-09-23 @ $116.85 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $993.22 |
| `ARQT` | 35 | 2026-09-23 @ $27.79 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $993.22 |
| `ADMA` | 101 | 2026-09-23 @ $9.81 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $993.22 |
| `FTRE` | 49 | 2026-09-23 @ $20.25 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $993.22 |
| `OMER` | 48 | 2026-09-23 @ $20.65 | flatten hold 5, stop −8% at 09:30 even inside hold; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $993.22 |
