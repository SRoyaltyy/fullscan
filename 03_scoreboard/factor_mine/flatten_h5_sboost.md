# Factor mine action — `flatten_h5_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **-14.17%** ($8,583) · signal-only (no cash/fees) was +4.47%. Starts YES **3/30**. Fills 131 · skips 355 · realized $+314.90.

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
- On a strong morning (S ≥ +5), spend 1.35× leftover and add 4 extra names — still cash-capped.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $274.76.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1111.11 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.82 | ▲ close $10,195.74 vs 09:30 $10,000.00 (session +227.81) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $101.42 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $87.76 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.76 | ▲ close $10,434.38 vs 09:30 $10,219.63 (session +215.18) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.76 | ▼ 09:30 equity $10,410.12 vs yday $10,434.38 (-24.26) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $79.57 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.97 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $71.02 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.97 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $61.20 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.97 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $51.48 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.97 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.48 | ▲ close $10,512.60 vs 09:30 $10,410.12 (session +102.86) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▼ 09:30 equity $10,384.26 vs yday $10,512.60 (-128.34) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.48 | ▲ close $10,567.13 vs 09:30 $10,384.26 (session +182.87) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▲ 09:30 equity $10,728.09 vs yday $10,567.13 (+160.96) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.48 | ▲ close $10,988.31 vs 09:30 $10,728.09 (session +260.22) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▼ 09:30 equity $10,904.20 vs yday $10,988.31 (-84.11) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 18 | $58.64 | $2.06 | $-24.99 | $1,104.93 | ▼ -24.99 after sell → book $10,902.13; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 24 | $42.46 | $2.08 | $-88.62 | $2,121.89 | ▼ -88.62 after sell → book $10,900.05; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 21 | $53.06 | $2.07 | $+47.05 | $3,234.08 | ▲ +47.05 after sell → book $10,897.98; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 22 | $51.65 | $2.08 | $+38.77 | $4,368.30 | ▲ +38.77 after sell → book $10,895.90; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $5,666.96 | ▲ +196.59 after sell → book $10,893.60; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 37 | $30.66 | $2.12 | $+29.82 | $6,799.26 | ▲ +29.82 after sell → book $10,891.48; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1371 | $1.30 | $17.93 | $+638.64 | $8,563.63 | ▲ +638.64 after sell → book $10,873.55; vs 09:30 mark -17.93 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 47 | $23.11 | $2.15 | $-14.62 | $9,647.65 | ▼ -14.62 after sell → book $10,871.40; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 50 | $23.05 | $2.16 | $+47.70 | $10,797.99 | ▲ +47.70 after sell → book $10,869.24; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 65 | $20.55 | $2.19 | — | $9,460.06 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,183.89 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,839.45 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 233 | $5.77 | $3.01 | — | $5,492.04 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 68 | $19.63 | $2.19 | — | $4,155.00 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,819.53 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 771 | $1.75 | $9.95 | — | $1,460.33 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1349.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $157.45 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1349.75 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.45 | ▲ close $11,099.09 vs 09:30 $10,904.20 (session +255.54) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.45 | ▲ 09:30 equity $11,391.10 vs yday $11,099.09 (+292.01) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $169.01 | ▲ +2.46 after sell → book $11,390.96; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $180.97 | ▼ -1.34 after sell → book $11,390.78; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $195.71 | ▲ +1.08 after sell → book $11,390.58; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $178.34 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $155.85 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $133.37 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $109.94 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $24.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $85.89 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $24.46 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.89 | ▲ close $11,392.87 vs 09:30 $11,391.10 (session +3.50) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.89 | ▲ 09:30 equity $11,509.59 vs yday $11,392.87 (+116.72) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $95.02 | ▲ +0.94 after sell → book $11,509.47; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $104.17 | ▲ +0.60 after sell → book $11,509.36; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $114.53 | ▲ +0.54 after sell → book $11,509.22; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $124.51 | ▲ +0.25 after sell → book $11,509.10; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.51 | ▼ close $11,473.62 vs 09:30 $11,509.59 (session -35.48) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.51 | ▼ 09:30 equity $11,293.61 vs yday $11,473.62 (-180.01) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.98 | $0.11 | — | $113.41 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $20.75 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $96.54 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $20.75 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 4 | $4.94 | $0.21 | — | $76.57 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $20.75 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.57 | ▲ close $11,748.32 vs 09:30 $11,293.61 (session +455.21) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.57 | ▼ 09:30 equity $11,536.78 vs yday $11,748.32 (-211.54) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `MOS` | 1 | $24.84 | $0.25 | — | $51.48 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+14.8; leftover $25.52 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.48 | ▼ close $11,374.88 vs 09:30 $11,536.78 (session -161.65) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▲ 09:30 equity $11,400.81 vs yday $11,374.88 (+25.93) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 65 | $20.93 | $2.21 | $+20.31 | $1,409.72 | ▲ +20.31 after sell → book $11,398.60; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,744.95 | ▲ +59.06 after sell → book $11,396.55; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.31 | $2.21 | $+38.51 | $4,127.89 | ▲ +38.51 after sell → book $11,394.34; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 233 | $5.49 | $3.05 | $-71.30 | $5,404.01 | ▼ -71.30 after sell → book $11,391.29; vs 09:30 mark -3.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 68 | $21.47 | $2.22 | $+120.71 | $6,861.75 | ▲ +120.71 after sell → book $11,389.07; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.32 | $2.15 | $+116.78 | $8,314.00 | ▲ +116.78 after sell → book $11,386.92; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 771 | $1.91 | $10.09 | $+103.33 | $9,776.53 | ▲ +103.33 after sell → book $11,376.84; vs 09:30 mark -10.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,177.50 | ▲ +98.09 after sell → book $11,374.80; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 89 | $41.44 | $2.26 | — | $7,487.08 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $3725.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 258 | $14.42 | $3.33 | — | $3,763.39 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $3725.83 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1433 | $2.60 | $18.49 | — | $19.11 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $3725.83 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.11 | ▲ close $11,480.06 vs 09:30 $11,400.81 (session +129.34) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.11 | ▲ 09:30 equity $11,546.16 vs yday $11,480.06 (+66.10) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $35.36 | ▼ -1.12 after sell → book $11,545.97; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $65.89 | ▲ +8.04 after sell → book $11,545.64; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.35 | $0.26 | $-1.59 | $86.78 | ▼ -1.59 after sell → book $11,545.38; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.06 | $0.30 | $+0.99 | $111.20 | ▲ +0.99 after sell → book $11,545.08; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.82 | $0.40 | $+8.31 | $143.55 | ▲ +8.31 after sell → book $11,544.67; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.55 | ▼ close $11,243.50 vs 09:30 $11,546.16 (session -301.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.55 | ▲ 09:30 equity $11,399.11 vs yday $11,243.50 (+155.61) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.55 | ▲ close $11,439.97 vs 09:30 $11,399.11 (session +40.86) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.55 | ▲ 09:30 equity $11,853.30 vs yday $11,439.97 (+413.33) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.42 | $0.13 | $-0.80 | $153.85 | ▼ -0.80 after sell → book $11,853.18; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $170.16 | ▼ -0.56 after sell → book $11,852.99; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 4 | $4.64 | $0.22 | $-1.63 | $188.50 | ▼ -1.63 after sell → book $11,852.77; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.50 | ▼ close $11,688.21 vs 09:30 $11,853.30 (session -164.56) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.50 | ▼ 09:30 equity $11,578.87 vs yday $11,688.21 (-109.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `MOS` | 1 | $24.70 | $0.27 | $-0.66 | $212.93 | ▼ -0.66 after sell → book $11,578.60; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.93 | ▼ close $11,456.50 vs 09:30 $11,578.87 (session -122.10) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.93 | ▲ 09:30 equity $11,543.47 vs yday $11,456.50 (+86.97) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 89 | $42.43 | $2.30 | $+83.55 | $3,986.90 | ▲ +83.55 after sell → book $11,541.17; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 258 | $15.45 | $3.40 | $+259.01 | $7,969.59 | ▲ +259.01 after sell → book $11,537.76; vs 09:30 mark -3.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1433 | $2.49 | $18.75 | $-194.87 | $11,519.01 | ▼ -194.87 after sell → book $11,519.01; vs 09:30 mark -18.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 43 | $52.88 | $2.12 | — | $9,243.05 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2303.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 53 | $42.93 | $2.15 | — | $6,965.61 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2303.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 634 | $3.63 | $8.18 | — | $4,656.01 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2303.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 286 | $8.03 | $3.69 | — | $2,355.74 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2303.80 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $132.45 | $2.04 | — | $102.05 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2303.80 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.05 | ▼ close $11,285.72 vs 09:30 $11,543.47 (session -215.11) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.05 | ▼ 09:30 equity $11,205.25 vs yday $11,285.72 (-80.47) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 6 | $2.52 | $0.17 | — | $86.76 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $17.01 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $73.20 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $17.01 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 8 | $1.90 | $0.18 | — | $57.83 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $17.01 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $4.78 | $0.15 | — | $43.34 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $17.01 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 10 | $1.59 | $0.19 | — | $27.25 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $17.01 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $15.82 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $17.01 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.82 | ▲ close $11,314.25 vs 09:30 $11,205.25 (session +109.93) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.82 | ▲ 09:30 equity $11,375.91 vs yday $11,314.25 (+61.66) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.82 | ▼ close $11,183.26 vs 09:30 $11,375.91 (session -192.65) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.82 | ▼ 09:30 equity $11,119.27 vs yday $11,183.26 (-63.99) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.82 | ▼ close $10,802.16 vs 09:30 $11,119.27 (session -317.12) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.82 | ▼ 09:30 equity $10,686.00 vs yday $10,802.16 (-116.16) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.82 | ▼ close $10,519.77 vs 09:30 $10,686.00 (session -166.23) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.82 | ▲ 09:30 equity $10,622.55 vs yday $10,519.77 (+102.78) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 43 | $53.53 | $2.15 | $+23.68 | $2,315.46 | ▲ +23.68 after sell → book $10,620.40; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 53 | $41.30 | $2.18 | $-90.72 | $4,502.19 | ▼ -90.72 after sell → book $10,618.23; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 634 | $2.77 | $8.30 | $-561.72 | $6,250.07 | ▼ -561.72 after sell → book $10,609.93; vs 09:30 mark -8.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 286 | $7.70 | $3.75 | $-101.82 | $8,448.51 | ▼ -101.82 after sell → book $10,606.17; vs 09:30 mark -3.76 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 17 | $122.40 | $2.07 | $-174.96 | $10,527.25 | ▼ -174.96 after sell → book $10,604.11; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 107 | $16.28 | $2.31 | — | $8,782.98 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1754.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 642 | $2.73 | $8.28 | — | $7,022.03 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $1754.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,365.30 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $1754.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $3,718.98 | — | S≥+5: sizeup + more names; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $1754.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 11 | $157.78 | $2.02 | — | $1,981.38 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $1754.54 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 31 | $56.09 | $2.08 | — | $240.50 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $1754.54 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.50 | ▼ close $10,553.65 vs 09:30 $10,622.55 (session -31.72) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.50 | ▼ 09:30 equity $10,137.25 vs yday $10,553.65 (-416.40) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 6 | $2.15 | $0.17 | $-2.56 | $253.24 | ▼ -2.56 after sell → book $10,137.09; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 2 | $5.93 | $0.14 | $-1.84 | $264.95 | ▼ -1.84 after sell → book $10,136.94; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 8 | $1.72 | $0.18 | $-1.84 | $278.49 | ▼ -1.84 after sell → book $10,136.76; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 3 | $4.13 | $0.15 | $-2.26 | $290.73 | ▼ -2.26 after sell → book $10,136.61; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 10 | $1.59 | $0.21 | $-0.40 | $306.42 | ▼ -0.40 after sell → book $10,136.40; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 1 | $10.73 | $0.13 | $-0.83 | $317.02 | ▼ -0.83 after sell → book $10,136.27; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.02 | ▼ close $10,010.30 vs 09:30 $10,137.25 (session -125.97) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.02 | ▲ 09:30 equity $10,085.78 vs yday $10,010.30 (+75.48) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $317.02 | ▼ close $9,883.00 vs 09:30 $10,085.78 (session -202.78) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $317.02 | ▲ 09:30 equity $9,967.08 vs yday $9,883.00 (+84.08) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 1 | $77.12 | $0.77 | — | $239.13 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $79.25 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 5 | $14.31 | $0.73 | — | $166.84 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $79.25 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 2 | $36.46 | $0.74 | — | $93.19 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $79.25 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.19 | ▲ close $10,052.38 vs 09:30 $9,967.08 (session +87.54) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.19 | ▲ 09:30 equity $10,291.07 vs yday $10,052.38 (+238.69) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 1 | $10.25 | $0.11 | — | $82.83 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $15.53 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $67.50 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $15.53 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.50 | ▲ close $10,306.72 vs 09:30 $10,291.07 (session +15.91) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.50 | ▲ 09:30 equity $10,342.66 vs yday $10,306.72 (+35.94) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 107 | $16.93 | $2.34 | $+64.90 | $1,876.66 | ▲ +64.90 after sell → book $10,340.31; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 642 | $2.68 | $8.40 | $-48.78 | $3,588.82 | ▼ -48.78 after sell → book $10,331.91; vs 09:30 mark -8.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 8 | $197.76 | $2.04 | $-76.69 | $5,168.86 | ▼ -76.69 after sell → book $10,329.87; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 10 | $150.47 | $2.04 | $-143.66 | $6,671.52 | ▼ -143.66 after sell → book $10,327.83; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 11 | $152.71 | $2.05 | $-59.84 | $8,349.29 | ▼ -59.84 after sell → book $10,325.79; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 31 | $55.80 | $2.11 | $-13.18 | $10,076.98 | ▼ -13.18 after sell → book $10,323.68; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 15 | $108.55 | $2.04 | — | $8,446.69 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1679.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $7,258.40 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1679.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 8 | $209.52 | $2.01 | — | $5,580.22 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1679.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 7 | $219.62 | $2.01 | — | $4,040.87 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1679.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 19 | $85.00 | $2.05 | — | $2,423.83 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1679.50 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 48 | $34.44 | $2.13 | — | $768.57 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1679.50 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $768.57 | ▼ close $10,140.05 vs 09:30 $10,342.66 (session -171.39) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $768.57 | ▲ 09:30 equity $10,249.21 vs yday $10,140.05 (+109.16) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 1 | $88.83 | $0.89 | — | $678.85 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $153.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 11 | $13.47 | $1.51 | — | $529.17 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $153.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 38 | $4.00 | $1.63 | — | $375.53 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $153.71 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $375.53 | ▲ close $10,376.05 vs 09:30 $10,249.21 (session +130.88) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $375.53 | ▼ 09:30 equity $10,369.20 vs yday $10,376.05 (-6.85) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $375.53 | ▼ close $10,336.26 vs 09:30 $10,369.20 (session -32.94) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $375.53 | ▲ 09:30 equity $10,633.34 vs yday $10,336.26 (+297.08) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 1 | $73.61 | $0.76 | $-5.04 | $448.38 | ▼ -5.04 after sell → book $10,632.58; vs 09:30 mark -0.76 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 5 | $13.12 | $0.69 | $-7.37 | $513.29 | ▼ -7.37 after sell → book $10,631.89; vs 09:30 mark -0.69 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 2 | $38.04 | $0.79 | $+1.64 | $588.58 | ▲ +1.64 after sell → book $10,631.10; vs 09:30 mark -0.79 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 3 | $27.79 | $0.84 | — | $504.37 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $98.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 9 | $9.81 | $0.91 | — | $415.17 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $98.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 4 | $20.25 | $0.82 | — | $333.35 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $98.10 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 4 | $20.65 | $0.84 | — | $249.91 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $98.10 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.91 | ▲ close $10,658.52 vs 09:30 $10,633.34 (session +30.83) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $249.91 | ▼ 09:30 equity $10,477.07 vs yday $10,658.52 (-181.45) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 1 | $10.39 | $0.13 | $-0.09 | $260.18 | ▼ -0.09 after sell → book $10,476.95; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 2 | $7.38 | $0.17 | $-0.75 | $274.76 | ▼ -0.75 after sell → book $10,476.77; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.76 | ▲ close $10,505.63 vs 09:30 $10,477.07 (session +28.87) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.67 | ▲ 09:30 equity $8,625.53 vs yday $8,601.65 (+23.88) | 09:30 open · cash $290.67 (unchanged overnight, no fees) · equity $8,625.53 vs prior close $8,601.65 (+23.88) · 9 name(s) re-marked at the open (per-name table). A×7 yday $172.84 → 09:30 $171.98 -6.02; ADMA×126 yday $9.52 → 09:30 $9.52 +0.00; ARQT×44 yday $26.27 → 09:30 $26.27 +0.00; CYPH×4 yday $4.08 → 09:30 $4.00 -0.30; DXCM×13 yday $87.47 → 09:30 $87.47 +0.00; FTRE×61 yday $20.02 → 09:30 $20.02 +0.00; HALO×10 yday $115.22 → 09:30 $115.36 +1.40; MGTX×1 yday $11.05 → 09:30 $11.05 +0.00; OMER×60 yday $20.13 → 09:30 $20.61 +28.80 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 2 | $38.51 | $0.78 | — | $212.87 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $96.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 12 | $7.65 | $0.95 | — | $120.12 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $96.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.12 | ▼ close $8,582.89 vs 09:30 $8,625.53 (session -40.91) | 16:00 close · cash $120.12 · equity $8,582.89 vs 09:30 $8,625.53 (-42.64; session marks -40.91) · 11 name(s) marked open→close (per-name table). A×7 09:30 $171.98 → close $172.79 +5.67; ADMA×126 09:30 $9.52 → close $9.52 +0.00; ARQT×44 09:30 $26.27 → close $26.27 +0.00; CYPH×4 09:30 $4.00 → close $4.12 +0.46; DXCM×13 09:30 $87.47 → close $87.47 +0.00; FTRE×61 09:30 $20.02 → close $20.02 +0.00; HALO×10 09:30 $115.36 → close $113.90 -14.60; MGTX×1 09:30 $11.05 → close $11.05 +0.00; OMER×60 09:30 $20.61 → close $20.08 -31.80; BLFS×2 09:30 $38.51 → close $38.49 -0.04; MRVI×12 09:30 $7.65 → close $7.60 -0.60 | — |

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
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 13.76 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 13.76 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 10.97 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 10.97 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.97 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.97 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 24.46 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 24.46 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 24.46 < 1 share @ 59.72 |
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
| 2026-08-25 | `MOS` | cash | leftover split 20.75 < 1 share @ 23.77 |
| 2026-08-25 | `INSP` | cash | leftover split 20.75 < 1 share @ 61.19 |
| 2026-08-25 | `HCA` | cash | leftover split 20.75 < 1 share @ 426.97 |
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
| 2026-08-26 | `HCA` | cash | leftover split 25.52 < 1 share @ 427.50 |
| 2026-08-26 | `INSP` | cash | leftover split 25.52 < 1 share @ 60.07 |
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
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-16 | `IQV` | cash | leftover split 79.25 < 1 share @ 270.89 |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 15.53 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 15.53 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 15.53 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 15.53 < 1 share @ 34.93 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 153.71 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 153.71 < 1 share @ 386.20 |
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
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 62.59 < 1 share @ 93.97 |
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
| 2026-09-23 | `A` | cash | leftover split 98.10 < 1 share @ 166.54 |
| 2026-09-23 | `HALO` | cash | leftover split 98.10 < 1 share @ 116.85 |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| `RBRK` | 15 | 2026-09-18 @ $108.55 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $1679.50 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; leftover $1679.50 |
| `GNRC` | 8 | 2026-09-18 @ $209.52 | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $1679.50 |
| `VICR` | 7 | 2026-09-18 @ $219.62 | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $1679.50 |
| `ECO` | 19 | 2026-09-18 @ $85.00 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $1679.50 |
| `FIVN` | 48 | 2026-09-18 @ $34.44 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $1679.50 |
| `DXCM` | 1 | 2026-09-21 @ $88.83 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $153.71 |
| `MGTX` | 11 | 2026-09-21 @ $13.47 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $153.71 |
| `CYPH` | 38 | 2026-09-21 @ $4.00 | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $153.71 |
| `ARQT` | 3 | 2026-09-23 @ $27.79 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $98.10 |
| `ADMA` | 9 | 2026-09-23 @ $9.81 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $98.10 |
| `FTRE` | 4 | 2026-09-23 @ $20.25 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $98.10 |
| `OMER` | 4 | 2026-09-23 @ $20.65 | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $98.10 |
