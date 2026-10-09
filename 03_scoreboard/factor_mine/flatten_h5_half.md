# Factor mine action — `flatten_h5_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **-8.79%** ($9,121) · signal-only (no cash/fees) was +4.47%. Starts YES **1/30**. Fills 245 · skips 395 · realized $-47.54.

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
- Only spend half of leftover cash; the rest stays cash.
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
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3,946.65.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $625.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,101.72 | ▲ close $10,071.15 vs 09:30 $10,000.00 (session +94.08) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,101.72 | ▲ 09:30 equity $10,084.41 vs yday $10,071.15 (+13.26) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $4,805.93 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $4,563.93 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $4,273.88 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.7; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $3,956.43 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 340 | $0.94 | $4.21 | — | $3,633.64 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 212 | $1.50 | $2.73 | — | $3,312.91 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $318.86 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,312.91 | ▲ close $10,212.64 vs 09:30 $10,084.41 (session +143.26) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,312.91 | ▼ 09:30 equity $10,196.68 vs yday $10,212.64 (-15.96) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $3,126.33 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $2,982.13 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,777.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $2,568.74 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $2,363.64 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $2,180.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $1,974.45 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 43 | $4.81 | $2.12 | — | $1,765.50 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $207.06 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,765.50 | ▲ close $10,250.96 vs 09:30 $10,196.68 (session +69.88) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,765.50 | ▼ 09:30 equity $10,145.54 vs yday $10,250.96 (-105.42) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,765.50 | ▲ close $10,192.93 vs 09:30 $10,145.54 (session +47.39) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,765.50 | ▲ 09:30 equity $10,289.35 vs yday $10,192.93 (+96.42) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,765.50 | ▲ close $10,495.12 vs 09:30 $10,289.35 (session +205.77) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,765.50 | ▼ 09:30 equity $10,488.06 vs yday $10,495.12 (-7.06) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 10 | $58.64 | $2.04 | $-15.66 | $2,349.86 | ▼ -15.66 after sell → book $10,486.02; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 13 | $42.46 | $2.05 | $-49.84 | $2,899.79 | ▼ -49.84 after sell → book $10,483.97; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 12 | $53.06 | $2.05 | $+25.17 | $3,534.46 | ▲ +25.17 after sell → book $10,481.92; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 12 | $51.65 | $2.05 | $+19.33 | $4,152.22 | ▲ +19.33 after sell → book $10,479.88; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 53 | $13.84 | $2.17 | $+109.10 | $4,883.57 | ▲ +109.10 after sell → book $10,477.71; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 21 | $30.66 | $2.07 | $+15.19 | $5,525.35 | ▲ +15.19 after sell → book $10,475.63; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 771 | $1.30 | $10.08 | $+359.15 | $6,517.57 | ▲ +359.15 after sell → book $10,465.55; vs 09:30 mark -10.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 26 | $23.11 | $2.09 | $-9.88 | $7,116.34 | ▼ -9.88 after sell → book $10,463.46; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 21 | $20.55 | $2.05 | — | $6,682.74 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $6,316.70 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 21 | $20.65 | $2.05 | — | $5,880.99 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 77 | $5.77 | $2.22 | — | $5,434.48 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 22 | $19.63 | $2.06 | — | $5,000.57 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $4,554.08 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 254 | $1.75 | $3.28 | — | $4,106.31 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $3,670.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $444.77 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,670.69 | ▲ close $10,582.85 vs 09:30 $10,488.06 (session +137.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,670.69 | ▲ 09:30 equity $10,738.62 vs yday $10,582.85 (+155.77) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $3,948.65 | ▼ -17.83 after sell → book $10,736.60; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $4,179.79 | ▼ -10.85 after sell → book $10,734.58; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 5 | $58.63 | $2.02 | $+1.07 | $4,470.92 | ▲ +1.07 after sell → book $10,732.56; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 35 | $11.70 | $2.12 | $+89.94 | $4,878.30 | ▲ +89.94 after sell → book $10,730.44; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 340 | $0.87 | $4.03 | $-32.04 | $5,169.05 | ▼ -32.04 after sell → book $10,726.41; vs 09:30 mark -4.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 212 | $1.66 | $2.78 | $+28.41 | $5,518.19 | ▲ +28.41 after sell → book $10,723.63; vs 09:30 mark -2.78 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $5,277.33 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 20 | $17.20 | $2.05 | — | $4,931.28 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,712.99 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 30 | $11.13 | $2.08 | — | $4,377.01 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 139 | $2.47 | $2.41 | — | $4,031.27 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 178 | $1.93 | $2.52 | — | $3,685.21 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,384.61 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 261 | $1.32 | $3.37 | — | $3,036.72 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $344.89 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,036.72 | ▲ close $10,834.53 vs 09:30 $10,738.62 (session +129.32) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,036.72 | ▲ 09:30 equity $10,961.36 vs yday $10,834.53 (+126.83) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.89 | $1.99 | $+6.99 | $3,230.29 | ▲ +6.99 after sell → book $10,959.38; vs 09:30 mark -1.98 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.07 | $1.54 | $+6.33 | $3,380.82 | ▲ +6.33 after sell → book $10,957.83; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $210.00 | $2.01 | $+3.29 | $3,588.80 | ▲ +3.29 after sell → book $10,955.82; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 51 | $4.62 | $2.16 | $+25.02 | $3,822.52 | ▲ +25.02 after sell → book $10,953.66; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 24 | $9.26 | $2.08 | $+15.06 | $4,042.67 | ▲ +15.06 after sell → book $10,951.57; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $102.20 | $2.02 | $+19.49 | $4,245.06 | ▲ +19.49 after sell → book $10,949.56; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 63 | $3.50 | $2.20 | $+12.00 | $4,463.36 | ▲ +12.00 after sell → book $10,947.36; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 43 | $5.05 | $2.14 | $+6.06 | $4,678.37 | ▲ +6.06 after sell → book $10,945.22; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,678.37 | ▼ close $10,905.93 vs 09:30 $10,961.36 (session -39.28) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,678.37 | ▼ 09:30 equity $10,817.05 vs yday $10,905.93 (-88.88) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 16 | $23.77 | $2.04 | — | $4,296.01 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $389.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 35 | $10.98 | $2.10 | — | $3,909.62 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $389.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 6 | $61.19 | $2.01 | — | $3,540.47 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $389.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 46 | $8.35 | $2.13 | — | $3,154.24 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $389.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 78 | $4.94 | $2.22 | — | $2,766.70 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $389.86 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,766.70 | ▲ close $11,098.21 vs 09:30 $10,817.05 (session +291.65) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,766.70 | ▼ 09:30 equity $11,001.85 vs yday $11,098.21 (-96.36) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 3 | $427.50 | $2.00 | — | $1,482.20 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.1; leftover $1383.35 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,482.20 | ▼ close $10,952.40 vs 09:30 $11,001.85 (session -47.45) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,482.20 | ▲ 09:30 equity $10,969.86 vs yday $10,952.40 (+17.46) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 21 | $20.93 | $2.07 | $+3.85 | $1,919.65 | ▲ +3.85 after sell → book $10,967.78; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 4 | $95.52 | $2.02 | $+14.02 | $2,299.71 | ▲ +14.02 after sell → book $10,965.76; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 21 | $21.31 | $2.07 | $+9.73 | $2,745.15 | ▲ +9.73 after sell → book $10,963.69; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 77 | $5.49 | $2.24 | $-26.02 | $3,165.64 | ▼ -26.02 after sell → book $10,961.45; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 22 | $21.47 | $2.08 | $+36.35 | $3,635.90 | ▲ +36.35 after sell → book $10,959.37; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.32 | $2.06 | $+36.26 | $4,118.64 | ▲ +36.26 after sell → book $10,957.31; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 254 | $1.91 | $3.33 | $+34.03 | $4,600.46 | ▲ +34.03 after sell → book $10,953.99; vs 09:30 mark -3.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $155.89 | $2.02 | $+30.03 | $5,066.11 | ▲ +30.03 after sell → book $10,951.97; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 20 | $41.44 | $2.05 | — | $4,235.26 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $844.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 58 | $14.42 | $2.16 | — | $3,396.73 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $844.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 324 | $2.60 | $4.18 | — | $2,550.15 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $844.35 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,550.15 | ▲ close $10,990.47 vs 09:30 $10,969.86 (session +46.90) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,550.15 | ▼ 09:30 equity $10,976.18 vs yday $10,990.47 (-14.29) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $119.19 | $2.02 | $-4.49 | $2,786.52 | ▼ -4.49 after sell → book $10,974.17; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 20 | $16.44 | $2.07 | $-19.32 | $3,113.25 | ▼ -19.32 after sell → book $10,972.10; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $216.31 | $2.01 | $-4.00 | $3,327.54 | ▼ -4.00 after sell → book $10,970.08; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 30 | $15.43 | $2.10 | $+124.82 | $3,788.34 | ▲ +124.82 after sell → book $10,967.98; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 139 | $2.35 | $2.44 | $-21.53 | $4,112.55 | ▼ -21.53 after sell → book $10,965.54; vs 09:30 mark -2.44 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 178 | $2.06 | $2.56 | $+18.05 | $4,476.67 | ▲ +18.05 after sell → book $10,962.98; vs 09:30 mark -2.56 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 5 | $58.22 | $2.02 | $-11.53 | $4,765.75 | ▼ -11.53 after sell → book $10,960.96; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 261 | $1.82 | $3.42 | $+123.71 | $5,237.35 | ▲ +123.71 after sell → book $10,957.54; vs 09:30 mark -3.42 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,237.35 | ▼ close $10,826.03 vs 09:30 $10,976.18 (session -131.51) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,237.35 | ▲ 09:30 equity $10,847.41 vs yday $10,826.03 (+21.38) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,237.35 | ▲ close $10,885.91 vs 09:30 $10,847.41 (session +38.50) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,237.35 | ▲ 09:30 equity $10,981.24 vs yday $10,885.91 (+95.33) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 16 | $23.94 | $2.06 | $-1.38 | $5,618.33 | ▼ -1.38 after sell → book $10,979.18; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 35 | $10.42 | $2.12 | $-23.81 | $5,980.91 | ▼ -23.81 after sell → book $10,977.06; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 6 | $63.00 | $2.03 | $+6.82 | $6,356.88 | ▲ +6.82 after sell → book $10,975.03; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 46 | $8.25 | $2.15 | $-8.88 | $6,734.24 | ▼ -8.88 after sell → book $10,972.89; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 78 | $4.64 | $2.25 | $-27.87 | $7,093.91 | ▼ -27.87 after sell → book $10,970.64; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,093.91 | ▼ close $10,918.57 vs 09:30 $10,981.24 (session -52.07) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,093.91 | ▼ 09:30 equity $10,890.65 vs yday $10,918.57 (-27.92) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `HCA` | 3 | $412.46 | $2.02 | $-49.14 | $8,329.27 | ▼ -49.14 after sell → book $10,888.63; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,329.27 | ▼ close $10,861.03 vs 09:30 $10,890.65 (session -27.60) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,329.27 | ▲ 09:30 equity $10,880.73 vs yday $10,861.03 (+19.70) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 20 | $42.43 | $2.07 | $+15.68 | $9,175.80 | ▲ +15.68 after sell → book $10,878.66; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 58 | $15.45 | $2.18 | $+55.39 | $10,069.72 | ▲ +55.39 after sell → book $10,876.48; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 324 | $2.49 | $4.24 | $-44.06 | $10,872.23 | ▼ -44.06 after sell → book $10,872.23; vs 09:30 mark -4.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 20 | $52.88 | $2.05 | — | $9,812.58 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $1087.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 25 | $42.93 | $2.06 | — | $8,737.27 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $1087.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 299 | $3.63 | $3.86 | — | $7,648.04 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $1087.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 135 | $8.03 | $2.40 | — | $6,561.60 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $1087.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $5,499.98 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $1087.22 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,499.98 | ▼ close $10,758.54 vs 09:30 $10,880.73 (session -101.31) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,499.98 | ▼ 09:30 equity $10,720.71 vs yday $10,758.54 (-37.83) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 181 | $2.52 | $2.53 | — | $5,041.33 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $458.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 68 | $6.71 | $2.19 | — | $4,582.86 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $458.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 241 | $1.90 | $3.11 | — | $4,121.85 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $458.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 95 | $4.78 | $2.27 | — | $3,665.47 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $458.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 288 | $1.59 | $3.72 | — | $3,203.84 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $458.33 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 40 | $11.31 | $2.11 | — | $2,749.33 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $458.33 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,749.33 | ▲ close $10,741.91 vs 09:30 $10,720.71 (session +37.13) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,749.33 | ▼ 09:30 equity $10,737.38 vs yday $10,741.91 (-4.53) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,749.33 | ▼ close $10,618.21 vs 09:30 $10,737.38 (session -119.17) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,749.33 | ▼ 09:30 equity $10,576.05 vs yday $10,618.21 (-42.16) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,749.33 | ▼ close $10,314.96 vs 09:30 $10,576.05 (session -261.09) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,749.33 | ▼ 09:30 equity $10,223.55 vs yday $10,314.96 (-91.41) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,749.33 | ▼ close $10,088.88 vs 09:30 $10,223.55 (session -134.67) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,749.33 | ▲ 09:30 equity $10,167.91 vs yday $10,088.88 (+79.03) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 20 | $53.53 | $2.07 | $+8.88 | $3,817.86 | ▲ +8.88 after sell → book $10,165.84; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 25 | $41.30 | $2.08 | $-44.90 | $4,848.27 | ▼ -44.90 after sell → book $10,163.75; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 299 | $2.77 | $3.92 | $-264.91 | $5,672.58 | ▼ -264.91 after sell → book $10,159.83; vs 09:30 mark -3.92 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 135 | $7.70 | $2.43 | $-49.37 | $6,709.66 | ▼ -49.37 after sell → book $10,157.41; vs 09:30 mark -2.42 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 8 | $122.40 | $2.03 | $-84.45 | $7,686.82 | ▼ -84.45 after sell → book $10,155.37; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 39 | $16.28 | $2.11 | — | $7,049.80 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $640.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 234 | $2.73 | $3.02 | — | $6,407.96 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $640.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 3 | $206.84 | $2.00 | — | $5,785.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $640.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $5,290.15 | — | deploy half leftover; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $640.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 4 | $157.78 | $2.00 | — | $4,657.03 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $640.57 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 11 | $56.09 | $2.02 | — | $4,038.01 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $640.57 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,038.01 | ▼ close $10,140.38 vs 09:30 $10,167.91 (session -1.84) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,038.01 | ▼ 09:30 equity $10,011.32 vs yday $10,140.38 (-129.06) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 181 | $2.15 | $2.57 | $-72.08 | $4,424.59 | ▼ -72.08 after sell → book $10,008.75; vs 09:30 mark -2.57 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 68 | $5.93 | $2.22 | $-57.45 | $4,825.62 | ▼ -57.45 after sell → book $10,006.53; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 241 | $1.72 | $3.16 | $-50.85 | $5,235.77 | ▼ -50.85 after sell → book $10,003.37; vs 09:30 mark -3.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 95 | $4.13 | $2.30 | $-66.33 | $5,625.82 | ▼ -66.33 after sell → book $10,001.07; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 288 | $1.59 | $3.77 | $-7.49 | $6,079.97 | ▼ -7.49 after sell → book $9,997.30; vs 09:30 mark -3.77 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 40 | $10.73 | $2.13 | $-27.44 | $6,507.04 | ▼ -27.44 after sell → book $9,995.17; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,507.04 | ▼ close $9,946.91 vs 09:30 $10,011.32 (session -48.26) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,507.04 | ▲ 09:30 equity $9,975.10 vs yday $9,946.91 (+28.19) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,507.04 | ▼ close $9,903.00 vs 09:30 $9,975.10 (session -72.10) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,507.04 | ▲ 09:30 equity $9,933.72 vs yday $9,903.00 (+30.72) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 3 | $270.89 | $2.00 | — | $5,692.37 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $813.38 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 10 | $77.12 | $2.02 | — | $4,919.15 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $813.38 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 56 | $14.31 | $2.16 | — | $4,115.63 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $813.38 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 22 | $36.46 | $2.06 | — | $3,311.46 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $813.38 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,311.46 | ▲ close $9,926.25 vs 09:30 $9,933.72 (session +0.76) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,311.46 | ▲ 09:30 equity $10,045.69 vs yday $9,926.25 (+119.44) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $3,075.61 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; leftover $275.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 1 | $151.43 | $1.52 | — | $2,922.67 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; leftover $275.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $2,773.58 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; leftover $275.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 26 | $10.25 | $2.07 | — | $2,505.01 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $275.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 36 | $7.59 | $2.10 | — | $2,229.67 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $275.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 7 | $34.93 | $2.01 | — | $1,983.15 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; leftover $275.95 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,983.15 | ▲ close $10,038.96 vs 09:30 $10,045.69 (session +4.44) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,983.15 | ▲ 09:30 equity $10,056.99 vs yday $10,038.96 (+18.03) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 39 | $16.93 | $2.13 | $+21.12 | $2,641.29 | ▲ +21.12 after sell → book $10,054.86; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 234 | $2.68 | $3.07 | $-17.79 | $3,265.34 | ▼ -17.79 after sell → book $10,051.79; vs 09:30 mark -3.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 3 | $197.76 | $2.02 | $-31.26 | $3,856.61 | ▼ -31.26 after sell → book $10,049.78; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 3 | $150.47 | $2.02 | $-45.90 | $4,306.00 | ▼ -45.90 after sell → book $10,047.76; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 4 | $152.71 | $2.02 | $-24.30 | $4,914.81 | ▼ -24.30 after sell → book $10,045.73; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 11 | $55.80 | $2.04 | $-7.26 | $5,526.57 | ▼ -7.26 after sell → book $10,043.69; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 4 | $108.55 | $2.00 | — | $5,090.37 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $460.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 2 | $209.52 | $2.00 | — | $4,669.33 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $460.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 2 | $219.62 | $2.00 | — | $4,228.10 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $460.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 5 | $85.00 | $2.00 | — | $3,801.09 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $460.55 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 13 | $34.44 | $2.03 | — | $3,351.34 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $460.55 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,351.34 | ▼ close $9,953.78 vs 09:30 $10,056.99 (session -79.88) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,351.34 | ▲ 09:30 equity $9,989.20 vs yday $9,953.78 (+35.42) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 2 | $157.87 | $2.00 | — | $3,033.61 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $335.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 3 | $88.83 | $2.00 | — | $2,765.12 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $335.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 24 | $13.47 | $2.06 | — | $2,439.78 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $335.13 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 83 | $4.00 | $2.24 | — | $2,105.54 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $335.13 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,105.54 | ▼ close $9,933.89 vs 09:30 $9,989.20 (session -47.02) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,105.54 | ▲ 09:30 equity $9,942.76 vs yday $9,933.89 (+8.87) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 1 | $93.97 | $0.94 | — | $2,010.63 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $175.46 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,010.63 | ▲ close $9,972.00 vs 09:30 $9,942.76 (session +30.18) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,010.63 | ▲ 09:30 equity $10,085.43 vs yday $9,972.00 (+113.43) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 3 | $270.66 | $2.02 | $-4.71 | $2,820.59 | ▼ -4.71 after sell → book $10,083.41; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 10 | $73.61 | $2.04 | $-39.16 | $3,554.65 | ▼ -39.16 after sell → book $10,081.37; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 56 | $13.12 | $2.18 | $-70.98 | $4,287.19 | ▼ -70.98 after sell → book $10,079.19; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 22 | $38.04 | $2.08 | $+30.63 | $5,121.99 | ▲ +30.63 after sell → book $10,077.12; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 4 | $116.85 | $2.00 | — | $4,652.59 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $512.20 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 18 | $27.79 | $2.04 | — | $4,150.33 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $512.20 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 52 | $9.81 | $2.15 | — | $3,638.06 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $512.20 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 25 | $20.25 | $2.06 | — | $3,129.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $512.20 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 24 | $20.65 | $2.06 | — | $2,632.08 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $512.20 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,632.08 | ▼ close $9,949.80 vs 09:30 $10,085.43 (session -117.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,632.08 | ▼ 09:30 equity $9,870.08 vs yday $9,949.80 (-79.72) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ILMN` | 1 | $253.79 | $2.01 | $+15.93 | $2,883.86 | ▲ +15.93 after sell → book $9,868.07; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 1 | $157.72 | $1.60 | $+3.17 | $3,039.98 | ▲ +3.17 after sell → book $9,866.47; vs 09:30 mark -1.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 1 | $141.79 | $1.44 | $-8.74 | $3,180.33 | ▼ -8.74 after sell → book $9,865.03; vs 09:30 mark -1.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 26 | $10.39 | $2.09 | $-0.52 | $3,448.38 | ▼ -0.52 after sell → book $9,862.94; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 36 | $7.38 | $2.12 | $-11.78 | $3,711.94 | ▼ -11.78 after sell → book $9,860.82; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 7 | $33.82 | $2.03 | $-11.81 | $3,946.65 | ▼ -11.81 after sell → book $9,858.79; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,946.65 | ▲ close $9,935.90 vs 09:30 $9,870.08 (session +77.12) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,026.15 | ▲ 09:30 equity $9,208.25 vs yday $9,203.78 (+4.47) | 09:30 open · cash $4,026.15 (unchanged overnight, no fees) · equity $9,208.25 vs prior close $9,203.78 (+4.47) · 20 name(s) re-marked at the open (per-name table). A×1 yday $172.84 → 09:30 $171.98 -0.86; ADMA×44 yday $9.52 → 09:30 $9.52 +0.00; ARQT×15 yday $26.27 → 09:30 $26.27 +0.00; CYPH×69 yday $4.08 → 09:30 $4.00 -5.17; DLO×10 yday $13.88 → 09:30 $13.88 +0.00; DXCM×3 yday $87.47 → 09:30 $87.47 +0.00; ECO×4 yday $78.22 → 09:30 $78.22 +0.00; EL×1 yday $95.37 → 09:30 $95.37 +0.00; FIVN×10 yday $36.66 → 09:30 $36.66 +0.00; FTRE×21 yday $20.02 → 09:30 $20.02 +0.00; GNRC×1 yday $198.05 → 09:30 $198.05 +0.00; HALO×3 yday $115.22 → 09:30 $115.36 +0.42; MGTX×20 yday $11.05 → 09:30 $11.05 +0.00; MKC×3 yday $47.82 → 09:30 $47.82 +0.00; OMER×21 yday $20.13 → 09:30 $20.61 +10.08; PACS×3 yday $41.46 → 09:30 $41.46 +0.00; RBRK×3 yday $113.80 → 09:30 $113.80 +0.00; TDC×5 yday $29.46 → 09:30 $29.46 +0.00; USFD×1 yday $93.82 → 09:30 $93.82 +0.00; VICR×1 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 17 | $38.51 | $2.04 | — | $3,369.44 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $671.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 87 | $7.65 | $2.25 | — | $2,701.64 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $671.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,701.64 | ▼ close $9,192.50 vs 09:30 $9,208.25 (session -11.45) | 16:00 close · cash $2,701.64 · equity $9,192.50 vs 09:30 $9,208.25 (-15.75; session marks -11.45) · 22 name(s) marked open→close (per-name table). A×1 09:30 $171.98 → close $172.79 +0.81; ADMA×44 09:30 $9.52 → close $9.52 +0.00; ARQT×15 09:30 $26.27 → close $26.27 +0.00; CYPH×69 09:30 $4.00 → close $4.12 +7.94; DLO×10 09:30 $13.88 → close $13.88 +0.00; DXCM×3 09:30 $87.47 → close $87.47 +0.00; ECO×4 09:30 $78.22 → close $78.22 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FIVN×10 09:30 $36.66 → close $36.66 -0.00; FTRE×21 09:30 $20.02 → close $20.02 +0.00; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×3 09:30 $115.36 → close $113.90 -4.38; MGTX×20 09:30 $11.05 → close $11.05 +0.00; MKC×3 09:30 $47.82 → close $47.82 -0.00; OMER×21 09:30 $20.61 → close $20.08 -11.13; PACS×3 09:30 $41.46 → close $41.46 -0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TDC×5 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; BLFS×17 09:30 $38.51 → close $38.49 -0.34; MRVI×87 09:30 $7.65 → close $7.60 -4.35 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,701.64 | ▼ 09:30 equity $9,114.09 vs yday $9,192.50 (-78.41) | 09:30 open · cash $2,701.64 (unchanged overnight, no fees) · equity $9,114.09 vs prior close $9,192.50 (-78.41) · 22 name(s) re-marked at the open (per-name table). A×1 yday $172.79 → 09:30 $170.00 -2.79; ADMA×44 yday $9.52 → 09:30 $9.38 -6.16; ARQT×15 yday $26.27 → 09:30 $26.70 +6.45; BLFS×17 yday $38.49 → 09:30 $38.13 -6.12; CYPH×69 yday $4.12 → 09:30 $4.03 -6.04; DLO×10 yday $13.88 → 09:30 $13.91 +0.30; DXCM×3 yday $87.47 → 09:30 $86.87 -1.80; ECO×4 yday $78.22 → 09:30 $79.55 +5.32; EL×1 yday $95.37 → 09:30 $92.92 -2.45; FIVN×10 yday $36.66 → 09:30 $34.75 -19.10; FTRE×21 yday $20.02 → 09:30 $19.54 -10.08; GNRC×1 yday $198.05 → 09:30 $207.41 +9.36; HALO×3 yday $113.90 → 09:30 $113.34 -1.68; MGTX×20 yday $11.05 → 09:30 $10.79 -5.20; MKC×3 yday $47.82 → 09:30 $47.83 +0.03; MRVI×87 yday $7.60 → 09:30 $7.49 -9.57; OMER×21 yday $20.08 → 09:30 $19.83 -5.25; PACS×3 yday $41.46 → 09:30 $41.27 -0.57; RBRK×3 yday $113.80 → 09:30 $106.94 -20.58; TDC×5 yday $29.46 → 09:30 $28.34 -5.60; USFD×1 yday $93.82 → 09:30 $93.00 -0.82; VICR×1 yday $276.06 → 09:30 $280.00 +3.94 | — |
| 2026-09-28 09:30 ET | **SELL** | `A` | 1 | $170.00 | $1.72 | $+8.83 | $2,869.92 | ▲ +8.83 after sell → book $9,112.37; vs 09:30 mark -1.72 | dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `ADMA` | 44 | $9.38 | $2.14 | $-23.18 | $3,280.49 | ▼ -23.18 after sell → book $9,110.23; vs 09:30 mark -2.14 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ARQT` | 15 | $26.70 | $2.06 | $-20.44 | $3,678.94 | ▼ -20.44 after sell → book $9,108.17; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 69 | $4.03 | $2.22 | $-2.17 | $3,954.96 | ▼ -2.17 after sell → book $9,105.95; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `DLO` | 10 | $13.91 | $1.44 | $-8.82 | $4,092.62 | ▼ -8.82 after sell → book $9,104.51; vs 09:30 mark -1.44 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DXCM` | 3 | $86.87 | $2.02 | $-9.90 | $4,351.21 | ▼ -9.90 after sell → book $9,102.49; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ECO` | 4 | $79.55 | $2.02 | $-25.82 | $4,667.39 | ▼ -25.82 after sell → book $9,100.47; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `EL` | 1 | $92.92 | $0.95 | $-7.04 | $4,759.36 | ▼ -7.04 after sell → book $9,099.52; vs 09:30 mark -0.95 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FIVN` | 10 | $34.75 | $2.04 | $-0.96 | $5,104.82 | ▼ -0.96 after sell → book $9,097.48; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FTRE` | 21 | $19.54 | $2.07 | $-19.04 | $5,513.09 | ▼ -19.04 after sell → book $9,095.41; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `GNRC` | 1 | $207.41 | $2.01 | $-6.12 | $5,718.48 | ▼ -6.12 after sell → book $9,093.39; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `MGTX` | 20 | $10.79 | $2.07 | $-57.72 | $5,932.21 | ▼ -57.72 after sell → book $9,091.32; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `MKC` | 3 | $47.83 | $1.46 | $-8.18 | $6,074.24 | ▼ -8.18 after sell → book $9,089.86; vs 09:30 mark -1.46 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `PACS` | 3 | $41.27 | $1.27 | $-4.51 | $6,196.78 | ▼ -4.51 after sell → book $9,088.59; vs 09:30 mark -1.27 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `RBRK` | 3 | $106.94 | $2.02 | $-8.85 | $6,515.58 | ▼ -8.85 after sell → book $9,086.57; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `TDC` | 5 | $28.34 | $1.45 | $-10.16 | $6,655.83 | ▼ -10.16 after sell → book $9,085.12; vs 09:30 mark -1.45 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `USFD` | 1 | $93.00 | $0.95 | $-2.98 | $6,747.88 | ▼ -2.98 after sell → book $9,084.17; vs 09:30 mark -0.95 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `VICR` | 1 | $280.00 | $2.01 | $+56.37 | $7,025.87 | ▲ +56.37 after sell → book $9,082.16; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,025.87 | ▲ close $9,101.93 vs 09:30 $9,114.09 (session +19.77) | 16:00 close · cash $7,025.87 · equity $9,101.93 vs 09:30 $9,114.09 (-12.16; session marks +19.77) · 4 name(s) marked open→close (per-name table). BLFS×17 09:30 $38.13 → close $39.15 +17.34; HALO×3 09:30 $113.34 → close $112.89 -1.35; MRVI×87 09:30 $7.49 → close $7.63 +12.18; OMER×21 09:30 $19.83 → close $19.43 -8.40 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,025.87 | ▼ 09:30 equity $9,086.92 vs yday $9,101.93 (-15.01) | 09:30 open · cash $7,025.87 (unchanged overnight, no fees) · equity $9,086.92 vs prior close $9,101.93 (-15.01) · 4 name(s) re-marked at the open (per-name table). BLFS×17 yday $39.15 → 09:30 $39.04 -1.87; HALO×3 yday $112.89 → 09:30 $112.89 +0.00; MRVI×87 yday $7.63 → 09:30 $7.52 -9.57; OMER×21 yday $19.43 → 09:30 $19.26 -3.57 | — |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 3 | $184.05 | $2.00 | — | $6,471.72 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.7; leftover $585.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 10 | $57.39 | $2.02 | — | $5,895.80 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+2.6; leftover $585.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 19 | $30.40 | $2.05 | — | $5,316.15 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+1.9; leftover $585.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 32 | $17.76 | $2.09 | — | $4,745.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+9.8; leftover $585.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 11 | $50.25 | $2.02 | — | $4,190.97 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.5; leftover $585.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 12 | $45.06 | $2.03 | — | $3,648.23 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.8; leftover $585.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,648.23 | ▲ close $9,081.16 vs 09:30 $9,086.92 (session +6.44) | 16:00 close · cash $3,648.23 · equity $9,081.16 vs 09:30 $9,086.92 (-5.76; session marks +6.44) · 10 name(s) marked open→close (per-name table). BLFS×17 09:30 $39.04 → close $39.22 +3.06; HALO×3 09:30 $112.89 → close $111.56 -3.99; MRVI×87 09:30 $7.52 → close $7.65 +11.31; OMER×21 09:30 $19.26 → close $19.25 -0.21; SN×3 09:30 $184.05 → close $182.44 -4.83; DT×10 09:30 $57.39 → close $57.53 +1.40; TOST×19 09:30 $30.40 → close $30.46 +1.14; SONO×32 09:30 $17.76 → close $17.85 +2.88; PDFS×11 09:30 $50.25 → close $49.77 -5.28; SHOO×12 09:30 $45.06 → close $45.14 +0.96 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,648.23 | ▲ 09:30 equity $9,081.16 vs yday $9,081.16 (+0.00) | 09:30 open · cash $3,648.23 (unchanged overnight, no fees) · equity $9,081.16 vs prior close $9,081.16 (+0.00) · 10 name(s) re-marked at the open (per-name table). BLFS×17 yday $39.22 → 09:30 $39.22 +0.00; DT×10 yday $57.53 → 09:30 $57.53 +0.00; HALO×3 yday $111.56 → 09:30 $111.56 +0.00; MRVI×87 yday $7.65 → 09:30 $7.65 +0.00; OMER×21 yday $19.25 → 09:30 $19.25 +0.00; PDFS×11 yday $49.77 → 09:30 $49.77 +0.00; SHOO×12 yday $45.14 → 09:30 $45.14 +0.00; SN×3 yday $182.44 → 09:30 $182.44 +0.00; SONO×32 yday $17.85 → 09:30 $17.85 +0.00; TOST×19 yday $30.46 → 09:30 $30.46 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,648.23 | ▲ close $9,081.16 vs 09:30 $9,081.16 (session +0.00) | 16:00 close · cash $3,648.23 · equity $9,081.16 vs 09:30 $9,081.16 (+0.00; session marks +0.00) · 10 name(s) marked open→close (per-name table). BLFS×17 09:30 $39.22 → close $39.22 +0.00; DT×10 09:30 $57.53 → close $57.53 +0.00; HALO×3 09:30 $111.56 → close $111.56 +0.00; MRVI×87 09:30 $7.65 → close $7.65 +0.00; OMER×21 09:30 $19.25 → close $19.25 +0.00; PDFS×11 09:30 $49.77 → close $49.77 +0.00; SHOO×12 09:30 $45.14 → close $45.14 +0.00; SN×3 09:30 $182.44 → close $182.44 +0.00; SONO×32 09:30 $17.85 → close $17.85 +0.00; TOST×19 09:30 $30.46 → close $30.46 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,648.23 | ▼ 09:30 equity $9,070.46 vs yday $9,081.16 (-10.70) | 09:30 open · cash $3,648.23 (unchanged overnight, no fees) · equity $9,070.46 vs prior close $9,081.16 (-10.70) · 10 name(s) re-marked at the open (per-name table). BLFS×17 yday $39.22 → 09:30 $38.90 -5.44; DT×10 yday $57.53 → 09:30 $59.04 +15.10; HALO×3 yday $111.56 → 09:30 $109.93 -4.89; MRVI×87 yday $7.65 → 09:30 $7.67 +1.74; OMER×21 yday $19.25 → 09:30 $18.87 -7.98; PDFS×11 yday $49.77 → 09:30 $51.41 +18.04; SHOO×12 yday $45.14 → 09:30 $44.46 -8.16; SN×3 yday $182.44 → 09:30 $182.44 +0.00; SONO×32 yday $17.85 → 09:30 $18.09 +7.68; TOST×19 yday $30.46 → 09:30 $29.05 -26.79 | — |
| 2026-10-01 09:30 ET | **SELL** | `BLFS` | 17 | $38.90 | $2.06 | $+2.53 | $4,307.47 | ▲ +2.53 after sell → book $9,068.40; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DT` | 10 | $59.04 | $2.04 | $+12.44 | $4,895.83 | ▲ +12.44 after sell → book $9,066.36; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `HALO` | 3 | $109.93 | $2.02 | $-24.78 | $5,223.60 | ▼ -24.78 after sell → book $9,064.34; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MRVI` | 87 | $7.67 | $2.28 | $-2.79 | $5,888.61 | ▼ -2.79 after sell → book $9,062.06; vs 09:30 mark -2.28 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `OMER` | 21 | $18.87 | $2.07 | $-41.51 | $6,282.81 | ▼ -41.51 after sell → book $9,059.99; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 11 | $51.41 | $2.04 | $+8.69 | $6,846.28 | ▲ +8.69 after sell → book $9,057.95; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SHOO` | 12 | $44.46 | $2.05 | $-11.27 | $7,377.75 | ▼ -11.27 after sell → book $9,055.90; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 32 | $18.09 | $2.11 | $+6.37 | $7,954.53 | ▲ +6.37 after sell → book $9,053.80; vs 09:30 mark -2.10 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TOST` | 19 | $29.05 | $2.07 | $-29.76 | $8,504.41 | ▼ -29.76 after sell → book $9,051.73; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `IT` | 4 | $196.19 | $2.00 | — | $7,717.65 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.5; leftover $850.44 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 9 | $92.93 | $2.02 | — | $6,879.26 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.2; leftover $850.44 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 21 | $38.99 | $2.05 | — | $6,058.42 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.4; leftover $850.44 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 59 | $14.27 | $2.17 | — | $5,214.32 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.3; leftover $850.44 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 39 | $21.28 | $2.11 | — | $4,382.29 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.7; leftover $850.44 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,382.29 | ▼ close $9,038.27 vs 09:30 $9,070.46 (session -3.11) | 16:00 close · cash $4,382.29 · equity $9,038.27 vs 09:30 $9,070.46 (-32.19; session marks -3.11) · 6 name(s) marked open→close (per-name table). SN×3 09:30 $182.44 → close $182.44 +0.00; IT×4 09:30 $196.19 → close $192.80 -13.56; KSPI×9 09:30 $92.93 → close $92.02 -8.19; IOT×21 09:30 $38.99 → close $40.04 +22.05; AVPT×59 09:30 $14.27 → close $14.08 -11.21; RELY×39 09:30 $21.28 → close $21.48 +7.80 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,382.29 | ▲ 09:30 equity $9,070.20 vs yday $9,038.27 (+31.93) | 09:30 open · cash $4,382.29 (unchanged overnight, no fees) · equity $9,070.20 vs prior close $9,038.27 (+31.93) · 6 name(s) re-marked at the open (per-name table). AVPT×59 yday $14.08 → 09:30 $14.22 +8.26; IOT×21 yday $40.04 → 09:30 $40.41 +7.77; IT×4 yday $192.80 → 09:30 $192.74 -0.24; KSPI×9 yday $92.02 → 09:30 $92.05 +0.27; RELY×39 yday $21.48 → 09:30 $21.80 +12.48; SN×3 yday $182.44 → 09:30 $183.57 +3.39 | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 3 | $183.57 | $2.02 | $-5.46 | $4,930.98 | ▼ -5.46 after sell → book $9,068.18; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **BUY** | `VEEV` | 1 | $283.10 | $1.99 | — | $4,645.89 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.6; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CORT` | 3 | $114.38 | $2.00 | — | $4,300.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.8; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ILMN` | 1 | $265.91 | $1.99 | — | $4,032.85 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-3.0; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 5 | $66.33 | $2.00 | — | $3,699.19 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.9; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 12 | $27.63 | $2.03 | — | $3,365.60 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.6; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `BLFS` | 9 | $37.02 | $2.02 | — | $3,030.41 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-4.7; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 6 | $52.42 | $2.01 | — | $2,713.88 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.6; leftover $352.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,713.88 | ▲ close $9,065.24 vs 09:30 $9,070.20 (session +11.10) | 16:00 close · cash $2,713.88 · equity $9,065.24 vs 09:30 $9,070.20 (-4.96; session marks +11.10) · 12 name(s) marked open→close (per-name table). AVPT×59 09:30 $14.22 → close $14.07 -8.85; IOT×21 09:30 $40.41 → close $41.12 +14.91; IT×4 09:30 $192.74 → close $184.80 -31.76; KSPI×9 09:30 $92.05 → close $94.05 +18.00; RELY×39 09:30 $21.80 → close $21.77 -1.17; VEEV×1 09:30 $283.10 → close $273.33 -9.77; CORT×3 09:30 $114.38 → close $116.23 +5.55; ILMN×1 09:30 $265.91 → close $273.04 +7.13; CDNA×5 09:30 $66.33 → close $67.15 +4.10; WRBY×12 09:30 $27.63 → close $27.03 -7.20; BLFS×9 09:30 $37.02 → close $37.30 +2.52; ETON×6 09:30 $52.42 → close $55.36 +17.64 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,713.88 | ▲ 09:30 equity $9,079.04 vs yday $9,065.24 (+13.80) | 09:30 open · cash $2,713.88 (unchanged overnight, no fees) · equity $9,079.04 vs prior close $9,065.24 (+13.80) · 12 name(s) re-marked at the open (per-name table). AVPT×59 yday $14.07 → 09:30 $13.91 -9.44; BLFS×9 yday $37.30 → 09:30 $37.16 -1.26; CDNA×5 yday $67.15 → 09:30 $66.90 -1.25; CORT×3 yday $116.23 → 09:30 $115.59 -1.92; ETON×6 yday $55.36 → 09:30 $55.85 +2.94; ILMN×1 yday $273.04 → 09:30 $273.00 -0.04; IOT×21 yday $41.12 → 09:30 $42.00 +18.48; IT×4 yday $184.80 → 09:30 $185.07 +1.08; KSPI×9 yday $94.05 → 09:30 $94.50 +4.05; RELY×39 yday $21.77 → 09:30 $21.76 -0.39; VEEV×1 yday $273.33 → 09:30 $275.00 +1.67; WRBY×12 yday $27.03 → 09:30 $27.02 -0.12 | — |
| 2026-10-05 09:30 ET | **BUY** | `COP` | 1 | $127.00 | $1.27 | — | $2,585.61 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.4; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `DVN` | 3 | $47.64 | $1.44 | — | $2,441.25 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.3; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EXE` | 2 | $84.70 | $1.70 | — | $2,270.15 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-1.5; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 4 | $38.10 | $1.54 | — | $2,116.21 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-1.0; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SM` | 4 | $35.27 | $1.42 | — | $1,973.71 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.9; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `MTDR` | 3 | $53.35 | $1.61 | — | $1,812.05 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.8; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `GPRK` | 15 | $10.87 | $1.68 | — | $1,647.32 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-3.1; leftover $169.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 16 | $10.26 | $1.69 | — | $1,481.48 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-2.7; leftover $169.62 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,481.48 | ▲ close $9,256.76 vs 09:30 $9,079.04 (session +190.06) | 16:00 close · cash $1,481.48 · equity $9,256.76 vs 09:30 $9,079.04 (+177.72; session marks +190.06) · 20 name(s) marked open→close (per-name table). AVPT×59 09:30 $13.91 → close $14.66 +44.25; BLFS×9 09:30 $37.16 → close $38.61 +13.05; CDNA×5 09:30 $66.90 → close $69.85 +14.75; CORT×3 09:30 $115.59 → close $122.03 +19.32; ETON×6 09:30 $55.85 → close $55.62 -1.38; ILMN×1 09:30 $273.00 → close $293.69 +20.69; IOT×21 09:30 $42.00 → close $42.39 +8.19; IT×4 09:30 $185.07 → close $187.76 +10.76; KSPI×9 09:30 $94.50 → close $93.98 -4.68; RELY×39 09:30 $21.76 → close $22.91 +44.85; VEEV×1 09:30 $275.00 → close $283.45 +8.45; WRBY×12 09:30 $27.02 → close $26.65 -4.44; COP×1 09:30 $127.00 → close $128.40 +1.40; DVN×3 09:30 $47.64 → close $47.98 +1.02; EXE×2 09:30 $84.70 → close $86.43 +3.46; RRC×4 09:30 $38.10 → close $38.67 +2.28; SM×4 09:30 $35.27 → close $35.05 -0.88; MTDR×3 09:30 $53.35 → close $53.01 -1.02; GPRK×15 09:30 $10.87 → close $11.28 +6.15; OBE×16 09:30 $10.26 → close $10.50 +3.84 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,481.48 | ▲ 09:30 equity $9,303.05 vs yday $9,256.76 (+46.29) | 09:30 open · cash $1,481.48 (unchanged overnight, no fees) · equity $9,303.05 vs prior close $9,256.76 (+46.29) · 20 name(s) re-marked at the open (per-name table). AVPT×59 yday $14.66 → 09:30 $14.77 +6.49; BLFS×9 yday $38.61 → 09:30 $38.61 +0.00; CDNA×5 yday $69.85 → 09:30 $70.89 +5.20; COP×1 yday $128.40 → 09:30 $128.19 -0.21; CORT×3 yday $122.03 → 09:30 $122.20 +0.51; DVN×3 yday $47.98 → 09:30 $47.48 -1.51; ETON×6 yday $55.62 → 09:30 $56.15 +3.18; EXE×2 yday $86.43 → 09:30 $86.38 -0.10; GPRK×15 yday $11.28 → 09:30 $11.32 +0.60; ILMN×1 yday $293.69 → 09:30 $301.58 +7.89; IOT×21 yday $42.39 → 09:30 $42.70 +6.51; IT×4 yday $187.76 → 09:30 $188.47 +2.84; KSPI×9 yday $93.98 → 09:30 $94.50 +4.68; MTDR×3 yday $53.01 → 09:30 $52.56 -1.35; OBE×16 yday $10.50 → 09:30 $10.44 -0.96; RELY×39 yday $22.91 → 09:30 $23.12 +8.19; RRC×4 yday $38.67 → 09:30 $38.71 +0.16; SM×4 yday $35.05 → 09:30 $34.90 -0.60; VEEV×1 yday $283.45 → 09:30 $285.34 +1.89; WRBY×12 yday $26.65 → 09:30 $26.89 +2.88 | — |
| 2026-10-06 09:30 ET | **BUY** | `SN` | 1 | $183.47 | $1.84 | — | $1,296.17 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+0.4; leftover $185.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `TOST` | 6 | $30.07 | $1.82 | — | $1,113.93 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-1.1; leftover $185.19 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PDFS` | 3 | $56.89 | $1.72 | — | $941.54 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+12.7; leftover $185.19 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $941.54 | ▼ close $9,167.52 vs 09:30 $9,303.05 (session -130.14) | 16:00 close · cash $941.54 · equity $9,167.52 vs 09:30 $9,303.05 (-135.53; session marks -130.14) · 23 name(s) marked open→close (per-name table). AVPT×59 09:30 $14.77 → close $14.59 -10.62; BLFS×9 09:30 $38.61 → close $38.61 +0.00; CDNA×5 09:30 $70.89 → close $63.79 -35.50; COP×1 09:30 $128.19 → close $129.35 +1.16; CORT×3 09:30 $122.20 → close $119.57 -7.89; DVN×3 09:30 $47.48 → close $48.02 +1.64; ETON×6 09:30 $56.15 → close $54.77 -8.28; EXE×2 09:30 $86.38 → close $88.23 +3.70; GPRK×15 09:30 $11.32 → close $11.44 +1.80; ILMN×1 09:30 $301.58 → close $273.54 -28.04; IOT×21 09:30 $42.70 → close $41.74 -20.16; IT×4 09:30 $188.47 → close $184.92 -14.20; KSPI×9 09:30 $94.50 → close $93.16 -12.06; MTDR×3 09:30 $52.56 → close $52.91 +1.05; OBE×16 09:30 $10.44 → close $10.62 +2.88; RELY×39 09:30 $23.12 → close $23.20 +3.12; RRC×4 09:30 $38.71 → close $40.02 +5.24; SM×4 09:30 $34.90 → close $35.10 +0.80; VEEV×1 09:30 $285.34 → close $283.50 -1.84; WRBY×12 09:30 $26.89 → close $26.23 -7.92; SN×1 09:30 $183.47 → close $184.72 +1.25; TOST×6 09:30 $30.07 → close $30.25 +1.08; PDFS×3 09:30 $56.89 → close $54.44 -7.35 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $941.54 | ▼ 09:30 equity $9,130.85 vs yday $9,167.52 (-36.67) | 09:30 open · cash $941.54 (unchanged overnight, no fees) · equity $9,130.85 vs prior close $9,167.52 (-36.67) · 23 name(s) re-marked at the open (per-name table). AVPT×59 yday $14.59 → 09:30 $14.51 -4.72; BLFS×9 yday $38.61 → 09:30 $38.61 +0.00; CDNA×5 yday $63.79 → 09:30 $61.38 -12.04; COP×1 yday $129.35 → 09:30 $131.29 +1.94; CORT×3 yday $119.57 → 09:30 $118.60 -2.91; DVN×3 yday $48.02 → 09:30 $48.40 +1.14; ETON×6 yday $54.77 → 09:30 $53.84 -5.58; EXE×2 yday $88.23 → 09:30 $88.97 +1.48; GPRK×15 yday $11.44 → 09:30 $11.50 +0.90; ILMN×1 yday $273.54 → 09:30 $271.61 -1.93; IOT×21 yday $41.74 → 09:30 $41.60 -2.94; IT×4 yday $184.92 → 09:30 $186.51 +6.36; KSPI×9 yday $93.16 → 09:30 $92.70 -4.14; MTDR×3 yday $52.91 → 09:30 $53.44 +1.59; OBE×16 yday $10.62 → 09:30 $10.72 +1.60; PDFS×3 yday $54.44 → 09:30 $52.54 -5.70; RELY×39 yday $23.20 → 09:30 $22.96 -9.16; RRC×4 yday $40.02 → 09:30 $40.20 +0.72; SM×4 yday $35.10 → 09:30 $35.46 +1.44; SN×1 yday $184.72 → 09:30 $183.00 -1.72; TOST×6 yday $30.25 → 09:30 $30.13 -0.72; VEEV×1 yday $283.50 → 09:30 $284.23 +0.73; WRBY×12 yday $26.23 → 09:30 $25.98 -3.00 | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $941.54 | ▼ close $9,077.02 vs 09:30 $9,130.85 (session -53.84) | 16:00 close · cash $941.54 · equity $9,077.02 vs 09:30 $9,130.85 (-53.83; session marks -53.84) · 23 name(s) marked open→close (per-name table). AVPT×59 09:30 $14.51 → close $14.28 -13.57; BLFS×9 09:30 $38.61 → close $38.61 +0.00; CDNA×5 09:30 $61.38 → close $63.04 +8.29; COP×1 09:30 $131.29 → close $129.84 -1.45; CORT×3 09:30 $118.60 → close $119.60 +3.00; DVN×3 09:30 $48.40 → close $47.88 -1.56; ETON×6 09:30 $53.84 → close $54.88 +6.24; EXE×2 09:30 $88.97 → close $88.12 -1.70; GPRK×15 09:30 $11.50 → close $11.11 -5.85; ILMN×1 09:30 $271.61 → close $267.76 -3.85; IOT×21 09:30 $41.60 → close $40.51 -22.89; IT×4 09:30 $186.51 → close $185.77 -2.96; KSPI×9 09:30 $92.70 → close $91.87 -7.47; MTDR×3 09:30 $53.44 → close $52.85 -1.77; OBE×16 09:30 $10.72 → close $10.63 -1.44; PDFS×3 09:30 $52.54 → close $53.17 +1.89; RELY×39 09:30 $22.96 → close $22.93 -1.37; RRC×4 09:30 $40.20 → close $39.83 -1.48; SM×4 09:30 $35.46 → close $35.25 -0.84; SN×1 09:30 $183.00 → close $185.19 +2.19; TOST×6 09:30 $30.13 → close $30.55 +2.52; VEEV×1 09:30 $284.23 → close $282.50 -1.73; WRBY×12 09:30 $25.98 → close $25.31 -8.04 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $941.54 | ▲ 09:30 equity $9,077.02 vs yday $9,077.02 (+0.00) | 09:30 open · cash $941.54 (unchanged overnight, no fees) · equity $9,077.02 vs prior close $9,077.02 (+0.00) · 23 name(s) re-marked at the open (per-name table). AVPT×59 yday $14.28 → 09:30 $14.28 +0.00; BLFS×9 yday $38.61 → 09:30 $38.61 +0.00; CDNA×5 yday $63.04 → 09:30 $63.04 +0.00; COP×1 yday $129.84 → 09:30 $129.84 +0.00; CORT×3 yday $119.60 → 09:30 $119.60 +0.00; DVN×3 yday $47.88 → 09:30 $47.88 +0.00; ETON×6 yday $54.88 → 09:30 $54.88 +0.00; EXE×2 yday $88.12 → 09:30 $88.12 +0.00; GPRK×15 yday $11.11 → 09:30 $11.11 +0.00; ILMN×1 yday $267.76 → 09:30 $267.76 +0.00; IOT×21 yday $40.51 → 09:30 $40.51 +0.00; IT×4 yday $185.77 → 09:30 $185.77 +0.00; KSPI×9 yday $91.87 → 09:30 $91.87 +0.00; MTDR×3 yday $52.85 → 09:30 $52.85 +0.00; OBE×16 yday $10.63 → 09:30 $10.63 +0.00; PDFS×3 yday $53.17 → 09:30 $53.17 +0.00; RELY×39 yday $22.93 → 09:30 $22.93 +0.00; RRC×4 yday $39.83 → 09:30 $39.83 +0.00; SM×4 yday $35.25 → 09:30 $35.25 +0.00; SN×1 yday $185.19 → 09:30 $185.19 +0.00; TOST×6 yday $30.55 → 09:30 $30.55 +0.00; VEEV×1 yday $282.50 → 09:30 $282.50 +0.00; WRBY×12 yday $25.31 → 09:30 $25.31 +0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $941.54 | ▲ close $9,077.02 vs 09:30 $9,077.02 (session +0.00) | 16:00 close · cash $941.54 · equity $9,077.02 vs 09:30 $9,077.02 (+0.00; session marks +0.00) · 23 name(s) marked open→close (per-name table). AVPT×59 09:30 $14.28 → close $14.28 +0.00; BLFS×9 09:30 $38.61 → close $38.61 +0.00; CDNA×5 09:30 $63.04 → close $63.04 +0.00; COP×1 09:30 $129.84 → close $129.84 +0.00; CORT×3 09:30 $119.60 → close $119.60 +0.00; DVN×3 09:30 $47.88 → close $47.88 +0.00; ETON×6 09:30 $54.88 → close $54.88 +0.00; EXE×2 09:30 $88.12 → close $88.12 +0.00; GPRK×15 09:30 $11.11 → close $11.11 +0.00; ILMN×1 09:30 $267.76 → close $267.76 +0.00; IOT×21 09:30 $40.51 → close $40.51 +0.00; IT×4 09:30 $185.77 → close $185.77 +0.00; KSPI×9 09:30 $91.87 → close $91.87 +0.00; MTDR×3 09:30 $52.85 → close $52.85 +0.00; OBE×16 09:30 $10.63 → close $10.63 +0.00; PDFS×3 09:30 $53.17 → close $53.17 +0.00; RELY×39 09:30 $22.93 → close $22.93 +0.00; RRC×4 09:30 $39.83 → close $39.83 +0.00; SM×4 09:30 $35.25 → close $35.25 +0.00; SN×1 09:30 $185.19 → close $185.19 +0.00; TOST×6 09:30 $30.55 → close $30.55 +0.00; VEEV×1 09:30 $282.50 → close $282.50 +0.00; WRBY×12 09:30 $25.31 → close $25.31 +0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $941.54 | ▲ 09:30 equity $9,208.96 vs yday $9,077.02 (+131.94) | 09:30 open · cash $941.54 (unchanged overnight, no fees) · equity $9,208.96 vs prior close $9,077.02 (+131.94) · 23 name(s) re-marked at the open (per-name table). AVPT×59 yday $14.28 → 09:30 $14.44 +9.44; BLFS×9 yday $38.61 → 09:30 $38.61 +0.00; CDNA×5 yday $63.04 → 09:30 $62.21 -4.15; COP×1 yday $129.84 → 09:30 $133.85 +4.01; CORT×3 yday $119.60 → 09:30 $118.00 -4.80; DVN×3 yday $47.88 → 09:30 $48.83 +2.85; ETON×6 yday $54.88 → 09:30 $54.86 -0.12; EXE×2 yday $88.12 → 09:30 $88.94 +1.64; GPRK×15 yday $11.11 → 09:30 $11.40 +4.35; ILMN×1 yday $267.76 → 09:30 $268.21 +0.45; IOT×21 yday $40.51 → 09:30 $41.71 +25.20; IT×4 yday $185.77 → 09:30 $194.05 +33.12; KSPI×9 yday $91.87 → 09:30 $93.64 +15.93; MTDR×3 yday $52.85 → 09:30 $54.40 +4.65; OBE×16 yday $10.63 → 09:30 $11.05 +6.72; PDFS×3 yday $53.17 → 09:30 $53.73 +1.68; RELY×39 yday $22.93 → 09:30 $23.50 +22.23; RRC×4 yday $39.83 → 09:30 $40.62 +3.16; SM×4 yday $35.25 → 09:30 $36.92 +6.68; SN×1 yday $185.19 → 09:30 $185.20 +0.01; TOST×6 yday $30.55 → 09:30 $30.61 +0.36; VEEV×1 yday $282.50 → 09:30 $285.71 +3.21; WRBY×12 yday $25.31 → 09:30 $24.92 -4.68 | — |
| 2026-10-09 09:30 ET | **SELL** | `AVPT` | 59 | $14.44 | $2.19 | $+5.68 | $1,791.31 | ▲ +5.68 after sell → book $9,206.77; vs 09:30 mark -2.19 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `CDNA` | 5 | $62.21 | $2.02 | $-24.63 | $2,100.34 | ▼ -24.63 after sell → book $9,204.75; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SELL** | `COP` | 1 | $133.85 | $1.36 | $+4.22 | $2,232.83 | ▲ +4.22 after sell → book $9,203.39; vs 09:30 mark -1.36 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `CORT` | 3 | $118.00 | $2.02 | $+6.84 | $2,584.81 | ▲ +6.84 after sell → book $9,201.37; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `DVN` | 3 | $48.83 | $1.49 | $+0.64 | $2,729.80 | ▲ +0.64 after sell → book $9,199.87; vs 09:30 mark -1.50 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `ETON` | 6 | $54.86 | $2.03 | $+10.60 | $3,056.94 | ▲ +10.60 after sell → book $9,197.85; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SELL** | `EXE` | 2 | $88.94 | $1.80 | $+4.98 | $3,233.01 | ▲ +4.98 after sell → book $9,196.04; vs 09:30 mark -1.81 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `GPRK` | 15 | $11.40 | $1.77 | $+4.50 | $3,402.24 | ▲ +4.50 after sell → book $9,194.27; vs 09:30 mark -1.77 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `ILMN` | 1 | $268.21 | $2.01 | $-1.71 | $3,668.43 | ▼ -1.71 after sell → book $9,192.25; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IOT` | 21 | $41.71 | $2.07 | $+52.99 | $4,542.27 | ▲ +52.99 after sell → book $9,190.18; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IT` | 4 | $194.05 | $2.02 | $-12.58 | $5,316.45 | ▼ -12.58 after sell → book $9,188.16; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `KSPI` | 9 | $93.64 | $2.04 | $+2.34 | $6,157.17 | ▲ +2.34 after sell → book $9,186.12; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `MTDR` | 3 | $54.40 | $1.66 | $-0.12 | $6,318.71 | ▼ -0.12 after sell → book $9,184.46; vs 09:30 mark -1.66 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `OBE` | 16 | $11.05 | $1.84 | $+9.11 | $6,493.67 | ▲ +9.11 after sell → book $9,182.62; vs 09:30 mark -1.84 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `PDFS` | 3 | $53.73 | $1.64 | $-12.84 | $6,653.22 | ▼ -12.84 after sell → book $9,180.98; vs 09:30 mark -1.64 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `RELY` | 39 | $23.50 | $2.13 | $+82.35 | $7,567.60 | ▲ +82.35 after sell → book $9,178.86; vs 09:30 mark -2.12 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `RRC` | 4 | $40.62 | $1.66 | $+6.89 | $7,728.42 | ▲ +6.89 after sell → book $9,177.20; vs 09:30 mark -1.66 | exit unpriced hold on first bar after 4 sess | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SELL** | `SM` | 4 | $36.92 | $1.51 | $+3.67 | $7,874.59 | ▲ +3.67 after sell → book $9,175.69; vs 09:30 mark -1.51 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `SN` | 1 | $185.20 | $1.88 | $-1.98 | $8,057.92 | ▼ -1.98 after sell → book $9,173.82; vs 09:30 mark -1.87 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `TOST` | 6 | $30.61 | $1.87 | $-0.46 | $8,239.70 | ▼ -0.46 after sell → book $9,171.94; vs 09:30 mark -1.88 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `VEEV` | 1 | $285.71 | $2.01 | $-1.40 | $8,523.40 | ▼ -1.40 after sell → book $9,169.93; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `WRBY` | 12 | $24.92 | $2.05 | $-36.59 | $8,820.39 | ▼ -36.59 after sell → book $9,167.88; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **BUY** | `PACS` | 100 | $43.75 | $2.29 | — | $4,443.10 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.6; leftover $4410.20 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,443.10 | ▼ close $9,120.59 vs 09:30 $9,208.96 (session -45.00) | 16:00 close · cash $4,443.10 · equity $9,120.59 vs 09:30 $9,208.96 (-88.37; session marks -45.00) · 2 name(s) marked open→close (per-name table). BLFS×9 09:30 $38.61 → close $38.61 +0.00; PACS×100 09:30 $43.75 → close $43.30 -45.00 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 318.86 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 318.86 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `VST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NRG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `SLG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `VST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NRG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `SLG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ELF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ELF` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HCA` | cash | leftover split 389.86 < 1 share @ 426.97 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `HCA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `HCA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `HCA` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `TWST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DELL` | cash | leftover split 460.55 < 1 share @ 593.15 |
| 2026-09-21 | `IQV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `HUM` | cash | leftover split 335.13 < 1 share @ 386.20 |
| 2026-09-22 | `IQV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ILMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `TWST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `AMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `ILMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `TWST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `AMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| `RBRK` | 4 | 2026-09-18 @ $108.55 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $460.55 |
| `GNRC` | 2 | 2026-09-18 @ $209.52 | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $460.55 |
| `VICR` | 2 | 2026-09-18 @ $219.62 | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $460.55 |
| `ECO` | 5 | 2026-09-18 @ $85.00 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $460.55 |
| `FIVN` | 13 | 2026-09-18 @ $34.44 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $460.55 |
| `A` | 2 | 2026-09-21 @ $157.87 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $335.13 |
| `DXCM` | 3 | 2026-09-21 @ $88.83 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $335.13 |
| `MGTX` | 24 | 2026-09-21 @ $13.47 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $335.13 |
| `CYPH` | 83 | 2026-09-21 @ $4.00 | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $335.13 |
| `USFD` | 1 | 2026-09-22 @ $93.97 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $175.46 |
| `HALO` | 4 | 2026-09-23 @ $116.85 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $512.20 |
| `ARQT` | 18 | 2026-09-23 @ $27.79 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $512.20 |
| `ADMA` | 52 | 2026-09-23 @ $9.81 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $512.20 |
| `FTRE` | 25 | 2026-09-23 @ $20.25 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $512.20 |
| `OMER` | 24 | 2026-09-23 @ $20.65 | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $512.20 |
