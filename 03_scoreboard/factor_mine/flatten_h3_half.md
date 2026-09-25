# Factor mine action — `flatten_h3_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **-11.53%** ($8,847) · signal-only (no cash/fees) was -10.41%. Starts YES **0/30**. Fills 176 · skips 239 · realized $-557.10.

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
- Only spend half of leftover cash; the rest stays cash.
- Skip a name if the slice cannot buy 1 share after fees.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,196.24.

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
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 10 | $60.00 | $2.04 | $-2.06 | $2,363.46 | ▼ -2.06 after sell → book $10,143.50; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 13 | $43.56 | $2.05 | $-35.54 | $2,927.69 | ▼ -35.54 after sell → book $10,141.45; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 12 | $51.77 | $2.05 | $+9.69 | $3,546.88 | ▲ +9.69 after sell → book $10,139.40; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 12 | $49.28 | $2.05 | $-9.11 | $4,136.20 | ▼ -9.11 after sell → book $10,137.36; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 53 | $12.66 | $2.17 | $+46.56 | $4,805.01 | ▲ +46.56 after sell → book $10,135.19; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 21 | $27.85 | $2.07 | $-43.82 | $5,387.78 | ▼ -43.82 after sell → book $10,133.11; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 771 | $1.14 | $10.08 | $+235.79 | $6,256.64 | ▲ +235.79 after sell → book $10,123.03; vs 09:30 mark -10.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 26 | $22.16 | $2.09 | $-34.58 | $6,830.71 | ▼ -34.58 after sell → book $10,120.94; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,830.71 | ▼ close $10,078.11 vs 09:30 $10,145.54 (session -42.83) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,830.71 | ▲ 09:30 equity $10,106.36 vs yday $10,078.11 (+28.25) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 2 | $140.74 | $2.02 | $-16.33 | $7,110.18 | ▼ -16.33 after sell → book $10,104.35; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 2 | $116.20 | $2.02 | $-11.61 | $7,340.56 | ▼ -11.61 after sell → book $10,102.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 5 | $57.50 | $2.02 | $-4.58 | $7,626.04 | ▼ -4.58 after sell → book $10,100.31; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 35 | $8.91 | $2.12 | $-7.71 | $7,935.77 | ▼ -7.71 after sell → book $10,098.19; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 340 | $0.88 | $4.08 | $-27.66 | $8,230.89 | ▼ -27.66 after sell → book $10,094.11; vs 09:30 mark -4.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 212 | $1.42 | $2.78 | $-22.47 | $8,529.15 | ▼ -22.47 after sell → book $10,091.33; vs 09:30 mark -2.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,529.15 | ▲ close $10,103.71 vs 09:30 $10,106.36 (session +12.38) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,529.15 | ▼ 09:30 equity $10,102.55 vs yday $10,103.71 (-1.16) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 4 | $49.02 | $1.99 | $+7.51 | $8,723.24 | ▲ +7.51 after sell → book $10,100.56; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $8,873.15 | ▲ +5.71 after sell → book $10,099.02; vs 09:30 mark -1.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,084.65 | ▲ +6.80 after sell → book $10,097.01; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 51 | $3.92 | $2.16 | $-10.94 | $9,282.41 | ▼ -10.94 after sell → book $10,094.85; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 24 | $8.35 | $2.08 | $-6.78 | $9,480.72 | ▼ -6.78 after sell → book $10,092.76; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ELF` | 2 | $98.15 | $1.99 | $+11.41 | $9,675.03 | ▲ +11.41 after sell → book $10,090.77; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 63 | $3.20 | $2.20 | $-6.90 | $9,874.44 | ▼ -6.90 after sell → book $10,088.58; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 43 | $4.98 | $2.14 | $+3.05 | $10,086.44 | ▲ +3.05 after sell → book $10,086.44; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,467.86 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,919.79 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,298.21 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 109 | $5.77 | $2.32 | — | $7,666.96 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 32 | $19.63 | $2.09 | — | $7,036.72 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,412.43 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 360 | $1.75 | $4.64 | — | $5,777.79 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,197.63 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $630.40 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,197.63 | ▲ close $10,182.57 vs 09:30 $10,102.55 (session +115.40) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,197.63 | ▲ 09:30 equity $10,315.69 vs yday $10,182.57 (+133.12) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $4,956.77 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 18 | $17.20 | $2.04 | — | $4,645.13 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,426.83 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 29 | $11.13 | $2.08 | — | $4,101.99 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 131 | $2.47 | $2.38 | — | $3,776.03 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 168 | $1.93 | $2.49 | — | $3,449.30 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,148.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 246 | $1.32 | $3.17 | — | $2,820.80 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $324.85 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,820.80 | ▲ close $10,359.67 vs 09:30 $10,315.69 (session +62.15) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,820.80 | ▲ 09:30 equity $10,504.11 vs yday $10,359.67 (+144.44) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,820.80 | ▼ close $10,462.13 vs 09:30 $10,504.11 (session -41.98) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,820.80 | ▼ 09:30 equity $10,350.48 vs yday $10,462.13 (-111.65) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 30 | $20.32 | $2.10 | $-11.08 | $3,428.30 | ▼ -11.08 after sell → book $10,348.38; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 6 | $95.86 | $2.03 | $+25.06 | $4,001.43 | ▲ +25.06 after sell → book $10,346.35; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 30 | $20.47 | $2.10 | $-9.58 | $4,613.43 | ▼ -9.58 after sell → book $10,344.25; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 109 | $5.53 | $2.35 | $-30.82 | $5,213.86 | ▼ -30.82 after sell → book $10,341.91; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 32 | $21.21 | $2.11 | $+46.37 | $5,890.47 | ▲ +46.37 after sell → book $10,339.80; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 21 | $32.32 | $2.07 | $+52.36 | $6,567.12 | ▲ +52.36 after sell → book $10,337.73; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 360 | $1.90 | $4.71 | $+44.64 | $7,246.41 | ▲ +44.64 after sell → book $10,333.02; vs 09:30 mark -4.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 4 | $156.51 | $2.02 | $+43.86 | $7,870.42 | ▲ +43.86 after sell → book $10,330.99; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 27 | $23.77 | $2.07 | — | $7,226.56 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $655.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 59 | $10.98 | $2.17 | — | $6,576.58 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $655.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 10 | $61.19 | $2.02 | — | $5,962.66 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $655.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 78 | $8.35 | $2.22 | — | $5,309.13 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $655.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 132 | $4.94 | $2.39 | — | $4,654.67 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $655.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $4,225.70 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $655.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,225.70 | ▲ close $10,475.10 vs 09:30 $10,350.48 (session +156.97) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,225.70 | ▼ 09:30 equity $10,447.61 vs yday $10,475.10 (-27.49) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 2 | $119.80 | $2.02 | $-3.27 | $4,463.29 | ▼ -3.27 after sell → book $10,445.60; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 18 | $16.60 | $2.06 | $-14.91 | $4,760.02 | ▼ -14.91 after sell → book $10,443.53; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AEM` | 1 | $219.50 | $2.01 | $-0.81 | $4,977.51 | ▼ -0.81 after sell → book $10,441.52; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 29 | $15.35 | $2.10 | $+118.21 | $5,420.56 | ▲ +118.21 after sell → book $10,439.42; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 131 | $2.41 | $2.41 | $-12.66 | $5,733.86 | ▼ -12.66 after sell → book $10,437.01; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 168 | $2.03 | $2.53 | $+11.77 | $6,072.37 | ▲ +11.77 after sell → book $10,434.48; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRSP` | 5 | $60.18 | $2.02 | $-1.73 | $6,371.24 | ▼ -1.73 after sell → book $10,432.45; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 246 | $1.60 | $3.22 | $+62.48 | $6,761.62 | ▲ +62.48 after sell → book $10,429.23; vs 09:30 mark -3.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,761.62 | ▼ close $10,414.23 vs 09:30 $10,447.61 (session -15.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,761.62 | ▼ 09:30 equity $10,413.86 vs yday $10,414.23 (-0.37) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 27 | $41.44 | $2.07 | — | $5,640.67 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $1126.94 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 78 | $14.42 | $2.22 | — | $4,513.68 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $1126.94 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 433 | $2.60 | $5.59 | — | $3,382.30 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $1126.94 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,382.30 | ▼ close $10,403.04 vs 09:30 $10,413.86 (session -0.94) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,382.30 | ▲ 09:30 equity $10,432.94 vs yday $10,403.04 (+29.90) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 59 | $10.97 | $2.19 | $-4.94 | $4,027.34 | ▼ -4.94 after sell → book $10,430.75; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 10 | $60.52 | $2.04 | $-10.76 | $4,630.50 | ▼ -10.76 after sell → book $10,428.71; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 78 | $8.28 | $2.25 | $-9.93 | $5,274.09 | ▼ -9.93 after sell → book $10,426.46; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 132 | $4.95 | $2.42 | $-3.48 | $5,925.07 | ▼ -3.48 after sell → book $10,424.04; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $423.76 | $2.01 | $-7.22 | $6,346.82 | ▼ -7.22 after sell → book $10,422.03; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,346.82 | ▼ close $10,322.21 vs 09:30 $10,432.94 (session -99.82) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,346.82 | ▲ 09:30 equity $10,371.44 vs yday $10,322.21 (+49.23) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 27 | $23.68 | $2.09 | $-6.59 | $6,984.09 | ▼ -6.59 after sell → book $10,369.35; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,984.09 | ▲ close $10,381.38 vs 09:30 $10,371.44 (session +12.03) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,984.09 | ▲ 09:30 equity $10,506.54 vs yday $10,381.38 (+125.16) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 27 | $41.94 | $2.09 | $+9.34 | $8,114.38 | ▲ +9.34 after sell → book $10,504.45; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 78 | $15.82 | $2.25 | $+104.73 | $9,346.09 | ▲ +104.73 after sell → book $10,502.20; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 433 | $2.67 | $5.67 | $+19.06 | $10,496.53 | ▲ +19.06 after sell → book $10,496.53; vs 09:30 mark -5.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,496.53 | ▲ close $10,496.53 vs 09:30 $10,506.54 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,496.53 | ▲ 09:30 equity $10,496.53 vs yday $10,496.53 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,496.53 | ▲ close $10,496.53 vs 09:30 $10,496.53 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,496.53 | ▲ 09:30 equity $10,496.53 vs yday $10,496.53 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 19 | $52.88 | $2.05 | — | $9,489.77 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $1049.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 24 | $42.93 | $2.06 | — | $8,457.39 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $1049.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 289 | $3.63 | $3.73 | — | $7,404.59 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $1049.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 130 | $8.03 | $2.38 | — | $6,358.31 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $1049.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $132.45 | $2.01 | — | $5,429.15 | — | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $1049.65 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,429.15 | ▼ close $10,388.06 vs 09:30 $10,496.53 (session -96.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,429.15 | ▼ 09:30 equity $10,352.17 vs yday $10,388.06 (-35.89) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 179 | $2.52 | $2.53 | — | $4,975.54 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $452.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 67 | $6.71 | $2.19 | — | $4,523.78 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $452.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 238 | $1.90 | $3.07 | — | $4,068.51 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $452.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 94 | $4.78 | $2.27 | — | $3,616.92 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $452.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 284 | $1.59 | $3.66 | — | $3,161.69 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $452.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 40 | $11.31 | $2.11 | — | $2,707.18 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $452.43 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,707.18 | ▲ close $10,371.61 vs 09:30 $10,352.17 (session +35.28) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,707.18 | ▼ 09:30 equity $10,366.77 vs yday $10,371.61 (-4.84) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,707.18 | ▼ close $10,252.32 vs 09:30 $10,366.77 (session -114.45) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,707.18 | ▼ 09:30 equity $10,212.44 vs yday $10,252.32 (-39.88) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 19 | $53.16 | $2.07 | $+1.21 | $3,715.16 | ▲ +1.21 after sell → book $10,210.38; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 24 | $42.01 | $2.08 | $-26.22 | $4,721.31 | ▼ -26.22 after sell → book $10,208.29; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 289 | $3.28 | $3.79 | $-108.66 | $5,665.45 | ▼ -108.66 after sell → book $10,204.51; vs 09:30 mark -3.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 130 | $8.01 | $2.41 | $-7.39 | $6,704.34 | ▼ -7.39 after sell → book $10,202.10; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 7 | $125.77 | $2.03 | $-50.80 | $7,582.69 | ▼ -50.80 after sell → book $10,200.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,582.69 | ▼ close $10,088.02 vs 09:30 $10,212.44 (session -112.04) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,582.69 | ▼ 09:30 equity $10,050.97 vs yday $10,088.02 (-37.05) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 179 | $2.22 | $2.57 | $-58.79 | $7,977.51 | ▼ -58.79 after sell → book $10,048.41; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 67 | $6.11 | $2.21 | $-44.60 | $8,384.67 | ▼ -44.60 after sell → book $10,046.19; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 238 | $1.83 | $3.12 | $-22.85 | $8,817.09 | ▼ -22.85 after sell → book $10,043.07; vs 09:30 mark -3.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 94 | $3.92 | $2.30 | $-85.22 | $9,183.46 | ▼ -85.22 after sell → book $10,040.78; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 284 | $1.53 | $3.72 | $-24.42 | $9,614.26 | ▼ -24.42 after sell → book $10,037.06; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 40 | $10.57 | $2.13 | $-33.84 | $10,034.93 | ▼ -33.84 after sell → book $10,034.93; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,034.93 | ▲ close $10,034.93 vs 09:30 $10,050.97 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,034.93 | ▲ 09:30 equity $10,034.93 vs yday $10,034.93 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 51 | $16.28 | $2.14 | — | $9,202.50 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $836.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 306 | $2.73 | $3.95 | — | $8,363.18 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; leftover $836.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 4 | $206.84 | $2.00 | — | $7,533.81 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; leftover $836.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 5 | $164.43 | $2.00 | — | $6,709.66 | — | deploy half leftover; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; leftover $836.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 5 | $157.78 | $2.00 | — | $5,918.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; leftover $836.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 14 | $56.09 | $2.03 | — | $5,131.46 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; leftover $836.24 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,131.46 | ▼ close $10,002.12 vs 09:30 $10,034.93 (session -18.67) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,131.46 | ▼ 09:30 equity $9,806.34 vs yday $10,002.12 (-195.78) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,131.46 | ▼ close $9,747.70 vs 09:30 $9,806.34 (session -58.64) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,131.46 | ▲ 09:30 equity $9,781.85 vs yday $9,747.70 (+34.15) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,131.46 | ▼ close $9,685.12 vs 09:30 $9,781.85 (session -96.73) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,131.46 | ▲ 09:30 equity $9,724.46 vs yday $9,685.12 (+39.34) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 51 | $16.16 | $2.16 | $-10.43 | $5,953.46 | ▼ -10.43 after sell → book $9,722.30; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 306 | $2.72 | $4.01 | $-11.02 | $6,781.77 | ▼ -11.02 after sell → book $9,718.29; vs 09:30 mark -4.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 4 | $194.84 | $2.02 | $-52.02 | $7,559.11 | ▼ -52.02 after sell → book $9,716.27; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 5 | $140.03 | $2.02 | $-126.03 | $8,257.23 | ▼ -126.03 after sell → book $9,714.24; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 5 | $147.79 | $2.02 | $-53.98 | $8,994.16 | ▼ -53.98 after sell → book $9,712.22; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 14 | $51.29 | $2.05 | $-71.28 | $9,710.17 | ▼ -71.28 after sell → book $9,710.17; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,624.60 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; leftover $1213.77 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,465.77 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; leftover $1213.77 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 84 | $14.31 | $2.24 | — | $6,261.49 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; leftover $1213.77 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $5,056.22 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; leftover $1213.77 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,056.22 | ▼ close $9,657.67 vs 09:30 $9,724.46 (session -44.13) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,056.22 | ▲ 09:30 equity $9,709.25 vs yday $9,657.67 (+51.58) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $4,820.38 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; leftover $421.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 2 | $151.43 | $2.00 | — | $4,515.52 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; leftover $421.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 2 | $147.61 | $2.00 | — | $4,218.30 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; leftover $421.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 41 | $10.25 | $2.11 | — | $3,795.94 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; leftover $421.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 55 | $7.59 | $2.15 | — | $3,376.34 | — | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; leftover $421.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 12 | $34.93 | $2.03 | — | $2,955.15 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; leftover $421.35 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,955.15 | ▲ close $9,697.17 vs 09:30 $9,709.25 (session +0.20) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,955.15 | ▲ 09:30 equity $9,703.81 vs yday $9,697.17 (+6.64) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 2 | $108.55 | $2.00 | — | $2,736.05 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; leftover $246.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 1 | $209.52 | $1.99 | — | $2,524.54 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; leftover $246.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 1 | $219.62 | $1.99 | — | $2,302.93 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; leftover $246.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 2 | $85.00 | $1.71 | — | $2,131.22 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; leftover $246.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 7 | $34.44 | $2.01 | — | $1,888.13 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; leftover $246.26 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,888.13 | ▼ close $9,613.40 vs 09:30 $9,703.81 (session -80.71) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,888.13 | ▲ 09:30 equity $9,648.81 vs yday $9,613.40 (+35.41) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $2,953.15 | ▼ -20.54 after sell → book $9,646.79; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $4,095.14 | ▼ -16.84 after sell → book $9,644.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 84 | $13.65 | $2.27 | $-59.95 | $5,239.48 | ▼ -59.95 after sell → book $9,642.47; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 33 | $36.70 | $2.11 | $+3.72 | $6,448.47 | ▲ +3.72 after sell → book $9,640.36; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 4 | $157.87 | $2.00 | — | $5,814.99 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; leftover $644.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $5,426.79 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; leftover $644.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 7 | $88.83 | $2.01 | — | $4,802.97 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; leftover $644.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 47 | $13.47 | $2.13 | — | $4,167.75 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; leftover $644.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 161 | $4.00 | $2.47 | — | $3,521.28 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; leftover $644.85 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,521.28 | ▼ close $9,522.91 vs 09:30 $9,648.81 (session -106.84) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,521.28 | ▲ 09:30 equity $9,540.21 vs yday $9,522.91 (+17.30) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 41 | $10.18 | $2.13 | $-7.12 | $3,936.53 | ▼ -7.12 after sell → book $9,538.08; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 3 | $93.97 | $2.00 | — | $3,652.62 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $328.04 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,652.62 | ▲ close $9,571.38 vs 09:30 $9,540.21 (session +35.30) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,652.62 | ▲ 09:30 equity $9,628.57 vs yday $9,571.38 (+57.19) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ILMN` | 1 | $248.79 | $2.01 | $+10.93 | $3,899.39 | ▲ +10.93 after sell → book $9,626.56; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TWST` | 2 | $164.35 | $2.02 | $+21.83 | $4,226.08 | ▲ +21.83 after sell → book $9,624.54; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 2 | $142.40 | $2.02 | $-14.43 | $4,508.86 | ▼ -14.43 after sell → book $9,622.53; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 12 | $34.78 | $2.05 | $-5.87 | $4,924.18 | ▼ -5.87 after sell → book $9,620.48; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RBRK` | 2 | $112.46 | $2.02 | $+3.81 | $5,147.08 | ▲ +3.81 after sell → book $9,618.46; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GNRC` | 1 | $204.39 | $2.01 | $-9.14 | $5,349.46 | ▼ -9.14 after sell → book $9,616.45; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 1 | $266.50 | $2.01 | $+42.87 | $5,613.94 | ▲ +42.87 after sell → book $9,614.44; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ECO` | 2 | $77.55 | $1.58 | $-18.18 | $5,767.47 | ▼ -18.18 after sell → book $9,612.86; vs 09:30 mark -1.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FIVN` | 7 | $38.91 | $2.03 | $+27.21 | $6,037.77 | ▲ +27.21 after sell → book $9,610.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 5 | $116.85 | $2.00 | — | $5,451.52 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $603.78 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 21 | $27.79 | $2.05 | — | $4,865.87 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $603.78 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 61 | $9.81 | $2.17 | — | $4,265.29 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $603.78 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 29 | $20.25 | $2.08 | — | $3,675.96 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $603.78 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 29 | $20.65 | $2.08 | — | $3,075.04 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $603.78 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,075.04 | ▼ close $9,425.08 vs 09:30 $9,628.57 (session -175.37) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,075.04 | ▼ 09:30 equity $9,351.04 vs yday $9,425.08 (-74.04) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 55 | $7.38 | $2.17 | $-15.88 | $3,478.76 | ▼ -15.88 after sell → book $9,348.87; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 4 | $163.95 | $2.02 | $+20.30 | $4,132.54 | ▲ +20.30 after sell → book $9,346.84; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 1 | $374.54 | $2.01 | $-15.67 | $4,505.07 | ▼ -15.67 after sell → book $9,344.83; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 7 | $87.67 | $2.03 | $-12.13 | $5,116.76 | ▼ -12.13 after sell → book $9,342.80; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 47 | $11.42 | $2.15 | $-100.63 | $5,651.35 | ▼ -100.63 after sell → book $9,340.65; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 161 | $3.40 | $2.51 | $-101.58 | $6,196.24 | ▼ -101.58 after sell → book $9,338.14; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,196.24 | ▲ close $9,350.54 vs 09:30 $9,351.04 (session +12.40) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,550.75 | ▲ 09:30 equity $8,867.79 vs yday $8,859.83 (+7.96) | 09:30 open · cash $4,550.75 (unchanged overnight, no fees) · equity $8,867.79 vs prior close $8,859.83 (+7.96) · 12 name(s) re-marked at the open (per-name table). ADMA×34 yday $9.52 → 09:30 $9.52 +0.00; ARQT×12 yday $26.27 → 09:30 $26.27 +0.00; DLO×31 yday $13.88 → 09:30 $13.88 +0.00; EL×4 yday $95.37 → 09:30 $95.37 +0.00; FTRE×16 yday $20.02 → 09:30 $20.02 +0.00; HALO×2 yday $115.22 → 09:30 $115.36 +0.28; MKC×9 yday $47.82 → 09:30 $47.82 +0.00; OMER×16 yday $20.13 → 09:30 $20.61 +7.68; PACS×10 yday $41.46 → 09:30 $41.46 +0.00; PGEN×42 yday $7.70 → 09:30 $7.70 +0.00; TDC×15 yday $29.46 → 09:30 $29.46 +0.00; USFD×4 yday $93.82 → 09:30 $93.82 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 19 | $38.51 | $2.05 | — | $3,817.01 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; leftover $758.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 99 | $7.65 | $2.29 | — | $3,057.38 | — | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; leftover $758.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,057.38 | ▼ close $8,846.73 vs 09:30 $8,867.79 (session -16.73) | 16:00 close · cash $3,057.38 · equity $8,846.73 vs 09:30 $8,867.79 (-21.06; session marks -16.73) · 14 name(s) marked open→close (per-name table). ADMA×34 09:30 $9.52 → close $9.52 +0.00; ARQT×12 09:30 $26.27 → close $26.27 +0.00; DLO×31 09:30 $13.88 → close $13.88 +0.00; EL×4 09:30 $95.37 → close $95.37 +0.00; FTRE×16 09:30 $20.02 → close $20.02 +0.00; HALO×2 09:30 $115.36 → close $113.90 -2.92; MKC×9 09:30 $47.82 → close $47.82 -0.00; OMER×16 09:30 $20.61 → close $20.08 -8.48; PACS×10 09:30 $41.46 → close $41.46 -0.00; PGEN×42 09:30 $7.70 → close $7.70 -0.00; TDC×15 09:30 $29.46 → close $29.46 -0.00; USFD×4 09:30 $93.82 → close $93.82 -0.00; BLFS×19 09:30 $38.51 → close $38.49 -0.38; MRVI×99 09:30 $7.65 → close $7.60 -4.95 | — |

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
| 2026-08-14 | `TLN` | cash | leftover split 318.86 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 318.86 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DELL` | cash | leftover split 246.26 < 1 share @ 593.15 |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ILMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TWST` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `USFD` | 3 | 2026-09-22 @ $93.97 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-0.6; leftover $328.04 |
| `HALO` | 5 | 2026-09-23 @ $116.85 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; leftover $603.78 |
| `ARQT` | 21 | 2026-09-23 @ $27.79 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; leftover $603.78 |
| `ADMA` | 61 | 2026-09-23 @ $9.81 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; leftover $603.78 |
| `FTRE` | 29 | 2026-09-23 @ $20.25 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; leftover $603.78 |
| `OMER` | 29 | 2026-09-23 @ $20.65 | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; leftover $603.78 |
