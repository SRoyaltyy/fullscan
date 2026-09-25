# Factor mine action — `union_h3_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **-10.82%** ($8,918) · signal-only (no cash/fees) was -4.92%. Starts YES **0/30**. Fills 224 · skips 313 · realized $-594.04.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
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

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,193.63.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | — | deploy half leftover; list flatten; ⚪; ret5=+12.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | — | deploy half leftover; list flatten; ⚪; ret5=+6.2; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | — | deploy half leftover; list flatten; ⚪; ret5=+13.2; leftover $625.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | — | deploy half leftover; list flatten; ⚪; ret5=+19.7; leftover $625.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,101.72 | ▲ close $10,071.15 vs 09:30 $10,000.00 (session +94.08) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,101.72 | ▲ 09:30 equity $10,084.41 vs yday $10,071.15 (+13.26) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $4,805.93 | — | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $4,563.93 | — | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $4,273.88 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+5.7; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $3,956.43 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 340 | $0.94 | $4.21 | — | $3,633.64 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $318.86 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 212 | $1.50 | $2.73 | — | $3,312.91 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $318.86 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,312.91 | ▲ close $10,212.64 vs 09:30 $10,084.41 (session +143.26) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,312.91 | ▼ 09:30 equity $10,196.68 vs yday $10,212.64 (-15.96) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $3,126.33 | — | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $2,982.13 | — | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,777.44 | — | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $2,568.74 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $2,363.64 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $2,180.75 | — | deploy half leftover; list flatten; ret5=-7.2; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $1,974.45 | — | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $207.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 43 | $4.81 | $2.12 | — | $1,765.50 | — | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $207.06 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,467.86 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,919.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,298.21 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 109 | $5.77 | $2.32 | — | $7,666.96 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 32 | $19.63 | $2.09 | — | $7,036.72 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,412.43 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 360 | $1.75 | $4.64 | — | $5,777.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $630.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,197.63 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $630.40 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,197.63 | ▲ close $10,182.57 vs 09:30 $10,102.55 (session +115.40) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,197.63 | ▲ 09:30 equity $10,315.69 vs yday $10,182.57 (+133.12) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $4,956.77 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 18 | $17.20 | $2.04 | — | $4,645.13 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,426.83 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 29 | $11.13 | $2.08 | — | $4,101.99 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 131 | $2.47 | $2.38 | — | $3,776.03 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 168 | $1.93 | $2.49 | — | $3,449.30 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,148.69 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $324.85 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 246 | $1.32 | $3.17 | — | $2,820.80 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $324.85 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 20 | $23.77 | $2.05 | — | $7,392.97 | — | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 44 | $10.98 | $2.12 | — | $6,907.73 | — | deploy half leftover; list flatten; 🔵; ret5=+1.2; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 8 | $61.19 | $2.01 | — | $6,416.20 | — | deploy half leftover; list flatten; 🔵; ret5=+7.4; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 58 | $8.35 | $2.16 | — | $5,929.73 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 99 | $4.94 | $2.29 | — | $5,438.39 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $5,009.42 | — | deploy half leftover; list flatten; ret5=+6.0; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 67 | $7.25 | $2.19 | — | $4,521.48 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $491.90 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 1374 | $0.36 | $9.04 | — | $4,020.55 | — | deploy half leftover; list probable,yday_gainer; ret5=-15.6; leftover $491.90 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,020.55 | ▲ close $10,520.02 vs 09:30 $10,350.48 (session +212.88) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,020.55 | ▼ 09:30 equity $10,489.71 vs yday $10,520.02 (-30.31) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 2 | $119.80 | $2.02 | $-3.27 | $4,258.13 | ▼ -3.27 after sell → book $10,487.70; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 18 | $16.60 | $2.06 | $-14.91 | $4,554.87 | ▼ -14.91 after sell → book $10,485.63; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AEM` | 1 | $219.50 | $2.01 | $-0.81 | $4,772.36 | ▼ -0.81 after sell → book $10,483.62; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 29 | $15.35 | $2.10 | $+118.21 | $5,215.41 | ▲ +118.21 after sell → book $10,481.52; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 131 | $2.41 | $2.41 | $-12.66 | $5,528.70 | ▼ -12.66 after sell → book $10,479.11; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 168 | $2.03 | $2.53 | $+11.77 | $5,867.21 | ▲ +11.77 after sell → book $10,476.57; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRSP` | 5 | $60.18 | $2.02 | $-1.73 | $6,166.09 | ▼ -1.73 after sell → book $10,474.55; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 246 | $1.60 | $3.22 | $+62.48 | $6,556.46 | ▲ +62.48 after sell → book $10,471.33; vs 09:30 mark -3.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 52 | $31.21 | $2.15 | — | $4,931.40 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1639.12 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 147 | $11.12 | $2.43 | — | $3,294.33 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1639.12 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,294.33 | ▲ close $10,564.07 vs 09:30 $10,489.71 (session +97.32) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,294.33 | ▲ 09:30 equity $10,606.66 vs yday $10,564.07 (+42.59) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 7 | $41.44 | $2.01 | — | $3,002.24 | — | deploy half leftover; list flatten; ret5=+3.1; leftover $329.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 22 | $14.42 | $2.06 | — | $2,682.94 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $329.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 126 | $2.60 | $2.37 | — | $2,352.97 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+13.0; leftover $329.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 25 | $12.98 | $2.06 | — | $2,026.41 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $329.43 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 34 | $9.68 | $2.09 | — | $1,695.19 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $329.43 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,695.19 | ▲ close $10,610.09 vs 09:30 $10,606.66 (session +14.02) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,695.19 | ▼ 09:30 equity $10,546.64 vs yday $10,610.09 (-63.45) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 44 | $10.97 | $2.14 | $-4.70 | $2,175.73 | ▼ -4.70 after sell → book $10,544.50; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 8 | $60.52 | $2.03 | $-9.41 | $2,657.86 | ▼ -9.41 after sell → book $10,542.47; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 58 | $8.28 | $2.18 | $-8.41 | $3,135.91 | ▼ -8.41 after sell → book $10,540.28; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 99 | $4.95 | $2.31 | $-3.61 | $3,623.65 | ▼ -3.61 after sell → book $10,537.97; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $423.76 | $2.01 | $-7.22 | $4,045.40 | ▼ -7.22 after sell → book $10,535.96; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 67 | $9.73 | $2.21 | $+161.76 | $4,695.10 | ▲ +161.76 after sell → book $10,533.75; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 1374 | $0.36 | $9.38 | $-8.80 | $5,187.23 | ▼ -8.80 after sell → book $10,524.37; vs 09:30 mark -9.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 19 | $32.90 | $2.05 | — | $4,560.08 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $648.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 41 | $15.66 | $2.11 | — | $3,915.91 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $648.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 8 | $79.42 | $2.01 | — | $3,278.54 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $648.40 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 195 | $3.32 | $2.58 | — | $2,628.56 | — | deploy half leftover; list probable,yday_gainer; ret5=+6.4; leftover $648.40 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,628.56 | ▼ close $10,308.49 vs 09:30 $10,546.64 (session -207.13) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,628.56 | ▼ 09:30 equity $10,288.86 vs yday $10,308.49 (-19.63) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 20 | $23.68 | $2.07 | $-5.92 | $3,100.09 | ▼ -5.92 after sell → book $10,286.79; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVBP` | 52 | $29.94 | $2.17 | $-70.35 | $4,654.80 | ▼ -70.35 after sell → book $10,284.62; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 147 | $10.82 | $2.47 | $-49.00 | $6,242.88 | ▼ -49.00 after sell → book $10,282.16; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,242.88 | ▲ close $10,334.41 vs 09:30 $10,288.86 (session +52.25) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,242.88 | ▼ 09:30 equity $10,330.08 vs yday $10,334.41 (-4.33) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 7 | $41.94 | $2.03 | $-0.54 | $6,534.42 | ▼ -0.54 after sell → book $10,328.04; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 22 | $15.82 | $2.08 | $+26.67 | $6,880.39 | ▲ +26.67 after sell → book $10,325.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 126 | $2.67 | $2.40 | $+4.05 | $7,214.41 | ▲ +4.05 after sell → book $10,323.57; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `KURA` | 25 | $12.54 | $2.08 | $-15.15 | $7,525.82 | ▼ -15.15 after sell → book $10,321.48; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ABX` | 34 | $9.43 | $2.11 | $-12.70 | $7,844.33 | ▼ -12.70 after sell → book $10,319.37; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,844.33 | ▲ close $10,344.45 vs 09:30 $10,330.08 (session +25.08) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,844.33 | ▼ 09:30 equity $10,334.50 vs yday $10,344.45 (-9.95) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 19 | $32.42 | $2.07 | $-13.23 | $8,458.25 | ▼ -13.23 after sell → book $10,332.44; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 41 | $13.92 | $2.13 | $-75.59 | $9,026.83 | ▼ -75.59 after sell → book $10,330.30; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 8 | $78.84 | $2.03 | $-8.69 | $9,655.52 | ▼ -8.69 after sell → book $10,328.27; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PYXS` | 195 | $3.45 | $2.62 | $+20.16 | $10,325.65 | ▲ +20.16 after sell → book $10,325.65; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,325.65 | ▲ close $10,325.65 vs 09:30 $10,334.50 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,325.65 | ▲ 09:30 equity $10,325.65 vs yday $10,325.65 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 12 | $52.88 | $2.03 | — | $9,689.06 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $42.93 | $2.04 | — | $9,043.08 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 177 | $3.63 | $2.52 | — | $8,398.05 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 80 | $8.03 | $2.23 | — | $7,753.42 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $132.45 | $2.00 | — | $7,221.62 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.45 | $2.11 | — | $6,586.05 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 4 | $145.94 | $2.00 | — | $6,000.27 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $645.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 38 | $16.77 | $2.10 | — | $5,360.91 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $645.35 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,360.91 | ▼ close $10,194.92 vs 09:30 $10,325.65 (session -113.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,360.91 | ▲ 09:30 equity $10,195.77 vs yday $10,194.92 (+0.85) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 177 | $2.52 | $2.52 | — | $4,912.35 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $446.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 66 | $6.71 | $2.19 | — | $4,467.30 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $446.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 235 | $1.90 | $3.03 | — | $4,017.77 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $446.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 93 | $4.78 | $2.27 | — | $3,570.96 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $446.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 280 | $1.59 | $3.61 | — | $3,122.15 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $446.74 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 39 | $11.31 | $2.11 | — | $2,678.95 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $446.74 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,678.95 | ▲ close $10,182.36 vs 09:30 $10,195.77 (session +2.32) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,678.95 | ▼ 09:30 equity $10,166.82 vs yday $10,182.36 (-15.54) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,678.95 | ▼ close $10,055.75 vs 09:30 $10,166.82 (session -111.07) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,678.95 | ▼ 09:30 equity $10,020.34 vs yday $10,055.75 (-35.41) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 12 | $53.16 | $2.05 | $-0.71 | $3,314.82 | ▼ -0.71 after sell → book $10,018.29; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 15 | $42.01 | $2.06 | $-17.89 | $3,942.92 | ▼ -17.89 after sell → book $10,016.24; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 80 | $8.01 | $2.25 | $-6.08 | $4,581.47 | ▼ -6.08 after sell → book $10,013.99; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 4 | $125.77 | $2.02 | $-30.74 | $5,082.52 | ▼ -30.74 after sell → book $10,011.96; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 41 | $15.16 | $2.13 | $-16.14 | $5,701.95 | ▼ -16.14 after sell → book $10,009.83; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 4 | $140.29 | $2.02 | $-26.62 | $6,261.11 | ▼ -26.62 after sell → book $10,007.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 38 | $15.46 | $2.12 | $-54.01 | $6,846.46 | ▼ -54.01 after sell → book $10,005.68; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,846.46 | ▼ close $9,829.64 vs 09:30 $10,020.34 (session -176.04) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,846.46 | ▼ 09:30 equity $9,782.54 vs yday $9,829.64 (-47.10) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 177 | $2.85 | $2.56 | $-143.14 | $7,348.35 | ▼ -143.14 after sell → book $9,779.98; vs 09:30 mark -2.56 | dropped from list after 4 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 177 | $2.22 | $2.56 | $-58.18 | $7,738.73 | ▼ -58.18 after sell → book $9,777.42; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 66 | $6.11 | $2.21 | $-44.00 | $8,139.78 | ▼ -44.00 after sell → book $9,775.21; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 235 | $1.83 | $3.08 | $-22.56 | $8,566.75 | ▼ -22.56 after sell → book $9,772.13; vs 09:30 mark -3.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 93 | $3.92 | $2.29 | $-84.36 | $8,929.21 | ▼ -84.36 after sell → book $9,769.84; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 280 | $1.53 | $3.67 | $-24.08 | $9,353.94 | ▼ -24.08 after sell → book $9,766.17; vs 09:30 mark -3.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 39 | $10.57 | $2.13 | $-33.09 | $9,764.04 | ▼ -33.09 after sell → book $9,764.04; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,764.04 | ▲ close $9,764.04 vs 09:30 $9,782.54 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,764.04 | ▲ 09:30 equity $9,764.04 vs yday $9,764.04 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 37 | $16.28 | $2.10 | — | $9,159.58 | — | deploy half leftover; list flatten; 🔵; ret5=-1.1; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 223 | $2.73 | $2.88 | — | $8,547.91 | — | deploy half leftover; list flatten; 🔵; ret5=-3.0; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 2 | $206.84 | $2.00 | — | $8,132.24 | — | deploy half leftover; list flatten; ret5=+8.3; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $7,636.95 | — | deploy half leftover; list flatten,earn_react; ⚪; ret5=+4.9; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 3 | $157.78 | $2.00 | — | $7,161.61 | — | deploy half leftover; list flatten; 🔵; ret5=+4.7; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 10 | $56.09 | $2.02 | — | $6,598.69 | — | deploy half leftover; list flatten; 🔵; ret5=+19.6; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 299 | $2.04 | $3.86 | — | $5,984.87 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $610.25 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 128 | $4.75 | $2.37 | — | $5,374.50 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $610.25 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,374.50 | ▼ close $9,728.80 vs 09:30 $9,764.04 (session -16.02) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,374.50 | ▼ 09:30 equity $9,609.48 vs yday $9,728.80 (-119.32) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,374.50 | ▼ close $9,594.61 vs 09:30 $9,609.48 (session -14.87) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,374.50 | ▲ 09:30 equity $9,604.07 vs yday $9,594.61 (+9.46) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,374.50 | ▼ close $9,487.58 vs 09:30 $9,604.07 (session -116.49) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,374.50 | ▲ 09:30 equity $9,515.57 vs yday $9,487.58 (+27.99) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `AUPH` | 37 | $16.16 | $2.12 | $-8.66 | $5,970.30 | ▼ -8.66 after sell → book $9,513.45; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `OVID` | 223 | $2.72 | $2.92 | $-8.03 | $6,573.93 | ▼ -8.03 after sell → book $9,510.52; vs 09:30 mark -2.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 2 | $194.84 | $2.02 | $-28.01 | $6,961.60 | ▼ -28.01 after sell → book $9,508.51; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 3 | $140.03 | $2.02 | $-77.22 | $7,379.67 | ▼ -77.22 after sell → book $9,506.49; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 3 | $147.79 | $2.02 | $-33.99 | $7,821.02 | ▼ -33.99 after sell → book $9,504.47; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COHU` | 10 | $51.29 | $2.04 | $-52.06 | $8,331.88 | ▼ -52.06 after sell → book $9,502.43; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 299 | $1.89 | $3.92 | $-52.62 | $8,893.07 | ▼ -52.62 after sell → book $9,498.51; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 128 | $4.73 | $2.41 | $-7.34 | $9,496.11 | ▼ -7.34 after sell → book $9,496.11; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $8,952.33 | — | deploy half leftover; list flatten; ret5=+4.0; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 7 | $77.12 | $2.01 | — | $8,410.48 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+7.2; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 41 | $14.31 | $2.11 | — | $7,821.66 | — | deploy half leftover; list flatten; ret5=+4.8; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 16 | $36.46 | $2.04 | — | $7,236.26 | — | deploy half leftover; list flatten; 🔵; ret5=+2.9; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 31 | $18.61 | $2.08 | — | $6,657.27 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 32 | $18.21 | $2.09 | — | $6,072.46 | — | deploy half leftover; list probable,yday_gainer; ret5=-19.1; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 8 | $68.79 | $2.01 | — | $5,520.13 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $593.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 101 | $5.87 | $2.29 | — | $4,924.96 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $593.51 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,924.96 | ▲ close $9,570.68 vs 09:30 $9,515.57 (session +91.21) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,924.96 | ▲ 09:30 equity $9,648.91 vs yday $9,570.68 (+78.23) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $4,689.12 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+11.7; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 2 | $151.43 | $2.00 | — | $4,384.26 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 2 | $147.61 | $2.00 | — | $4,087.05 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+17.7; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 30 | $10.25 | $2.08 | — | $3,777.47 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 40 | $7.59 | $2.11 | — | $3,471.76 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 8 | $34.93 | $2.01 | — | $3,190.30 | — | deploy half leftover; list flatten; ret5=+1.6; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 1810 | $0.17 | $8.51 | — | $2,874.10 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $307.81 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 19 | $15.87 | $2.05 | — | $2,570.52 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $307.81 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,570.52 | ▲ close $9,667.82 vs 09:30 $9,648.91 (session +41.65) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,570.52 | ▲ 09:30 equity $9,727.38 vs yday $9,667.82 (+59.56) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 1 | $108.55 | $1.09 | — | $2,460.88 | — | deploy half leftover; list flatten; ⚪; ret5=+21.3; leftover $160.66 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 1 | $85.00 | $0.85 | — | $2,375.03 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $160.66 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 4 | $34.44 | $1.39 | — | $2,235.88 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $160.66 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 165 | $0.97 | $2.10 | — | $2,073.73 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $160.66 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 77 | $2.08 | $1.83 | — | $1,911.74 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $160.66 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,911.74 | ▼ close $9,629.35 vs 09:30 $9,727.38 (session -90.77) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,911.74 | ▲ 09:30 equity $9,705.95 vs yday $9,629.35 (+76.60) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 2 | $266.76 | $2.02 | $-12.27 | $2,443.24 | ▼ -12.27 after sell → book $9,703.93; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 7 | $76.27 | $2.03 | $-9.99 | $2,975.10 | ▼ -9.99 after sell → book $9,701.90; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 41 | $13.65 | $2.13 | $-31.31 | $3,532.62 | ▼ -31.31 after sell → book $9,699.77; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 16 | $36.70 | $2.06 | $-0.26 | $4,117.76 | ▼ -0.26 after sell → book $9,697.71; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BBNX` | 31 | $22.11 | $2.10 | $+104.31 | $4,801.07 | ▲ +104.31 after sell → book $9,695.61; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQQ` | 32 | $20.55 | $2.11 | $+70.69 | $5,456.56 | ▲ +70.69 after sell → book $9,693.50; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 8 | $79.08 | $2.03 | $+78.27 | $6,087.17 | ▲ +78.27 after sell → book $9,691.47; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 101 | $5.62 | $2.32 | $-29.86 | $6,652.47 | ▼ -29.86 after sell → book $9,689.15; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 3 | $157.87 | $2.00 | — | $6,176.86 | — | deploy half leftover; list flatten; ret5=+6.5; leftover $554.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $5,788.67 | — | deploy half leftover; list flatten; ret5=-5.8; leftover $554.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 6 | $88.83 | $2.01 | — | $5,253.68 | — | deploy half leftover; list flatten; ret5=+7.6; leftover $554.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 41 | $13.47 | $2.11 | — | $4,699.30 | — | deploy half leftover; list flatten; ret5=+3.6; leftover $554.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 138 | $4.00 | $2.40 | — | $4,144.89 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $554.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 59 | $9.31 | $2.17 | — | $3,593.44 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $554.37 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,593.44 | ▼ close $9,544.07 vs 09:30 $9,705.95 (session -132.40) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,593.44 | ▲ 09:30 equity $9,558.95 vs yday $9,544.07 (+14.88) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 30 | $10.18 | $2.10 | $-6.28 | $3,896.74 | ▼ -6.28 after sell → book $9,556.85; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 1810 | $0.16 | $8.64 | $-35.24 | $4,177.70 | ▼ -35.24 after sell → book $9,548.21; vs 09:30 mark -8.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 2 | $93.97 | $1.89 | — | $3,987.87 | — | deploy half leftover; list flatten; ret5=-0.6; leftover $261.11 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 450 | $0.58 | $3.96 | — | $3,722.91 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $261.11 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,722.91 | ▲ close $9,550.14 vs 09:30 $9,558.95 (session +7.78) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,722.91 | ▲ 09:30 equity $9,608.77 vs yday $9,550.14 (+58.63) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ILMN` | 1 | $248.79 | $2.01 | $+10.93 | $3,969.69 | ▲ +10.93 after sell → book $9,606.76; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TWST` | 2 | $164.35 | $2.02 | $+21.83 | $4,296.37 | ▲ +21.83 after sell → book $9,604.74; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 2 | $142.40 | $2.02 | $-14.43 | $4,579.16 | ▼ -14.43 after sell → book $9,602.73; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 8 | $34.78 | $2.03 | $-5.25 | $4,855.36 | ▼ -5.25 after sell → book $9,600.69; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 19 | $17.10 | $2.07 | $+19.26 | $5,178.20 | ▲ +19.26 after sell → book $9,598.63; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RBRK` | 1 | $112.46 | $1.15 | $+1.67 | $5,289.51 | ▲ +1.67 after sell → book $9,597.48; vs 09:30 mark -1.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ECO` | 1 | $77.55 | $0.80 | $-9.10 | $5,366.26 | ▼ -9.10 after sell → book $9,596.68; vs 09:30 mark -0.80 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FIVN` | 4 | $38.91 | $1.59 | $+14.88 | $5,520.29 | ▲ +14.88 after sell → book $9,595.09; vs 09:30 mark -1.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 165 | $0.89 | $2.00 | $-17.30 | $5,665.14 | ▼ -17.30 after sell → book $9,593.09; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWRD` | 77 | $2.16 | $1.92 | $+2.41 | $5,829.55 | ▲ +2.41 after sell → book $9,591.18; vs 09:30 mark -1.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 4 | $116.85 | $2.00 | — | $5,360.14 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $582.95 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 20 | $27.79 | $2.05 | — | $4,802.29 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $582.95 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 59 | $9.81 | $2.17 | — | $4,221.34 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $582.95 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 28 | $20.25 | $2.07 | — | $3,652.26 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $582.95 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 28 | $20.65 | $2.07 | — | $3,071.99 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $582.95 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,071.99 | ▼ close $9,371.53 vs 09:30 $9,608.77 (session -209.28) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,071.99 | ▼ 09:30 equity $9,292.75 vs yday $9,371.53 (-78.78) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 40 | $7.38 | $2.13 | $-12.64 | $3,365.06 | ▼ -12.64 after sell → book $9,290.62; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 3 | $163.95 | $2.02 | $+14.22 | $3,854.89 | ▲ +14.22 after sell → book $9,288.60; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 1 | $374.54 | $2.01 | $-15.67 | $4,227.42 | ▼ -15.67 after sell → book $9,286.59; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 6 | $87.67 | $2.03 | $-10.97 | $4,751.44 | ▼ -10.97 after sell → book $9,284.56; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MGTX` | 41 | $11.42 | $2.13 | $-88.30 | $5,217.53 | ▼ -88.30 after sell → book $9,282.43; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CYPH` | 138 | $3.40 | $2.44 | $-87.64 | $5,684.29 | ▼ -87.64 after sell → book $9,279.99; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 59 | $8.67 | $2.19 | $-42.11 | $6,193.63 | ▼ -42.11 after sell → book $9,277.80; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,193.63 | ▲ close $9,292.83 vs 09:30 $9,292.75 (session +15.03) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,740.48 | ▲ 09:30 equity $8,914.59 vs yday $8,907.11 (+7.48) | 09:30 open · cash $4,740.48 (unchanged overnight, no fees) · equity $8,914.59 vs prior close $8,907.11 (+7.48) · 14 name(s) re-marked at the open (per-name table). ADMA×31 yday $9.52 → 09:30 $9.52 +0.00; ARQT×11 yday $26.27 → 09:30 $26.27 +0.00; DEFT×587 yday $0.53 → 09:30 $0.53 +0.00; DLO×23 yday $13.88 → 09:30 $13.88 +0.00; EL×3 yday $95.37 → 09:30 $95.37 +0.00; FJET×171 yday $1.80 → 09:30 $1.80 +0.00; FTRE×15 yday $20.02 → 09:30 $20.02 +0.00; HALO×2 yday $115.22 → 09:30 $115.36 +0.28; MKC×6 yday $47.82 → 09:30 $47.82 +0.00; OMER×15 yday $20.13 → 09:30 $20.61 +7.20; PACS×8 yday $41.46 → 09:30 $41.46 +0.00; PGEN×39 yday $7.70 → 09:30 $7.70 +0.00; TDC×11 yday $29.46 → 09:30 $29.46 +0.00; USFD×3 yday $93.82 → 09:30 $93.82 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 10 | $38.51 | $2.02 | — | $4,353.36 | — | deploy half leftover; list flatten; ret5=+4.7; leftover $395.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 51 | $7.65 | $2.14 | — | $3,961.07 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+5.2; leftover $395.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 15 | $26.27 | $2.04 | — | $3,564.98 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $395.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 4 | $83.76 | $2.00 | — | $3,227.94 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $395.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 43 | $9.05 | $2.12 | — | $2,836.67 | — | deploy half leftover; list probable,yday_gainer; ret5=-27.1; leftover $395.04 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,836.67 | ▲ close $8,918.39 vs 09:30 $8,914.59 (session +14.11) | 16:00 close · cash $2,836.67 · equity $8,918.39 vs 09:30 $8,914.59 (+3.80; session marks +14.11) · 19 name(s) marked open→close (per-name table). ADMA×31 09:30 $9.52 → close $9.52 +0.00; ARQT×11 09:30 $26.27 → close $26.27 +0.00; DEFT×587 09:30 $0.53 → close $0.53 +0.00; DLO×23 09:30 $13.88 → close $13.88 +0.00; EL×3 09:30 $95.37 → close $95.37 +0.00; FJET×171 09:30 $1.80 → close $1.80 -0.00; FTRE×15 09:30 $20.02 → close $20.02 +0.00; HALO×2 09:30 $115.36 → close $113.90 -2.92; MKC×6 09:30 $47.82 → close $47.82 -0.00; OMER×15 09:30 $20.61 → close $20.08 -7.95; PACS×8 09:30 $41.46 → close $41.46 -0.00; PGEN×39 09:30 $7.70 → close $7.70 -0.00; TDC×11 09:30 $29.46 → close $29.46 -0.00; USFD×3 09:30 $93.82 → close $93.82 -0.00; BLFS×10 09:30 $38.51 → close $38.49 -0.20; MRVI×51 09:30 $7.65 → close $7.60 -2.55; WRBY×15 09:30 $26.27 → close $26.71 +6.60; TXG×4 09:30 $83.76 → close $85.71 +7.80; AEHL×43 09:30 $9.05 → close $9.36 +13.33 | — |

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
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DELL` | cash | leftover split 160.66 < 1 share @ 593.15 |
| 2026-09-18 | `GNRC` | cash | leftover split 160.66 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 160.66 < 1 share @ 219.62 |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ILMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TWST` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `USFD` | 2 | 2026-09-22 @ $93.97 | deploy half leftover; list flatten; ret5=-0.6; leftover $261.11 |
| `DEFT` | 450 | 2026-09-22 @ $0.58 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $261.11 |
| `HALO` | 4 | 2026-09-23 @ $116.85 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $582.95 |
| `ARQT` | 20 | 2026-09-23 @ $27.79 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $582.95 |
| `ADMA` | 59 | 2026-09-23 @ $9.81 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $582.95 |
| `FTRE` | 28 | 2026-09-23 @ $20.25 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $582.95 |
| `OMER` | 28 | 2026-09-23 @ $20.65 | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $582.95 |
