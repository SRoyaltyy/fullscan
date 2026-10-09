# Factor mine action — `union_h5_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **-7.10%** ($9,290) · signal-only (no cash/fees) was +16.74%. Starts YES **5/30**. Fills 316 · skips 512 · realized $+185.68.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,327.16.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 21 | $20.55 | $2.05 | — | $6,682.74 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $6,316.70 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 21 | $20.65 | $2.05 | — | $5,880.99 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 77 | $5.77 | $2.22 | — | $5,434.48 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 22 | $19.63 | $2.06 | — | $5,000.57 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $4,554.08 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 254 | $1.75 | $3.28 | — | $4,106.31 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $444.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $3,670.69 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $444.77 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,670.69 | ▲ close $10,582.85 vs 09:30 $10,488.06 (session +137.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,670.69 | ▲ 09:30 equity $10,738.62 vs yday $10,582.85 (+155.77) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $3,948.65 | ▼ -17.83 after sell → book $10,736.60; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $4,179.79 | ▼ -10.85 after sell → book $10,734.58; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 5 | $58.63 | $2.02 | $+1.07 | $4,470.92 | ▲ +1.07 after sell → book $10,732.56; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 35 | $11.70 | $2.12 | $+89.94 | $4,878.30 | ▲ +89.94 after sell → book $10,730.44; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 340 | $0.87 | $4.03 | $-32.04 | $5,169.05 | ▼ -32.04 after sell → book $10,726.41; vs 09:30 mark -4.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 212 | $1.66 | $2.78 | $+28.41 | $5,518.19 | ▲ +28.41 after sell → book $10,723.63; vs 09:30 mark -2.78 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $5,277.33 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 20 | $17.20 | $2.05 | — | $4,931.28 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,712.99 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 30 | $11.13 | $2.08 | — | $4,377.01 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 139 | $2.47 | $2.41 | — | $4,031.27 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 178 | $1.93 | $2.52 | — | $3,685.21 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,384.61 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $344.89 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 261 | $1.32 | $3.37 | — | $3,036.72 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $344.89 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 12 | $23.77 | $2.03 | — | $4,391.10 | — | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $292.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 26 | $10.98 | $2.07 | — | $4,103.56 | — | deploy half leftover; list flatten; 🔵; ret5=+1.2; leftover $292.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 4 | $61.19 | $2.00 | — | $3,856.79 | — | deploy half leftover; list flatten; 🔵; ret5=+7.4; leftover $292.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 35 | $8.35 | $2.10 | — | $3,562.45 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $292.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 59 | $4.94 | $2.17 | — | $3,268.82 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $292.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 40 | $7.25 | $2.11 | — | $2,976.71 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $292.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 816 | $0.36 | $5.37 | — | $2,679.21 | — | deploy half leftover; list probable,yday_gainer; ret5=-15.6; leftover $292.40 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,679.21 | ▲ close $11,124.70 vs 09:30 $10,817.05 (session +325.49) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,679.21 | ▼ 09:30 equity $11,027.61 vs yday $11,124.70 (-97.09) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 1 | $427.50 | $1.99 | — | $2,249.72 | — | deploy half leftover; list flatten; ret5=+4.1; leftover $446.54 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 14 | $31.21 | $2.03 | — | $1,810.75 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $446.54 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 40 | $11.12 | $2.11 | — | $1,363.84 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $446.54 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,363.84 | ▲ close $11,043.04 vs 09:30 $11,027.61 (session +21.56) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,363.84 | ▲ 09:30 equity $11,076.02 vs yday $11,043.04 (+32.98) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 21 | $20.93 | $2.07 | $+3.85 | $1,801.30 | ▲ +3.85 after sell → book $11,073.95; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 4 | $95.52 | $2.02 | $+14.02 | $2,181.35 | ▲ +14.02 after sell → book $11,071.93; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 21 | $21.31 | $2.07 | $+9.73 | $2,626.79 | ▲ +9.73 after sell → book $11,069.85; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 77 | $5.49 | $2.24 | $-26.02 | $3,047.28 | ▼ -26.02 after sell → book $11,067.61; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 22 | $21.47 | $2.08 | $+36.35 | $3,517.54 | ▲ +36.35 after sell → book $11,065.53; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.32 | $2.06 | $+36.26 | $4,000.29 | ▲ +36.26 after sell → book $11,063.48; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 254 | $1.91 | $3.33 | $+34.03 | $4,482.10 | ▲ +34.03 after sell → book $11,060.15; vs 09:30 mark -3.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $155.89 | $2.02 | $+30.03 | $4,947.75 | ▲ +30.03 after sell → book $11,058.13; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 11 | $41.44 | $2.02 | — | $4,489.89 | — | deploy half leftover; list flatten; ret5=+3.1; leftover $494.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 34 | $14.42 | $2.09 | — | $3,997.51 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $494.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 190 | $2.60 | $2.56 | — | $3,500.95 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+13.0; leftover $494.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 38 | $12.98 | $2.10 | — | $3,005.61 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $494.77 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 51 | $9.68 | $2.14 | — | $2,509.79 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $494.77 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,509.79 | ▲ close $11,128.81 vs 09:30 $11,076.02 (session +81.60) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,509.79 | ▼ 09:30 equity $11,066.74 vs yday $11,128.81 (-62.07) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $119.19 | $2.02 | $-4.49 | $2,746.15 | ▼ -4.49 after sell → book $11,064.72; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 20 | $16.44 | $2.07 | $-19.32 | $3,072.88 | ▼ -19.32 after sell → book $11,062.65; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $216.31 | $2.01 | $-4.00 | $3,287.18 | ▼ -4.00 after sell → book $11,060.64; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 30 | $15.43 | $2.10 | $+124.82 | $3,747.98 | ▲ +124.82 after sell → book $11,058.54; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 139 | $2.35 | $2.44 | $-21.53 | $4,072.19 | ▼ -21.53 after sell → book $11,056.10; vs 09:30 mark -2.44 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 178 | $2.06 | $2.56 | $+18.05 | $4,436.30 | ▲ +18.05 after sell → book $11,053.53; vs 09:30 mark -2.57 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 5 | $58.22 | $2.02 | $-11.53 | $4,725.38 | ▼ -11.53 after sell → book $11,051.51; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 261 | $1.82 | $3.42 | $+123.71 | $5,196.98 | ▲ +123.71 after sell → book $11,048.09; vs 09:30 mark -3.42 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 19 | $32.90 | $2.05 | — | $4,569.83 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $649.62 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 41 | $15.66 | $2.11 | — | $3,925.66 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $649.62 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 8 | $79.42 | $2.01 | — | $3,288.29 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $649.62 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 195 | $3.32 | $2.58 | — | $2,638.31 | — | deploy half leftover; list probable,yday_gainer; ret5=+6.4; leftover $649.62 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,638.31 | ▼ close $10,822.06 vs 09:30 $11,066.74 (session -217.28) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,638.31 | ▼ 09:30 equity $10,815.88 vs yday $10,822.06 (-6.18) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,638.31 | ▲ close $10,901.21 vs 09:30 $10,815.88 (session +85.32) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,638.31 | ▲ 09:30 equity $10,941.78 vs yday $10,901.21 (+40.57) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 12 | $23.94 | $2.05 | $-2.03 | $2,923.54 | ▼ -2.03 after sell → book $10,939.73; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 26 | $10.42 | $2.09 | $-18.72 | $3,192.38 | ▼ -18.72 after sell → book $10,937.65; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 4 | $63.00 | $2.02 | $+3.22 | $3,442.35 | ▲ +3.22 after sell → book $10,935.62; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 35 | $8.25 | $2.12 | $-7.71 | $3,728.99 | ▼ -7.71 after sell → book $10,933.51; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 59 | $4.64 | $2.19 | $-22.05 | $4,000.56 | ▼ -22.05 after sell → book $10,931.32; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 40 | $10.77 | $2.13 | $+136.56 | $4,429.23 | ▲ +136.56 after sell → book $10,929.19; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 816 | $0.36 | $5.57 | $-5.23 | $4,721.50 | ▼ -5.23 after sell → book $10,923.62; vs 09:30 mark -5.57 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,721.50 | ▼ close $10,917.81 vs 09:30 $10,941.78 (session -5.81) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,721.50 | ▼ 09:30 equity $10,899.80 vs yday $10,917.81 (-18.01) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `HCA` | 1 | $412.46 | $2.01 | $-19.05 | $5,131.95 | ▼ -19.05 after sell → book $10,897.79; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVBP` | 14 | $30.33 | $2.05 | $-16.40 | $5,554.52 | ▼ -16.40 after sell → book $10,895.74; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `FLNC` | 40 | $10.38 | $2.13 | $-33.64 | $5,967.79 | ▼ -33.64 after sell → book $10,893.61; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,967.79 | ▲ close $10,976.97 vs 09:30 $10,899.80 (session +83.36) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,967.79 | ▲ 09:30 equity $10,997.98 vs yday $10,976.97 (+21.01) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 11 | $42.43 | $2.04 | $+6.82 | $6,432.47 | ▲ +6.82 after sell → book $10,995.94; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 190 | $2.49 | $2.60 | $-26.06 | $6,902.97 | ▼ -26.06 after sell → book $10,993.34; vs 09:30 mark -2.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 38 | $13.25 | $2.12 | $+6.03 | $7,404.35 | ▲ +6.03 after sell → book $10,991.21; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 51 | $9.68 | $2.16 | $-4.31 | $7,895.86 | ▼ -4.31 after sell → book $10,989.05; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 10 | $52.88 | $2.02 | — | $7,365.04 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $563.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 13 | $42.93 | $2.03 | — | $6,804.92 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $563.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 155 | $3.63 | $2.46 | — | $6,239.82 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $563.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 70 | $8.03 | $2.20 | — | $5,675.52 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $563.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $132.45 | $2.00 | — | $5,143.72 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $563.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 3 | $145.94 | $2.00 | — | $4,703.88 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $563.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 33 | $16.77 | $2.09 | — | $4,148.38 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $563.99 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,148.38 | ▼ close $10,850.61 vs 09:30 $10,997.98 (session -123.65) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,148.38 | ▼ 09:30 equity $10,828.34 vs yday $10,850.61 (-22.27) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 34 | $15.00 | $2.11 | $+15.52 | $4,656.27 | ▲ +15.52 after sell → book $10,826.23; vs 09:30 mark -2.11 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 19 | $33.86 | $2.07 | $+14.13 | $5,297.55 | ▲ +14.13 after sell → book $10,824.17; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 41 | $13.56 | $2.13 | $-90.35 | $5,851.37 | ▼ -90.35 after sell → book $10,822.03; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 8 | $79.55 | $2.03 | $-3.01 | $6,485.74 | ▼ -3.01 after sell → book $10,820.00; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 195 | $3.53 | $2.62 | $+35.76 | $7,171.47 | ▲ +35.76 after sell → book $10,817.38; vs 09:30 mark -2.62 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 237 | $2.52 | $3.06 | — | $6,571.17 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $597.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 89 | $6.71 | $2.26 | — | $5,971.73 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $597.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 314 | $1.90 | $4.05 | — | $5,371.08 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $597.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 125 | $4.78 | $2.37 | — | $4,771.21 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $597.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 375 | $1.59 | $4.84 | — | $4,170.12 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $597.62 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 52 | $11.31 | $2.15 | — | $3,579.86 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $597.62 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,579.86 | ▼ close $10,788.30 vs 09:30 $10,828.34 (session -10.37) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,579.86 | ▼ 09:30 equity $10,748.64 vs yday $10,788.30 (-39.66) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,579.86 | ▼ close $10,652.91 vs 09:30 $10,748.64 (session -95.73) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,579.86 | ▼ 09:30 equity $10,616.03 vs yday $10,652.91 (-36.88) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,579.86 | ▼ close $10,353.77 vs 09:30 $10,616.03 (session -262.26) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,579.86 | ▼ 09:30 equity $10,260.21 vs yday $10,353.77 (-93.56) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,579.86 | ▼ close $10,140.69 vs 09:30 $10,260.21 (session -119.52) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,579.86 | ▲ 09:30 equity $10,212.25 vs yday $10,140.69 (+71.56) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 10 | $53.53 | $2.04 | $+2.44 | $4,113.12 | ▲ +2.44 after sell → book $10,210.21; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 13 | $41.30 | $2.05 | $-25.27 | $4,647.97 | ▼ -25.27 after sell → book $10,208.16; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 155 | $2.77 | $2.49 | $-138.25 | $5,074.83 | ▼ -138.25 after sell → book $10,205.67; vs 09:30 mark -2.49 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 70 | $7.70 | $2.22 | $-27.52 | $5,611.61 | ▼ -27.52 after sell → book $10,203.45; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 4 | $122.40 | $2.02 | $-44.22 | $6,099.18 | ▼ -44.22 after sell → book $10,201.43; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MRNA` | 3 | $137.91 | $2.02 | $-28.14 | $6,510.88 | ▼ -28.14 after sell → book $10,199.41; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 33 | $14.06 | $2.11 | $-93.63 | $6,972.75 | ▼ -93.63 after sell → book $10,197.30; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 26 | $16.28 | $2.07 | — | $6,547.40 | — | deploy half leftover; list flatten; 🔵; ret5=-1.1; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 159 | $2.73 | $2.47 | — | $6,110.87 | — | deploy half leftover; list flatten; 🔵; ret5=-3.0; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 2 | $206.84 | $2.00 | — | $5,695.19 | — | deploy half leftover; list flatten; ret5=+8.3; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 2 | $164.43 | $2.00 | — | $5,364.33 | — | deploy half leftover; list flatten,earn_react; ⚪; ret5=+4.9; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 2 | $157.78 | $2.00 | — | $5,046.78 | — | deploy half leftover; list flatten; 🔵; ret5=+4.7; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 7 | $56.09 | $2.01 | — | $4,652.14 | — | deploy half leftover; list flatten; 🔵; ret5=+19.6; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 213 | $2.04 | $2.75 | — | $4,214.87 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $435.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 91 | $4.75 | $2.26 | — | $3,780.36 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $435.80 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,780.36 | ▼ close $10,175.12 vs 09:30 $10,212.25 (session -4.64) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,780.36 | ▼ 09:30 equity $10,109.67 vs yday $10,175.12 (-65.45) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 237 | $2.15 | $3.11 | $-93.85 | $4,286.80 | ▼ -93.85 after sell → book $10,106.57; vs 09:30 mark -3.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 89 | $5.93 | $2.28 | $-73.96 | $4,812.29 | ▼ -73.96 after sell → book $10,104.28; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 314 | $1.72 | $4.11 | $-66.25 | $5,346.69 | ▼ -66.25 after sell → book $10,100.17; vs 09:30 mark -4.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 125 | $4.13 | $2.40 | $-86.01 | $5,860.54 | ▼ -86.01 after sell → book $10,097.77; vs 09:30 mark -2.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 375 | $1.59 | $4.91 | $-9.75 | $6,451.88 | ▼ -9.75 after sell → book $10,092.87; vs 09:30 mark -4.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 52 | $10.73 | $2.17 | $-34.47 | $7,007.67 | ▼ -34.47 after sell → book $10,090.70; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,007.67 | ▼ close $10,074.17 vs 09:30 $10,109.67 (session -16.52) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,007.67 | ▲ 09:30 equity $10,081.68 vs yday $10,074.17 (+7.51) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,007.67 | ▼ close $9,996.14 vs 09:30 $10,081.68 (session -85.54) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,007.67 | ▲ 09:30 equity $10,017.66 vs yday $9,996.14 (+21.52) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 1 | $270.89 | $1.99 | — | $6,734.79 | — | deploy half leftover; list flatten; ret5=+4.0; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 5 | $77.12 | $2.00 | — | $6,347.19 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+7.2; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 30 | $14.31 | $2.08 | — | $5,915.81 | — | deploy half leftover; list flatten; ret5=+4.8; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 12 | $36.46 | $2.03 | — | $5,476.26 | — | deploy half leftover; list flatten; 🔵; ret5=+2.9; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 23 | $18.61 | $2.06 | — | $5,046.17 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 24 | $18.21 | $2.06 | — | $4,607.07 | — | deploy half leftover; list probable,yday_gainer; ret5=-19.1; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 6 | $68.79 | $2.01 | — | $4,192.32 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $437.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 74 | $5.87 | $2.21 | — | $3,755.73 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $437.98 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,755.73 | ▲ close $10,078.01 vs 09:30 $10,017.66 (session +76.79) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,755.73 | ▲ 09:30 equity $10,194.64 vs yday $10,078.01 (+116.63) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $3,519.89 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+11.7; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 1 | $151.43 | $1.52 | — | $3,366.94 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $3,217.85 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+17.7; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 22 | $10.25 | $2.06 | — | $2,990.29 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 30 | $7.59 | $2.08 | — | $2,760.51 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 6 | $34.93 | $2.01 | — | $2,548.93 | — | deploy half leftover; list flatten; ret5=+1.6; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 1380 | $0.17 | $6.49 | — | $2,307.84 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $234.73 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 14 | $15.87 | $2.03 | — | $2,083.63 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $234.73 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,083.63 | ▲ close $10,196.07 vs 09:30 $10,194.64 (session +21.08) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,083.63 | ▲ 09:30 equity $10,248.79 vs yday $10,196.07 (+52.72) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 26 | $16.93 | $2.09 | $+12.74 | $2,521.72 | ▲ +12.74 after sell → book $10,246.70; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 159 | $2.68 | $2.50 | $-12.92 | $2,945.34 | ▼ -12.92 after sell → book $10,244.20; vs 09:30 mark -2.50 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 2 | $197.76 | $2.02 | $-22.17 | $3,338.84 | ▼ -22.17 after sell → book $10,242.18; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 2 | $150.47 | $2.02 | $-31.93 | $3,637.76 | ▼ -31.93 after sell → book $10,240.16; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 2 | $152.71 | $2.02 | $-14.15 | $3,941.17 | ▼ -14.15 after sell → book $10,238.15; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 7 | $55.80 | $2.03 | $-6.07 | $4,329.74 | ▼ -6.07 after sell → book $10,236.12; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 213 | $1.90 | $2.79 | $-35.36 | $4,731.64 | ▼ -35.36 after sell → book $10,233.32; vs 09:30 mark -2.80 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 91 | $4.50 | $2.29 | $-27.30 | $5,138.86 | ▼ -27.30 after sell → book $10,231.04; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 2 | $108.55 | $2.00 | — | $4,919.76 | — | deploy half leftover; list flatten; ⚪; ret5=+21.3; leftover $321.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 1 | $209.52 | $1.99 | — | $4,708.25 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $321.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 1 | $219.62 | $1.99 | — | $4,486.63 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $321.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 3 | $85.00 | $2.00 | — | $4,229.64 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $321.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 9 | $34.44 | $2.02 | — | $3,917.66 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $321.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 331 | $0.97 | $4.20 | — | $3,592.38 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $321.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 154 | $2.08 | $2.45 | — | $3,269.61 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $321.18 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,269.61 | ▼ close $10,123.67 vs 09:30 $10,248.79 (session -90.71) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,269.61 | ▲ 09:30 equity $10,195.95 vs yday $10,123.67 (+72.28) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $3,110.16 | — | deploy half leftover; list flatten; ret5=+6.5; leftover $272.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 3 | $88.83 | $2.00 | — | $2,841.67 | — | deploy half leftover; list flatten; ret5=+7.6; leftover $272.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 20 | $13.47 | $2.05 | — | $2,570.22 | — | deploy half leftover; list flatten; ret5=+3.6; leftover $272.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 68 | $4.00 | $2.19 | — | $2,296.03 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $272.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 29 | $9.31 | $2.08 | — | $2,023.96 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $272.47 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,023.96 | ▼ close $10,147.14 vs 09:30 $10,195.95 (session -38.91) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,023.96 | ▲ 09:30 equity $10,147.50 vs yday $10,147.14 (+0.36) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 1 | $93.97 | $0.94 | — | $1,929.05 | — | deploy half leftover; list flatten; ret5=-0.6; leftover $126.50 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 218 | $0.58 | $1.92 | — | $1,800.69 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $126.50 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,800.69 | ▲ close $10,177.40 vs 09:30 $10,147.50 (session +32.76) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,800.69 | ▲ 09:30 equity $10,284.47 vs yday $10,177.40 (+107.07) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 1 | $270.66 | $2.01 | $-4.24 | $2,069.34 | ▼ -4.24 after sell → book $10,282.46; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 5 | $73.61 | $2.02 | $-21.58 | $2,435.36 | ▼ -21.58 after sell → book $10,280.44; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 30 | $13.12 | $2.10 | $-39.88 | $2,826.86 | ▼ -39.88 after sell → book $10,278.34; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 12 | $38.04 | $2.05 | $+14.89 | $3,281.30 | ▲ +14.89 after sell → book $10,276.29; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 23 | $23.00 | $2.08 | $+96.83 | $3,808.22 | ▲ +96.83 after sell → book $10,274.21; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 24 | $23.30 | $2.08 | $+118.02 | $4,365.33 | ▲ +118.02 after sell → book $10,272.13; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 6 | $76.47 | $2.03 | $+42.04 | $4,822.13 | ▲ +42.04 after sell → book $10,270.10; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 74 | $5.53 | $2.23 | $-29.61 | $5,229.11 | ▼ -29.61 after sell → book $10,267.87; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 4 | $116.85 | $2.00 | — | $4,759.71 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $522.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 18 | $27.79 | $2.04 | — | $4,257.45 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $522.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 53 | $9.81 | $2.15 | — | $3,735.37 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $522.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 25 | $20.25 | $2.06 | — | $3,227.05 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $522.91 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 25 | $20.65 | $2.06 | — | $2,708.74 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $522.91 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,708.74 | ▼ close $10,180.89 vs 09:30 $10,284.47 (session -76.66) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,708.74 | ▼ 09:30 equity $10,124.40 vs yday $10,180.89 (-56.49) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ILMN` | 1 | $253.79 | $2.01 | $+15.93 | $2,960.51 | ▲ +15.93 after sell → book $10,122.39; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 1 | $157.72 | $1.60 | $+3.17 | $3,116.63 | ▲ +3.17 after sell → book $10,120.79; vs 09:30 mark -1.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 1 | $141.79 | $1.44 | $-8.74 | $3,256.98 | ▼ -8.74 after sell → book $10,119.35; vs 09:30 mark -1.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 22 | $10.39 | $2.08 | $-1.05 | $3,483.49 | ▼ -1.05 after sell → book $10,117.27; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 30 | $7.38 | $2.10 | $-10.48 | $3,702.79 | ▼ -10.48 after sell → book $10,115.17; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 6 | $33.82 | $2.03 | $-10.70 | $3,903.68 | ▼ -10.70 after sell → book $10,113.15; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 1380 | $0.15 | $6.45 | $-40.54 | $4,104.23 | ▼ -40.54 after sell → book $10,106.70; vs 09:30 mark -6.45 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 14 | $16.07 | $2.05 | $-1.28 | $4,327.16 | ▼ -1.28 after sell → book $10,104.65; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,327.16 | ▲ close $10,219.30 vs 09:30 $10,124.40 (session +114.67) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,024.31 | ▲ 09:30 equity $9,336.75 vs yday $9,331.08 (+5.67) | 09:30 open · cash $4,024.31 (unchanged overnight, no fees) · equity $9,336.75 vs prior close $9,331.08 (+5.67) · 25 name(s) re-marked at the open (per-name table). A×1 yday $172.84 → 09:30 $171.98 -0.86; ADMA×44 yday $9.52 → 09:30 $9.52 +0.00; ARQT×15 yday $26.27 → 09:30 $26.27 +0.00; BHVN×18 yday $13.19 → 09:30 $13.19 +0.00; BTDR×15 yday $12.15 → 09:30 $12.15 +0.00; CYPH×53 yday $4.08 → 09:30 $4.00 -3.97; DEFT×170 yday $0.53 → 09:30 $0.53 +0.00; DLO×6 yday $13.88 → 09:30 $13.88 +0.00; DXCM×2 yday $87.47 → 09:30 $87.47 +0.00; ECO×3 yday $78.22 → 09:30 $78.22 +0.00; EL×1 yday $95.37 → 09:30 $95.37 +0.00; EYPT×67 yday $3.65 → 09:30 $3.65 +0.00; FIVN×7 yday $36.66 → 09:30 $36.66 +0.00; FJET×49 yday $1.80 → 09:30 $1.80 +0.00; FTRE×21 yday $20.02 → 09:30 $20.02 +0.00; GNRC×1 yday $198.05 → 09:30 $198.05 +0.00; HALO×3 yday $115.22 → 09:30 $115.36 +0.42; MGTX×15 yday $11.05 → 09:30 $11.05 +0.00; MKC×2 yday $47.82 → 09:30 $47.82 +0.00; OMER×21 yday $20.13 → 09:30 $20.61 +10.08; PACS×2 yday $41.46 → 09:30 $41.46 +0.00; RBRK×2 yday $113.80 → 09:30 $113.80 +0.00; TDC×3 yday $29.46 → 09:30 $29.46 +0.00; USFD×1 yday $93.82 → 09:30 $93.82 +0.00; VICR×1 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 8 | $38.51 | $2.01 | — | $3,714.22 | — | deploy half leftover; list flatten; ret5=+4.7; leftover $335.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 43 | $7.65 | $2.12 | — | $3,383.15 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+5.2; leftover $335.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 12 | $26.27 | $2.03 | — | $3,065.88 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $335.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 4 | $83.76 | $2.00 | — | $2,728.84 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $335.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 37 | $9.05 | $2.10 | — | $2,391.89 | — | deploy half leftover; list probable,yday_gainer; ret5=-27.1; leftover $335.36 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,391.89 | ▲ close $9,340.12 vs 09:30 $9,336.75 (session +13.64) | 16:00 close · cash $2,391.89 · equity $9,340.12 vs 09:30 $9,336.75 (+3.37; session marks +13.64) · 30 name(s) marked open→close (per-name table). A×1 09:30 $171.98 → close $172.79 +0.81; ADMA×44 09:30 $9.52 → close $9.52 +0.00; ARQT×15 09:30 $26.27 → close $26.27 +0.00; BHVN×18 09:30 $13.19 → close $13.19 -0.00; BTDR×15 09:30 $12.15 → close $12.15 -0.00; CYPH×53 09:30 $4.00 → close $4.12 +6.10; DEFT×170 09:30 $0.53 → close $0.53 +0.00; DLO×6 09:30 $13.88 → close $13.88 +0.00; DXCM×2 09:30 $87.47 → close $87.47 +0.00; ECO×3 09:30 $78.22 → close $78.22 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; EYPT×67 09:30 $3.65 → close $3.65 +0.00; FIVN×7 09:30 $36.66 → close $36.66 -0.00; FJET×49 09:30 $1.80 → close $1.80 -0.00; FTRE×21 09:30 $20.02 → close $20.02 +0.00; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×3 09:30 $115.36 → close $113.90 -4.38; MGTX×15 09:30 $11.05 → close $11.05 +0.00; MKC×2 09:30 $47.82 → close $47.82 -0.00; OMER×21 09:30 $20.61 → close $20.08 -11.13; PACS×2 09:30 $41.46 → close $41.46 -0.00; RBRK×2 09:30 $113.80 → close $113.80 +0.00; TDC×3 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; BLFS×8 09:30 $38.51 → close $38.49 -0.16; MRVI×43 09:30 $7.65 → close $7.60 -2.15; WRBY×12 09:30 $26.27 → close $26.71 +5.28; TXG×4 09:30 $83.76 → close $85.71 +7.80; AEHL×37 09:30 $9.05 → close $9.36 +11.47 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,391.89 | ▼ 09:30 equity $9,239.58 vs yday $9,340.12 (-100.54) | 09:30 open · cash $2,391.89 (unchanged overnight, no fees) · equity $9,239.58 vs prior close $9,340.12 (-100.54) · 30 name(s) re-marked at the open (per-name table). A×1 yday $172.79 → 09:30 $170.00 -2.79; ADMA×44 yday $9.52 → 09:30 $9.38 -6.16; AEHL×37 yday $9.36 → 09:30 $9.01 -12.95; ARQT×15 yday $26.27 → 09:30 $26.70 +6.45; BHVN×18 yday $13.19 → 09:30 $12.82 -6.66; BLFS×8 yday $38.49 → 09:30 $38.13 -2.88; BTDR×15 yday $12.15 → 09:30 $11.37 -11.70; CYPH×53 yday $4.12 → 09:30 $4.03 -4.64; DEFT×170 yday $0.53 → 09:30 $0.51 -4.45; DLO×6 yday $13.88 → 09:30 $13.91 +0.18; DXCM×2 yday $87.47 → 09:30 $86.87 -1.20; ECO×3 yday $78.22 → 09:30 $79.55 +3.99; EL×1 yday $95.37 → 09:30 $92.92 -2.45; EYPT×67 yday $3.65 → 09:30 $3.59 -4.02; FIVN×7 yday $36.66 → 09:30 $34.75 -13.37; FJET×49 yday $1.80 → 09:30 $1.79 -0.49; FTRE×21 yday $20.02 → 09:30 $19.54 -10.08; GNRC×1 yday $198.05 → 09:30 $207.41 +9.36; HALO×3 yday $113.90 → 09:30 $113.34 -1.68; MGTX×15 yday $11.05 → 09:30 $10.79 -3.90; MKC×2 yday $47.82 → 09:30 $47.83 +0.02; MRVI×43 yday $7.60 → 09:30 $7.49 -4.73; OMER×21 yday $20.08 → 09:30 $19.83 -5.25; PACS×2 yday $41.46 → 09:30 $41.27 -0.38; RBRK×2 yday $113.80 → 09:30 $106.94 -13.72; TDC×3 yday $29.46 → 09:30 $28.34 -3.36; TXG×4 yday $85.71 → 09:30 $86.14 +1.72; USFD×1 yday $93.82 → 09:30 $93.00 -0.82; VICR×1 yday $276.06 → 09:30 $280.00 +3.94; WRBY×12 yday $26.71 → 09:30 $26.00 -8.52 | — |
| 2026-09-28 09:30 ET | **SELL** | `A` | 1 | $170.00 | $1.72 | $+8.83 | $2,560.17 | ▲ +8.83 after sell → book $9,237.86; vs 09:30 mark -1.72 | dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `ADMA` | 44 | $9.38 | $2.14 | $-23.18 | $2,970.74 | ▼ -23.18 after sell → book $9,235.71; vs 09:30 mark -2.15 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ARQT` | 15 | $26.70 | $2.06 | $-20.44 | $3,369.19 | ▼ -20.44 after sell → book $9,233.66; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `BHVN` | 18 | $12.82 | $2.06 | $-26.61 | $3,597.89 | ▼ -26.61 after sell → book $9,231.59; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `BTDR` | 15 | $11.37 | $1.77 | $-35.38 | $3,766.67 | ▼ -35.38 after sell → book $9,229.82; vs 09:30 mark -1.77 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 53 | $4.03 | $2.17 | $-2.60 | $3,978.22 | ▼ -2.60 after sell → book $9,227.66; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `DEFT` | 170 | $0.51 | $1.41 | $-17.24 | $4,062.80 | ▼ -17.24 after sell → book $9,226.25; vs 09:30 mark -1.41 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DLO` | 6 | $13.91 | $0.87 | $-5.30 | $4,145.38 | ▼ -5.30 after sell → book $9,225.37; vs 09:30 mark -0.88 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DXCM` | 2 | $86.87 | $1.76 | $-7.47 | $4,317.36 | ▼ -7.47 after sell → book $9,223.61; vs 09:30 mark -1.76 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ECO` | 3 | $79.55 | $2.02 | $-20.37 | $4,553.99 | ▼ -20.37 after sell → book $9,221.59; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `EL` | 1 | $92.92 | $0.95 | $-7.04 | $4,645.96 | ▼ -7.04 after sell → book $9,220.64; vs 09:30 mark -0.95 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `EYPT` | 67 | $3.59 | $2.21 | $-28.52 | $4,884.28 | ▼ -28.52 after sell → book $9,218.43; vs 09:30 mark -2.21 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FIVN` | 7 | $34.75 | $2.03 | $-1.87 | $5,125.50 | ▼ -1.87 after sell → book $9,216.40; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FJET` | 49 | $1.79 | $1.04 | $-13.45 | $5,212.16 | ▼ -13.45 after sell → book $9,215.35; vs 09:30 mark -1.05 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FTRE` | 21 | $19.54 | $2.07 | $-19.04 | $5,620.43 | ▼ -19.04 after sell → book $9,213.28; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `GNRC` | 1 | $207.41 | $2.01 | $-6.12 | $5,825.83 | ▼ -6.12 after sell → book $9,211.27; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `MGTX` | 15 | $10.79 | $1.68 | $-43.92 | $5,985.99 | ▼ -43.92 after sell → book $9,209.58; vs 09:30 mark -1.69 | exit unpriced hold on first bar after 5 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `MKC` | 2 | $47.83 | $0.98 | $-5.46 | $6,080.67 | ▼ -5.46 after sell → book $9,208.60; vs 09:30 mark -0.98 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `PACS` | 2 | $41.27 | $0.85 | $-3.02 | $6,162.36 | ▼ -3.02 after sell → book $9,207.75; vs 09:30 mark -0.85 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `RBRK` | 2 | $106.94 | $2.02 | $-7.23 | $6,374.22 | ▼ -7.23 after sell → book $9,205.73; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `TDC` | 3 | $28.34 | $0.88 | $-6.10 | $6,458.36 | ▼ -6.10 after sell → book $9,204.85; vs 09:30 mark -0.88 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `USFD` | 1 | $93.00 | $0.95 | $-2.98 | $6,550.41 | ▼ -2.98 after sell → book $9,203.90; vs 09:30 mark -0.95 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `VICR` | 1 | $280.00 | $2.01 | $+56.37 | $6,828.40 | ▲ +56.37 after sell → book $9,201.89; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 6 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,828.40 | ▼ close $9,170.82 vs 09:30 $9,239.58 (session -31.07) | 16:00 close · cash $6,828.40 · equity $9,170.82 vs 09:30 $9,239.58 (-68.76; session marks -31.07) · 7 name(s) marked open→close (per-name table). AEHL×37 09:30 $9.01 → close $8.23 -28.86; BLFS×8 09:30 $38.13 → close $39.15 +8.16; HALO×3 09:30 $113.34 → close $112.89 -1.35; MRVI×43 09:30 $7.49 → close $7.63 +6.02; OMER×21 09:30 $19.83 → close $19.43 -8.40; TXG×4 09:30 $86.14 → close $88.50 +9.44; WRBY×12 09:30 $26.00 → close $24.66 -16.08 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,828.40 | ▼ 09:30 equity $9,166.88 vs yday $9,170.82 (-3.94) | 09:30 open · cash $6,828.40 (unchanged overnight, no fees) · equity $9,166.88 vs prior close $9,170.82 (-3.94) · 7 name(s) re-marked at the open (per-name table). AEHL×37 yday $8.23 → 09:30 $8.26 +0.93; BLFS×8 yday $39.15 → 09:30 $39.04 -0.88; HALO×3 yday $112.89 → 09:30 $112.89 +0.00; MRVI×43 yday $7.63 → 09:30 $7.52 -4.73; OMER×21 yday $19.43 → 09:30 $19.26 -3.57; TXG×4 yday $88.50 → 09:30 $88.83 +1.32; WRBY×12 yday $24.66 → 09:30 $24.91 +3.00 | — |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 2 | $184.05 | $2.00 | — | $6,458.30 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.7; leftover $426.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 7 | $57.39 | $2.01 | — | $6,054.56 | — | deploy half leftover; list flatten; ⚪; ret5=+2.6; leftover $426.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 14 | $30.40 | $2.03 | — | $5,626.93 | — | deploy half leftover; list flatten; ⚪; ret5=+1.9; leftover $426.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 24 | $17.76 | $2.06 | — | $5,198.63 | — | deploy half leftover; list flatten; ⚪; ret5=+9.8; leftover $426.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 8 | $50.25 | $2.01 | — | $4,794.62 | — | deploy half leftover; list flatten; ⚪; ret5=+6.5; leftover $426.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 9 | $45.06 | $2.02 | — | $4,387.06 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.8; leftover $426.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SFIX` | 172 | $2.47 | $2.51 | — | $3,959.71 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-13.3; leftover $426.77 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ANNX` | 106 | $3.99 | $2.31 | — | $3,534.46 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=-5.0; leftover $426.77 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,534.46 | ▲ close $9,223.52 vs 09:30 $9,166.88 (session +73.58) | 16:00 close · cash $3,534.46 · equity $9,223.52 vs 09:30 $9,166.88 (+56.64; session marks +73.58) · 15 name(s) marked open→close (per-name table). AEHL×37 09:30 $8.26 → close $8.59 +12.39; BLFS×8 09:30 $39.04 → close $39.22 +1.44; HALO×3 09:30 $112.89 → close $111.56 -3.99; MRVI×43 09:30 $7.52 → close $7.65 +5.59; OMER×21 09:30 $19.26 → close $19.25 -0.21; TXG×4 09:30 $88.83 → close $90.04 +4.84; WRBY×12 09:30 $24.91 → close $27.47 +30.72; SN×2 09:30 $184.05 → close $182.44 -3.22; DT×7 09:30 $57.39 → close $57.53 +0.98; TOST×14 09:30 $30.40 → close $30.46 +0.84; SONO×24 09:30 $17.76 → close $17.85 +2.16; PDFS×8 09:30 $50.25 → close $49.77 -3.84; SHOO×9 09:30 $45.06 → close $45.14 +0.72; SFIX×172 09:30 $2.47 → close $2.53 +10.32; ANNX×106 09:30 $3.99 → close $4.13 +14.84 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,534.46 | ▲ 09:30 equity $9,223.52 vs yday $9,223.52 (+0.00) | 09:30 open · cash $3,534.46 (unchanged overnight, no fees) · equity $9,223.52 vs prior close $9,223.52 (+0.00) · 15 name(s) re-marked at the open (per-name table). AEHL×37 yday $8.59 → 09:30 $8.59 +0.00; ANNX×106 yday $4.13 → 09:30 $4.13 +0.00; BLFS×8 yday $39.22 → 09:30 $39.22 +0.00; DT×7 yday $57.53 → 09:30 $57.53 +0.00; HALO×3 yday $111.56 → 09:30 $111.56 +0.00; MRVI×43 yday $7.65 → 09:30 $7.65 +0.00; OMER×21 yday $19.25 → 09:30 $19.25 +0.00; PDFS×8 yday $49.77 → 09:30 $49.77 +0.00; SFIX×172 yday $2.53 → 09:30 $2.53 +0.00; SHOO×9 yday $45.14 → 09:30 $45.14 +0.00; SN×2 yday $182.44 → 09:30 $182.44 +0.00; SONO×24 yday $17.85 → 09:30 $17.85 +0.00; TOST×14 yday $30.46 → 09:30 $30.46 +0.00; TXG×4 yday $90.04 → 09:30 $90.04 +0.00; WRBY×12 yday $27.47 → 09:30 $27.47 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,534.46 | ▲ close $9,223.52 vs 09:30 $9,223.52 (session +0.00) | 16:00 close · cash $3,534.46 · equity $9,223.52 vs 09:30 $9,223.52 (+0.00; session marks +0.00) · 15 name(s) marked open→close (per-name table). AEHL×37 09:30 $8.59 → close $8.59 +0.00; ANNX×106 09:30 $4.13 → close $4.13 +0.00; BLFS×8 09:30 $39.22 → close $39.22 +0.00; DT×7 09:30 $57.53 → close $57.53 +0.00; HALO×3 09:30 $111.56 → close $111.56 +0.00; MRVI×43 09:30 $7.65 → close $7.65 +0.00; OMER×21 09:30 $19.25 → close $19.25 +0.00; PDFS×8 09:30 $49.77 → close $49.77 +0.00; SFIX×172 09:30 $2.53 → close $2.53 +0.00; SHOO×9 09:30 $45.14 → close $45.14 +0.00; SN×2 09:30 $182.44 → close $182.44 +0.00; SONO×24 09:30 $17.85 → close $17.85 +0.00; TOST×14 09:30 $30.46 → close $30.46 +0.00; TXG×4 09:30 $90.04 → close $90.04 +0.00; WRBY×12 09:30 $27.47 → close $27.47 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,534.46 | ▲ 09:30 equity $9,263.99 vs yday $9,223.52 (+40.47) | 09:30 open · cash $3,534.46 (unchanged overnight, no fees) · equity $9,263.99 vs prior close $9,223.52 (+40.47) · 15 name(s) re-marked at the open (per-name table). AEHL×37 yday $8.59 → 09:30 $9.74 +42.55; ANNX×106 yday $4.13 → 09:30 $4.18 +5.30; BLFS×8 yday $39.22 → 09:30 $38.90 -2.56; DT×7 yday $57.53 → 09:30 $59.04 +10.57; HALO×3 yday $111.56 → 09:30 $109.93 -4.89; MRVI×43 yday $7.65 → 09:30 $7.67 +0.86; OMER×21 yday $19.25 → 09:30 $18.87 -7.98; PDFS×8 yday $49.77 → 09:30 $51.41 +13.12; SFIX×172 yday $2.53 → 09:30 $2.55 +3.44; SHOO×9 yday $45.14 → 09:30 $44.46 -6.12; SN×2 yday $182.44 → 09:30 $182.44 +0.00; SONO×24 yday $17.85 → 09:30 $18.09 +5.76; TOST×14 yday $30.46 → 09:30 $29.05 -19.74; TXG×4 yday $90.04 → 09:30 $89.30 -2.96; WRBY×12 yday $27.47 → 09:30 $27.73 +3.12 | — |
| 2026-10-01 09:30 ET | **SELL** | `AEHL` | 37 | $9.74 | $2.12 | $+21.31 | $3,892.72 | ▲ +21.31 after sell → book $9,261.87; vs 09:30 mark -2.12 | exit unpriced hold on first bar after 4 sess | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `ANNX` | 106 | $4.18 | $2.34 | $+15.50 | $4,333.46 | ▲ +15.50 after sell → book $9,259.53; vs 09:30 mark -2.34 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `BLFS` | 8 | $38.90 | $2.03 | $-0.93 | $4,642.63 | ▼ -0.93 after sell → book $9,257.50; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DT` | 7 | $59.04 | $2.03 | $+7.51 | $5,053.88 | ▲ +7.51 after sell → book $9,255.47; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `HALO` | 3 | $109.93 | $2.02 | $-24.78 | $5,381.65 | ▼ -24.78 after sell → book $9,253.45; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MRVI` | 43 | $7.67 | $2.14 | $-3.40 | $5,709.32 | ▼ -3.40 after sell → book $9,251.31; vs 09:30 mark -2.14 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `OMER` | 21 | $18.87 | $2.07 | $-41.51 | $6,103.52 | ▼ -41.51 after sell → book $9,249.24; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 8 | $51.41 | $2.03 | $+5.23 | $6,512.76 | ▲ +5.23 after sell → book $9,247.20; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SFIX` | 172 | $2.55 | $2.54 | $+8.71 | $6,948.82 | ▲ +8.71 after sell → book $9,244.66; vs 09:30 mark -2.54 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SHOO` | 9 | $44.46 | $2.04 | $-9.45 | $7,346.92 | ▼ -9.45 after sell → book $9,242.62; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 24 | $18.09 | $2.08 | $+3.78 | $7,779.00 | ▲ +3.78 after sell → book $9,240.54; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `TOST` | 14 | $29.05 | $2.05 | $-22.98 | $8,183.65 | ▼ -22.98 after sell → book $9,238.49; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `TXG` | 4 | $89.30 | $2.02 | $+18.14 | $8,538.83 | ▲ +18.14 after sell → book $9,236.47; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `WRBY` | 12 | $27.73 | $2.05 | $+13.45 | $8,869.54 | ▲ +13.45 after sell → book $9,234.42; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `IT` | 3 | $196.19 | $2.00 | — | $8,278.97 | — | deploy half leftover; list flatten; ret5=+4.5; leftover $633.54 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 6 | $92.93 | $2.01 | — | $7,719.38 | — | deploy half leftover; list flatten; 🔵; ret5=+0.2; leftover $633.54 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 16 | $38.99 | $2.04 | — | $7,093.50 | — | deploy half leftover; list flatten; 🔵; ret5=-2.4; leftover $633.54 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 44 | $14.27 | $2.12 | — | $6,463.50 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $633.54 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 29 | $21.28 | $2.08 | — | $5,844.31 | — | deploy half leftover; list flatten; 🔵; ret5=+5.7; leftover $633.54 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 570 | $1.11 | $7.35 | — | $5,204.25 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; leftover $633.54 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IVA` | 187 | $3.38 | $2.55 | — | $4,570.58 | — | deploy half leftover; list probable,yday_gainer; ret5=+7.0; leftover $633.54 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,570.58 | ▲ close $9,245.88 vs 09:30 $9,263.99 (session +31.60) | 16:00 close · cash $4,570.58 · equity $9,245.88 vs 09:30 $9,263.99 (-18.11; session marks +31.60) · 8 name(s) marked open→close (per-name table). SN×2 09:30 $182.44 → close $182.44 +0.00; IT×3 09:30 $196.19 → close $192.80 -10.17; KSPI×6 09:30 $92.93 → close $92.02 -5.46; IOT×16 09:30 $38.99 → close $40.04 +16.80; AVPT×44 09:30 $14.27 → close $14.08 -8.36; RELY×29 09:30 $21.28 → close $21.48 +5.80; TLSA×570 09:30 $1.11 → close $1.14 +17.10; IVA×187 09:30 $3.38 → close $3.46 +15.89 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,570.58 | ▲ 09:30 equity $9,301.47 vs yday $9,245.88 (+55.59) | 09:30 open · cash $4,570.58 (unchanged overnight, no fees) · equity $9,301.47 vs prior close $9,245.88 (+55.59) · 8 name(s) re-marked at the open (per-name table). AVPT×44 yday $14.08 → 09:30 $14.22 +6.16; IOT×16 yday $40.04 → 09:30 $40.41 +5.92; IT×3 yday $192.80 → 09:30 $192.74 -0.18; IVA×187 yday $3.46 → 09:30 $3.57 +20.57; KSPI×6 yday $92.02 → 09:30 $92.05 +0.18; RELY×29 yday $21.48 → 09:30 $21.80 +9.28; SN×2 yday $182.44 → 09:30 $183.57 +2.26; TLSA×570 yday $1.14 → 09:30 $1.16 +11.40 | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 2 | $183.57 | $2.02 | $-4.97 | $4,935.70 | ▼ -4.97 after sell → book $9,299.45; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **BUY** | `VEEV` | 1 | $283.10 | $1.99 | — | $4,650.61 | — | deploy half leftover; list flatten; 🔵; ret5=+2.6; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CORT` | 2 | $114.38 | $2.00 | — | $4,419.85 | — | deploy half leftover; list flatten; 🔵; ret5=-3.8; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ILMN` | 1 | $265.91 | $1.99 | — | $4,151.95 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-3.0; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 4 | $66.33 | $2.00 | — | $3,884.63 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 11 | $27.63 | $2.02 | — | $3,578.68 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.6; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `BLFS` | 8 | $37.02 | $2.01 | — | $3,280.50 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-4.7; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 5 | $52.42 | $2.00 | — | $3,016.40 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.6; leftover $308.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `QNC` | 182 | $1.69 | $2.54 | — | $2,706.28 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-7.9; leftover $308.48 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,706.28 | ▼ close $9,246.77 vs 09:30 $9,301.47 (session -36.12) | 16:00 close · cash $2,706.28 · equity $9,246.77 vs 09:30 $9,301.47 (-54.70; session marks -36.12) · 15 name(s) marked open→close (per-name table). AVPT×44 09:30 $14.22 → close $14.07 -6.60; IOT×16 09:30 $40.41 → close $41.12 +11.36; IT×3 09:30 $192.74 → close $184.80 -23.82; IVA×187 09:30 $3.57 → close $3.56 -1.87; KSPI×6 09:30 $92.05 → close $94.05 +12.00; RELY×29 09:30 $21.80 → close $21.77 -0.87; TLSA×570 09:30 $1.16 → close $1.12 -22.80; VEEV×1 09:30 $283.10 → close $273.33 -9.77; CORT×2 09:30 $114.38 → close $116.23 +3.70; ILMN×1 09:30 $265.91 → close $273.04 +7.13; CDNA×4 09:30 $66.33 → close $67.15 +3.28; WRBY×11 09:30 $27.63 → close $27.03 -6.60; BLFS×8 09:30 $37.02 → close $37.30 +2.24; ETON×5 09:30 $52.42 → close $55.36 +14.70; QNC×182 09:30 $1.69 → close $1.59 -18.20 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,706.28 | ▲ 09:30 equity $9,255.58 vs yday $9,246.77 (+8.81) | 09:30 open · cash $2,706.28 (unchanged overnight, no fees) · equity $9,255.58 vs prior close $9,246.77 (+8.81) · 15 name(s) re-marked at the open (per-name table). AVPT×44 yday $14.07 → 09:30 $13.91 -7.04; BLFS×8 yday $37.30 → 09:30 $37.16 -1.12; CDNA×4 yday $67.15 → 09:30 $66.90 -1.00; CORT×2 yday $116.23 → 09:30 $115.59 -1.28; ETON×5 yday $55.36 → 09:30 $55.85 +2.45; ILMN×1 yday $273.04 → 09:30 $273.00 -0.04; IOT×16 yday $41.12 → 09:30 $42.00 +14.08; IT×3 yday $184.80 → 09:30 $185.07 +0.81; IVA×187 yday $3.56 → 09:30 $3.52 -7.48; KSPI×6 yday $94.05 → 09:30 $94.50 +2.70; QNC×182 yday $1.59 → 09:30 $1.62 +5.46; RELY×29 yday $21.77 → 09:30 $21.76 -0.29; TLSA×570 yday $1.12 → 09:30 $1.12 +0.00; VEEV×1 yday $273.33 → 09:30 $275.00 +1.67; WRBY×11 yday $27.03 → 09:30 $27.02 -0.11 | — |
| 2026-10-05 09:30 ET | **BUY** | `COP` | 1 | $127.00 | $1.27 | — | $2,578.01 | — | deploy half leftover; list flatten; ret5=-0.4; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `DVN` | 3 | $47.64 | $1.44 | — | $2,433.65 | — | deploy half leftover; list flatten; ret5=+1.3; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EXE` | 1 | $84.70 | $0.85 | — | $2,348.10 | — | deploy half leftover; list flatten; ret5=-1.5; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 4 | $38.10 | $1.54 | — | $2,194.16 | — | deploy half leftover; list flatten; ret5=-1.0; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SM` | 4 | $35.27 | $1.42 | — | $2,051.66 | — | deploy half leftover; list flatten; ret5=+3.9; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `MTDR` | 3 | $53.35 | $1.61 | — | $1,890.00 | — | deploy half leftover; list flatten; ret5=+3.8; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `GPRK` | 15 | $10.87 | $1.68 | — | $1,725.27 | — | deploy half leftover; list flatten; ret5=-3.1; leftover $169.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 16 | $10.26 | $1.69 | — | $1,559.43 | — | deploy half leftover; list flatten; ret5=-2.7; leftover $169.14 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,559.43 | ▲ close $9,364.01 vs 09:30 $9,255.58 (session +119.92) | 16:00 close · cash $1,559.43 · equity $9,364.01 vs 09:30 $9,255.58 (+108.43; session marks +119.92) · 23 name(s) marked open→close (per-name table). AVPT×44 09:30 $13.91 → close $14.66 +33.00; BLFS×8 09:30 $37.16 → close $38.61 +11.60; CDNA×4 09:30 $66.90 → close $69.85 +11.80; CORT×2 09:30 $115.59 → close $122.03 +12.88; ETON×5 09:30 $55.85 → close $55.62 -1.15; ILMN×1 09:30 $273.00 → close $293.69 +20.69; IOT×16 09:30 $42.00 → close $42.39 +6.24; IT×3 09:30 $185.07 → close $187.76 +8.07; IVA×187 09:30 $3.52 → close $3.48 -7.48; KSPI×6 09:30 $94.50 → close $93.98 -3.12; QNC×182 09:30 $1.62 → close $1.64 +3.64; RELY×29 09:30 $21.76 → close $22.91 +33.35; TLSA×570 09:30 $1.12 → close $1.07 -28.50; VEEV×1 09:30 $275.00 → close $283.45 +8.45; WRBY×11 09:30 $27.02 → close $26.65 -4.07; COP×1 09:30 $127.00 → close $128.40 +1.40; DVN×3 09:30 $47.64 → close $47.98 +1.02; EXE×1 09:30 $84.70 → close $86.43 +1.73; RRC×4 09:30 $38.10 → close $38.67 +2.28; SM×4 09:30 $35.27 → close $35.05 -0.88; MTDR×3 09:30 $53.35 → close $53.01 -1.02; GPRK×15 09:30 $10.87 → close $11.28 +6.15; OBE×16 09:30 $10.26 → close $10.50 +3.84 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,559.43 | ▲ 09:30 equity $9,414.02 vs yday $9,364.01 (+50.01) | 09:30 open · cash $1,559.43 (unchanged overnight, no fees) · equity $9,414.02 vs prior close $9,364.01 (+50.01) · 23 name(s) re-marked at the open (per-name table). AVPT×44 yday $14.66 → 09:30 $14.77 +4.84; BLFS×8 yday $38.61 → 09:30 $38.61 +0.00; CDNA×4 yday $69.85 → 09:30 $70.89 +4.16; COP×1 yday $128.40 → 09:30 $128.19 -0.21; CORT×2 yday $122.03 → 09:30 $122.20 +0.34; DVN×3 yday $47.98 → 09:30 $47.48 -1.51; ETON×5 yday $55.62 → 09:30 $56.15 +2.65; EXE×1 yday $86.43 → 09:30 $86.38 -0.05; GPRK×15 yday $11.28 → 09:30 $11.32 +0.60; ILMN×1 yday $293.69 → 09:30 $301.58 +7.89; IOT×16 yday $42.39 → 09:30 $42.70 +4.96; IT×3 yday $187.76 → 09:30 $188.47 +2.13; IVA×187 yday $3.48 → 09:30 $3.53 +9.35; KSPI×6 yday $93.98 → 09:30 $94.50 +3.12; MTDR×3 yday $53.01 → 09:30 $52.56 -1.35; OBE×16 yday $10.50 → 09:30 $10.44 -0.96; QNC×182 yday $1.64 → 09:30 $1.63 -1.82; RELY×29 yday $22.91 → 09:30 $23.12 +6.09; RRC×4 yday $38.67 → 09:30 $38.71 +0.16; SM×4 yday $35.05 → 09:30 $34.90 -0.60; TLSA×570 yday $1.07 → 09:30 $1.08 +5.70; VEEV×1 yday $283.45 → 09:30 $285.34 +1.89; WRBY×11 yday $26.65 → 09:30 $26.89 +2.64 | — |
| 2026-10-06 09:30 ET | **BUY** | `TOST` | 4 | $30.07 | $1.21 | — | $1,437.94 | — | deploy half leftover; list flatten; ⚪; ret5=-1.1; leftover $129.95 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PDFS` | 2 | $56.89 | $1.14 | — | $1,323.01 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+12.7; leftover $129.95 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `EBS` | 18 | $7.16 | $1.34 | — | $1,192.79 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.5; leftover $129.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ARCT` | 8 | $14.70 | $1.20 | — | $1,073.99 | — | deploy half leftover; list probable,yday_gainer; ret5=+1.4; leftover $129.95 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,073.99 | ▼ close $9,290.64 vs 09:30 $9,414.02 (session -118.48) | 16:00 close · cash $1,073.99 · equity $9,290.64 vs 09:30 $9,414.02 (-123.38; session marks -118.48) · 27 name(s) marked open→close (per-name table). AVPT×44 09:30 $14.77 → close $14.59 -7.92; BLFS×8 09:30 $38.61 → close $38.61 +0.00; CDNA×4 09:30 $70.89 → close $63.79 -28.40; COP×1 09:30 $128.19 → close $129.35 +1.16; CORT×2 09:30 $122.20 → close $119.57 -5.26; DVN×3 09:30 $47.48 → close $48.02 +1.64; ETON×5 09:30 $56.15 → close $54.77 -6.90; EXE×1 09:30 $86.38 → close $88.23 +1.85; GPRK×15 09:30 $11.32 → close $11.44 +1.80; ILMN×1 09:30 $301.58 → close $273.54 -28.04; IOT×16 09:30 $42.70 → close $41.74 -15.36; IT×3 09:30 $188.47 → close $184.92 -10.65; IVA×187 09:30 $3.53 → close $3.46 -13.09; KSPI×6 09:30 $94.50 → close $93.16 -8.04; MTDR×3 09:30 $52.56 → close $52.91 +1.05; OBE×16 09:30 $10.44 → close $10.62 +2.88; QNC×182 09:30 $1.63 → close $1.63 +0.00; RELY×29 09:30 $23.12 → close $23.20 +2.32; RRC×4 09:30 $38.71 → close $40.02 +5.24; SM×4 09:30 $34.90 → close $35.10 +0.80; TLSA×570 09:30 $1.08 → close $1.11 +17.10; VEEV×1 09:30 $285.34 → close $283.50 -1.84; WRBY×11 09:30 $26.89 → close $26.23 -7.26; TOST×4 09:30 $30.07 → close $30.25 +0.72; PDFS×2 09:30 $56.89 → close $54.44 -4.90; EBS×18 09:30 $7.16 → close $6.95 -3.78; ARCT×8 09:30 $14.70 → close $13.00 -13.60 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,073.99 | ▼ 09:30 equity $9,259.71 vs yday $9,290.64 (-30.93) | 09:30 open · cash $1,073.99 (unchanged overnight, no fees) · equity $9,259.71 vs prior close $9,290.64 (-30.93) · 27 name(s) re-marked at the open (per-name table). ARCT×8 yday $13.00 → 09:30 $12.71 -2.32; AVPT×44 yday $14.59 → 09:30 $14.51 -3.52; BLFS×8 yday $38.61 → 09:30 $38.61 +0.00; CDNA×4 yday $63.79 → 09:30 $61.38 -9.63; COP×1 yday $129.35 → 09:30 $131.29 +1.94; CORT×2 yday $119.57 → 09:30 $118.60 -1.94; DVN×3 yday $48.02 → 09:30 $48.40 +1.14; EBS×18 yday $6.95 → 09:30 $7.05 +1.80; ETON×5 yday $54.77 → 09:30 $53.84 -4.65; EXE×1 yday $88.23 → 09:30 $88.97 +0.74; GPRK×15 yday $11.44 → 09:30 $11.50 +0.90; ILMN×1 yday $273.54 → 09:30 $271.61 -1.93; IOT×16 yday $41.74 → 09:30 $41.60 -2.24; IT×3 yday $184.92 → 09:30 $186.51 +4.77; IVA×187 yday $3.46 → 09:30 $3.46 +0.00; KSPI×6 yday $93.16 → 09:30 $92.70 -2.76; MTDR×3 yday $52.91 → 09:30 $53.44 +1.59; OBE×16 yday $10.62 → 09:30 $10.72 +1.60; PDFS×2 yday $54.44 → 09:30 $52.54 -3.80; QNC×182 yday $1.63 → 09:30 $1.60 -5.46; RELY×29 yday $23.20 → 09:30 $22.96 -6.81; RRC×4 yday $40.02 → 09:30 $40.20 +0.72; SM×4 yday $35.10 → 09:30 $35.46 +1.44; TLSA×570 yday $1.11 → 09:30 $1.11 +0.00; TOST×4 yday $30.25 → 09:30 $30.13 -0.48; VEEV×1 yday $283.50 → 09:30 $284.23 +0.73; WRBY×11 yday $26.23 → 09:30 $25.98 -2.75 | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,073.99 | ▼ close $9,182.58 vs 09:30 $9,259.71 (session -77.14) | 16:00 close · cash $1,073.99 · equity $9,182.58 vs 09:30 $9,259.71 (-77.13; session marks -77.14) · 27 name(s) marked open→close (per-name table). ARCT×8 09:30 $12.71 → close $12.77 +0.44; AVPT×44 09:30 $14.51 → close $14.28 -10.12; BLFS×8 09:30 $38.61 → close $38.61 +0.00; CDNA×4 09:30 $61.38 → close $63.04 +6.63; COP×1 09:30 $131.29 → close $129.84 -1.45; CORT×2 09:30 $118.60 → close $119.60 +2.00; DVN×3 09:30 $48.40 → close $47.88 -1.56; EBS×18 09:30 $7.05 → close $7.07 +0.36; ETON×5 09:30 $53.84 → close $54.88 +5.20; EXE×1 09:30 $88.97 → close $88.12 -0.85; GPRK×15 09:30 $11.50 → close $11.11 -5.85; ILMN×1 09:30 $271.61 → close $267.76 -3.85; IOT×16 09:30 $41.60 → close $40.51 -17.44; IT×3 09:30 $186.51 → close $185.77 -2.22; IVA×187 09:30 $3.46 → close $3.37 -17.76; KSPI×6 09:30 $92.70 → close $91.87 -4.98; MTDR×3 09:30 $53.44 → close $52.85 -1.77; OBE×16 09:30 $10.72 → close $10.63 -1.44; PDFS×2 09:30 $52.54 → close $53.17 +1.26; QNC×182 09:30 $1.60 → close $1.56 -7.28; RELY×29 09:30 $22.96 → close $22.93 -1.02; RRC×4 09:30 $40.20 → close $39.83 -1.48; SM×4 09:30 $35.46 → close $35.25 -0.84; TLSA×570 09:30 $1.11 → close $1.10 -5.70; TOST×4 09:30 $30.13 → close $30.55 +1.68; VEEV×1 09:30 $284.23 → close $282.50 -1.73; WRBY×11 09:30 $25.98 → close $25.31 -7.37 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,073.99 | ▲ 09:30 equity $9,182.58 vs yday $9,182.58 (-0.00) | 09:30 open · cash $1,073.99 (unchanged overnight, no fees) · equity $9,182.58 vs prior close $9,182.58 (-0.00) · 27 name(s) re-marked at the open (per-name table). ARCT×8 yday $12.77 → 09:30 $12.77 +0.00; AVPT×44 yday $14.28 → 09:30 $14.28 +0.00; BLFS×8 yday $38.61 → 09:30 $38.61 +0.00; CDNA×4 yday $63.04 → 09:30 $63.04 +0.00; COP×1 yday $129.84 → 09:30 $129.84 +0.00; CORT×2 yday $119.60 → 09:30 $119.60 +0.00; DVN×3 yday $47.88 → 09:30 $47.88 +0.00; EBS×18 yday $7.07 → 09:30 $7.07 +0.00; ETON×5 yday $54.88 → 09:30 $54.88 +0.00; EXE×1 yday $88.12 → 09:30 $88.12 +0.00; GPRK×15 yday $11.11 → 09:30 $11.11 +0.00; ILMN×1 yday $267.76 → 09:30 $267.76 +0.00; IOT×16 yday $40.51 → 09:30 $40.51 +0.00; IT×3 yday $185.77 → 09:30 $185.77 +0.00; IVA×187 yday $3.37 → 09:30 $3.37 +0.00; KSPI×6 yday $91.87 → 09:30 $91.87 +0.00; MTDR×3 yday $52.85 → 09:30 $52.85 +0.00; OBE×16 yday $10.63 → 09:30 $10.63 +0.00; PDFS×2 yday $53.17 → 09:30 $53.17 +0.00; QNC×182 yday $1.56 → 09:30 $1.56 +0.00; RELY×29 yday $22.93 → 09:30 $22.93 +0.00; RRC×4 yday $39.83 → 09:30 $39.83 +0.00; SM×4 yday $35.25 → 09:30 $35.25 +0.00; TLSA×570 yday $1.10 → 09:30 $1.10 +0.00; TOST×4 yday $30.55 → 09:30 $30.55 +0.00; VEEV×1 yday $282.50 → 09:30 $282.50 +0.00; WRBY×11 yday $25.31 → 09:30 $25.31 +0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,073.99 | ▲ close $9,182.58 vs 09:30 $9,182.58 (session +0.00) | 16:00 close · cash $1,073.99 · equity $9,182.58 vs 09:30 $9,182.58 (-0.00; session marks +0.00) · 27 name(s) marked open→close (per-name table). ARCT×8 09:30 $12.77 → close $12.77 +0.00; AVPT×44 09:30 $14.28 → close $14.28 +0.00; BLFS×8 09:30 $38.61 → close $38.61 +0.00; CDNA×4 09:30 $63.04 → close $63.04 +0.00; COP×1 09:30 $129.84 → close $129.84 +0.00; CORT×2 09:30 $119.60 → close $119.60 +0.00; DVN×3 09:30 $47.88 → close $47.88 +0.00; EBS×18 09:30 $7.07 → close $7.07 +0.00; ETON×5 09:30 $54.88 → close $54.88 +0.00; EXE×1 09:30 $88.12 → close $88.12 +0.00; GPRK×15 09:30 $11.11 → close $11.11 +0.00; ILMN×1 09:30 $267.76 → close $267.76 +0.00; IOT×16 09:30 $40.51 → close $40.51 +0.00; IT×3 09:30 $185.77 → close $185.77 +0.00; IVA×187 09:30 $3.37 → close $3.37 +0.00; KSPI×6 09:30 $91.87 → close $91.87 +0.00; MTDR×3 09:30 $52.85 → close $52.85 +0.00; OBE×16 09:30 $10.63 → close $10.63 +0.00; PDFS×2 09:30 $53.17 → close $53.17 +0.00; QNC×182 09:30 $1.56 → close $1.56 +0.00; RELY×29 09:30 $22.93 → close $22.93 +0.00; RRC×4 09:30 $39.83 → close $39.83 +0.00; SM×4 09:30 $35.25 → close $35.25 +0.00; TLSA×570 09:30 $1.10 → close $1.10 +0.00; TOST×4 09:30 $30.55 → close $30.55 +0.00; VEEV×1 09:30 $282.50 → close $282.50 +0.00; WRBY×11 09:30 $25.31 → close $25.31 +0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,073.99 | ▲ 09:30 equity $9,308.99 vs yday $9,182.58 (+126.41) | 09:30 open · cash $1,073.99 (unchanged overnight, no fees) · equity $9,308.99 vs prior close $9,182.58 (+126.41) · 27 name(s) re-marked at the open (per-name table). ARCT×8 yday $12.77 → 09:30 $13.18 +3.32; AVPT×44 yday $14.28 → 09:30 $14.44 +7.04; BLFS×8 yday $38.61 → 09:30 $38.61 +0.00; CDNA×4 yday $63.04 → 09:30 $62.21 -3.32; COP×1 yday $129.84 → 09:30 $133.85 +4.01; CORT×2 yday $119.60 → 09:30 $118.00 -3.20; DVN×3 yday $47.88 → 09:30 $48.83 +2.85; EBS×18 yday $7.07 → 09:30 $6.73 -6.12; ETON×5 yday $54.88 → 09:30 $54.86 -0.10; EXE×1 yday $88.12 → 09:30 $88.94 +0.82; GPRK×15 yday $11.11 → 09:30 $11.40 +4.35; ILMN×1 yday $267.76 → 09:30 $268.21 +0.45; IOT×16 yday $40.51 → 09:30 $41.71 +19.20; IT×3 yday $185.77 → 09:30 $194.05 +24.84; IVA×187 yday $3.37 → 09:30 $3.35 -2.81; KSPI×6 yday $91.87 → 09:30 $93.64 +10.62; MTDR×3 yday $52.85 → 09:30 $54.40 +4.65; OBE×16 yday $10.63 → 09:30 $11.05 +6.72; PDFS×2 yday $53.17 → 09:30 $53.73 +1.12; QNC×182 yday $1.56 → 09:30 $1.58 +3.64; RELY×29 yday $22.93 → 09:30 $23.50 +16.53; RRC×4 yday $39.83 → 09:30 $40.62 +3.16; SM×4 yday $35.25 → 09:30 $36.92 +6.68; TLSA×570 yday $1.10 → 09:30 $1.14 +22.80; TOST×4 yday $30.55 → 09:30 $30.61 +0.24; VEEV×1 yday $282.50 → 09:30 $285.71 +3.21; WRBY×11 yday $25.31 → 09:30 $24.92 -4.29 | — |
| 2026-10-09 09:30 ET | **SELL** | `ARCT` | 8 | $13.18 | $1.10 | $-14.46 | $1,178.33 | ▼ -14.46 after sell → book $9,307.89; vs 09:30 mark -1.10 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `AVPT` | 44 | $14.44 | $2.14 | $+3.22 | $1,811.55 | ▲ +3.22 after sell → book $9,305.75; vs 09:30 mark -2.14 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `CDNA` | 4 | $62.21 | $2.02 | $-20.50 | $2,058.37 | ▼ -20.50 after sell → book $9,303.73; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SELL** | `COP` | 1 | $133.85 | $1.36 | $+4.22 | $2,190.86 | ▲ +4.22 after sell → book $9,302.37; vs 09:30 mark -1.36 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `CORT` | 2 | $118.00 | $2.02 | $+3.23 | $2,424.84 | ▲ +3.23 after sell → book $9,300.35; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `DVN` | 3 | $48.83 | $1.49 | $+0.64 | $2,569.84 | ▲ +0.64 after sell → book $9,298.86; vs 09:30 mark -1.49 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `EBS` | 18 | $6.73 | $1.29 | $-10.37 | $2,689.69 | ▼ -10.37 after sell → book $9,297.57; vs 09:30 mark -1.29 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `ETON` | 5 | $54.86 | $2.02 | $+8.17 | $2,961.97 | ▲ +8.17 after sell → book $9,295.55; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 5 sess | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SELL** | `EXE` | 1 | $88.94 | $0.91 | $+2.48 | $3,049.99 | ▲ +2.48 after sell → book $9,294.63; vs 09:30 mark -0.92 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `GPRK` | 15 | $11.40 | $1.77 | $+4.50 | $3,219.22 | ▲ +4.50 after sell → book $9,292.86; vs 09:30 mark -1.77 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `ILMN` | 1 | $268.21 | $2.01 | $-1.71 | $3,485.42 | ▼ -1.71 after sell → book $9,290.85; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IOT` | 16 | $41.71 | $2.06 | $+39.42 | $4,150.72 | ▲ +39.42 after sell → book $9,288.79; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IT` | 3 | $194.05 | $2.02 | $-10.44 | $4,730.85 | ▼ -10.44 after sell → book $9,286.77; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IVA` | 187 | $3.35 | $2.59 | $-9.82 | $5,354.71 | ▼ -9.82 after sell → book $9,284.18; vs 09:30 mark -2.59 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `KSPI` | 6 | $93.64 | $2.03 | $+0.22 | $5,914.52 | ▲ +0.22 after sell → book $9,282.15; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `MTDR` | 3 | $54.40 | $1.66 | $-0.12 | $6,076.06 | ▼ -0.12 after sell → book $9,280.49; vs 09:30 mark -1.66 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `OBE` | 16 | $11.05 | $1.84 | $+9.11 | $6,251.02 | ▲ +9.11 after sell → book $9,278.65; vs 09:30 mark -1.84 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `PDFS` | 2 | $53.73 | $1.10 | $-8.56 | $6,357.38 | ▼ -8.56 after sell → book $9,277.55; vs 09:30 mark -1.10 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `QNC` | 182 | $1.58 | $2.58 | $-25.13 | $6,642.36 | ▼ -25.13 after sell → book $9,274.97; vs 09:30 mark -2.58 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `RELY` | 29 | $23.50 | $2.10 | $+60.21 | $7,321.77 | ▲ +60.21 after sell → book $9,272.88; vs 09:30 mark -2.09 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `RRC` | 4 | $40.62 | $1.66 | $+6.89 | $7,482.59 | ▲ +6.89 after sell → book $9,271.22; vs 09:30 mark -1.66 | exit unpriced hold on first bar after 4 sess | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-09 09:30 ET | **SELL** | `SM` | 4 | $36.92 | $1.51 | $+3.67 | $7,628.76 | ▲ +3.67 after sell → book $9,269.71; vs 09:30 mark -1.51 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `TLSA` | 570 | $1.14 | $7.46 | $+2.29 | $8,271.10 | ▲ +2.29 after sell → book $9,262.25; vs 09:30 mark -7.46 | exit unpriced hold on first bar after 6 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `TOST` | 4 | $30.61 | $1.26 | $-0.31 | $8,392.29 | ▼ -0.31 after sell → book $9,261.00; vs 09:30 mark -1.25 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `VEEV` | 1 | $285.71 | $2.01 | $-1.40 | $8,675.99 | ▼ -1.40 after sell → book $9,258.99; vs 09:30 mark -2.01 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `WRBY` | 11 | $24.92 | $2.04 | $-33.88 | $8,948.06 | ▼ -33.88 after sell → book $9,256.94; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 5 sess | — |
| 2026-10-09 09:30 ET | **BUY** | `PACS` | 20 | $43.75 | $2.05 | — | $8,071.01 | — | deploy half leftover; list flatten; 🔵; ret5=+6.6; leftover $894.81 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-09 09:30 ET | **BUY** | `IPSC` | 612 | $1.46 | $7.89 | — | $7,169.60 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.3; leftover $894.81 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ACRS` | 237 | $3.76 | $3.06 | — | $6,275.42 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-14.1; leftover $894.81 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `SECZ` | 67 | $13.20 | $2.19 | — | $5,388.83 | — | deploy half leftover; list probable,yday_gainer; ret5=-7.9; leftover $894.81 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `CLVT` | 555 | $1.61 | $7.16 | — | $4,488.12 | — | deploy half leftover; list probable,yday_gainer; ret5=+1.9; leftover $894.81 | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,488.12 | ▲ close $9,289.84 vs 09:30 $9,308.99 (session +55.25) | 16:00 close · cash $4,488.12 · equity $9,289.84 vs 09:30 $9,308.99 (-19.15; session marks +55.25) · 6 name(s) marked open→close (per-name table). BLFS×8 09:30 $38.61 → close $38.61 +0.00; PACS×20 09:30 $43.75 → close $43.30 -9.00; IPSC×612 09:30 $1.46 → close $1.47 +6.12; ACRS×237 09:30 $3.76 → close $3.83 +16.59; SECZ×67 09:30 $13.20 → close $13.82 +41.54; CLVT×555 09:30 $1.61 → close $1.61 +0.00 | — |

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
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-25 | `HCA` | cash | leftover split 292.40 < 1 share @ 426.97 |
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
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `HCA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `HCA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `HCA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-02 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `URBN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PYXS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `URBN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `PYXS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `MRNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `MRNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
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
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `OVID` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ORCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COHU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `OVID` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `TWST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DELL` | cash | leftover split 321.18 < 1 share @ 593.15 |
| 2026-09-21 | `IQV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GNRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SWRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `HUM` | cash | leftover split 272.47 < 1 share @ 386.20 |
| 2026-09-22 | `IQV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ILMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `TWST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `AMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GNRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SWRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `ILMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `TWST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `AMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GNRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FIVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SWRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GNRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FIVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SWRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `MGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 2 | 2026-09-18 @ $108.55 | deploy half leftover; list flatten; ⚪; ret5=+21.3; leftover $321.18 |
| `GNRC` | 1 | 2026-09-18 @ $209.52 | deploy half leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $321.18 |
| `VICR` | 1 | 2026-09-18 @ $219.62 | deploy half leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $321.18 |
| `ECO` | 3 | 2026-09-18 @ $85.00 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $321.18 |
| `FIVN` | 9 | 2026-09-18 @ $34.44 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $321.18 |
| `TLSA` | 331 | 2026-09-18 @ $0.97 | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $321.18 |
| `SWRD` | 154 | 2026-09-18 @ $2.08 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $321.18 |
| `A` | 1 | 2026-09-21 @ $157.87 | deploy half leftover; list flatten; ret5=+6.5; leftover $272.47 |
| `DXCM` | 3 | 2026-09-21 @ $88.83 | deploy half leftover; list flatten; ret5=+7.6; leftover $272.47 |
| `MGTX` | 20 | 2026-09-21 @ $13.47 | deploy half leftover; list flatten; ret5=+3.6; leftover $272.47 |
| `CYPH` | 68 | 2026-09-21 @ $4.00 | deploy half leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $272.47 |
| `BKKT` | 29 | 2026-09-21 @ $9.31 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $272.47 |
| `USFD` | 1 | 2026-09-22 @ $93.97 | deploy half leftover; list flatten; ret5=-0.6; leftover $126.50 |
| `DEFT` | 218 | 2026-09-22 @ $0.58 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $126.50 |
| `HALO` | 4 | 2026-09-23 @ $116.85 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $522.91 |
| `ARQT` | 18 | 2026-09-23 @ $27.79 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $522.91 |
| `ADMA` | 53 | 2026-09-23 @ $9.81 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $522.91 |
| `FTRE` | 25 | 2026-09-23 @ $20.25 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $522.91 |
| `OMER` | 25 | 2026-09-23 @ $20.65 | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $522.91 |
