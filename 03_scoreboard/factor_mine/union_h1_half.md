# Factor mine action — `union_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **-7.35%** ($9,265) · signal-only (no cash/fees) was -0.80%. Starts YES **0/30**. Fills 247 · skips 107 · realized $-342.82.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,657.21.

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
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 10 | $59.65 | $2.04 | $-5.56 | $5,696.18 | ▼ -5.56 after sell → book $10,082.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 13 | $44.09 | $2.05 | $-28.65 | $6,267.30 | ▼ -28.65 after sell → book $10,080.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 12 | $55.29 | $2.05 | $+51.93 | $6,928.74 | ▲ +51.93 after sell → book $10,078.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 12 | $47.27 | $2.05 | $-33.23 | $7,493.93 | ▼ -33.23 after sell → book $10,076.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 53 | $12.40 | $2.17 | $+32.78 | $8,148.96 | ▲ +32.78 after sell → book $10,074.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 21 | $29.15 | $2.07 | $-16.52 | $8,759.04 | ▼ -16.52 after sell → book $10,071.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 771 | $0.93 | $9.62 | $+74.34 | $9,466.45 | ▲ +74.34 after sell → book $10,062.37; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 26 | $22.92 | $2.09 | $-14.82 | $10,060.28 | ▼ -14.82 after sell → book $10,060.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $9,698.46 | — | deploy half leftover; list flatten; 🔵; ret5=+5.9; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 4 | $146.90 | $2.00 | — | $9,108.86 | — | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 5 | $120.00 | $2.00 | — | $8,506.85 | — | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 1 | $330.91 | $1.99 | — | $8,173.95 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 10 | $57.61 | $2.02 | — | $7,595.83 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+5.7; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 69 | $9.01 | $2.20 | — | $6,971.94 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 671 | $0.94 | $8.30 | — | $6,334.91 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $628.77 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 419 | $1.50 | $5.41 | — | $5,701.01 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $628.77 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,701.01 | ▲ close $10,077.45 vs 09:30 $10,084.41 (session +43.09) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,701.01 | ▼ 09:30 equity $10,075.66 vs yday $10,077.45 (-1.79) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $6,066.87 | ▲ +4.04 after sell → book $10,073.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 4 | $149.37 | $2.02 | $+5.86 | $6,662.33 | ▲ +5.86 after sell → book $10,071.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 5 | $127.40 | $2.02 | $+32.97 | $7,297.31 | ▲ +32.97 after sell → book $10,069.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 1 | $336.94 | $2.01 | $+2.02 | $7,632.23 | ▲ +2.02 after sell → book $10,067.59; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 10 | $55.37 | $2.04 | $-26.46 | $8,183.89 | ▼ -26.46 after sell → book $10,065.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 69 | $9.22 | $2.22 | $+10.07 | $8,817.86 | ▲ +10.07 after sell → book $10,063.33; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 671 | $0.91 | $8.22 | $-36.65 | $9,418.23 | ▼ -36.65 after sell → book $10,055.11; vs 09:30 mark -8.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 419 | $1.52 | $5.48 | $-2.51 | $10,049.63 | ▼ -2.51 after sell → book $10,049.63; vs 09:30 mark -5.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 13 | $46.18 | $2.03 | — | $9,447.26 | — | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 4 | $142.77 | $2.00 | — | $8,874.18 | — | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 3 | $202.70 | $2.00 | — | $8,264.08 | — | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 155 | $4.05 | $2.46 | — | $7,633.87 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 74 | $8.46 | $2.21 | — | $7,005.62 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 6 | $90.54 | $2.01 | — | $6,460.37 | — | deploy half leftover; list flatten; ret5=-7.2; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 193 | $3.24 | $2.57 | — | $5,832.48 | — | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $628.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 130 | $4.81 | $2.38 | — | $5,204.80 | — | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $628.10 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,204.80 | ▲ close $10,048.64 vs 09:30 $10,075.66 (session +16.67) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,204.80 | ▼ 09:30 equity $10,025.02 vs yday $10,048.64 (-23.62) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 13 | $48.00 | $2.05 | $+19.58 | $5,826.76 | ▲ +19.58 after sell → book $10,022.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 4 | $148.04 | $2.02 | $+17.06 | $6,416.89 | ▲ +17.06 after sell → book $10,020.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 3 | $208.93 | $2.02 | $+14.67 | $7,041.66 | ▲ +14.67 after sell → book $10,018.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 155 | $3.72 | $2.49 | $-56.10 | $7,615.77 | ▼ -56.10 after sell → book $10,016.44; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 74 | $8.55 | $2.23 | $+2.21 | $8,246.24 | ▲ +2.21 after sell → book $10,014.21; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 6 | $93.44 | $2.03 | $+13.36 | $8,804.85 | ▲ +13.36 after sell → book $10,012.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 193 | $3.11 | $2.61 | $-30.27 | $9,402.47 | ▼ -30.27 after sell → book $10,009.57; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 130 | $4.67 | $2.41 | $-22.99 | $10,007.16 | ▼ -22.99 after sell → book $10,007.16; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.16 | ▲ close $10,007.16 vs 09:30 $10,025.02 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.16 | ▲ 09:30 equity $10,007.16 vs yday $10,007.16 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.16 | ▲ close $10,007.16 vs 09:30 $10,007.16 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.16 | ▲ 09:30 equity $10,007.16 vs yday $10,007.16 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,388.58 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,840.51 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,218.93 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,593.46 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,982.84 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,358.56 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,729.21 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $625.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,149.04 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $625.45 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,149.04 | ▲ close $10,102.66 vs 09:30 $10,007.16 (session +114.73) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,149.04 | ▲ 09:30 equity $10,234.89 vs yday $10,102.66 (+132.23) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,803.94 | ▲ +36.32 after sell → book $10,232.79; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,376.24 | ▲ +24.22 after sell → book $10,230.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,026.64 | ▲ +28.82 after sell → book $10,228.67; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,636.65 | ▼ -15.46 after sell → book $10,226.32; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,290.82 | ▲ +43.55 after sell → book $10,224.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,964.32 | ▲ +49.21 after sell → book $10,222.15; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,598.67 | ▲ +5.00 after sell → book $10,217.47; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,215.45 | ▲ +36.62 after sell → book $10,215.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,616.30 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,977.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,543.20 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,906.63 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,266.04 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,624.88 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,025.66 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $638.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,381.87 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $638.47 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,381.87 | ▲ close $10,319.23 vs 09:30 $10,234.89 (session +127.88) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,381.87 | ▲ 09:30 equity $10,502.37 vs yday $10,319.23 (+183.14) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $5,982.40 | ▲ +1.37 after sell → book $10,500.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.57 | $2.12 | $-27.53 | $6,593.37 | ▼ -27.53 after sell → book $10,498.23; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,025.41 | ▼ -2.55 after sell → book $10,496.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.33 | $2.18 | $+121.06 | $7,783.04 | ▲ +121.06 after sell → book $10,494.03; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.40 | $3.38 | $-24.77 | $8,398.86 | ▼ -24.77 after sell → book $10,490.65; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.88 | $4.32 | $-25.08 | $9,014.94 | ▼ -25.08 after sell → book $10,486.33; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.75 | $2.04 | $-13.76 | $9,600.40 | ▼ -13.76 after sell → book $10,484.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,477.97 | ▲ +233.78 after sell → book $10,477.97; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,477.97 | ▲ close $10,477.97 vs 09:30 $10,502.37 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,477.97 | ▲ 09:30 equity $10,477.97 vs yday $10,477.97 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 27 | $23.77 | $2.07 | — | $9,834.10 | — | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 59 | $10.98 | $2.17 | — | $9,184.12 | — | deploy half leftover; list flatten; 🔵; ret5=+1.2; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 10 | $61.19 | $2.02 | — | $8,570.20 | — | deploy half leftover; list flatten; 🔵; ret5=+7.4; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 78 | $8.35 | $2.22 | — | $7,916.67 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 132 | $4.94 | $2.39 | — | $7,262.21 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $6,833.24 | — | deploy half leftover; list flatten; ret5=+6.0; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 90 | $7.25 | $2.26 | — | $6,178.48 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $654.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 1829 | $0.36 | $12.03 | — | $5,511.67 | — | deploy half leftover; list probable,yday_gainer; ret5=-15.6; leftover $654.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,511.67 | ▲ close $10,570.90 vs 09:30 $10,477.97 (session +120.09) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,511.67 | ▲ 09:30 equity $10,571.01 vs yday $10,570.90 (+0.11) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 90 | $8.29 | $2.28 | $+89.06 | $6,255.48 | ▲ +89.06 after sell → book $10,568.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 1829 | $0.35 | $12.26 | $-33.44 | $6,888.86 | ▼ -33.44 after sell → book $10,556.47; vs 09:30 mark -12.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 55 | $31.21 | $2.15 | — | $5,170.16 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1722.22 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 154 | $11.12 | $2.45 | — | $3,455.23 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1722.22 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,455.23 | ▼ close $10,526.86 vs 09:30 $10,571.01 (session -25.01) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,455.23 | ▲ 09:30 equity $10,575.00 vs yday $10,526.86 (+48.14) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 59 | $10.63 | $2.19 | $-25.00 | $4,080.21 | ▼ -25.00 after sell → book $10,572.81; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 10 | $62.10 | $2.04 | $+5.04 | $4,699.17 | ▲ +5.04 after sell → book $10,570.77; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 78 | $8.49 | $2.25 | $+6.45 | $5,359.14 | ▲ +6.45 after sell → book $10,568.52; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 132 | $5.07 | $2.42 | $+12.36 | $6,025.96 | ▲ +12.36 after sell → book $10,566.10; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 1 | $424.61 | $2.01 | $-6.37 | $6,448.56 | ▼ -6.37 after sell → book $10,564.09; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 15 | $41.44 | $2.04 | — | $5,824.93 | — | deploy half leftover; list flatten; ret5=+3.1; leftover $644.86 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 44 | $14.42 | $2.12 | — | $5,188.32 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $644.86 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 248 | $2.60 | $3.20 | — | $4,540.32 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+13.0; leftover $644.86 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 49 | $12.98 | $2.14 | — | $3,902.17 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $644.86 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 66 | $9.68 | $2.19 | — | $3,261.10 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $644.86 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,261.10 | ▲ close $10,585.04 vs 09:30 $10,575.00 (session +32.63) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,261.10 | ▼ 09:30 equity $10,548.47 vs yday $10,585.04 (-36.57) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 55 | $30.53 | $2.18 | $-41.73 | $4,938.07 | ▼ -41.73 after sell → book $10,546.29; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 154 | $11.27 | $2.49 | $+18.16 | $6,671.16 | ▲ +18.16 after sell → book $10,543.80; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 49 | $13.05 | $2.16 | $-0.86 | $7,308.45 | ▼ -0.86 after sell → book $10,541.64; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 66 | $9.88 | $2.21 | $+8.80 | $7,958.32 | ▲ +8.80 after sell → book $10,539.43; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 30 | $32.90 | $2.08 | — | $6,969.24 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $994.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 63 | $15.66 | $2.18 | — | $5,980.48 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $994.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 12 | $79.42 | $2.03 | — | $5,025.42 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $994.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 299 | $3.32 | $3.86 | — | $4,028.88 | — | deploy half leftover; list probable,yday_gainer; ret5=+6.4; leftover $994.79 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,028.88 | ▼ close $10,338.12 vs 09:30 $10,548.47 (session -191.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,028.88 | ▲ 09:30 equity $10,344.14 vs yday $10,338.12 (+6.02) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 27 | $23.68 | $2.09 | $-6.59 | $4,666.15 | ▼ -6.59 after sell → book $10,342.05; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 15 | $42.00 | $2.06 | $+4.31 | $5,294.10 | ▲ +4.31 after sell → book $10,340.00; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 44 | $14.54 | $2.14 | $+1.02 | $5,931.71 | ▲ +1.02 after sell → book $10,337.85; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 248 | $2.58 | $3.25 | $-11.41 | $6,568.30 | ▼ -11.41 after sell → book $10,334.60; vs 09:30 mark -3.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 30 | $31.15 | $2.10 | $-56.68 | $7,500.70 | ▼ -56.68 after sell → book $10,332.50; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 63 | $14.44 | $2.20 | $-81.24 | $8,408.22 | ▼ -81.24 after sell → book $10,330.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 12 | $80.44 | $2.05 | $+8.17 | $9,371.46 | ▲ +8.17 after sell → book $10,328.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 299 | $3.20 | $3.92 | $-43.65 | $10,324.34 | ▼ -43.65 after sell → book $10,324.34; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.34 | ▲ close $10,324.34 vs 09:30 $10,344.14 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.34 | ▲ 09:30 equity $10,324.34 vs yday $10,324.34 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.34 | ▲ close $10,324.34 vs 09:30 $10,324.34 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.34 | ▲ 09:30 equity $10,324.34 vs yday $10,324.34 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,324.34 | ▲ close $10,324.34 vs 09:30 $10,324.34 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,324.34 | ▲ 09:30 equity $10,324.34 vs yday $10,324.34 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 12 | $52.88 | $2.03 | — | $9,687.75 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $42.93 | $2.04 | — | $9,041.77 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 177 | $3.63 | $2.52 | — | $8,396.74 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 80 | $8.03 | $2.23 | — | $7,752.11 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $132.45 | $2.00 | — | $7,220.31 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.45 | $2.11 | — | $6,584.74 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 4 | $145.94 | $2.00 | — | $5,998.96 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $645.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 38 | $16.77 | $2.10 | — | $5,359.60 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $645.27 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,359.60 | ▼ close $10,193.61 vs 09:30 $10,324.34 (session -113.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,359.60 | ▲ 09:30 equity $10,194.46 vs yday $10,193.61 (+0.85) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 15 | $41.50 | $2.06 | $-25.54 | $5,980.04 | ▼ -25.54 after sell → book $10,192.40; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 80 | $7.91 | $2.25 | $-14.08 | $6,610.59 | ▼ -14.08 after sell → book $10,190.15; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 4 | $130.03 | $2.02 | $-13.70 | $7,128.69 | ▼ -13.70 after sell → book $10,188.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 41 | $15.00 | $2.13 | $-22.70 | $7,741.55 | ▼ -22.70 after sell → book $10,185.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 4 | $153.62 | $2.02 | $+26.68 | $8,354.01 | ▲ +26.68 after sell → book $10,183.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 38 | $15.61 | $2.12 | $-48.31 | $8,945.07 | ▼ -48.31 after sell → book $10,181.85; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 295 | $2.52 | $3.81 | — | $8,197.86 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $745.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 111 | $6.71 | $2.32 | — | $7,450.73 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $745.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 392 | $1.90 | $5.06 | — | $6,700.87 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $745.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 155 | $4.78 | $2.46 | — | $5,957.52 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $745.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 468 | $1.59 | $6.04 | — | $5,207.36 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $745.42 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 65 | $11.31 | $2.19 | — | $4,470.03 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $745.42 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,470.03 | ▼ close $10,130.77 vs 09:30 $10,194.46 (session -29.22) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,470.03 | ▼ 09:30 equity $10,102.87 vs yday $10,130.77 (-27.90) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 12 | $54.31 | $2.05 | $+13.09 | $5,119.70 | ▲ +13.09 after sell → book $10,100.82; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 177 | $3.43 | $2.56 | $-40.48 | $5,724.25 | ▼ -40.48 after sell → book $10,098.26; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 295 | $2.38 | $3.86 | $-48.97 | $6,422.49 | ▼ -48.97 after sell → book $10,094.40; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 111 | $6.57 | $2.35 | $-20.21 | $7,149.40 | ▼ -20.21 after sell → book $10,092.04; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 392 | $2.00 | $5.13 | $+29.01 | $7,928.27 | ▲ +29.01 after sell → book $10,086.91; vs 09:30 mark -5.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 155 | $4.30 | $2.49 | $-79.35 | $8,592.28 | ▼ -79.35 after sell → book $10,084.42; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 468 | $1.63 | $6.12 | $+6.56 | $9,349.00 | ▲ +6.56 after sell → book $10,078.30; vs 09:30 mark -6.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 65 | $11.22 | $2.21 | $-10.24 | $10,076.09 | ▼ -10.24 after sell → book $10,076.09; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,076.09 | ▲ close $10,076.09 vs 09:30 $10,102.87 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.09 | ▲ 09:30 equity $10,076.09 vs yday $10,076.09 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,076.09 | ▲ close $10,076.09 vs 09:30 $10,076.09 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.09 | ▲ 09:30 equity $10,076.09 vs yday $10,076.09 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,076.09 | ▲ close $10,076.09 vs 09:30 $10,076.09 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.09 | ▲ 09:30 equity $10,076.09 vs yday $10,076.09 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 38 | $16.28 | $2.10 | — | $9,455.35 | — | deploy half leftover; list flatten; 🔵; ret5=-1.1; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 230 | $2.73 | $2.97 | — | $8,824.48 | — | deploy half leftover; list flatten; 🔵; ret5=-3.0; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 3 | $206.84 | $2.00 | — | $8,201.96 | — | deploy half leftover; list flatten; ret5=+8.3; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $7,706.67 | — | deploy half leftover; list flatten,earn_react; ⚪; ret5=+4.9; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 3 | $157.78 | $2.00 | — | $7,231.33 | — | deploy half leftover; list flatten; 🔵; ret5=+4.7; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 11 | $56.09 | $2.02 | — | $6,612.32 | — | deploy half leftover; list flatten; 🔵; ret5=+19.6; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 308 | $2.04 | $3.97 | — | $5,980.03 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $629.76 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 132 | $4.75 | $2.39 | — | $5,350.64 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $629.76 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,350.64 | ▼ close $10,050.32 vs 09:30 $10,076.09 (session -6.32) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,350.64 | ▼ 09:30 equity $9,917.04 vs yday $10,050.32 (-133.28) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 38 | $16.03 | $2.12 | $-13.73 | $5,957.66 | ▼ -13.73 after sell → book $9,914.92; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 230 | $2.75 | $3.02 | $-0.23 | $6,588.29 | ▼ -0.23 after sell → book $9,911.90; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 3 | $206.50 | $2.02 | $-5.04 | $7,205.77 | ▼ -5.04 after sell → book $9,909.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 3 | $141.42 | $2.02 | $-73.05 | $7,628.01 | ▼ -73.05 after sell → book $9,907.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 11 | $52.23 | $2.04 | $-46.53 | $8,200.50 | ▼ -46.53 after sell → book $9,905.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 308 | $2.01 | $4.03 | $-17.25 | $8,815.55 | ▼ -17.25 after sell → book $9,901.79; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 132 | $4.82 | $2.42 | $+4.44 | $9,449.37 | ▲ +4.44 after sell → book $9,899.37; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.37 | ▼ close $9,889.29 vs 09:30 $9,917.04 (session -10.08) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.37 | ▲ 09:30 equity $9,902.73 vs yday $9,889.29 (+13.44) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 3 | $151.12 | $2.02 | $-24.00 | $9,900.71 | ▼ -24.00 after sell → book $9,900.71; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,900.71 | ▲ close $9,900.71 vs 09:30 $9,902.73 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,900.71 | ▲ 09:30 equity $9,900.71 vs yday $9,900.71 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $9,356.93 | — | deploy half leftover; list flatten; ret5=+4.0; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 8 | $77.12 | $2.01 | — | $8,737.96 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+7.2; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 43 | $14.31 | $2.12 | — | $8,120.51 | — | deploy half leftover; list flatten; ret5=+4.8; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 16 | $36.46 | $2.04 | — | $7,535.11 | — | deploy half leftover; list flatten; 🔵; ret5=+2.9; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 33 | $18.61 | $2.09 | — | $6,918.89 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 33 | $18.21 | $2.09 | — | $6,315.87 | — | deploy half leftover; list probable,yday_gainer; ret5=-19.1; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 8 | $68.79 | $2.01 | — | $5,763.54 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $618.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 105 | $5.87 | $2.31 | — | $5,144.88 | — | deploy half leftover; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $618.79 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,144.88 | ▲ close $9,980.43 vs 09:30 $9,900.71 (session +96.39) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,144.88 | ▲ 09:30 equity $10,060.76 vs yday $9,980.43 (+80.33) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 2 | $273.15 | $2.02 | $+0.51 | $5,689.17 | ▲ +0.51 after sell → book $10,058.75; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 8 | $76.44 | $2.03 | $-9.49 | $6,298.65 | ▼ -9.49 after sell → book $10,056.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 43 | $14.33 | $2.14 | $-3.40 | $6,912.71 | ▼ -3.40 after sell → book $10,054.58; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 16 | $36.67 | $2.06 | $-0.74 | $7,497.37 | ▼ -0.74 after sell → book $10,052.52; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 33 | $22.46 | $2.11 | $+122.85 | $8,236.44 | ▲ +122.85 after sell → book $10,050.41; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 33 | $19.59 | $2.11 | $+41.34 | $8,880.80 | ▲ +41.34 after sell → book $10,048.30; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 8 | $72.70 | $2.03 | $+27.23 | $9,460.37 | ▲ +27.23 after sell → book $10,046.27; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 105 | $5.58 | $2.33 | $-35.09 | $10,043.93 | ▼ -35.09 after sell → book $10,043.93; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 2 | $233.85 | $2.00 | — | $9,574.24 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+11.7; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 4 | $151.43 | $2.00 | — | $8,966.52 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $8,374.07 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+17.7; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 61 | $10.25 | $2.17 | — | $7,746.65 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 82 | $7.59 | $2.24 | — | $7,122.03 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 17 | $34.93 | $2.04 | — | $6,526.18 | — | deploy half leftover; list flatten; ret5=+1.6; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 3692 | $0.17 | $17.35 | — | $5,881.19 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $627.75 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 39 | $15.87 | $2.11 | — | $5,260.15 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $627.75 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,260.15 | ▲ close $10,044.82 vs 09:30 $10,060.76 (session +32.80) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,260.15 | ▲ 09:30 equity $10,142.89 vs yday $10,044.82 (+98.07) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 2 | $249.13 | $2.02 | $+26.55 | $5,756.40 | ▲ +26.55 after sell → book $10,140.88; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 4 | $158.04 | $2.02 | $+22.42 | $6,386.54 | ▲ +22.42 after sell → book $10,138.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 4 | $146.50 | $2.02 | $-8.46 | $6,970.51 | ▼ -8.46 after sell → book $10,136.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 61 | $10.12 | $2.19 | $-12.30 | $7,585.64 | ▼ -12.30 after sell → book $10,134.64; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 82 | $7.98 | $2.26 | $+27.48 | $8,237.74 | ▲ +27.48 after sell → book $10,132.38; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 17 | $34.52 | $2.06 | $-11.07 | $8,822.52 | ▼ -11.07 after sell → book $10,130.32; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 3692 | $0.17 | $17.98 | $-35.33 | $9,432.19 | ▼ -35.33 after sell → book $10,112.35; vs 09:30 mark -17.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 39 | $17.44 | $2.13 | $+57.00 | $10,110.22 | ▲ +57.00 after sell → book $10,110.22; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 5 | $108.55 | $2.00 | — | $9,565.46 | — | deploy half leftover; list flatten; ⚪; ret5=+21.3; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $8,970.32 | — | deploy half leftover; list flatten,ohlc_hot; ret5=+16.1; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 3 | $209.52 | $2.00 | — | $8,339.76 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 2 | $219.62 | $2.00 | — | $7,898.53 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 7 | $85.00 | $2.01 | — | $7,301.51 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+18.3; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 18 | $34.44 | $2.04 | — | $6,679.55 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+14.0; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 651 | $0.97 | $8.27 | — | $6,039.81 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $631.89 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 303 | $2.08 | $3.91 | — | $5,405.66 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $631.89 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,405.66 | ▼ close $9,998.00 vs 09:30 $10,142.89 (session -87.99) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,405.66 | ▲ 09:30 equity $10,057.98 vs yday $9,998.00 (+59.98) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 5 | $107.57 | $2.02 | $-8.93 | $5,941.49 | ▼ -8.93 after sell → book $10,055.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 1 | $586.77 | $2.01 | $-10.39 | $6,526.25 | ▼ -10.39 after sell → book $10,053.95; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 3 | $210.00 | $2.02 | $-2.58 | $7,154.23 | ▼ -2.58 after sell → book $10,051.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 2 | $230.25 | $2.02 | $+17.25 | $7,612.71 | ▲ +17.25 after sell → book $10,049.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 7 | $82.83 | $2.03 | $-19.23 | $8,190.49 | ▼ -19.23 after sell → book $10,047.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 18 | $33.00 | $2.06 | $-30.03 | $8,782.43 | ▼ -30.03 after sell → book $10,045.82; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 651 | $0.94 | $8.19 | $-35.99 | $9,386.18 | ▼ -35.99 after sell → book $10,037.63; vs 09:30 mark -8.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 303 | $2.15 | $3.97 | $+13.33 | $10,033.66 | ▲ +13.33 after sell → book $10,033.66; vs 09:30 mark -3.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 3 | $157.87 | $2.00 | — | $9,558.05 | — | deploy half leftover; list flatten; ret5=+6.5; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $9,169.85 | — | deploy half leftover; list flatten; ret5=-5.8; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 7 | $88.83 | $2.01 | — | $8,546.03 | — | deploy half leftover; list flatten; ret5=+7.6; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 79 | $7.84 | $2.23 | — | $7,924.45 | — | deploy half leftover; list flatten; ret5=+13.6; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 60 | $10.43 | $2.17 | — | $7,296.48 | — | deploy half leftover; list flatten; ret5=+19.2; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 46 | $13.47 | $2.13 | — | $6,674.73 | — | deploy half leftover; list flatten; ret5=+3.6; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 156 | $4.00 | $2.46 | — | $6,048.27 | — | deploy half leftover; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $627.10 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 67 | $9.31 | $2.19 | — | $5,422.31 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $627.10 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,422.31 | ▼ close $9,875.06 vs 09:30 $10,057.98 (session -141.42) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,422.31 | ▲ 09:30 equity $9,891.62 vs yday $9,875.06 (+16.56) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 60 | $10.18 | $2.19 | $-19.36 | $6,030.92 | ▼ -19.36 after sell → book $9,889.43; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 156 | $3.51 | $2.49 | $-81.39 | $6,575.99 | ▼ -81.39 after sell → book $9,886.94; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 4 | $93.97 | $2.00 | — | $6,198.10 | — | deploy half leftover; list flatten; ret5=-0.6; leftover $411.00 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 708 | $0.58 | $6.23 | — | $5,781.23 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $411.00 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,781.23 | ▼ close $9,843.14 vs 09:30 $9,891.62 (session -35.56) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,781.23 | ▲ 09:30 equity $9,888.84 vs yday $9,843.14 (+45.70) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 1 | $370.00 | $2.01 | $-20.21 | $6,149.22 | ▼ -20.21 after sell → book $9,886.83; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 46 | $12.26 | $2.15 | $-59.94 | $6,711.03 | ▼ -59.94 after sell → book $9,884.68; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 67 | $9.50 | $2.21 | $+8.33 | $7,345.32 | ▲ +8.33 after sell → book $9,882.47; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 4 | $93.97 | $2.02 | $-4.02 | $7,719.18 | ▼ -4.02 after sell → book $9,880.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 708 | $0.57 | $6.32 | $-16.09 | $8,119.96 | ▼ -16.09 after sell → book $9,874.13; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 6 | $116.85 | $2.01 | — | $7,416.85 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+3.3; leftover $812.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 29 | $27.79 | $2.08 | — | $6,608.86 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+7.0; leftover $812.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 82 | $9.81 | $2.24 | — | $5,802.20 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+4.0; leftover $812.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 40 | $20.25 | $2.11 | — | $4,990.09 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+15.0; leftover $812.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 39 | $20.65 | $2.11 | — | $4,182.64 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $812.00 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,182.64 | ▼ close $9,705.19 vs 09:30 $9,888.84 (session -158.40) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,182.64 | ▼ 09:30 equity $9,674.15 vs yday $9,705.19 (-31.04) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 3 | $163.95 | $2.02 | $+14.22 | $4,672.47 | ▲ +14.22 after sell → book $9,672.13; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 7 | $87.67 | $2.03 | $-12.13 | $5,284.16 | ▼ -12.13 after sell → book $9,670.10; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 79 | $7.38 | $2.25 | $-40.82 | $5,864.93 | ▼ -40.82 after sell → book $9,667.85; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 6 | $112.22 | $2.03 | $-31.82 | $6,536.22 | ▼ -31.82 after sell → book $9,665.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 29 | $26.22 | $2.10 | $-49.70 | $7,294.51 | ▼ -49.70 after sell → book $9,663.73; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 82 | $9.67 | $2.26 | $-15.98 | $8,085.19 | ▼ -15.98 after sell → book $9,661.47; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 40 | $19.40 | $2.13 | $-38.24 | $8,859.06 | ▼ -38.24 after sell → book $9,659.34; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 39 | $20.52 | $2.13 | $-9.30 | $9,657.21 | ▼ -9.30 after sell → book $9,657.21; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,657.21 | ▲ close $9,657.21 vs 09:30 $9,674.15 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,265.20 | ▲ 09:30 equity $9,265.20 vs yday $9,265.20 (+0.00) | 09:30 open · cash $9,265.20 · no holdings · equity $9,265.20 vs prior close $9,265.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 5 | $115.36 | $2.00 | — | $8,686.40 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+5.1; leftover $579.08 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 28 | $20.61 | $2.07 | — | $8,107.24 | — | deploy half leftover; list flatten; 🔵; ret5=+9.1; leftover $579.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 15 | $38.51 | $2.04 | — | $7,527.56 | — | deploy half leftover; list flatten; ret5=+4.7; leftover $579.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 75 | $7.65 | $2.21 | — | $6,951.59 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+5.2; leftover $579.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 22 | $26.27 | $2.06 | — | $6,371.60 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $579.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 6 | $83.76 | $2.01 | — | $5,867.03 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $579.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 63 | $9.05 | $2.18 | — | $5,294.70 | — | deploy half leftover; list probable,yday_gainer; ret5=-27.1; leftover $579.08 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,294.70 | ▲ close $9,265.35 vs 09:30 $9,265.20 (session +14.72) | 16:00 close · cash $5,294.70 · equity $9,265.35 vs 09:30 $9,265.20 (+0.15; session marks +14.72) · 7 name(s) marked open→close (per-name table). HALO×5 09:30 $115.36 → close $113.90 -7.30; OMER×28 09:30 $20.61 → close $20.08 -14.84; BLFS×15 09:30 $38.51 → close $38.49 -0.30; MRVI×75 09:30 $7.65 → close $7.60 -3.75; WRBY×22 09:30 $26.27 → close $26.71 +9.68; TXG×6 09:30 $83.76 → close $85.71 +11.70; AEHL×63 09:30 $9.05 → close $9.36 +19.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
