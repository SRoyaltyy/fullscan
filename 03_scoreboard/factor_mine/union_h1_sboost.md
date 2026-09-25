# Factor mine action — `union_h1_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **-13.90%** ($8,610) · signal-only (no cash/fees) was -0.80%. Starts YES **0/30**. Fills 282 · skips 108 · realized $-308.78.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On a strong morning (S ≥ +5), spend 1.35× leftover and add 4 extra names — still cash-capped.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,691.24.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.82 | ▲ close $10,195.74 vs 09:30 $10,000.00 (session +227.81) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 18 | $59.65 | $2.06 | $-6.81 | $1,195.45 | ▼ -6.81 after sell → book $10,217.56; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 24 | $44.09 | $2.08 | $-49.50 | $2,251.53 | ▼ -49.50 after sell → book $10,215.48; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,410.55 | ▲ +93.88 after sell → book $10,213.41; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 22 | $47.27 | $2.08 | $-57.59 | $4,448.41 | ▼ -57.59 after sell → book $10,211.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $5,611.71 | ▲ +61.23 after sell → book $10,209.03; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 37 | $29.15 | $2.12 | $-26.05 | $6,688.14 | ▼ -26.05 after sell → book $10,206.91; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1371 | $0.93 | $17.10 | $+132.20 | $7,946.07 | ▲ +132.20 after sell → book $10,189.81; vs 09:30 mark -17.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $9,021.16 | ▼ -23.55 after sell → book $10,187.66; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 50 | $23.33 | $2.16 | $+61.70 | $10,185.50 | ▲ +61.70 after sell → book $10,185.50; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $9,463.84 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.9; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $8,727.34 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+3.6; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $7,885.33 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+0.6; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $7,221.51 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-8.6; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $6,412.94 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.7; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 94 | $9.01 | $2.27 | — | $5,563.73 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 905 | $0.94 | $11.19 | — | $4,704.55 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 565 | $1.50 | $7.29 | — | $3,849.76 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 57 | $14.80 | $2.16 | — | $3,004.00 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-9.9; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 196 | $4.31 | $2.58 | — | $2,156.66 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 41 | $20.60 | $2.11 | — | $1,309.95 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+4.4; leftover $848.79 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 203 | $4.18 | $2.62 | — | $458.79 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $848.79 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $458.79 | ▲ close $10,152.17 vs 09:30 $10,219.63 (session +6.94) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $458.79 | ▲ 09:30 equity $10,201.84 vs yday $10,152.17 (+49.67) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $1,192.53 | ▲ +12.09 after sell → book $10,199.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $1,937.36 | ▲ +8.32 after sell → book $10,197.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $2,827.13 | ▲ +47.76 after sell → book $10,195.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $3,498.99 | ▲ +8.05 after sell → book $10,193.76; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $4,272.12 | ▼ -35.44 after sell → book $10,191.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 94 | $9.22 | $2.30 | $+15.17 | $5,136.50 | ▲ +15.17 after sell → book $10,189.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 905 | $0.91 | $11.08 | $-49.43 | $5,946.25 | ▼ -49.43 after sell → book $10,178.32; vs 09:30 mark -11.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 565 | $1.52 | $7.39 | $-3.38 | $6,797.66 | ▼ -3.38 after sell → book $10,170.93; vs 09:30 mark -7.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 57 | $13.67 | $2.18 | $-68.75 | $7,574.67 | ▼ -68.75 after sell → book $10,168.75; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 196 | $4.60 | $2.62 | $+51.64 | $8,473.65 | ▲ +51.64 after sell → book $10,166.13; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 41 | $20.98 | $2.13 | $+11.33 | $9,331.70 | ▲ +11.33 after sell → book $10,164.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 203 | $4.10 | $2.66 | $-21.52 | $10,161.33 | ▼ -21.52 after sell → book $10,161.33; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,912.40 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+6.7; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,768.23 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.8; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,550.02 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+8.3; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,278.33 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,006.89 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,737.30 | — | S≥+5: sizeup + more names; list flatten; ret5=-7.2; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 392 | $3.24 | $5.06 | — | $1,462.16 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1270.17 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 264 | $4.81 | $3.41 | — | $188.92 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $1270.17 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.92 | ▲ close $10,178.28 vs 09:30 $10,201.84 (session +40.01) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.92 | ▼ 09:30 equity $10,129.84 vs yday $10,178.28 (-48.44) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,482.83 | ▲ +44.98 after sell → book $10,127.75; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.11 | ▲ +38.11 after sell → book $10,125.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,916.67 | ▲ +33.34 after sell → book $10,123.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,076.93 | ▼ -111.43 after sell → book $10,119.59; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,356.95 | ▲ +8.58 after sell → book $10,117.11; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.06 | ▲ +36.52 after sell → book $10,115.06; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 392 | $3.11 | $5.13 | $-61.15 | $8,877.05 | ▼ -61.15 after sell → book $10,109.93; vs 09:30 mark -5.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 264 | $4.67 | $3.46 | $-43.83 | $10,106.47 | ▼ -43.83 after sell → book $10,106.47; vs 09:30 mark -3.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.47 | ▲ close $10,106.47 vs 09:30 $10,129.84 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.47 | ▲ 09:30 equity $10,106.47 vs yday $10,106.47 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.47 | ▲ close $10,106.47 vs 09:30 $10,106.47 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.47 | ▲ 09:30 equity $10,106.47 vs yday $10,106.47 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,850.74 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,665.58 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,403.76 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,143.09 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,884.59 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,638.01 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,366.96 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1263.31 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.63 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1263.31 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.63 | ▲ close $10,316.19 vs 09:30 $10,106.47 (session +234.52) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.63 | ▲ 09:30 equity $10,585.91 vs yday $10,316.19 (+269.72) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,542.33 | ▲ +77.98 after sell → book $10,583.71; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,784.64 | ▲ +57.15 after sell → book $10,581.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,109.20 | ▲ +62.73 after sell → book $10,579.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,342.40 | ▼ -27.47 after sell → book $10,576.61; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,695.08 | ▲ +94.17 after sell → book $10,574.41; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,044.08 | ▲ +102.43 after sell → book $10,572.27; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,325.24 | ▲ +10.11 after sell → book $10,562.84; vs 09:30 mark -9.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,560.81 | ▲ +77.23 after sell → book $10,560.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,245.05 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,935.64 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,635.83 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,320.14 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,994.27 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,667.27 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,351.38 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1320.10 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1000 | $1.32 | $12.90 | — | $18.48 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1320.10 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.48 | ▲ close $10,787.08 vs 09:30 $10,585.91 (session +265.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.48 | ▲ 09:30 equity $11,166.67 vs yday $10,787.08 (+379.59) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,342.04 | ▲ +7.81 after sell → book $11,164.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,599.12 | ▼ -52.34 after sell → book $11,162.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,899.28 | ▲ +0.34 after sell → book $11,160.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.33 | $2.38 | $+254.88 | $5,469.84 | ▲ +254.88 after sell → book $11,157.98; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.40 | $6.99 | $-51.26 | $6,744.45 | ▼ -51.26 after sell → book $11,150.99; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.88 | $8.93 | $-51.90 | $8,019.56 | ▼ -51.90 after sell → book $11,142.06; vs 09:30 mark -8.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.75 | $2.08 | $-25.47 | $9,309.98 | ▼ -25.47 after sell → book $11,139.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1000 | $1.83 | $13.08 | $+484.02 | $11,126.90 | ▲ +484.02 after sell → book $11,126.90; vs 09:30 mark -13.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,126.90 | ▲ close $11,126.90 vs 09:30 $11,166.67 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,126.90 | ▲ 09:30 equity $11,126.90 vs yday $11,126.90 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,746.08 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.0; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.98 | $2.37 | — | $8,360.23 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+1.2; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $7,011.99 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+7.4; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.35 | $2.49 | — | $5,623.40 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 281 | $4.94 | $3.62 | — | $4,231.64 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,948.73 | — | S≥+5: sizeup + more names; list flatten; ret5=+6.0; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 191 | $7.25 | $2.56 | — | $1,561.42 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1390.86 | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3885 | $0.36 | $25.56 | — | $145.02 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-15.6; leftover $1390.86 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.02 | ▲ close $11,340.83 vs 09:30 $11,126.90 (session +256.76) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.02 | ▼ 09:30 equity $11,339.53 vs yday $11,340.83 (-1.30) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 191 | $8.29 | $2.61 | $+193.47 | $1,725.81 | ▲ +193.47 after sell → book $11,336.92; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3885 | $0.35 | $26.02 | $-71.01 | $3,071.19 | ▼ -71.01 after sell → book $11,310.90; vs 09:30 mark -26.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 49 | $31.21 | $2.14 | — | $1,539.76 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1535.59 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 138 | $11.12 | $2.40 | — | $2.80 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1535.59 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.80 | ▼ close $11,266.06 vs 09:30 $11,339.53 (session -40.30) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.80 | ▲ 09:30 equity $11,306.69 vs yday $11,266.06 (+40.63) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.63 | $2.40 | $-48.87 | $1,339.78 | ▼ -48.87 after sell → book $11,304.29; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+15.89 | $2,703.90 | ▲ +15.89 after sell → book $11,302.21; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 166 | $8.49 | $2.53 | $+18.23 | $4,110.71 | ▲ +18.23 after sell → book $11,299.68; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 281 | $5.07 | $3.68 | $+29.22 | $5,531.70 | ▲ +29.22 after sell → book $11,296.00; vs 09:30 mark -3.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $6,803.51 | ▼ -11.10 after sell → book $11,293.98; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $5,475.34 | — | S≥+5: sizeup + more names; list flatten; ret5=+3.1; leftover $1360.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 94 | $14.42 | $2.27 | — | $4,117.59 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.1; leftover $1360.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 523 | $2.60 | $6.75 | — | $2,751.05 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+13.0; leftover $1360.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 104 | $12.98 | $2.30 | — | $1,398.82 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1360.70 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 140 | $9.68 | $2.41 | — | $41.21 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1360.70 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.21 | ▲ close $11,350.03 vs 09:30 $11,306.69 (session +71.87) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.21 | ▼ 09:30 equity $11,334.48 vs yday $11,350.03 (-15.55) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 49 | $30.53 | $2.16 | $-37.62 | $1,535.02 | ▼ -37.62 after sell → book $11,332.32; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 138 | $11.27 | $2.44 | $+15.86 | $3,087.85 | ▲ +15.86 after sell → book $11,329.89; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 104 | $13.05 | $2.33 | $+2.65 | $4,442.71 | ▲ +2.65 after sell → book $11,327.56; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 140 | $9.88 | $2.44 | $+23.15 | $5,823.47 | ▲ +23.15 after sell → book $11,325.11; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 44 | $32.90 | $2.12 | — | $4,373.75 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1455.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 92 | $15.66 | $2.27 | — | $2,930.76 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1455.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 18 | $79.42 | $2.04 | — | $1,499.16 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1455.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 438 | $3.32 | $5.65 | — | $39.35 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+6.4; leftover $1455.87 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.35 | ▼ close $10,993.90 vs 09:30 $11,334.48 (session -319.13) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.35 | ▲ 09:30 equity $11,021.49 vs yday $10,993.90 (+27.59) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-9.57 | $1,410.60 | ▼ -9.57 after sell → book $11,019.30; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,752.50 | ▲ +13.73 after sell → book $11,017.20; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 94 | $14.54 | $2.30 | $+6.71 | $4,116.96 | ▲ +6.71 after sell → book $11,014.90; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 523 | $2.58 | $6.84 | $-24.05 | $5,459.45 | ▼ -24.05 after sell → book $11,008.05; vs 09:30 mark -6.85 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 44 | $31.15 | $2.14 | $-81.27 | $6,827.91 | ▼ -81.27 after sell → book $11,005.91; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 92 | $14.44 | $2.29 | $-116.80 | $8,154.10 | ▼ -116.80 after sell → book $11,003.62; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 18 | $80.44 | $2.07 | $+14.25 | $9,599.95 | ▲ +14.25 after sell → book $11,001.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 438 | $3.20 | $5.73 | $-63.94 | $10,995.82 | ▼ -63.94 after sell → book $10,995.82; vs 09:30 mark -5.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,995.82 | ▲ close $10,995.82 vs 09:30 $11,021.49 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,995.82 | ▲ 09:30 equity $10,995.82 vs yday $10,995.82 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,995.82 | ▲ close $10,995.82 vs 09:30 $10,995.82 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,995.82 | ▲ 09:30 equity $10,995.82 vs yday $10,995.82 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,995.82 | ▲ close $10,995.82 vs 09:30 $10,995.82 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,995.82 | ▲ 09:30 equity $10,995.82 vs yday $10,995.82 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,671.75 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $8,295.91 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 378 | $3.63 | $4.88 | — | $6,918.89 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 171 | $8.03 | $2.50 | — | $5,543.26 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,216.74 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,854.89 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,539.36 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1374.48 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $178.76 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1374.48 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.76 | ▼ close $10,731.89 vs 09:30 $10,995.82 (session -243.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.76 | ▲ 09:30 equity $10,735.29 vs yday $10,731.89 (+3.40) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $41.50 | $2.11 | $-49.95 | $1,504.65 | ▼ -49.95 after sell → book $10,733.18; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 171 | $7.91 | $2.54 | $-25.57 | $2,854.72 | ▼ -25.57 after sell → book $10,730.64; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,152.98 | ▼ -28.26 after sell → book $10,728.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $5,470.70 | ▼ -44.13 after sell → book $10,726.32; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,851.24 | ▲ +65.02 after sell → book $10,724.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $8,113.40 | ▼ -98.45 after sell → book $10,722.03; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 536 | $2.52 | $6.91 | — | $6,755.76 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1352.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 201 | $6.71 | $2.60 | — | $5,404.45 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1352.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 711 | $1.90 | $9.17 | — | $4,044.38 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1352.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 282 | $4.78 | $3.64 | — | $2,692.78 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1352.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 850 | $1.59 | $10.96 | — | $1,330.32 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1352.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 117 | $11.31 | $2.34 | — | $4.71 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1352.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.71 | ▼ close $10,631.92 vs 09:30 $10,735.29 (session -54.48) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.71 | ▼ 09:30 equity $10,588.09 vs yday $10,631.92 (-43.83) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,360.37 | ▲ +31.60 after sell → book $10,586.00; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 378 | $3.43 | $4.95 | $-85.43 | $2,651.96 | ▼ -85.43 after sell → book $10,581.05; vs 09:30 mark -4.95 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 536 | $2.38 | $7.01 | $-88.97 | $3,920.63 | ▼ -88.97 after sell → book $10,574.04; vs 09:30 mark -7.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 201 | $6.57 | $2.64 | $-33.38 | $5,238.56 | ▼ -33.38 after sell → book $10,571.40; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 711 | $2.00 | $9.30 | $+52.63 | $6,651.26 | ▲ +52.63 after sell → book $10,562.10; vs 09:30 mark -9.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 282 | $4.30 | $3.69 | $-142.69 | $7,860.16 | ▼ -142.69 after sell → book $10,558.40; vs 09:30 mark -3.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 850 | $1.63 | $11.12 | $+11.92 | $9,234.55 | ▲ +11.92 after sell → book $10,547.29; vs 09:30 mark -11.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 117 | $11.22 | $2.37 | $-15.24 | $10,544.91 | ▼ -15.24 after sell → book $10,544.91; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.91 | ▲ close $10,544.91 vs 09:30 $10,588.09 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.91 | ▲ 09:30 equity $10,544.91 vs yday $10,544.91 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.91 | ▲ close $10,544.91 vs 09:30 $10,544.91 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.91 | ▲ 09:30 equity $10,544.91 vs yday $10,544.91 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.91 | ▲ close $10,544.91 vs 09:30 $10,544.91 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.91 | ▲ 09:30 equity $10,544.91 vs yday $10,544.91 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 80 | $16.28 | $2.23 | — | $9,240.28 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-1.1; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 482 | $2.73 | $6.22 | — | $7,918.21 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-3.0; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $6,675.16 | — | S≥+5: sizeup + more names; list flatten; ret5=+8.3; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $5,357.70 | — | S≥+5: sizeup + more names; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $4,093.45 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+4.7; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $2,801.32 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+19.6; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 646 | $2.04 | $8.33 | — | $1,475.15 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1318.11 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 277 | $4.75 | $3.57 | — | $155.82 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1318.11 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.82 | ▼ close $10,484.12 vs 09:30 $10,544.91 (session -32.34) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.82 | ▼ 09:30 equity $10,171.38 vs yday $10,484.12 (-312.74) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 80 | $16.03 | $2.25 | $-24.48 | $1,435.97 | ▼ -24.48 after sell → book $10,169.13; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 482 | $2.75 | $6.31 | $-0.48 | $2,757.57 | ▼ -0.48 after sell → book $10,162.82; vs 09:30 mark -6.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $3,994.54 | ▼ -6.08 after sell → book $10,160.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 8 | $141.42 | $2.03 | $-188.13 | $5,123.87 | ▼ -188.13 after sell → book $10,158.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 23 | $52.23 | $2.08 | $-92.92 | $6,323.08 | ▼ -92.92 after sell → book $10,156.68; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 646 | $2.01 | $8.45 | $-36.16 | $7,613.09 | ▼ -36.16 after sell → book $10,148.23; vs 09:30 mark -8.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 277 | $4.82 | $3.63 | $+12.19 | $8,944.60 | ▲ +12.19 after sell → book $10,144.60; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,944.60 | ▼ close $10,117.72 vs 09:30 $10,171.38 (session -26.88) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,944.60 | ▲ 09:30 equity $10,153.56 vs yday $10,117.72 (+35.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $10,151.53 | ▼ -57.33 after sell → book $10,151.53; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,151.53 | ▲ close $10,151.53 vs 09:30 $10,153.56 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,151.53 | ▲ 09:30 equity $10,151.53 vs yday $10,151.53 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 3 | $270.89 | $2.00 | — | $9,336.86 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.0; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 10 | $77.12 | $2.02 | — | $8,563.64 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+7.2; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 59 | $14.31 | $2.17 | — | $7,717.18 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.8; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 23 | $36.46 | $2.06 | — | $6,876.54 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+2.9; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 45 | $18.61 | $2.12 | — | $6,036.97 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 46 | $18.21 | $2.13 | — | $5,197.18 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-19.1; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 12 | $68.79 | $2.03 | — | $4,369.67 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 144 | $5.87 | $2.42 | — | $3,521.97 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 311 | $2.72 | $4.01 | — | $2,672.04 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-0.4; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 9 | $87.40 | $2.02 | — | $1,883.42 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `KRMN` | 22 | $38.01 | $2.06 | — | $1,045.15 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=-5.6; leftover $845.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 31 | $27.09 | $2.08 | — | $203.27 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $845.96 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.27 | ▲ close $10,267.50 vs 09:30 $10,151.53 (session +143.09) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.27 | ▲ 09:30 equity $10,435.61 vs yday $10,267.50 (+168.11) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `IQV` | 3 | $273.15 | $2.02 | $+2.76 | $1,020.70 | ▲ +2.76 after sell → book $10,433.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 10 | $76.44 | $2.04 | $-10.86 | $1,783.06 | ▼ -10.86 after sell → book $10,431.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 59 | $14.33 | $2.19 | $-3.17 | $2,626.35 | ▼ -3.17 after sell → book $10,429.37; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BLFS` | 23 | $36.67 | $2.08 | $+0.69 | $3,467.68 | ▲ +0.69 after sell → book $10,427.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `BBNX` | 45 | $22.46 | $2.15 | $+168.98 | $4,476.23 | ▲ +168.98 after sell → book $10,425.14; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ARQQ` | 46 | $19.59 | $2.15 | $+59.20 | $5,375.22 | ▲ +59.20 after sell → book $10,422.99; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TEM` | 12 | $72.70 | $2.05 | $+42.85 | $6,245.58 | ▲ +42.85 after sell → book $10,420.95; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 144 | $5.58 | $2.46 | $-46.64 | $7,046.64 | ▼ -46.64 after sell → book $10,418.49; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 311 | $2.94 | $4.07 | $+60.33 | $7,956.91 | ▲ +60.33 after sell → book $10,414.42; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 9 | $83.20 | $2.04 | $-41.85 | $8,703.67 | ▼ -41.85 after sell → book $10,412.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KRMN` | 22 | $37.89 | $2.08 | $-6.77 | $9,535.18 | ▼ -6.77 after sell → book $10,410.31; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 31 | $28.23 | $2.10 | $+31.15 | $10,408.20 | ▲ +31.15 after sell → book $10,408.20; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 3 | $233.85 | $2.00 | — | $9,704.65 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+11.7; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 5 | $151.43 | $2.00 | — | $8,945.50 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 5 | $147.61 | $2.00 | — | $8,205.44 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+17.7; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 84 | $10.25 | $2.24 | — | $7,342.20 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 114 | $7.59 | $2.33 | — | $6,474.61 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 24 | $34.93 | $2.06 | — | $5,634.23 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.6; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 5102 | $0.17 | $23.98 | — | $4,742.91 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 54 | $15.87 | $2.15 | — | $3,883.78 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `AXTI` | 12 | $67.91 | $2.03 | — | $3,066.83 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 33 | $25.95 | $2.09 | — | $2,208.39 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 5 | $170.85 | $2.00 | — | $1,352.14 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $867.35 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 361 | $2.40 | $4.66 | — | $481.08 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $867.35 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $481.08 | ▲ close $10,427.82 vs 09:30 $10,435.61 (session +69.17) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $481.08 | ▲ 09:30 equity $10,586.15 vs yday $10,427.82 (+158.33) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 3 | $249.13 | $2.02 | $+41.82 | $1,226.45 | ▲ +41.82 after sell → book $10,584.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TWST` | 5 | $158.04 | $2.02 | $+29.02 | $2,014.63 | ▲ +29.02 after sell → book $10,582.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 5 | $146.50 | $2.02 | $-9.58 | $2,745.10 | ▼ -9.58 after sell → book $10,580.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 84 | $10.12 | $2.27 | $-15.43 | $3,592.92 | ▼ -15.43 after sell → book $10,577.82; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 114 | $7.98 | $2.36 | $+39.77 | $4,500.27 | ▲ +39.77 after sell → book $10,575.45; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 24 | $34.52 | $2.08 | $-13.98 | $5,326.67 | ▼ -13.98 after sell → book $10,573.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 5102 | $0.17 | $24.84 | $-48.82 | $6,169.18 | ▼ -48.82 after sell → book $10,548.54; vs 09:30 mark -24.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 54 | $17.44 | $2.17 | $+80.46 | $7,108.76 | ▲ +80.46 after sell → book $10,546.36; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AXTI` | 12 | $69.72 | $2.05 | $+17.65 | $7,943.36 | ▲ +17.65 after sell → book $10,544.32; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 33 | $26.14 | $2.11 | $+2.07 | $8,803.87 | ▲ +2.07 after sell → book $10,542.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 5 | $182.33 | $2.02 | $+53.37 | $9,713.49 | ▲ +53.37 after sell → book $10,540.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 361 | $2.29 | $4.73 | $-49.09 | $10,535.46 | ▼ -49.09 after sell → book $10,535.46; vs 09:30 mark -4.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 12 | $108.55 | $2.03 | — | $9,230.83 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+21.3; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $8,042.54 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ret5=+16.1; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $6,783.41 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $5,683.30 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $4,406.27 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 38 | $34.44 | $2.10 | — | $3,095.44 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1357 | $0.97 | $17.23 | — | $1,761.92 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1316.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `SWRD` | 633 | $2.08 | $8.17 | — | $437.11 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-4.5; leftover $1316.93 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $437.11 | ▼ close $10,315.92 vs 09:30 $10,586.15 (session -181.96) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $437.11 | ▲ 09:30 equity $10,445.72 vs yday $10,315.92 (+129.80) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 12 | $107.57 | $2.05 | $-15.83 | $1,725.91 | ▼ -15.83 after sell → book $10,443.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DELL` | 2 | $586.77 | $2.02 | $-16.77 | $2,897.43 | ▼ -16.77 after sell → book $10,441.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $4,155.40 | ▼ -1.16 after sell → book $10,439.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $5,304.63 | ▲ +49.12 after sell → book $10,437.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $6,545.02 | ▼ -36.64 after sell → book $10,435.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 38 | $33.00 | $2.12 | $-58.95 | $7,796.90 | ▼ -58.95 after sell → book $10,433.43; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1357 | $0.94 | $17.06 | $-75.01 | $9,055.42 | ▼ -75.01 after sell → book $10,416.37; vs 09:30 mark -17.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWRD` | 633 | $2.15 | $8.28 | $+27.86 | $10,408.09 | ▲ +27.86 after sell → book $10,408.09; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 5 | $157.87 | $2.00 | — | $9,616.73 | — | S≥+5: sizeup + more names; list flatten; ret5=+6.5; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 2 | $386.20 | $2.00 | — | $8,842.33 | — | S≥+5: sizeup + more names; list flatten; ret5=-5.8; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 9 | $88.83 | $2.02 | — | $8,040.85 | — | S≥+5: sizeup + more names; list flatten; ret5=+7.6; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 110 | $7.84 | $2.32 | — | $7,176.13 | — | S≥+5: sizeup + more names; list flatten; ret5=+13.6; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `IOVA` | 83 | $10.43 | $2.24 | — | $6,308.20 | — | S≥+5: sizeup + more names; list flatten; ret5=+19.2; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 64 | $13.47 | $2.18 | — | $5,443.94 | — | S≥+5: sizeup + more names; list flatten; ret5=+3.6; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 216 | $4.00 | $2.79 | — | $4,577.15 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover; ret5=+58.9; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 93 | $9.31 | $2.27 | — | $3,709.05 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 64 | $13.47 | $2.18 | — | $2,844.47 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 781 | $1.11 | $10.07 | — | $1,967.48 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 86 | $9.99 | $2.25 | — | $1,106.10 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $867.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 475 | $1.82 | $6.13 | — | $233.09 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $867.34 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.09 | ▼ close $10,100.58 vs 09:30 $10,445.72 (session -269.05) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.09 | ▲ 09:30 equity $10,105.62 vs yday $10,100.58 (+5.04) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 83 | $10.18 | $2.26 | $-25.25 | $1,075.77 | ▼ -25.25 after sell → book $10,103.36; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 216 | $3.51 | $2.83 | $-111.46 | $1,831.10 | ▼ -111.46 after sell → book $10,100.52; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ORBS` | 781 | $1.05 | $10.21 | $-67.15 | $2,640.93 | ▼ -67.15 after sell → book $10,090.31; vs 09:30 mark -10.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 86 | $9.91 | $2.27 | $-11.40 | $3,490.92 | ▼ -11.40 after sell → book $10,088.04; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 475 | $1.79 | $6.22 | $-26.59 | $4,337.33 | ▼ -26.59 after sell → book $10,081.82; vs 09:30 mark -6.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `USFD` | 5 | $93.97 | $2.00 | — | $3,865.48 | — | S≥+5: sizeup + more names; list flatten; ret5=-0.6; leftover $542.17 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 934 | $0.58 | $8.22 | — | $3,315.54 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $542.17 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,315.54 | ▼ close $10,024.70 vs 09:30 $10,105.62 (session -46.90) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,315.54 | ▲ 09:30 equity $10,065.04 vs yday $10,024.70 (+40.34) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 2 | $370.00 | $2.02 | $-36.41 | $4,053.52 | ▼ -36.41 after sell → book $10,063.02; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MGTX` | 64 | $12.26 | $2.20 | $-81.82 | $4,835.96 | ▼ -81.82 after sell → book $10,060.82; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 93 | $9.50 | $2.29 | $+13.11 | $5,717.16 | ▲ +13.11 after sell → book $10,058.52; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 64 | $12.84 | $2.20 | $-45.02 | $6,536.72 | ▼ -45.02 after sell → book $10,056.32; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `USFD` | 5 | $93.97 | $2.02 | $-4.03 | $7,004.55 | ▼ -4.03 after sell → book $10,054.30; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 934 | $0.57 | $8.34 | $-21.23 | $7,533.26 | ▼ -21.23 after sell → book $10,045.96; vs 09:30 mark -8.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 12 | $116.85 | $2.03 | — | $6,129.03 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1506.65 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 54 | $27.79 | $2.15 | — | $4,626.22 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1506.65 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 153 | $9.81 | $2.45 | — | $3,122.84 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1506.65 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 74 | $20.25 | $2.21 | — | $1,622.13 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1506.65 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 72 | $20.65 | $2.21 | — | $133.12 | — | S≥+5: sizeup + more names; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1506.65 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.12 | ▼ close $9,763.35 vs 09:30 $10,065.04 (session -271.56) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.12 | ▼ 09:30 equity $9,708.82 vs yday $9,763.35 (-54.53) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 5 | $163.95 | $2.02 | $+26.37 | $950.85 | ▲ +26.37 after sell → book $9,706.79; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 9 | $87.67 | $2.04 | $-14.45 | $1,737.89 | ▼ -14.45 after sell → book $9,704.76; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 110 | $7.38 | $2.35 | $-55.27 | $2,547.34 | ▼ -55.27 after sell → book $9,702.41; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 12 | $112.22 | $2.05 | $-59.63 | $3,891.93 | ▼ -59.63 after sell → book $9,700.36; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 54 | $26.22 | $2.17 | $-89.11 | $5,305.64 | ▼ -89.11 after sell → book $9,698.19; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 153 | $9.67 | $2.49 | $-26.36 | $6,782.66 | ▼ -26.36 after sell → book $9,695.70; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 74 | $19.40 | $2.24 | $-67.35 | $8,216.03 | ▼ -67.35 after sell → book $9,693.47; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 72 | $20.52 | $2.23 | $-13.80 | $9,691.24 | ▼ -13.80 after sell → book $9,691.24; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,691.24 | ▲ close $9,691.24 vs 09:30 $9,708.82 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,613.74 | ▲ 09:30 equity $8,613.74 vs yday $8,613.74 (+0.00) | 09:30 open · cash $8,613.74 · no holdings · equity $8,613.74 vs prior close $8,613.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $7,807.88 | — | S≥+5: sizeup + more names; list flatten; ret5=+0.8; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $6,767.62 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 52 | $20.61 | $2.15 | — | $5,693.75 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+9.1; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $4,651.91 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.7; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 140 | $7.65 | $2.41 | — | $3,578.50 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $2,525.59 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $1,518.45 | — | S≥+5: sizeup + more names; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1076.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 118 | $9.05 | $2.34 | — | $448.20 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=-27.1; leftover $1076.72 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $448.20 | ▲ close $8,610.13 vs 09:30 $8,613.74 (session +13.51) | 16:00 close · cash $448.20 · equity $8,610.13 vs 09:30 $8,613.74 (-3.61; session marks +13.51) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×52 09:30 $20.61 → close $20.08 -27.56; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×140 09:30 $7.65 → close $7.60 -7.00; WRBY×40 09:30 $26.27 → close $26.71 +17.60; TXG×12 09:30 $83.76 → close $85.71 +23.40; AEHL×118 09:30 $9.05 → close $9.36 +36.58 | — |

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
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
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
