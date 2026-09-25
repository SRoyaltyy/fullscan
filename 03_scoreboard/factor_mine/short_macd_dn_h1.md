# Factor mine action — `short_macd_dn_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · MACD histogram < 0

Cash book **-0.33%** ($9,967) · signal-only (no cash/fees) was +6.85%. Starts YES **5/30**. Fills 259 · skips 108 · realized $-743.47.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior MACD histogram is below zero (momentum still down).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `macd_down=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,256.52.

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
| 2026-08-13 09:30 ET | **SHORT** | `BTSG` | 20 | $59.80 | $2.10 | — | $11,193.90 | — | MACD histogram < 0; gate macd_down=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $12,434.28 | — | MACD histogram < 0; gate macd_down=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $13,672.11 | — | MACD histogram < 0; gate macd_down=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **SHORT** | `INO` | 1543 | $0.81 | $17.43 | — | $14,904.51 | — | MACD histogram < 0; gate macd_down=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,904.51 | ▼ close $9,802.55 vs 09:30 $10,000.00 (session -173.43) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,904.51 | ▼ 09:30 equity $9,780.37 vs yday $9,802.55 (-22.18) | — | — |
| 2026-08-14 09:30 ET | **COVER** | `BTSG` | 20 | $59.65 | $2.05 | $-1.15 | $13,709.46 | ▼ -1.15 after sell → book $9,778.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `TGTX` | 25 | $47.27 | $2.06 | $+56.57 | $12,525.65 | ▲ +56.57 after sell → book $9,776.26; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `SLS` | 106 | $12.40 | $2.31 | $-78.88 | $11,208.94 | ▼ -78.88 after sell → book $9,773.95; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `INO` | 1543 | $0.93 | $18.98 | $-221.57 | $9,754.97 | ▼ -221.57 after sell → book $9,754.97; vs 09:30 mark -18.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `VST` | 4 | $146.90 | $2.04 | — | $10,340.53 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+3.6; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $10,938.49 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+0.6; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `DAVE` | 1 | $330.91 | $2.02 | — | $11,267.38 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 67 | $9.01 | $2.23 | — | $11,868.82 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LDI` | 650 | $0.94 | $8.18 | — | $12,469.69 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BETR` | 41 | $14.80 | $2.15 | — | $13,074.34 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `ANGX` | 141 | $4.31 | $2.46 | — | $13,679.59 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $609.69 | — |
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 29 | $20.60 | $2.11 | — | $14,274.88 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+4.4; leftover $609.69 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,274.88 | ▼ close $9,728.17 vs 09:30 $9,780.37 (session -3.57) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,274.88 | ▼ 09:30 equity $9,678.68 vs yday $9,728.17 (-49.49) | — | — |
| 2026-08-17 09:30 ET | **COVER** | `VST` | 4 | $149.37 | $2.00 | $-13.92 | $13,675.40 | ▼ -13.92 after sell → book $9,676.68; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `NRG` | 5 | $127.40 | $2.00 | $-41.05 | $13,036.39 | ▼ -41.05 after sell → book $9,674.67; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `DAVE` | 1 | $336.94 | $1.99 | $-10.05 | $12,697.46 | ▼ -10.05 after sell → book $9,672.68; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MARA` | 67 | $9.22 | $2.19 | $-18.49 | $12,077.53 | ▼ -18.49 after sell → book $9,670.49; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LDI` | 650 | $0.91 | $7.85 | $+3.48 | $11,480.13 | ▲ +3.48 after sell → book $9,662.64; vs 09:30 mark -7.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `BETR` | 41 | $13.67 | $2.11 | $+42.07 | $10,917.55 | ▲ +42.07 after sell → book $9,660.53; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `ANGX` | 141 | $4.60 | $2.41 | $-45.77 | $10,266.54 | ▼ -45.77 after sell → book $9,658.12; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `WWW` | 29 | $20.98 | $2.08 | $-15.21 | $9,656.04 | ▼ -15.21 after sell → book $9,656.04; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `EOG` | 4 | $142.77 | $2.04 | — | $10,225.08 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+5.8; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CDNL` | 15 | $39.85 | $2.07 | — | $10,820.76 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ABX` | 66 | $9.12 | $2.23 | — | $11,420.45 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERA` | 19 | $31.30 | $2.08 | — | $12,013.07 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-3.8; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 47 | $12.83 | $2.17 | — | $12,613.91 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 39 | $15.40 | $2.14 | — | $13,212.37 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INV` | 372 | $1.62 | $4.89 | — | $13,810.12 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $603.50 | — |
| 2026-08-17 09:30 ET | **SHORT** | `KLC` | 230 | $2.62 | $3.03 | — | $14,409.69 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $603.50 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,409.69 | ▲ close $9,808.26 vs 09:30 $9,678.68 (session +172.87) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,409.69 | ▲ 09:30 equity $9,841.30 vs yday $9,808.26 (+33.04) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `EOG` | 4 | $148.04 | $2.00 | $-25.12 | $13,815.53 | ▼ -25.12 after sell → book $9,839.30; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `CDNL` | 15 | $41.57 | $2.04 | $-29.91 | $13,189.94 | ▼ -29.91 after sell → book $9,837.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ABX` | 66 | $9.03 | $2.19 | $+1.53 | $12,591.78 | ▲ +1.53 after sell → book $9,835.08; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `VERA` | 19 | $31.31 | $2.05 | $-4.32 | $11,994.84 | ▼ -4.32 after sell → book $9,833.03; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 47 | $11.12 | $2.13 | $+76.07 | $11,470.07 | ▲ +76.07 after sell → book $9,830.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NU` | 39 | $14.53 | $2.11 | $+29.68 | $10,901.29 | ▲ +29.68 after sell → book $9,828.79; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `INV` | 372 | $1.32 | $4.80 | $+100.05 | $10,403.59 | ▲ +100.05 after sell → book $9,823.99; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `KLC` | 230 | $2.52 | $2.97 | $+17.00 | $9,821.02 | ▲ +17.00 after sell → book $9,821.02; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,821.02 | ▲ close $9,821.02 vs 09:30 $9,841.30 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,821.02 | ▲ 09:30 equity $9,821.02 vs yday $9,821.02 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,821.02 | ▲ close $9,821.02 vs 09:30 $9,821.02 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,821.02 | ▲ 09:30 equity $9,821.02 vs yday $9,821.02 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `HDSN` | 106 | $5.77 | $2.35 | — | $10,430.29 | — | MACD histogram < 0; gate macd_down=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 82 | $7.44 | $2.28 | — | $11,038.10 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `DNA` | 82 | $7.45 | $2.28 | — | $11,646.72 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `PACB` | 487 | $1.26 | $6.39 | — | $12,253.95 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+14.4; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `LZB` | 18 | $33.61 | $2.08 | — | $12,856.85 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-17.4; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1733 | $0.35 | $11.65 | — | $13,458.68 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-29.4; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 68 | $9.01 | $2.23 | — | $14,069.13 | — | MACD histogram < 0; gate macd_down=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $613.81 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ATHM` | 27 | $22.44 | $2.11 | — | $14,672.90 | — | MACD histogram < 0; gate macd_down=True; list earn_react; ret5=-2.1; leftover $613.81 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,672.90 | ▲ close $9,822.93 vs 09:30 $9,821.02 (session +33.27) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,672.90 | ▼ 09:30 equity $9,781.65 vs yday $9,822.93 (-41.28) | — | — |
| 2026-08-21 09:30 ET | **COVER** | `HDSN` | 106 | $5.67 | $2.31 | $+5.94 | $14,069.58 | ▲ +5.94 after sell → book $9,779.35; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `MRVI` | 82 | $8.28 | $2.24 | $-73.39 | $13,388.38 | ▼ -73.39 after sell → book $9,777.11; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `DNA` | 82 | $7.09 | $2.24 | $+25.01 | $12,804.76 | ▲ +25.01 after sell → book $9,774.87; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `PACB` | 487 | $1.24 | $6.28 | $-2.93 | $12,194.60 | ▼ -2.93 after sell → book $9,768.59; vs 09:30 mark -6.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `LZB` | 18 | $33.63 | $2.04 | $-4.48 | $11,587.22 | ▼ -4.48 after sell → book $9,766.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SAFX` | 1733 | $0.35 | $11.26 | $-15.98 | $10,969.40 | ▼ -15.98 after sell → book $9,755.28; vs 09:30 mark -11.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AEG` | 68 | $9.04 | $2.19 | $-6.47 | $10,352.49 | ▼ -6.47 after sell → book $9,753.09; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ATHM` | 27 | $22.20 | $2.07 | $+2.30 | $9,751.02 | ▲ +2.30 after sell → book $9,751.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 356 | $1.71 | $4.68 | — | $10,355.10 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $609.44 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QDEL` | 40 | $14.96 | $2.15 | — | $10,951.35 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-1.6; leftover $609.44 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 195 | $3.11 | $2.63 | — | $11,555.17 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $609.44 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AAP` | 14 | $42.41 | $2.07 | — | $12,146.84 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-26.1; leftover $609.44 | — |
| 2026-08-21 09:30 ET | **SHORT** | `EYPT` | 111 | $5.48 | $2.37 | — | $12,752.75 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-59.9; leftover $609.44 | — |
| 2026-08-21 09:30 ET | **SHORT** | `WMT` | 5 | $103.69 | $2.04 | — | $13,269.16 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-10.3; leftover $609.44 | — |
| 2026-08-21 09:30 ET | **SHORT** | `BEKE` | 33 | $17.93 | $2.13 | — | $13,858.89 | — | MACD histogram < 0; gate macd_down=True; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $609.44 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,858.89 | ▲ close $9,783.03 vs 09:30 $9,781.65 (session +50.07) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,858.89 | ▼ 09:30 equity $9,773.72 vs yday $9,783.03 (-9.31) | — | — |
| 2026-08-24 09:30 ET | **COVER** | `ENHA` | 356 | $1.74 | $4.59 | $-19.95 | $13,234.86 | ▼ -19.95 after sell → book $9,769.13; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `QDEL` | 40 | $14.74 | $2.11 | $+4.54 | $12,643.15 | ▲ +4.54 after sell → book $9,767.02; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 195 | $2.99 | $2.58 | $+18.19 | $12,057.53 | ▲ +18.19 after sell → book $9,764.44; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AAP` | 14 | $43.05 | $2.03 | $-13.06 | $11,452.79 | ▼ -13.06 after sell → book $9,762.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `EYPT` | 111 | $5.17 | $2.32 | $+29.72 | $10,876.60 | ▲ +29.72 after sell → book $9,760.09; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `WMT` | 5 | $104.14 | $2.00 | $-6.29 | $10,353.90 | ▼ -6.29 after sell → book $9,758.08; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `BEKE` | 33 | $18.05 | $2.09 | $-8.17 | $9,755.99 | ▼ -8.17 after sell → book $9,755.99; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.99 | ▲ close $9,755.99 vs 09:30 $9,773.72 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.99 | ▲ 09:30 equity $9,755.99 vs yday $9,755.99 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **SHORT** | `SAFX` | 1703 | $0.36 | $11.52 | — | $10,354.15 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-15.6; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `VITL` | 54 | $11.12 | $2.19 | — | $10,952.44 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-0.7; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CCOI` | 64 | $9.49 | $2.22 | — | $11,557.58 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ZIP` | 134 | $4.55 | $2.44 | — | $12,164.84 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ALIT` | 41 | $14.78 | $2.15 | — | $12,768.67 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+10.6; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `RGNX` | 74 | $8.14 | $2.25 | — | $13,368.78 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-28.9; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AAOI` | 5 | $111.78 | $2.04 | — | $13,925.62 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-30.5; leftover $609.75 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CTKB` | 137 | $4.45 | $2.45 | — | $14,532.82 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-6.3; leftover $609.75 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,532.82 | ▼ close $9,648.88 vs 09:30 $9,755.99 (session -79.86) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,532.82 | ▲ 09:30 equity $9,688.23 vs yday $9,648.88 (+39.35) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `SAFX` | 1703 | $0.35 | $11.12 | $-14.12 | $13,920.54 | ▼ -14.12 after sell → book $9,677.11; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `VITL` | 54 | $11.03 | $2.15 | $+0.52 | $13,322.76 | ▲ +0.52 after sell → book $9,674.95; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CCOI` | 64 | $9.89 | $2.18 | $-30.00 | $12,687.62 | ▼ -30.00 after sell → book $9,672.77; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ZIP` | 134 | $4.31 | $2.39 | $+27.33 | $12,107.69 | ▲ +27.33 after sell → book $9,670.38; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ALIT` | 41 | $14.85 | $2.11 | $-7.13 | $11,496.73 | ▼ -7.13 after sell → book $9,668.27; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `RGNX` | 74 | $8.85 | $2.21 | $-57.00 | $10,839.62 | ▼ -57.00 after sell → book $9,666.06; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `AAOI` | 5 | $110.59 | $2.00 | $+1.88 | $10,284.66 | ▲ +1.88 after sell → book $9,664.05; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CTKB` | 137 | $4.53 | $2.40 | $-15.81 | $9,661.65 | ▼ -15.81 after sell → book $9,661.65; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `INSP` | 10 | $60.07 | $2.06 | — | $10,260.29 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+6.4; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 34 | $17.51 | $2.13 | — | $10,853.50 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AXTI` | 9 | $65.34 | $2.05 | — | $11,439.51 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BZ` | 36 | $16.77 | $2.13 | — | $12,041.10 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ASPN` | 116 | $5.20 | $2.38 | — | $12,641.91 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-6.3; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 12 | $46.96 | $2.06 | — | $13,203.37 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-3.9; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `TMCI` | 126 | $4.78 | $2.42 | — | $13,803.24 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+8.1; leftover $603.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BKSY` | 24 | $24.94 | $2.10 | — | $14,399.70 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-12.9; leftover $603.85 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,399.70 | ▼ close $9,537.80 vs 09:30 $9,688.23 (session -106.52) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,399.70 | ▼ 09:30 equity $9,467.38 vs yday $9,537.80 (-70.42) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `INSP` | 10 | $62.10 | $2.02 | $-24.38 | $13,776.68 | ▼ -24.38 after sell → book $9,465.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BZ` | 36 | $18.50 | $2.10 | $-66.51 | $13,108.58 | ▼ -66.51 after sell → book $9,463.26; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `ALOY` | 205 | $11.53 | $2.76 | — | $15,469.47 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-7.7; leftover $2365.81 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SVC` | 302 | $7.81 | $4.03 | — | $17,824.06 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-2.1; leftover $2365.81 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,824.06 | ▲ close $9,583.36 vs 09:30 $9,467.38 (session +126.89) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,824.06 | ▲ 09:30 equity $9,633.83 vs yday $9,583.36 (+50.47) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVEX` | 34 | $18.75 | $2.09 | $-46.38 | $17,184.47 | ▼ -46.38 after sell → book $9,631.74; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `AXTI` | 9 | $65.29 | $2.02 | $-3.62 | $16,594.84 | ▼ -3.62 after sell → book $9,629.72; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `ASPN` | 116 | $5.25 | $2.34 | $-10.52 | $15,983.50 | ▼ -10.52 after sell → book $9,627.38; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `IRDM` | 12 | $47.61 | $2.03 | $-11.89 | $15,410.16 | ▼ -11.89 after sell → book $9,625.36; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TMCI` | 126 | $4.65 | $2.37 | $+11.60 | $14,821.89 | ▲ +11.60 after sell → book $9,622.99; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `BKSY` | 24 | $24.44 | $2.06 | $+7.84 | $14,233.27 | ▲ +7.84 after sell → book $9,620.93; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `ALOY` | 205 | $11.20 | $2.64 | $+62.24 | $11,934.62 | ▲ +62.24 after sell → book $9,618.28; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `SVC` | 302 | $7.67 | $3.90 | $+34.35 | $9,614.38 | ▲ +34.35 after sell → book $9,614.38; vs 09:30 mark -3.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 180 | $3.32 | $2.59 | — | $10,209.40 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+6.4; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1646 | $0.36 | $11.25 | — | $10,798.94 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+7.6; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BBWI` | 32 | $18.75 | $2.12 | — | $11,396.82 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-5.0; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `QFIN` | 65 | $9.15 | $2.22 | — | $11,989.35 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-19.9; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SYRE` | 6 | $91.75 | $2.04 | — | $12,537.81 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-13.2; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `JKS` | 44 | $13.37 | $2.16 | — | $13,123.93 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-14.9; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `DY` | 1 | $306.34 | $2.02 | — | $13,428.25 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-23.0; leftover $600.90 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CLYM` | 40 | $14.86 | $2.15 | — | $14,020.50 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-11.5; leftover $600.90 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,020.50 | ▲ close $9,646.09 vs 09:30 $9,633.83 (session +58.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,020.50 | ▲ 09:30 equity $9,655.28 vs yday $9,646.09 (+9.19) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 180 | $3.20 | $2.53 | $+16.48 | $13,441.97 | ▲ +16.48 after sell → book $9,652.75; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1646 | $0.36 | $10.90 | $-17.20 | $12,835.22 | ▼ -17.20 after sell → book $9,641.85; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BBWI` | 32 | $19.25 | $2.09 | $-20.21 | $12,217.14 | ▼ -20.21 after sell → book $9,639.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `QFIN` | 65 | $8.70 | $2.19 | $+24.84 | $11,649.45 | ▲ +24.84 after sell → book $9,637.58; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SYRE` | 6 | $89.15 | $2.01 | $+11.55 | $11,112.54 | ▲ +11.55 after sell → book $9,635.57; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `JKS` | 44 | $13.54 | $2.12 | $-11.76 | $10,514.66 | ▼ -11.76 after sell → book $9,633.45; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `DY` | 1 | $298.01 | $1.99 | $+4.32 | $10,214.66 | ▲ +4.32 after sell → book $9,631.46; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CLYM` | 40 | $14.58 | $2.11 | $+6.94 | $9,629.35 | ▲ +6.94 after sell → book $9,629.35; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,629.35 | ▲ close $9,629.35 vs 09:30 $9,655.28 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,629.35 | ▲ 09:30 equity $9,629.35 vs yday $9,629.35 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,629.35 | ▲ close $9,629.35 vs 09:30 $9,629.35 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,629.35 | ▲ 09:30 equity $9,629.35 vs yday $9,629.35 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,629.35 | ▲ close $9,629.35 vs 09:30 $9,629.35 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,629.35 | ▲ 09:30 equity $9,629.35 vs yday $9,629.35 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 40 | $14.85 | $2.15 | — | $10,221.20 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $55.42 | $2.06 | — | $10,773.35 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-25.9; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CLYM` | 43 | $13.96 | $2.16 | — | $11,371.47 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 46 | $12.83 | $2.16 | — | $11,959.49 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-0.2; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 37 | $15.95 | $2.14 | — | $12,547.50 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-14.0; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ALMS` | 58 | $10.38 | $2.20 | — | $13,147.05 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-56.2; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `LX` | 727 | $0.83 | $8.34 | — | $13,739.94 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-30.4; leftover $601.83 | — |
| 2026-09-03 09:30 ET | **SHORT** | `RZLV` | 257 | $2.34 | $3.38 | — | $14,337.93 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-19.9; leftover $601.83 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,337.93 | ▼ close $9,513.04 vs 09:30 $9,629.35 (session -91.73) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,337.93 | ▲ 09:30 equity $9,542.72 vs yday $9,513.04 (+29.68) | — | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 40 | $14.63 | $2.11 | $+4.54 | $13,750.62 | ▲ +4.54 after sell → book $9,540.61; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `EIX` | 10 | $55.79 | $2.02 | $-7.78 | $13,190.70 | ▼ -7.78 after sell → book $9,538.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CLYM` | 43 | $14.49 | $2.12 | $-27.06 | $12,565.51 | ▼ -27.06 after sell → book $9,536.47; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `GMRS` | 46 | $13.29 | $2.13 | $-25.45 | $11,952.05 | ▼ -25.45 after sell → book $9,534.34; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `KLRA` | 37 | $15.60 | $2.10 | $+8.71 | $11,372.74 | ▲ +8.71 after sell → book $9,532.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `ALMS` | 58 | $11.23 | $2.16 | $-53.95 | $10,719.24 | ▼ -53.95 after sell → book $9,530.07; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `LX` | 727 | $0.86 | $8.42 | $-39.30 | $10,087.06 | ▼ -39.30 after sell → book $9,521.66; vs 09:30 mark -8.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `RZLV` | 257 | $2.20 | $3.32 | $+29.28 | $9,518.34 | ▲ +29.28 after sell → book $9,518.34; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `BHC` | 88 | $6.71 | $2.29 | — | $10,106.53 | — | MACD histogram < 0; gate macd_down=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 37 | $15.90 | $2.14 | — | $10,692.69 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `UAMY` | 113 | $5.25 | $2.37 | — | $11,283.56 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ASTS` | 9 | $63.40 | $2.05 | — | $11,852.11 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+1.1; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `SCZM` | 59 | $10.03 | $2.20 | — | $12,441.68 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=+4.0; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PLAY` | 69 | $8.59 | $2.23 | — | $13,032.15 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-5.6; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `CRDO` | 3 | $162.10 | $2.03 | — | $13,516.42 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-31.7; leftover $594.90 | — |
| 2026-09-04 09:30 ET | **SHORT** | `AIIO` | 339 | $1.75 | $4.46 | — | $14,105.22 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-24.6; leftover $594.90 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,105.22 | ▼ close $9,480.22 vs 09:30 $9,542.72 (session -18.34) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,105.22 | ▼ 09:30 equity $9,475.65 vs yday $9,480.22 (-4.57) | — | — |
| 2026-09-08 09:30 ET | **COVER** | `BHC` | 88 | $6.57 | $2.25 | $+7.77 | $13,524.80 | ▲ +7.77 after sell → book $9,473.40; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `HQ` | 37 | $15.40 | $2.10 | $+14.26 | $12,952.90 | ▲ +14.26 after sell → book $9,471.30; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `UAMY` | 113 | $5.28 | $2.33 | $-8.09 | $12,353.93 | ▼ -8.09 after sell → book $9,468.97; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `ASTS` | 9 | $63.16 | $2.02 | $-1.91 | $11,783.48 | ▼ -1.91 after sell → book $9,466.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `SCZM` | 59 | $9.90 | $2.17 | $+3.30 | $11,197.21 | ▲ +3.30 after sell → book $9,464.78; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `PLAY` | 69 | $8.80 | $2.20 | $-18.92 | $10,587.81 | ▼ -18.92 after sell → book $9,462.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `CRDO` | 3 | $170.54 | $2.00 | $-29.37 | $10,074.18 | ▼ -29.37 after sell → book $9,460.59; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `AIIO` | 339 | $1.81 | $4.37 | $-29.17 | $9,456.21 | ▼ -29.17 after sell → book $9,456.21; vs 09:30 mark -4.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,456.21 | ▲ close $9,456.21 vs 09:30 $9,475.65 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,456.21 | ▲ 09:30 equity $9,456.21 vs yday $9,456.21 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,456.21 | ▲ close $9,456.21 vs 09:30 $9,456.21 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,456.21 | ▲ 09:30 equity $9,456.21 vs yday $9,456.21 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,456.21 | ▲ close $9,456.21 vs 09:30 $9,456.21 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,456.21 | ▲ 09:30 equity $9,456.21 vs yday $9,456.21 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `AUPH` | 36 | $16.28 | $2.13 | — | $10,040.16 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=-1.1; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `OVID` | 216 | $2.73 | $2.85 | — | $10,626.99 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=-3.0; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `AMTX` | 289 | $2.04 | $3.80 | — | $11,212.75 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `TYRA` | 25 | $23.63 | $2.10 | — | $11,801.40 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-6.3; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `LDI` | 695 | $0.85 | $8.13 | — | $12,384.01 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-7.8; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `NAVN` | 28 | $20.61 | $2.11 | — | $12,958.98 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-24.7; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 167 | $3.52 | $2.54 | — | $13,544.28 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-19.2; leftover $591.01 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BHVN` | 45 | $13.03 | $2.16 | — | $14,128.47 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-18.5; leftover $591.01 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,128.47 | ▲ close $9,502.50 vs 09:30 $9,456.21 (session +72.13) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,128.47 | ▼ 09:30 equity $9,469.30 vs yday $9,502.50 (-33.20) | — | — |
| 2026-09-14 09:30 ET | **COVER** | `AUPH` | 36 | $16.03 | $2.10 | $+4.77 | $13,549.29 | ▲ +4.77 after sell → book $9,467.20; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `OVID` | 216 | $2.75 | $2.79 | $-11.03 | $12,951.42 | ▼ -11.03 after sell → book $9,464.41; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `AMTX` | 289 | $2.01 | $3.73 | $+1.14 | $12,366.81 | ▲ +1.14 after sell → book $9,460.69; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `TYRA` | 25 | $23.20 | $2.06 | $+6.58 | $11,784.74 | ▲ +6.58 after sell → book $9,458.62; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `LDI` | 695 | $0.84 | $7.91 | $-7.70 | $11,194.42 | ▼ -7.70 after sell → book $9,450.71; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `NAVN` | 28 | $21.10 | $2.07 | $-17.90 | $10,601.55 | ▼ -17.90 after sell → book $9,448.64; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `RWT` | 167 | $3.53 | $2.49 | $-6.71 | $10,009.55 | ▼ -6.71 after sell → book $9,446.15; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `BHVN` | 45 | $12.52 | $2.12 | $+18.66 | $9,444.02 | ▲ +18.66 after sell → book $9,444.02; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,444.02 | ▲ close $9,444.02 vs 09:30 $9,469.30 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,444.02 | ▲ 09:30 equity $9,444.02 vs yday $9,444.02 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,444.02 | ▲ close $9,444.02 vs 09:30 $9,444.02 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,444.02 | ▲ 09:30 equity $9,444.02 vs yday $9,444.02 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **SHORT** | `IQV` | 2 | $270.89 | $2.03 | — | $9,983.77 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+4.0; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `RDNT` | 7 | $77.12 | $2.05 | — | $10,521.57 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; ret5=+7.2; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 41 | $14.31 | $2.15 | — | $11,106.13 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+4.8; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BLFS` | 16 | $36.46 | $2.07 | — | $11,687.41 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+2.9; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 31 | $18.61 | $2.12 | — | $12,262.20 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `ARQQ` | 32 | $18.21 | $2.12 | — | $12,842.80 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-19.1; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `TEM` | 8 | $68.79 | $2.05 | — | $13,391.07 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $590.25 | — |
| 2026-09-16 09:30 ET | **SHORT** | `RIG` | 100 | $5.87 | $2.33 | — | $13,975.74 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $590.25 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,975.74 | ▼ close $9,335.56 vs 09:30 $9,444.02 (session -91.54) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,975.74 | ▼ 09:30 equity $9,257.37 vs yday $9,335.56 (-78.19) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `IQV` | 2 | $273.15 | $2.00 | $-8.55 | $13,427.44 | ▼ -8.55 after sell → book $9,255.37; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `RDNT` | 7 | $76.44 | $2.01 | $+0.70 | $12,890.35 | ▲ +0.70 after sell → book $9,253.36; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `AVAH` | 41 | $14.33 | $2.11 | $-5.08 | $12,300.71 | ▼ -5.08 after sell → book $9,251.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `BLFS` | 16 | $36.67 | $2.04 | $-7.47 | $11,711.95 | ▼ -7.47 after sell → book $9,249.21; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `ARQQ` | 32 | $19.59 | $2.09 | $-48.37 | $11,082.99 | ▼ -48.37 after sell → book $9,247.13; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `TEM` | 8 | $72.70 | $2.01 | $-35.34 | $10,499.37 | ▼ -35.34 after sell → book $9,245.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **COVER** | `RIG` | 100 | $5.58 | $2.29 | $+24.38 | $9,939.08 | ▲ +24.38 after sell → book $9,242.82; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `ILMN` | 2 | $233.85 | $2.03 | — | $10,404.75 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; ret5=+11.7; leftover $660.20 | — |
| 2026-09-17 09:30 ET | **SHORT** | `TWST` | 4 | $151.43 | $2.04 | — | $11,008.43 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $660.20 | — |
| 2026-09-17 09:30 ET | **SHORT** | `PGEN` | 86 | $7.59 | $2.29 | — | $11,658.88 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $660.20 | — |
| 2026-09-17 09:30 ET | **SHORT** | `DVLT` | 3883 | $0.17 | $18.92 | — | $12,300.07 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $660.20 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BRUN` | 41 | $15.87 | $2.15 | — | $12,948.59 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $660.20 | — |
| 2026-09-17 09:30 ET | **SHORT** | `AXTI` | 9 | $67.91 | $2.05 | — | $13,557.73 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $660.20 | — |
| 2026-09-17 09:30 ET | **SHORT** | `EROC` | 52 | $12.64 | $2.18 | — | $14,212.82 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-3.6; leftover $660.20 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,212.82 | ▼ close $9,173.36 vs 09:30 $9,257.37 (session -37.79) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,212.82 | ▼ 09:30 equity $9,057.19 vs yday $9,173.36 (-116.17) | — | — |
| 2026-09-18 09:30 ET | **COVER** | `BBNX` | 31 | $21.30 | $2.08 | $-87.59 | $13,550.44 | ▼ -87.59 after sell → book $9,055.11; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `ILMN` | 2 | $249.13 | $2.00 | $-34.58 | $13,050.18 | ▼ -34.58 after sell → book $9,053.11; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `TWST` | 4 | $158.04 | $2.00 | $-30.48 | $12,416.02 | ▼ -30.48 after sell → book $9,051.11; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `PGEN` | 86 | $7.98 | $2.25 | $-38.08 | $11,727.49 | ▼ -38.08 after sell → book $9,048.86; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `DVLT` | 3883 | $0.17 | $18.25 | $-37.17 | $11,049.13 | ▼ -37.17 after sell → book $9,030.61; vs 09:30 mark -18.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BRUN` | 41 | $17.44 | $2.11 | $-68.63 | $10,331.98 | ▼ -68.63 after sell → book $9,028.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `AXTI` | 9 | $69.72 | $2.02 | $-20.36 | $9,702.48 | ▼ -20.36 after sell → book $9,026.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `EROC` | 52 | $13.00 | $2.15 | $-23.05 | $9,024.34 | ▼ -23.05 after sell → book $9,024.34; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 16 | $34.44 | $2.07 | — | $9,573.30 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `TLSA` | 581 | $0.97 | $7.50 | — | $10,129.37 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `BHVN` | 40 | $14.07 | $2.15 | — | $10,690.03 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `DDD` | 157 | $3.58 | $2.51 | — | $11,249.58 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `RANI` | 663 | $0.85 | $7.76 | — | $11,805.37 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+3.6; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `RARE` | 38 | $14.79 | $2.14 | — | $12,365.25 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `DCX` | 1593 | $0.35 | $10.71 | — | $12,918.46 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $564.02 | — |
| 2026-09-18 09:30 ET | **SHORT** | `USDE` | 59 | $9.54 | $2.20 | — | $13,479.12 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; ret5=+15.8; leftover $564.02 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,479.12 | ▲ close $9,380.21 vs 09:30 $9,057.19 (session +392.91) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,479.12 | ▼ 09:30 equity $9,145.07 vs yday $9,380.21 (-235.14) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `FIVN` | 16 | $33.00 | $2.04 | $+18.93 | $12,949.08 | ▲ +18.93 after sell → book $9,143.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `TLSA` | 581 | $0.94 | $7.20 | $+2.73 | $12,395.74 | ▲ +2.73 after sell → book $9,135.83; vs 09:30 mark -7.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `BHVN` | 40 | $13.90 | $2.11 | $+2.54 | $11,837.63 | ▲ +2.54 after sell → book $9,133.72; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `DDD` | 157 | $3.71 | $2.46 | $-25.38 | $11,252.69 | ▼ -25.38 after sell → book $9,131.26; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `RANI` | 663 | $0.86 | $7.72 | $-24.76 | $10,672.15 | ▼ -24.76 after sell → book $9,123.54; vs 09:30 mark -7.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `RARE` | 38 | $14.58 | $2.10 | $+3.74 | $10,116.00 | ▲ +3.74 after sell → book $9,121.44; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `DCX` | 1593 | $0.14 | $7.03 | $+321.58 | $9,884.36 | ▲ +321.58 after sell → book $9,114.41; vs 09:30 mark -7.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **COVER** | `USDE` | 59 | $13.05 | $2.17 | $-211.46 | $9,112.25 | ▼ -211.46 after sell → book $9,112.25; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `HUM` | 1 | $386.20 | $2.02 | — | $9,496.42 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=-5.8; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `DXCM` | 6 | $88.83 | $2.04 | — | $10,027.36 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+7.6; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `MGTX` | 42 | $13.47 | $2.15 | — | $10,590.95 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+3.6; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `BKKT` | 61 | $9.31 | $2.21 | — | $11,156.65 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ORBS` | 513 | $1.11 | $6.73 | — | $11,719.35 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SBET` | 57 | $9.99 | $2.20 | — | $12,286.58 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SGML` | 56 | $10.13 | $2.19 | — | $12,851.95 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+4.9; leftover $569.52 | — |
| 2026-09-21 09:30 ET | **SHORT** | `GLXY` | 21 | $25.95 | $2.09 | — | $13,394.81 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $569.52 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,394.81 | ▲ close $9,144.21 vs 09:30 $9,145.07 (session +53.60) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,394.81 | ▲ 09:30 equity $9,148.20 vs yday $9,144.21 (+3.99) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `ORBS` | 513 | $1.05 | $6.62 | $+17.43 | $12,849.55 | ▲ +17.43 after sell → book $9,141.59; vs 09:30 mark -6.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **COVER** | `SBET` | 57 | $9.91 | $2.16 | $+0.20 | $12,282.51 | ▲ +0.20 after sell → book $9,139.42; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 6 | $93.97 | $2.04 | — | $12,844.29 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=-0.6; leftover $571.21 | — |
| 2026-09-22 09:30 ET | **SHORT** | `DEFT` | 984 | $0.58 | $8.85 | — | $13,406.16 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $571.21 | — |
| 2026-09-22 09:30 ET | **SHORT** | `ALOY` | 60 | $9.40 | $2.21 | — | $13,967.96 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+9.5; leftover $571.21 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,967.96 | ▲ close $9,197.97 vs 09:30 $9,148.20 (session +71.64) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,967.96 | ▼ 09:30 equity $9,170.18 vs yday $9,197.97 (-27.79) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `HUM` | 1 | $370.00 | $1.99 | $+12.18 | $13,595.96 | ▲ +12.18 after sell → book $9,168.18; vs 09:30 mark -2.00 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `MGTX` | 42 | $12.26 | $2.12 | $+46.55 | $13,078.93 | ▲ +46.55 after sell → book $9,166.07; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `BKKT` | 61 | $9.50 | $2.17 | $-15.97 | $12,497.26 | ▼ -15.97 after sell → book $9,163.90; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `SGML` | 56 | $10.26 | $2.16 | $-11.35 | $11,920.54 | ▼ -11.35 after sell → book $9,161.74; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `GLXY` | 21 | $26.58 | $2.05 | $-17.37 | $11,360.30 | ▼ -17.37 after sell → book $9,159.68; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `USFD` | 6 | $93.97 | $2.01 | $-4.05 | $10,794.48 | ▼ -4.05 after sell → book $9,157.68; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `DEFT` | 984 | $0.57 | $8.61 | $-12.54 | $10,220.07 | ▼ -12.54 after sell → book $9,149.07; vs 09:30 mark -8.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `ALOY` | 60 | $8.90 | $2.17 | $+25.62 | $9,683.90 | ▲ +25.62 after sell → book $9,146.90; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 5 | $116.85 | $2.04 | — | $10,266.11 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $653.35 | — |
| 2026-09-23 09:30 ET | **SHORT** | `ADMA` | 66 | $9.81 | $2.23 | — | $10,911.34 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $653.35 | — |
| 2026-09-23 09:30 ET | **SHORT** | `OMER` | 31 | $20.65 | $2.12 | — | $11,549.37 | — | MACD histogram < 0; gate macd_down=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $653.35 | — |
| 2026-09-23 09:30 ET | **SHORT** | `MAZE` | 23 | $28.30 | $2.10 | — | $12,198.17 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $653.35 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SGRY` | 41 | $15.72 | $2.15 | — | $12,840.54 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $653.35 | — |
| 2026-09-23 09:30 ET | **SHORT** | `CLPT` | 42 | $15.55 | $2.15 | — | $13,491.49 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $653.35 | — |
| 2026-09-23 09:30 ET | **SHORT** | `NMRA` | 850 | $0.77 | $9.25 | — | $14,135.04 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $653.35 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,135.04 | ▲ close $9,270.20 vs 09:30 $9,170.18 (session +145.34) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,135.04 | ▲ 09:30 equity $9,279.98 vs yday $9,270.20 (+9.78) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `DXCM` | 6 | $87.67 | $2.01 | $+2.88 | $13,606.98 | ▲ +2.88 after sell → book $9,277.97; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `HALO` | 5 | $112.22 | $2.00 | $+19.10 | $13,043.88 | ▲ +19.10 after sell → book $9,275.97; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `ADMA` | 66 | $9.67 | $2.19 | $+4.83 | $12,403.47 | ▲ +4.83 after sell → book $9,273.78; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `OMER` | 31 | $20.52 | $2.08 | $-0.17 | $11,765.27 | ▼ -0.17 after sell → book $9,271.70; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `MAZE` | 23 | $28.15 | $2.06 | $-0.71 | $11,115.76 | ▼ -0.71 after sell → book $9,269.64; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `SGRY` | 41 | $14.38 | $2.11 | $+50.68 | $10,524.06 | ▲ +50.68 after sell → book $9,267.52; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `CLPT` | 42 | $14.82 | $2.12 | $+26.39 | $9,899.51 | ▲ +26.39 after sell → book $9,265.41; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `NMRA` | 850 | $0.75 | $8.89 | $+0.56 | $9,256.52 | ▲ +0.56 after sell → book $9,256.52; vs 09:30 mark -8.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,256.52 | ▲ close $9,256.52 vs 09:30 $9,279.98 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,774.88 | ▲ 09:30 equity $9,774.88 vs yday $9,774.88 (+0.00) | 09:30 open · cash $9,774.88 · no holdings · equity $9,774.88 vs prior close $9,774.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **SHORT** | `HALO` | 5 | $115.36 | $2.04 | — | $10,349.64 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+5.1; leftover $610.93 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `AEHL` | 67 | $9.05 | $2.23 | — | $10,953.76 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-27.1; leftover $610.93 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `BRVE` | 25 | $23.58 | $2.10 | — | $11,541.16 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $610.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `NEOV` | 255 | $2.39 | $3.36 | — | $12,147.25 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-31.4; leftover $610.93 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SFIX` | 277 | $2.20 | $3.65 | — | $12,753.00 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-24.1; leftover $610.93 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `LRMR` | 184 | $3.32 | $2.60 | — | $13,361.29 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-12.6; leftover $610.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `ACAD` | 27 | $22.21 | $2.11 | — | $13,958.85 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-19.2; leftover $610.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,958.85 | ▲ close $9,967.18 vs 09:30 $9,774.88 (session +210.39) | 16:00 close · cash $13,958.85 · equity $9,967.18 vs 09:30 $9,774.88 (+192.30; session marks +210.39) · 7 name(s) marked open→close (per-name table). HALO×5 09:30 $115.36 → close $113.90 +7.30; AEHL×67 09:30 $9.05 → close $9.36 -20.77; BRVE×25 09:30 $23.58 → close $20.62 +74.00; NEOV×255 09:30 $2.39 → close $2.19 +51.00; SFIX×277 09:30 $2.20 → close $2.15 +12.47; LRMR×184 09:30 $3.32 → close $3.08 +45.08; ACAD×27 09:30 $22.21 → close $20.68 +41.31 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `DE` | cash | leftover split 609.44 < 1 share @ 623.26 |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WRAP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AREC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RARE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNTB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `QMCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ZJYL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `REAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MGTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
