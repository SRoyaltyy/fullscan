# Factor mine action — `short_macd_dn_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · MACD histogram < 0

Cash book **+4.08%** ($10,408) · signal-only (no cash/fees) was -1.81%. Starts YES **18/30**. Fills 246 · skips 334 · realized $-941.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `macd_down=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15,056.82.

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
| 2026-08-14 09:30 ET | **SHORT** | `VST` | 4 | $146.90 | $2.04 | — | $15,490.07 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+3.6; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $16,088.03 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+0.6; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `DAVE` | 1 | $330.91 | $2.02 | — | $16,416.92 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 67 | $9.01 | $2.23 | — | $17,018.36 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LDI` | 652 | $0.94 | $8.20 | — | $17,621.08 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `BETR` | 41 | $14.80 | $2.15 | — | $18,225.73 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `ANGX` | 141 | $4.31 | $2.46 | — | $18,830.98 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $611.27 | — |
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 29 | $20.60 | $2.11 | — | $19,426.27 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+4.4; leftover $611.27 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,426.27 | ▼ close $9,388.51 vs 09:30 $9,780.37 (session -368.60) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,426.27 | ▼ 09:30 equity $9,370.26 vs yday $9,388.51 (-18.25) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `EOG` | 4 | $142.77 | $2.04 | — | $19,995.31 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+5.8; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CDNL` | 14 | $39.85 | $2.07 | — | $20,551.14 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ABX` | 64 | $9.12 | $2.22 | — | $21,132.60 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERA` | 18 | $31.30 | $2.08 | — | $21,693.93 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-3.8; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 45 | $12.83 | $2.16 | — | $22,269.11 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 38 | $15.40 | $2.14 | — | $22,852.17 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `INV` | 361 | $1.62 | $4.74 | — | $23,432.25 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $585.64 | — |
| 2026-08-17 09:30 ET | **SHORT** | `KLC` | 223 | $2.62 | $2.94 | — | $24,013.57 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $585.64 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24,013.57 | ▲ close $9,426.57 vs 09:30 $9,370.26 (session +76.69) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,013.57 | ▲ 09:30 equity $9,564.09 vs yday $9,426.57 (+137.52) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `BTSG` | 20 | $60.00 | $2.05 | $-8.15 | $22,811.52 | ▼ -8.15 after sell → book $9,562.04; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `TGTX` | 25 | $49.28 | $2.06 | $+6.32 | $21,577.46 | ▲ +6.32 after sell → book $9,559.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `SLS` | 106 | $12.66 | $2.31 | $-106.44 | $20,233.19 | ▼ -106.44 after sell → book $9,557.66; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 1543 | $1.14 | $19.90 | $-546.52 | $18,454.26 | ▼ -546.52 after sell → book $9,537.76; vs 09:30 mark -19.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,454.26 | ▼ close $9,434.97 vs 09:30 $9,564.09 (session -102.79) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,454.26 | ▼ 09:30 equity $9,399.72 vs yday $9,434.97 (-35.25) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `VST` | 4 | $140.74 | $2.00 | $+20.60 | $17,889.30 | ▲ +20.60 after sell → book $9,397.72; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `NRG` | 5 | $116.20 | $2.00 | $+14.95 | $17,306.30 | ▲ +14.95 after sell → book $9,395.72; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `DAVE` | 1 | $334.00 | $1.99 | $-7.11 | $16,970.30 | ▼ -7.11 after sell → book $9,393.72; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MARA` | 67 | $8.91 | $2.19 | $+2.28 | $16,371.14 | ▲ +2.28 after sell → book $9,391.53; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LDI` | 652 | $0.88 | $7.69 | $+21.27 | $15,789.69 | ▲ +21.27 after sell → book $9,383.84; vs 09:30 mark -7.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BETR` | 41 | $13.03 | $2.11 | $+68.31 | $15,253.35 | ▲ +68.31 after sell → book $9,381.73; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `ANGX` | 141 | $4.79 | $2.41 | $-72.56 | $14,575.54 | ▼ -72.56 after sell → book $9,379.31; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `WWW` | 29 | $20.08 | $2.08 | $+10.89 | $13,991.15 | ▲ +10.89 after sell → book $9,377.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,991.15 | ▼ close $9,226.41 vs 09:30 $9,399.72 (session -150.83) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,991.15 | ▲ 09:30 equity $9,241.80 vs yday $9,226.41 (+15.39) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `EOG` | 4 | $151.45 | $2.00 | $-38.76 | $13,383.34 | ▼ -38.76 after sell → book $9,239.79; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CDNL` | 14 | $43.13 | $2.03 | $-50.02 | $12,777.49 | ▼ -50.02 after sell → book $9,237.76; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ABX` | 64 | $9.13 | $2.18 | $-5.04 | $12,190.99 | ▼ -5.04 after sell → book $9,235.58; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERA` | 18 | $32.30 | $2.04 | $-22.03 | $11,607.64 | ▼ -22.03 after sell → book $9,233.54; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 45 | $13.60 | $2.12 | $-38.94 | $10,993.51 | ▼ -38.94 after sell → book $9,231.41; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NU` | 38 | $14.74 | $2.10 | $+20.65 | $10,431.10 | ▲ +20.65 after sell → book $9,229.31; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INV` | 361 | $1.55 | $4.66 | $+15.87 | $9,866.89 | ▲ +15.87 after sell → book $9,224.65; vs 09:30 mark -4.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `KLC` | 223 | $2.88 | $2.88 | $-63.80 | $9,221.77 | ▼ -63.80 after sell → book $9,221.77; vs 09:30 mark -2.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `HDSN` | 99 | $5.77 | $2.33 | — | $9,790.68 | — | MACD histogram < 0; gate macd_down=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 77 | $7.44 | $2.26 | — | $10,361.30 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `DNA` | 77 | $7.45 | $2.26 | — | $10,932.69 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `PACB` | 457 | $1.26 | $6.00 | — | $11,502.51 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+14.4; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `LZB` | 17 | $33.61 | $2.08 | — | $12,071.80 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-17.4; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1628 | $0.35 | $10.94 | — | $12,637.17 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-29.4; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 63 | $9.01 | $2.22 | — | $13,202.59 | — | MACD histogram < 0; gate macd_down=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $576.36 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ATHM` | 25 | $22.44 | $2.10 | — | $13,761.49 | — | MACD histogram < 0; gate macd_down=True; list earn_react; ret5=-2.1; leftover $576.36 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,761.49 | ▲ close $9,222.61 vs 09:30 $9,241.80 (session +31.02) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,761.49 | ▼ 09:30 equity $9,183.96 vs yday $9,222.61 (-38.65) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 335 | $1.71 | $4.40 | — | $14,329.93 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $574.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QDEL` | 38 | $14.96 | $2.14 | — | $14,896.27 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-1.6; leftover $574.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 184 | $3.11 | $2.60 | — | $15,465.92 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $574.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AAP` | 13 | $42.41 | $2.06 | — | $16,015.18 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-26.1; leftover $574.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `EYPT` | 104 | $5.48 | $2.34 | — | $16,582.76 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-59.9; leftover $574.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `WMT` | 5 | $103.69 | $2.04 | — | $17,099.17 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-10.3; leftover $574.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `BEKE` | 32 | $17.93 | $2.12 | — | $17,670.97 | — | MACD histogram < 0; gate macd_down=True; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $574.00 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,670.97 | ▼ close $9,157.73 vs 09:30 $9,183.96 (session -8.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,670.97 | ▼ 09:30 equity $9,118.71 vs yday $9,157.73 (-39.02) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,670.97 | ▲ close $9,311.56 vs 09:30 $9,118.71 (session +192.85) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,670.97 | ▼ 09:30 equity $9,271.93 vs yday $9,311.56 (-39.63) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `HDSN` | 99 | $5.53 | $2.29 | $+19.14 | $17,121.21 | ▲ +19.14 after sell → book $9,269.64; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `MRVI` | 77 | $8.53 | $2.22 | $-88.41 | $16,462.18 | ▼ -88.41 after sell → book $9,267.42; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `DNA` | 77 | $6.94 | $2.22 | $+34.79 | $15,925.58 | ▲ +34.79 after sell → book $9,265.20; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `PACB` | 457 | $1.33 | $5.90 | $-43.88 | $15,311.87 | ▼ -43.88 after sell → book $9,259.31; vs 09:30 mark -5.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `LZB` | 17 | $32.33 | $2.04 | $+17.64 | $14,760.22 | ▲ +17.64 after sell → book $9,257.27; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEG` | 63 | $9.23 | $2.18 | $-18.25 | $14,176.55 | ▼ -18.25 after sell → book $9,255.09; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ATHM` | 25 | $21.85 | $2.06 | $+10.58 | $13,628.24 | ▲ +10.58 after sell → book $9,253.02; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `VITL` | 59 | $11.12 | $2.20 | — | $14,282.11 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-0.7; leftover $660.93 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CCOI` | 69 | $9.49 | $2.24 | — | $14,934.68 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $660.93 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ZIP` | 145 | $4.55 | $2.48 | — | $15,591.96 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $660.93 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ALIT` | 44 | $14.78 | $2.16 | — | $16,240.12 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+10.6; leftover $660.93 | — |
| 2026-08-25 09:30 ET | **SHORT** | `RGNX` | 81 | $8.14 | $2.27 | — | $16,897.18 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-28.9; leftover $660.93 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AAOI` | 5 | $111.78 | $2.04 | — | $17,454.02 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-30.5; leftover $660.93 | — |
| 2026-08-25 09:30 ET | **SHORT** | `CTKB` | 148 | $4.45 | $2.49 | — | $18,110.13 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-6.3; leftover $660.93 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,110.13 | ▼ close $9,119.43 vs 09:30 $9,271.93 (session -117.72) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,110.13 | ▲ 09:30 equity $9,142.03 vs yday $9,119.43 (+22.60) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `SAFX` | 1628 | $0.35 | $10.63 | $-19.95 | $17,524.82 | ▼ -19.95 after sell → book $9,131.40; vs 09:30 mark -10.63 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ENHA` | 335 | $1.63 | $4.32 | $+18.08 | $16,974.44 | ▲ +18.08 after sell → book $9,127.07; vs 09:30 mark -4.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QDEL` | 38 | $15.09 | $2.10 | $-9.18 | $16,398.92 | ▼ -9.18 after sell → book $9,124.97; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 184 | $2.83 | $2.54 | $+46.38 | $15,875.66 | ▲ +46.38 after sell → book $9,122.43; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AAP` | 13 | $43.87 | $2.03 | $-23.07 | $15,303.32 | ▼ -23.07 after sell → book $9,120.40; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `EYPT` | 104 | $5.03 | $2.30 | $+42.15 | $14,777.90 | ▲ +42.15 after sell → book $9,118.10; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `WMT` | 5 | $105.51 | $2.00 | $-13.14 | $14,248.34 | ▼ -13.14 after sell → book $9,116.09; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `BEKE` | 32 | $18.14 | $2.09 | $-10.77 | $13,665.78 | ▼ -10.77 after sell → book $9,114.01; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `INSP` | 9 | $60.07 | $2.05 | — | $14,204.36 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+6.4; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AVEX` | 32 | $17.51 | $2.12 | — | $14,762.55 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AXTI` | 8 | $65.34 | $2.05 | — | $15,283.23 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BZ` | 33 | $16.77 | $2.12 | — | $15,834.51 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ASPN` | 109 | $5.20 | $2.36 | — | $16,398.95 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-6.3; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `IRDM` | 12 | $46.96 | $2.06 | — | $16,960.41 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-3.9; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `TMCI` | 119 | $4.78 | $2.39 | — | $17,526.84 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+8.1; leftover $569.63 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BKSY` | 22 | $24.94 | $2.09 | — | $18,073.43 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-12.9; leftover $569.63 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,073.43 | ▼ close $8,951.12 vs 09:30 $9,142.03 (session -145.64) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,073.43 | ▼ 09:30 equity $8,883.10 vs yday $8,951.12 (-68.02) | — | — |
| 2026-08-27 09:30 ET | **SHORT** | `ALOY` | 192 | $11.53 | $2.68 | — | $20,284.51 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-7.7; leftover $2220.77 | — |
| 2026-08-27 09:30 ET | **SHORT** | `SVC` | 284 | $7.81 | $3.79 | — | $22,498.76 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-2.1; leftover $2220.77 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,498.76 | ▲ close $9,085.82 vs 09:30 $8,883.10 (session +209.19) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,498.76 | ▲ 09:30 equity $9,100.75 vs yday $9,085.82 (+14.93) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `VITL` | 59 | $10.47 | $2.17 | $+33.98 | $21,878.86 | ▲ +33.98 after sell → book $9,098.58; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CCOI` | 69 | $9.70 | $2.20 | $-18.92 | $21,207.37 | ▼ -18.92 after sell → book $9,096.39; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ZIP` | 145 | $4.21 | $2.42 | $+44.40 | $20,594.49 | ▲ +44.40 after sell → book $9,093.96; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ALIT` | 44 | $14.21 | $2.12 | $+20.80 | $19,967.13 | ▲ +20.80 after sell → book $9,091.84; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `RGNX` | 81 | $10.00 | $2.23 | $-155.17 | $19,154.90 | ▼ -155.17 after sell → book $9,089.61; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AAOI` | 5 | $110.46 | $2.00 | $+2.53 | $18,600.59 | ▲ +2.53 after sell → book $9,087.60; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CTKB` | 148 | $4.62 | $2.43 | $-30.08 | $17,914.40 | ▼ -30.08 after sell → book $9,085.17; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 171 | $3.32 | $2.56 | — | $18,479.56 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+6.4; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1555 | $0.36 | $10.62 | — | $19,036.51 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+7.6; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `BBWI` | 30 | $18.75 | $2.12 | — | $19,596.90 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-5.0; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `QFIN` | 62 | $9.15 | $2.21 | — | $20,161.98 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-19.9; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SYRE` | 6 | $91.75 | $2.04 | — | $20,710.44 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-13.2; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `JKS` | 42 | $13.37 | $2.15 | — | $21,269.83 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-14.9; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `DY` | 1 | $306.34 | $2.02 | — | $21,574.15 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-23.0; leftover $567.82 | — |
| 2026-08-28 09:30 ET | **SHORT** | `CLYM` | 38 | $14.86 | $2.14 | — | $22,136.69 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-11.5; leftover $567.82 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,136.69 | ▲ close $9,364.47 vs 09:30 $9,100.75 (session +305.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,136.69 | ▲ 09:30 equity $9,434.36 vs yday $9,364.47 (+69.89) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `INSP` | 9 | $60.43 | $2.02 | $-7.31 | $21,590.80 | ▼ -7.31 after sell → book $9,432.34; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AVEX` | 32 | $17.63 | $2.09 | $-8.05 | $21,024.56 | ▼ -8.05 after sell → book $9,430.26; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AXTI` | 8 | $60.05 | $2.01 | $+38.26 | $20,542.14 | ▲ +38.26 after sell → book $9,428.24; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BZ` | 33 | $17.70 | $2.09 | $-34.90 | $19,955.95 | ▼ -34.90 after sell → book $9,426.15; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ASPN` | 109 | $4.96 | $2.32 | $+21.48 | $19,413.00 | ▲ +21.48 after sell → book $9,423.84; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `IRDM` | 12 | $46.64 | $2.03 | $-0.25 | $18,851.29 | ▼ -0.25 after sell → book $9,421.81; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `TMCI` | 119 | $4.60 | $2.35 | $+16.68 | $18,301.54 | ▲ +16.68 after sell → book $9,419.46; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BKSY` | 22 | $23.50 | $2.06 | $+27.53 | $17,782.49 | ▲ +27.53 after sell → book $9,417.41; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,782.49 | ▲ close $9,480.76 vs 09:30 $9,434.36 (session +63.35) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,782.49 | ▲ 09:30 equity $9,585.03 vs yday $9,480.76 (+104.27) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `ALOY` | 192 | $10.00 | $2.57 | $+288.52 | $15,859.92 | ▲ +288.52 after sell → book $9,582.47; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `SVC` | 284 | $7.72 | $3.66 | $+18.11 | $13,663.78 | ▲ +18.11 after sell → book $9,578.80; vs 09:30 mark -3.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,663.78 | ▼ close $9,502.42 vs 09:30 $9,585.03 (session -76.39) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,663.78 | ▼ 09:30 equity $9,494.31 vs yday $9,502.42 (-8.11) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 171 | $3.45 | $2.50 | $-27.29 | $13,071.32 | ▼ -27.29 after sell → book $9,491.80; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1555 | $0.39 | $10.73 | $-60.23 | $12,454.14 | ▼ -60.23 after sell → book $9,481.07; vs 09:30 mark -10.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `BBWI` | 30 | $18.41 | $2.08 | $+6.00 | $11,899.76 | ▲ +6.00 after sell → book $9,478.99; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `QFIN` | 62 | $8.38 | $2.18 | $+43.35 | $11,378.03 | ▲ +43.35 after sell → book $9,476.82; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SYRE` | 6 | $88.05 | $2.01 | $+18.15 | $10,847.72 | ▲ +18.15 after sell → book $9,474.81; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `JKS` | 42 | $12.45 | $2.12 | $+34.37 | $10,322.70 | ▲ +34.37 after sell → book $9,472.69; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `DY` | 1 | $287.99 | $1.99 | $+14.34 | $10,032.72 | ▲ +14.34 after sell → book $9,470.70; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `CLYM` | 38 | $14.79 | $2.10 | $-1.58 | $9,468.60 | ▼ -1.58 after sell → book $9,468.60; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,468.60 | ▲ close $9,468.60 vs 09:30 $9,494.31 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,468.60 | ▲ 09:30 equity $9,468.60 vs yday $9,468.60 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 39 | $14.85 | $2.14 | — | $10,045.60 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $55.42 | $2.06 | — | $10,597.75 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-25.9; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `CLYM` | 42 | $13.96 | $2.15 | — | $11,181.92 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `GMRS` | 46 | $12.83 | $2.16 | — | $11,769.93 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=-0.2; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `KLRA` | 37 | $15.95 | $2.14 | — | $12,357.95 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-14.0; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `ALMS` | 57 | $10.38 | $2.20 | — | $12,947.12 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-56.2; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `LX` | 715 | $0.83 | $8.20 | — | $13,530.23 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-30.4; leftover $591.79 | — |
| 2026-09-03 09:30 ET | **SHORT** | `RZLV` | 252 | $2.34 | $3.32 | — | $14,116.59 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-19.9; leftover $591.79 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,116.59 | ▼ close $9,353.66 vs 09:30 $9,468.60 (session -90.57) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,116.59 | ▲ 09:30 equity $9,383.02 vs yday $9,353.66 (+29.36) | — | — |
| 2026-09-04 09:30 ET | **SHORT** | `BHC` | 87 | $6.71 | $2.29 | — | $14,698.07 | — | MACD histogram < 0; gate macd_down=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 36 | $15.90 | $2.13 | — | $15,268.33 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-4.1; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `UAMY` | 111 | $5.25 | $2.37 | — | $15,848.71 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ASTS` | 9 | $63.40 | $2.05 | — | $16,417.26 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=+1.1; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `SCZM` | 58 | $10.03 | $2.20 | — | $16,996.80 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; ret5=+4.0; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PLAY` | 68 | $8.59 | $2.23 | — | $17,578.69 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-5.6; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `CRDO` | 3 | $162.10 | $2.03 | — | $18,062.96 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-31.7; leftover $586.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `AIIO` | 335 | $1.75 | $4.40 | — | $18,644.81 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-24.6; leftover $586.44 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,644.81 | ▼ close $9,249.88 vs 09:30 $9,383.02 (session -113.43) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,644.81 | ▼ 09:30 equity $9,246.15 vs yday $9,249.88 (-3.73) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,644.81 | ▼ close $9,136.76 vs 09:30 $9,246.15 (session -109.39) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,644.81 | ▲ 09:30 equity $9,192.37 vs yday $9,136.76 (+55.61) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 39 | $13.60 | $2.11 | $+44.50 | $18,112.30 | ▲ +44.50 after sell → book $9,190.26; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `EIX` | 10 | $59.49 | $2.02 | $-44.78 | $17,515.38 | ▼ -44.78 after sell → book $9,188.24; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `CLYM` | 42 | $15.82 | $2.12 | $-82.39 | $16,848.82 | ▼ -82.39 after sell → book $9,186.13; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `GMRS` | 46 | $13.80 | $2.13 | $-48.91 | $16,211.89 | ▼ -48.91 after sell → book $9,184.00; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `KLRA` | 37 | $16.27 | $2.10 | $-16.08 | $15,607.80 | ▼ -16.08 after sell → book $9,181.90; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `ALMS` | 57 | $10.49 | $2.16 | $-10.91 | $15,007.71 | ▼ -10.91 after sell → book $9,179.74; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `LX` | 715 | $0.83 | $8.12 | $-22.04 | $14,402.57 | ▼ -22.04 after sell → book $9,171.62; vs 09:30 mark -8.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `RZLV` | 252 | $2.30 | $3.25 | $+3.51 | $13,819.72 | ▲ +3.51 after sell → book $9,168.37; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,819.72 | ▲ close $9,265.37 vs 09:30 $9,192.37 (session +97.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,819.72 | ▲ 09:30 equity $9,398.57 vs yday $9,265.37 (+133.20) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `BHC` | 87 | $6.11 | $2.25 | $+47.66 | $13,285.90 | ▲ +47.66 after sell → book $9,396.32; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `HQ` | 36 | $15.53 | $2.10 | $+9.09 | $12,724.72 | ▲ +9.09 after sell → book $9,394.22; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `UAMY` | 111 | $5.10 | $2.32 | $+11.96 | $12,156.30 | ▲ +11.96 after sell → book $9,391.90; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `ASTS` | 9 | $60.70 | $2.02 | $+20.23 | $11,607.98 | ▲ +20.23 after sell → book $9,389.88; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `SCZM` | 58 | $9.93 | $2.16 | $+1.44 | $11,029.88 | ▲ +1.44 after sell → book $9,387.72; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PLAY` | 68 | $8.07 | $2.19 | $+30.93 | $10,478.92 | ▲ +30.93 after sell → book $9,385.52; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `CRDO` | 3 | $162.35 | $2.00 | $-4.78 | $9,989.88 | ▼ -4.78 after sell → book $9,383.53; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `AIIO` | 335 | $1.81 | $4.32 | $-28.82 | $9,379.20 | ▼ -28.82 after sell → book $9,379.20; vs 09:30 mark -4.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,379.20 | ▲ close $9,379.20 vs 09:30 $9,398.57 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,379.20 | ▲ 09:30 equity $9,379.20 vs yday $9,379.20 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `AUPH` | 36 | $16.28 | $2.13 | — | $9,963.15 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=-1.1; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `OVID` | 214 | $2.73 | $2.82 | — | $10,544.55 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=-3.0; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `AMTX` | 287 | $2.04 | $3.78 | — | $11,126.25 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `TYRA` | 24 | $23.63 | $2.10 | — | $11,691.27 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-6.3; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `LDI` | 689 | $0.85 | $8.06 | — | $12,268.86 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer; 🔵; ret5=-7.8; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `NAVN` | 28 | $20.61 | $2.11 | — | $12,843.83 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-24.7; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 166 | $3.52 | $2.54 | — | $13,425.61 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-19.2; leftover $586.20 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BHVN` | 44 | $13.03 | $2.16 | — | $13,996.77 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-18.5; leftover $586.20 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,996.77 | ▲ close $9,423.51 vs 09:30 $9,379.20 (session +70.01) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,996.77 | ▼ 09:30 equity $9,391.41 vs yday $9,423.51 (-32.10) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,996.77 | ▼ close $9,294.08 vs 09:30 $9,391.41 (session -97.32) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,996.77 | ▲ 09:30 equity $9,331.18 vs yday $9,294.08 (+37.10) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,996.77 | ▲ close $9,353.53 vs 09:30 $9,331.18 (session +22.35) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,996.77 | ▼ 09:30 equity $9,339.28 vs yday $9,353.53 (-14.25) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `AUPH` | 36 | $16.16 | $2.10 | $+0.09 | $13,412.91 | ▲ +0.09 after sell → book $9,337.18; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `OVID` | 214 | $2.72 | $2.76 | $-3.44 | $12,828.07 | ▼ -3.44 after sell → book $9,334.42; vs 09:30 mark -2.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `AMTX` | 287 | $1.89 | $3.70 | $+35.57 | $12,281.94 | ▲ +35.57 after sell → book $9,330.72; vs 09:30 mark -3.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `TYRA` | 24 | $25.58 | $2.06 | $-50.96 | $11,665.96 | ▼ -50.96 after sell → book $9,328.66; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `LDI` | 689 | $0.73 | $7.10 | $+66.82 | $11,155.20 | ▲ +66.82 after sell → book $9,321.56; vs 09:30 mark -7.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `NAVN` | 28 | $22.50 | $2.07 | $-57.10 | $10,523.12 | ▼ -57.10 after sell → book $9,319.48; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 166 | $3.98 | $2.49 | $-81.39 | $9,859.95 | ▼ -81.39 after sell → book $9,316.99; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BHVN` | 44 | $12.34 | $2.12 | $+26.08 | $9,314.87 | ▲ +26.08 after sell → book $9,314.87; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `IQV` | 2 | $270.89 | $2.03 | — | $9,854.62 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+4.0; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `RDNT` | 7 | $77.12 | $2.05 | — | $10,392.41 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; ret5=+7.2; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `AVAH` | 40 | $14.31 | $2.15 | — | $10,962.67 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+4.8; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BLFS` | 15 | $36.46 | $2.07 | — | $11,507.50 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ret5=+2.9; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 31 | $18.61 | $2.12 | — | $12,082.29 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `ARQQ` | 31 | $18.21 | $2.12 | — | $12,644.68 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-19.1; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `TEM` | 8 | $68.79 | $2.05 | — | $13,192.95 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $582.18 | — |
| 2026-09-16 09:30 ET | **SHORT** | `RIG` | 99 | $5.87 | $2.33 | — | $13,771.75 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $582.18 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,771.75 | ▼ close $9,206.49 vs 09:30 $9,339.28 (session -91.47) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,771.75 | ▼ 09:30 equity $9,129.55 vs yday $9,206.49 (-76.94) | — | — |
| 2026-09-17 09:30 ET | **SHORT** | `ILMN` | 2 | $233.85 | $2.03 | — | $14,237.42 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; ret5=+11.7; leftover $652.11 | — |
| 2026-09-17 09:30 ET | **SHORT** | `TWST` | 4 | $151.43 | $2.04 | — | $14,841.11 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; 🔵; ret5=+14.0; leftover $652.11 | — |
| 2026-09-17 09:30 ET | **SHORT** | `PGEN` | 85 | $7.59 | $2.29 | — | $15,483.97 | — | MACD histogram < 0; gate macd_down=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $652.11 | — |
| 2026-09-17 09:30 ET | **SHORT** | `DVLT` | 3835 | $0.17 | $18.69 | — | $16,117.23 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $652.11 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BRUN` | 41 | $15.87 | $2.15 | — | $16,765.75 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $652.11 | — |
| 2026-09-17 09:30 ET | **SHORT** | `AXTI` | 9 | $67.91 | $2.05 | — | $17,374.89 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.7; leftover $652.11 | — |
| 2026-09-17 09:30 ET | **SHORT** | `EROC` | 51 | $12.64 | $2.18 | — | $18,017.35 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-3.6; leftover $652.11 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,017.35 | ▼ close $9,003.11 vs 09:30 $9,129.55 (session -95.02) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,017.35 | ▼ 09:30 equity $8,872.34 vs yday $9,003.11 (-130.77) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 16 | $34.44 | $2.07 | — | $18,566.31 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `TLSA` | 571 | $0.97 | $7.37 | — | $19,112.81 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `BHVN` | 39 | $14.07 | $2.14 | — | $19,659.40 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `DDD` | 154 | $3.58 | $2.50 | — | $20,208.22 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `RANI` | 652 | $0.85 | $7.63 | — | $20,754.78 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+3.6; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `RARE` | 37 | $14.79 | $2.14 | — | $21,299.88 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `DCX` | 1566 | $0.35 | $10.53 | — | $21,843.72 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $554.52 | — |
| 2026-09-18 09:30 ET | **SHORT** | `USDE` | 58 | $9.54 | $2.20 | — | $22,394.84 | — | MACD histogram < 0; gate macd_down=True; list yday_gainer,yday_mover; ret5=+15.8; leftover $554.52 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,394.84 | ▲ close $9,331.94 vs 09:30 $8,872.34 (session +496.20) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,394.84 | ▼ 09:30 equity $8,955.55 vs yday $9,331.94 (-376.39) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `IQV` | 2 | $266.76 | $2.00 | $+4.23 | $21,859.32 | ▲ +4.23 after sell → book $8,953.56; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `RDNT` | 7 | $76.27 | $2.01 | $+1.89 | $21,323.42 | ▲ +1.89 after sell → book $8,951.55; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `AVAH` | 40 | $13.65 | $2.11 | $+22.14 | $20,775.31 | ▲ +22.14 after sell → book $8,949.44; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BLFS` | 15 | $36.70 | $2.04 | $-7.70 | $20,222.77 | ▼ -7.70 after sell → book $8,947.40; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 31 | $22.11 | $2.08 | $-112.70 | $19,535.28 | ▼ -112.70 after sell → book $8,945.32; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `ARQQ` | 31 | $20.55 | $2.08 | $-76.74 | $18,896.15 | ▼ -76.74 after sell → book $8,943.23; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `TEM` | 8 | $79.08 | $2.01 | $-86.38 | $18,261.49 | ▼ -86.38 after sell → book $8,941.22; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `RIG` | 99 | $5.62 | $2.29 | $+20.13 | $17,702.83 | ▲ +20.13 after sell → book $8,938.93; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `HUM` | 1 | $386.20 | $2.02 | — | $18,087.00 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=-5.8; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `DXCM` | 6 | $88.83 | $2.04 | — | $18,617.94 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+7.6; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `MGTX` | 41 | $13.47 | $2.15 | — | $19,168.06 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=+3.6; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `BKKT` | 60 | $9.31 | $2.21 | — | $19,724.46 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `ORBS` | 503 | $1.11 | $6.60 | — | $20,276.19 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SBET` | 55 | $9.99 | $2.19 | — | $20,823.45 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `SGML` | 55 | $10.13 | $2.19 | — | $21,378.68 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+4.9; leftover $558.68 | — |
| 2026-09-21 09:30 ET | **SHORT** | `GLXY` | 21 | $25.95 | $2.09 | — | $21,921.55 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $558.68 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,921.55 | ▲ close $9,045.79 vs 09:30 $8,955.55 (session +128.35) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,921.55 | ▲ 09:30 equity $9,069.05 vs yday $9,045.79 (+23.26) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `DVLT` | 3835 | $0.16 | $17.64 | $+2.02 | $21,290.31 | ▲ +2.02 after sell → book $9,051.41; vs 09:30 mark -17.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **COVER** | `AXTI` | 9 | $76.64 | $2.02 | $-82.64 | $20,598.53 | ▼ -82.64 after sell → book $9,049.39; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 6 | $93.97 | $2.04 | — | $21,160.31 | — | MACD histogram < 0; gate macd_down=True; list flatten; ret5=-0.6; leftover $565.59 | — |
| 2026-09-22 09:30 ET | **SHORT** | `DEFT` | 975 | $0.58 | $8.77 | — | $21,717.04 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $565.59 | — |
| 2026-09-22 09:30 ET | **SHORT** | `ALOY` | 60 | $9.40 | $2.21 | — | $22,278.83 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+9.5; leftover $565.59 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,278.83 | ▼ close $8,976.21 vs 09:30 $9,069.05 (session -60.17) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,278.83 | ▼ 09:30 equity $8,930.10 vs yday $8,976.21 (-46.11) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `ILMN` | 2 | $248.79 | $2.00 | $-33.90 | $21,779.26 | ▼ -33.90 after sell → book $8,928.11; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `TWST` | 4 | $164.35 | $2.00 | $-55.72 | $21,119.85 | ▼ -55.72 after sell → book $8,926.11; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `PGEN` | 85 | $7.95 | $2.25 | $-35.13 | $20,441.86 | ▼ -35.13 after sell → book $8,923.86; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `BRUN` | 41 | $17.10 | $2.11 | $-54.69 | $19,738.65 | ▼ -54.69 after sell → book $8,921.75; vs 09:30 mark -2.11 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `EROC` | 51 | $12.82 | $2.14 | $-13.50 | $19,082.68 | ▼ -13.50 after sell → book $8,919.60; vs 09:30 mark -2.15 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `FIVN` | 16 | $38.91 | $2.04 | $-75.55 | $18,458.17 | ▼ -75.55 after sell → book $8,917.57; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `TLSA` | 571 | $0.89 | $6.79 | $+31.51 | $17,943.18 | ▲ +31.51 after sell → book $8,910.77; vs 09:30 mark -6.80 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `BHVN` | 39 | $14.84 | $2.11 | $-34.28 | $17,362.31 | ▼ -34.28 after sell → book $8,908.66; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `DDD` | 154 | $3.59 | $2.45 | $-6.49 | $16,807.00 | ▼ -6.49 after sell → book $8,906.21; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `RANI` | 652 | $0.81 | $7.24 | $+11.21 | $16,271.64 | ▲ +11.21 after sell → book $8,898.98; vs 09:30 mark -7.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `RARE` | 37 | $15.40 | $2.10 | $-26.81 | $15,699.74 | ▼ -26.81 after sell → book $8,896.87; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `DCX` | 1566 | $0.09 | $6.09 | $+398.37 | $15,554.28 | ▲ +398.37 after sell → book $8,890.78; vs 09:30 mark -6.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **COVER** | `USDE` | 58 | $13.22 | $2.16 | $-217.80 | $14,785.35 | ▼ -217.80 after sell → book $8,888.62; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 5 | $116.85 | $2.04 | — | $15,367.56 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $634.90 | — |
| 2026-09-23 09:30 ET | **SHORT** | `ADMA` | 64 | $9.81 | $2.22 | — | $15,993.18 | — | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $634.90 | — |
| 2026-09-23 09:30 ET | **SHORT** | `OMER` | 30 | $20.65 | $2.12 | — | $16,610.57 | — | MACD histogram < 0; gate macd_down=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $634.90 | — |
| 2026-09-23 09:30 ET | **SHORT** | `MAZE` | 22 | $28.30 | $2.09 | — | $17,231.07 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $634.90 | — |
| 2026-09-23 09:30 ET | **SHORT** | `SGRY` | 40 | $15.72 | $2.15 | — | $17,857.73 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $634.90 | — |
| 2026-09-23 09:30 ET | **SHORT** | `CLPT` | 40 | $15.55 | $2.15 | — | $18,477.58 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $634.90 | — |
| 2026-09-23 09:30 ET | **SHORT** | `NMRA` | 826 | $0.77 | $8.99 | — | $19,102.96 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $634.90 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,102.96 | ▲ close $9,206.81 vs 09:30 $8,930.10 (session +339.94) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,102.96 | ▲ 09:30 equity $9,303.43 vs yday $9,206.81 (+96.62) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `HUM` | 1 | $374.54 | $1.99 | $+7.64 | $18,726.43 | ▲ +7.64 after sell → book $9,301.44; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `DXCM` | 6 | $87.67 | $2.01 | $+2.88 | $18,198.37 | ▲ +2.88 after sell → book $9,299.43; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `MGTX` | 41 | $11.42 | $2.11 | $+79.79 | $17,728.04 | ▲ +79.79 after sell → book $9,297.32; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `BKKT` | 60 | $8.67 | $2.17 | $+34.02 | $17,205.67 | ▲ +34.02 after sell → book $9,295.15; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `ORBS` | 503 | $1.05 | $6.49 | $+17.09 | $16,671.03 | ▲ +17.09 after sell → book $9,288.66; vs 09:30 mark -6.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `SBET` | 55 | $9.80 | $2.15 | $+6.10 | $16,129.87 | ▲ +6.10 after sell → book $9,286.50; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `SGML` | 55 | $9.89 | $2.15 | $+9.13 | $15,583.77 | ▲ +9.13 after sell → book $9,284.35; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **COVER** | `GLXY` | 21 | $25.00 | $2.05 | $+15.91 | $15,056.82 | ▲ +15.91 after sell → book $9,282.29; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,056.82 | ▲ close $9,364.94 vs 09:30 $9,303.43 (session +82.65) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,203.54 | ▼ 09:30 equity $10,160.77 vs yday $10,175.39 (-14.62) | 09:30 open · cash $19,203.54 (unchanged overnight, no fees) · equity $10,160.77 vs prior close $10,175.39 (-14.62) · 16 name(s) re-marked at the open (per-name table). ALOY×64 yday $8.52 → 09:30 $8.52 -0.00; CNXC×19 yday $29.39 → 09:30 $29.39 -0.00; DEFT×1034 yday $0.53 → 09:30 $0.53 -0.00; DLO×42 yday $13.88 → 09:30 $13.88 -0.00; EL×6 yday $95.37 → 09:30 $95.37 -0.00; FJET×302 yday $1.80 → 09:30 $1.80 -0.00; GT×110 yday $5.07 → 09:30 $5.07 -0.00; HALO×5 yday $115.22 → 09:30 $115.36 -0.70; HYMC×27 yday $20.89 → 09:30 $20.89 -0.00; MKC×12 yday $47.82 → 09:30 $47.82 -0.00; NMRA×785 yday $0.70 → 09:30 $0.70 -0.00; OMER×29 yday $20.13 → 09:30 $20.61 -13.92; PACS×14 yday $41.46 → 09:30 $41.46 -0.00; TLYS×138 yday $4.24 → 09:30 $4.24 -0.00; TTAN×9 yday $59.98 → 09:30 $59.98 -0.00; USFD×6 yday $93.82 → 09:30 $93.82 -0.00 | — |
| 2026-09-25 09:30 ET | **SHORT** | `AEHL` | 80 | $9.05 | $2.27 | — | $19,925.27 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=-27.1; leftover $725.77 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `BRVE` | 30 | $23.58 | $2.12 | — | $20,630.55 | — | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $725.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `NEOV` | 303 | $2.39 | $3.99 | — | $21,350.73 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; ret5=-31.4; leftover $725.77 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `SFIX` | 329 | $2.20 | $4.33 | — | $22,070.20 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-24.1; leftover $725.77 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `LRMR` | 218 | $3.32 | $2.88 | — | $22,791.08 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-12.6; leftover $725.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `ACAD` | 32 | $22.21 | $2.13 | — | $23,499.68 | — | MACD histogram < 0; gate macd_down=True; list yday_mover; 🔵; ret5=-19.2; leftover $725.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,499.68 | ▲ close $10,407.50 vs 09:30 $10,160.77 (session +264.45) | 16:00 close · cash $23,499.68 · equity $10,407.50 vs 09:30 $10,160.77 (+246.73; session marks +264.45) · 22 name(s) marked open→close (per-name table). ALOY×64 09:30 $8.52 → close $8.52 -0.00; CNXC×19 09:30 $29.39 → close $29.39 +0.00; DEFT×1034 09:30 $0.53 → close $0.53 -0.00; DLO×42 09:30 $13.88 → close $13.88 -0.00; EL×6 09:30 $95.37 → close $95.37 -0.00; FJET×302 09:30 $1.80 → close $1.80 +0.00; GT×110 09:30 $5.07 → close $5.07 -0.00; HALO×5 09:30 $115.36 → close $113.90 +7.30; HYMC×27 09:30 $20.89 → close $20.89 +0.00; MKC×12 09:30 $47.82 → close $47.82 +0.00; NMRA×785 09:30 $0.70 → close $0.70 -0.00; OMER×29 09:30 $20.61 → close $20.08 +15.37; PACS×14 09:30 $41.46 → close $41.46 +0.00; TLYS×138 09:30 $4.24 → close $4.24 +0.00; TTAN×9 09:30 $59.98 → close $59.98 +0.00; USFD×6 09:30 $93.82 → close $93.82 +0.00; AEHL×80 09:30 $9.05 → close $9.36 -24.80; BRVE×30 09:30 $23.58 → close $20.62 +88.80; NEOV×303 09:30 $2.39 → close $2.19 +60.60; SFIX×329 09:30 $2.20 → close $2.15 +14.81; LRMR×218 09:30 $3.32 → close $3.08 +53.41; ACAD×32 09:30 $22.21 → close $20.68 +48.96 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DAVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DAVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KLC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KLC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PACB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 574.00 < 1 share @ 623.26 |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PACB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BEKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BEKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AAOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CTKB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AAOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CTKB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AVEX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AXTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ASPN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `IRDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TMCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BKSY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SVC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SVC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `QFIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SYRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `JKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WRAP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `QFIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SYRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `JKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GMRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `KLRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RZLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GMRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `KLRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RZLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UAMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASTS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CRDO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AIIO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RARE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNTB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `QMCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UAMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ASTS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `CRDO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AIIO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SARO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ZJYL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `OVID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `REAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OVID` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AXTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `EROC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AXTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EROC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ILMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TWST` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EROC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `USFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `USFD` | 6 | 2026-09-22 @ $93.97 | MACD histogram < 0; gate macd_down=True; list flatten; ret5=-0.6; leftover $565.59 |
| `DEFT` | 975 | 2026-09-22 @ $0.58 | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $565.59 |
| `ALOY` | 60 | 2026-09-22 @ $9.40 | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer; ret5=+9.5; leftover $565.59 |
| `HALO` | 5 | 2026-09-23 @ $116.85 | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $634.90 |
| `ADMA` | 64 | 2026-09-23 @ $9.81 | MACD histogram < 0; gate macd_down=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $634.90 |
| `OMER` | 30 | 2026-09-23 @ $20.65 | MACD histogram < 0; gate macd_down=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $634.90 |
| `MAZE` | 22 | 2026-09-23 @ $28.30 | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $634.90 |
| `SGRY` | 40 | 2026-09-23 @ $15.72 | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $634.90 |
| `CLPT` | 40 | 2026-09-23 @ $15.55 | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $634.90 |
| `NMRA` | 826 | 2026-09-23 @ $0.77 | MACD histogram < 0; gate macd_down=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $634.90 |
