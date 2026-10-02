# Factor mine action — `union_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **-16.34%** ($8,366) · signal-only (no cash/fees) was +1.25%. Starts YES **0/30**. Fills 279 · skips 105 · realized $-1315.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
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
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,684.63.

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
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,106.28 vs 09:30 $10,000.00 (session +115.74) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▲ 09:30 equity $10,117.74 vs yday $10,106.28 (+11.46) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 50 | $47.27 | $2.17 | $-125.81 | $2,389.48 | ▼ -125.81 after sell → book $10,115.57; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 213 | $12.40 | $2.80 | $+143.55 | $5,027.88 | ▲ +143.55 after sell → book $10,112.77; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 84 | $29.15 | $2.28 | $-54.08 | $7,474.20 | ▼ -54.08 after sell → book $10,110.49; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $10,108.12 | ▲ +144.46 after sell → book $10,108.12; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,026.63 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $7,824.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,560.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $5,306.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $4,051.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 216 | $5.84 | $2.79 | — | $2,787.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `LFTO` | 61 | $20.57 | $2.17 | — | $1,530.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-14.0; leftover $1263.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 61 | $20.56 | $2.17 | — | $274.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1263.52 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.27 | ▲ close $10,268.88 vs 09:30 $10,117.74 (session +178.77) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.27 | ▼ 09:30 equity $10,238.82 vs yday $10,268.88 (-30.06) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,375.89 | ▲ +20.13 after sell → book $10,236.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $2,647.85 | ▲ +69.94 after sell → book $10,234.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $3,936.20 | ▲ +24.55 after sell → book $10,232.31; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $5,186.48 | ▼ -4.38 after sell → book $10,230.11; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 95 | $13.84 | $2.30 | $+58.12 | $6,498.98 | ▲ +58.12 after sell → book $10,227.81; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 216 | $5.45 | $2.83 | $-89.86 | $7,673.35 | ▼ -89.86 after sell → book $10,224.98; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LFTO` | 61 | $21.00 | $2.19 | $+21.86 | $8,952.15 | ▲ +21.86 after sell → book $10,222.78; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `REZI` | 61 | $20.83 | $2.19 | $+12.10 | $10,220.59 | ▲ +12.10 after sell → book $10,220.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 315 | $4.05 | $4.06 | — | $8,940.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 151 | $8.46 | $2.44 | — | $7,660.87 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $6,391.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=-7.2; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 394 | $3.24 | $5.08 | — | $5,109.64 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 185 | $6.87 | $2.54 | — | $3,836.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 82 | $15.40 | $2.24 | — | $2,571.11 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 788 | $1.62 | $10.17 | — | $1,284.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1277.57 | — |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 487 | $2.62 | $6.28 | — | $2.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1277.57 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.16 | ▼ close $10,007.11 vs 09:30 $10,238.82 (session -178.63) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.16 | ▼ 09:30 equity $9,848.81 vs yday $10,007.11 (-158.30) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 315 | $3.72 | $4.13 | $-112.14 | $1,169.83 | ▼ -112.14 after sell → book $9,844.68; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 151 | $8.55 | $2.48 | $+8.67 | $2,458.41 | ▲ +8.67 after sell → book $9,842.21; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $3,764.51 | ▲ +36.52 after sell → book $9,840.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 394 | $3.11 | $5.16 | $-61.46 | $4,984.70 | ▼ -61.46 after sell → book $9,835.00; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 185 | $7.50 | $2.59 | $+111.42 | $6,369.61 | ▲ +111.42 after sell → book $9,832.41; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 82 | $14.53 | $2.26 | $-75.84 | $7,558.81 | ▼ -75.84 after sell → book $9,830.15; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 788 | $1.32 | $10.31 | $-252.93 | $8,592.60 | ▼ -252.93 after sell → book $9,819.84; vs 09:30 mark -10.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 487 | $2.52 | $6.37 | $-61.36 | $9,813.47 | ▼ -61.36 after sell → book $9,813.47; vs 09:30 mark -6.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,848.81 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,813.47 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,628.31 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.44 | $2.48 | — | $7,405.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 14 | $82.99 | $2.03 | — | $6,241.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 57 | $21.40 | $2.16 | — | $5,019.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 276 | $4.43 | $3.56 | — | $3,793.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4088 | $0.30 | $24.53 | — | $2,542.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3465 | $0.35 | $22.66 | — | $1,293.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1226.68 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $73.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1226.68 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.21 | ▲ close $9,872.25 vs 09:30 $9,813.47 (session +120.31) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.21 | ▲ 09:30 equity $10,009.36 vs yday $9,872.25 (+137.11) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,315.52 | ▲ +57.15 after sell → book $10,007.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 164 | $8.28 | $2.52 | $+132.76 | $2,670.92 | ▲ +132.76 after sell → book $10,004.79; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CRCL` | 14 | $87.98 | $2.05 | $+65.78 | $3,900.59 | ▲ +65.78 after sell → book $10,002.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 57 | $21.54 | $2.18 | $+3.64 | $5,126.19 | ▲ +3.64 after sell → book $10,000.56; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 276 | $4.68 | $3.62 | $+61.82 | $6,414.25 | ▲ +61.82 after sell → book $9,996.94; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4088 | $0.31 | $25.63 | $-9.27 | $7,655.90 | ▼ -9.27 after sell → book $9,971.31; vs 09:30 mark -25.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAFX` | 3465 | $0.35 | $23.11 | $-59.63 | $8,845.55 | ▼ -59.63 after sell → book $9,948.21; vs 09:30 mark -23.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 26 | $42.41 | $2.09 | $-119.60 | $9,946.12 | ▼ -119.60 after sell → book $9,946.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 503 | $2.47 | $6.49 | — | $8,697.22 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 644 | $1.93 | $8.31 | — | $7,445.99 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $6,249.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,095.72 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 37 | $33.36 | $2.10 | — | $3,859.30 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 727 | $1.71 | $9.38 | — | $2,606.75 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4228 | $0.29 | $25.11 | — | $1,338.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1243.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `PRQR` | 545 | $2.28 | $7.03 | — | $88.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+22.7; leftover $1243.26 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.98 | ▲ close $10,189.78 vs 09:30 $10,009.36 (session +306.15) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.98 | ▲ 09:30 equity $10,271.29 vs yday $10,189.78 (+81.51) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 503 | $2.40 | $6.58 | $-48.28 | $1,289.59 | ▼ -48.28 after sell → book $10,264.71; vs 09:30 mark -6.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 644 | $1.88 | $8.42 | $-48.93 | $2,491.89 | ▼ -48.93 after sell → book $10,256.28; vs 09:30 mark -8.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,699.85 | ▲ +54.14 after sell → book $10,254.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 37 | $32.82 | $2.12 | $-24.20 | $4,912.07 | ▼ -24.20 after sell → book $10,252.12; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 727 | $1.74 | $9.51 | $+2.92 | $6,167.54 | ▲ +2.92 after sell → book $10,242.61; vs 09:30 mark -9.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4228 | $0.38 | $29.59 | $+321.59 | $7,757.27 | ▲ +321.59 after sell → book $10,213.02; vs 09:30 mark -29.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PRQR` | 545 | $2.35 | $7.13 | $+23.99 | $9,030.89 | ▲ +23.99 after sell → book $10,205.89; vs 09:30 mark -7.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,030.89 | ▼ close $10,172.39 vs 09:30 $10,271.29 (session -33.50) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,030.89 | ▲ 09:30 equity $10,189.49 vs yday $10,172.39 (+17.10) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-39.92 | $10,187.42 | ▼ -39.92 after sell → book $10,187.42; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 53 | $23.77 | $2.15 | — | $8,925.46 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+13.0; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 115 | $10.98 | $2.33 | — | $7,660.43 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+1.2; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.19 | $2.05 | — | $6,434.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+7.4; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 257 | $4.94 | $3.32 | — | $5,161.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+7.1; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $4,305.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+6.0; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 175 | $7.25 | $2.52 | — | $3,034.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 335 | $3.80 | $4.32 | — | $1,757.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1273.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 816 | $1.56 | $10.53 | — | $473.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1273.43 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $473.67 | ▲ close $10,432.96 vs 09:30 $10,189.49 (session +274.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $473.67 | ▼ 09:30 equity $10,416.09 vs yday $10,432.96 (-16.87) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 53 | $24.84 | $2.17 | $+52.39 | $1,788.02 | ▲ +52.39 after sell → book $10,413.92; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RZLT` | 257 | $5.01 | $3.37 | $+11.31 | $3,072.22 | ▲ +11.31 after sell → book $10,410.55; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-2.95 | $3,925.21 | ▼ -2.95 after sell → book $10,408.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 175 | $8.29 | $2.56 | $+176.93 | $5,373.40 | ▲ +176.93 after sell → book $10,405.98; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 335 | $3.83 | $4.39 | $+3.02 | $6,653.74 | ▲ +3.02 after sell → book $10,401.59; vs 09:30 mark -4.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 816 | $1.60 | $10.67 | $+11.44 | $7,948.67 | ▲ +11.44 after sell → book $10,390.92; vs 09:30 mark -10.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 119 | $11.12 | $2.35 | — | $6,623.04 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1324.78 | — |
| 2026-08-26 09:30 ET | **BUY** | `AVEX` | 75 | $17.51 | $2.21 | — | $5,307.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.4; leftover $1324.78 | — |
| 2026-08-26 09:30 ET | **BUY** | `AXTI` | 20 | $65.34 | $2.05 | — | $3,998.73 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1324.78 | — |
| 2026-08-26 09:30 ET | **BUY** | `INDP` | 1215 | $1.09 | $15.67 | — | $2,658.70 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $1324.78 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVTS` | 105 | $12.60 | $2.31 | — | $1,333.40 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-5.5; leftover $1324.78 | — |
| 2026-08-26 09:30 ET | **BUY** | `IRDM` | 28 | $46.96 | $2.07 | — | $16.44 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-3.9; leftover $1324.78 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.44 | ▲ close $10,525.66 vs 09:30 $10,416.09 (session +161.41) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.44 | ▲ 09:30 equity $10,725.75 vs yday $10,525.66 (+200.09) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 115 | $10.63 | $2.36 | $-44.95 | $1,236.53 | ▼ -44.95 after sell → book $10,723.39; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+14.08 | $2,476.46 | ▲ +14.08 after sell → book $10,721.32; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVEX` | 75 | $18.43 | $2.24 | $+64.55 | $3,856.47 | ▲ +64.55 after sell → book $10,719.08; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INDP` | 1215 | $1.13 | $15.89 | $+17.04 | $5,213.53 | ▲ +17.04 after sell → book $10,703.19; vs 09:30 mark -15.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVTS` | 105 | $13.18 | $2.33 | $+56.26 | $6,595.10 | ▲ +56.26 after sell → book $10,700.86; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `IRDM` | 28 | $47.46 | $2.09 | $+9.83 | $7,921.89 | ▲ +9.83 after sell → book $10,698.77; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $6,599.73 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+8.7; leftover $1320.31 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 101 | $12.98 | $2.29 | — | $5,286.46 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1320.31 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 42 | $30.79 | $2.12 | — | $3,991.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1320.31 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 136 | $9.68 | $2.40 | — | $2,672.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1320.31 | — |
| 2026-08-27 09:30 ET | **BUY** | `SENS` | 141 | $9.33 | $2.41 | — | $1,354.34 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=+2.5; leftover $1320.31 | — |
| 2026-08-27 09:30 ET | **BUY** | `ACRS` | 214 | $6.15 | $2.76 | — | $35.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.9; leftover $1320.31 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.48 | ▼ close $10,654.55 vs 09:30 $10,725.75 (session -30.08) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.48 | ▼ 09:30 equity $10,573.04 vs yday $10,654.55 (-81.51) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 119 | $11.27 | $2.38 | $+13.13 | $1,374.23 | ▲ +13.13 after sell → book $10,570.66; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 20 | $65.29 | $2.07 | $-5.12 | $2,677.96 | ▼ -5.12 after sell → book $10,568.59; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 101 | $13.05 | $2.32 | $+2.46 | $3,993.69 | ▲ +2.46 after sell → book $10,566.27; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 42 | $30.53 | $2.14 | $-15.17 | $5,273.82 | ▼ -15.17 after sell → book $10,564.14; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 136 | $9.88 | $2.43 | $+22.37 | $6,615.07 | ▲ +22.37 after sell → book $10,561.71; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SENS` | 141 | $9.39 | $2.45 | $+3.60 | $7,936.61 | ▲ +3.60 after sell → book $10,559.26; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACRS` | 214 | $6.10 | $2.81 | $-16.27 | $9,239.20 | ▼ -16.27 after sell → book $10,556.45; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $7,921.09 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1319.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 84 | $15.66 | $2.24 | — | $6,603.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1319.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $5,330.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1319.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $4,067.45 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1319.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 83 | $15.88 | $2.24 | — | $2,747.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+19.4; leftover $1319.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 72 | $18.15 | $2.21 | — | $1,438.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $1319.89 | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 949 | $1.39 | $12.24 | — | $106.81 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1319.89 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.81 | ▼ close $10,239.92 vs 09:30 $10,573.04 (session -291.45) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.81 | ▼ 09:30 equity $10,181.74 vs yday $10,239.92 (-58.18) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.68 | $2.18 | $-21.93 | $1,407.03 | ▼ -21.93 after sell → book $10,179.56; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $2,650.90 | ▼ -74.24 after sell → book $10,177.43; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 84 | $14.44 | $2.27 | $-106.99 | $3,861.60 | ▼ -106.99 after sell → book $10,175.17; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $5,146.58 | ▲ +12.22 after sell → book $10,173.11; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $6,379.80 | ▼ -29.98 after sell → book $10,171.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 83 | $15.46 | $2.26 | $-39.36 | $7,660.72 | ▼ -39.36 after sell → book $10,168.82; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 72 | $17.70 | $2.23 | $-36.83 | $8,932.89 | ▼ -36.83 after sell → book $10,166.59; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 949 | $1.30 | $12.41 | $-110.06 | $10,154.18 | ▼ -110.06 after sell → book $10,154.18; vs 09:30 mark -12.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,154.18 | ▲ close $10,154.18 vs 09:30 $10,181.74 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,154.18 | ▲ 09:30 equity $10,154.18 vs yday $10,154.18 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,154.18 | ▲ close $10,154.18 vs 09:30 $10,154.18 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,154.18 | ▲ 09:30 equity $10,154.18 vs yday $10,154.18 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,154.18 | ▲ close $10,154.18 vs 09:30 $10,154.18 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,154.18 | ▲ 09:30 equity $10,154.18 vs yday $10,154.18 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.45 | $2.24 | — | $8,885.05 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $7,715.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $6,494.18 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-25.9; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3366 | $0.38 | $22.79 | — | $5,202.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-2.3; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 69 | $18.28 | $2.20 | — | $3,938.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+16.5; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1952 | $0.65 | $18.54 | — | $2,651.55 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `GMRS` | 98 | $12.83 | $2.28 | — | $1,391.92 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-0.2; leftover $1269.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `KLRA` | 79 | $15.95 | $2.23 | — | $129.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-14.0; leftover $1269.27 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.65 | ▲ close $10,127.91 vs 09:30 $10,154.18 (session +28.07) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.65 | ▲ 09:30 equity $10,161.66 vs yday $10,127.91 (+33.75) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 82 | $15.00 | $2.26 | $-41.40 | $1,357.39 | ▼ -41.40 after sell → book $10,159.40; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $2,584.31 | ▲ +57.35 after sell → book $10,157.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $3,809.62 | ▲ +4.01 after sell → book $10,155.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3366 | $0.38 | $23.39 | $-42.81 | $5,058.57 | ▼ -42.81 after sell → book $10,131.90; vs 09:30 mark -23.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 69 | $17.27 | $2.22 | $-74.11 | $6,247.99 | ▼ -74.11 after sell → book $10,129.69; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1952 | $0.69 | $19.66 | $+39.88 | $7,575.21 | ▲ +39.88 after sell → book $10,110.03; vs 09:30 mark -19.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GMRS` | 98 | $13.29 | $2.31 | $+40.49 | $8,875.32 | ▲ +40.49 after sell → book $10,107.72; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `KLRA` | 79 | $15.60 | $2.25 | $-32.13 | $10,105.47 | ▼ -32.13 after sell → book $10,105.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 365 | $3.46 | $4.71 | — | $8,837.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 501 | $2.52 | $6.46 | — | $7,568.87 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 188 | $6.71 | $2.55 | — | $6,304.84 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 664 | $1.90 | $8.57 | — | $5,034.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 264 | $4.78 | $3.41 | — | $3,769.35 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 794 | $1.59 | $10.24 | — | $2,496.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 111 | $11.31 | $2.32 | — | $1,238.91 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1263.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.03 | $2.06 | — | $40.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1263.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.16 | ▼ close $10,014.35 vs 09:30 $10,161.66 (session -50.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.16 | ▼ 09:30 equity $9,971.62 vs yday $10,014.35 (-42.73) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 365 | $3.43 | $4.78 | $-20.44 | $1,287.34 | ▼ -20.44 after sell → book $9,966.85; vs 09:30 mark -4.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 501 | $2.38 | $6.56 | $-83.16 | $2,473.16 | ▼ -83.16 after sell → book $9,960.29; vs 09:30 mark -6.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 188 | $6.57 | $2.60 | $-31.47 | $3,705.72 | ▼ -31.47 after sell → book $9,957.69; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 664 | $2.00 | $8.69 | $+49.15 | $5,025.04 | ▲ +49.15 after sell → book $9,949.01; vs 09:30 mark -8.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 264 | $4.30 | $3.46 | $-133.59 | $6,156.78 | ▼ -133.59 after sell → book $9,945.55; vs 09:30 mark -3.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 794 | $1.63 | $10.38 | $+11.13 | $7,440.61 | ▲ +11.13 after sell → book $9,935.16; vs 09:30 mark -10.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 111 | $11.22 | $2.35 | $-14.66 | $8,683.68 | ▼ -14.66 after sell → book $9,932.81; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+48.30 | $9,930.73 | ▲ +48.30 after sell → book $9,930.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,930.73 | ▲ close $9,930.73 vs 09:30 $9,971.62 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,930.73 | ▲ 09:30 equity $9,930.73 vs yday $9,930.73 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,930.73 | ▲ close $9,930.73 vs 09:30 $9,930.73 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,930.73 | ▲ 09:30 equity $9,930.73 vs yday $9,930.73 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,930.73 | ▲ close $9,930.73 vs 09:30 $9,930.73 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,930.73 | ▲ 09:30 equity $9,930.73 vs yday $9,930.73 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 76 | $16.28 | $2.22 | — | $8,691.23 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=-1.1; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 454 | $2.73 | $5.86 | — | $7,445.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=-3.0; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $6,292.94 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $5,186.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+4.7; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 60 | $20.61 | $2.17 | — | $3,947.70 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-24.7; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `SLBT` | 593 | $2.09 | $7.65 | — | $2,700.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-36.6; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `BHVN` | 95 | $13.03 | $2.27 | — | $1,460.55 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-18.5; leftover $1241.34 | — |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 84 | $14.71 | $2.24 | — | $222.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-12.8; leftover $1241.34 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.67 | ▼ close $9,808.06 vs 09:30 $9,930.73 (session -96.24) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.67 | ▼ 09:30 equity $9,632.32 vs yday $9,808.06 (-175.74) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AUPH` | 76 | $16.03 | $2.24 | $-23.46 | $1,438.71 | ▼ -23.46 after sell → book $9,630.08; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `OVID` | 454 | $2.75 | $5.94 | $-0.45 | $2,683.54 | ▼ -0.45 after sell → book $9,624.14; vs 09:30 mark -5.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $3,671.45 | ▼ -165.11 after sell → book $9,622.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NVT` | 7 | $150.00 | $2.03 | $-58.50 | $4,719.41 | ▼ -58.50 after sell → book $9,620.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 60 | $21.10 | $2.19 | $+25.04 | $5,983.22 | ▲ +25.04 after sell → book $9,617.88; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SLBT` | 593 | $2.02 | $7.76 | $-56.92 | $7,173.33 | ▼ -56.92 after sell → book $9,610.13; vs 09:30 mark -7.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHVN` | 95 | $12.52 | $2.30 | $-53.03 | $8,360.43 | ▼ -53.03 after sell → book $9,607.83; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 84 | $14.85 | $2.27 | $+7.25 | $9,605.56 | ▲ +7.25 after sell → book $9,605.56; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,605.56 | ▲ close $9,605.56 vs 09:30 $9,632.32 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,605.56 | ▲ 09:30 equity $9,605.56 vs yday $9,605.56 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,605.56 | ▲ close $9,605.56 vs 09:30 $9,605.56 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,605.56 | ▲ 09:30 equity $9,605.56 vs yday $9,605.56 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 83 | $14.31 | $2.24 | — | $8,415.59 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+4.8; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 45 | $26.27 | $2.12 | — | $7,231.32 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWRD` | 600 | $2.00 | $7.74 | — | $6,023.58 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+0.0; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 116 | $10.30 | $2.34 | — | $4,826.44 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-23.0; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 175 | $6.86 | $2.52 | — | $3,623.42 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-22.4; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 7504 | $0.16 | $34.52 | — | $2,388.26 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.8; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `USDE` | 198 | $6.06 | $2.58 | — | $1,185.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-19.6; leftover $1200.69 | — |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1385 | $0.84 | $15.84 | — | $1.02 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-33.5; leftover $1200.69 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.02 | ▼ close $9,326.93 vs 09:30 $9,605.56 (session -208.72) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.02 | ▼ 09:30 equity $9,293.40 vs yday $9,326.93 (-33.53) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `AVAH` | 83 | $14.33 | $2.26 | $-2.84 | $1,188.14 | ▼ -2.84 after sell → book $9,291.13; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 45 | $26.51 | $2.15 | $+6.53 | $2,378.95 | ▲ +6.53 after sell → book $9,288.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWRD` | 600 | $1.72 | $7.85 | $-183.59 | $3,403.10 | ▼ -183.59 after sell → book $9,281.14; vs 09:30 mark -7.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 175 | $6.96 | $2.55 | $+12.43 | $4,618.54 | ▲ +12.43 after sell → book $9,278.58; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DVLT` | 7504 | $0.17 | $36.52 | $+4.00 | $5,857.70 | ▲ +4.00 after sell → book $9,242.06; vs 09:30 mark -36.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `USDE` | 198 | $6.61 | $2.63 | $+103.69 | $7,163.85 | ▲ +103.69 after sell → book $9,239.43; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `NMRA` | 1385 | $0.78 | $15.20 | $-119.68 | $8,228.95 | ▼ -119.68 after sell → book $9,224.23; vs 09:30 mark -15.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 439 | $2.67 | $5.66 | — | $7,048.97 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-0.4; leftover $1175.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `MRLN` | 517 | $2.27 | $6.67 | — | $5,868.71 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.6; leftover $1175.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `PALI` | 671 | $1.75 | $8.66 | — | $4,685.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-17.6; leftover $1175.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 664 | $1.77 | $8.57 | — | $3,501.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-10.2; leftover $1175.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $2,545.55 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-11.6; leftover $1175.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `INDP` | 356 | $3.30 | $4.59 | — | $1,366.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=+54.3; leftover $1175.56 | — |
| 2026-09-17 09:30 ET | **BUY** | `BTGO` | 179 | $6.56 | $2.53 | — | $189.39 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-14.6; leftover $1175.56 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.39 | ▲ close $9,496.92 vs 09:30 $9,293.40 (session +311.37) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.39 | ▼ 09:30 equity $9,461.45 vs yday $9,496.92 (-35.47) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 116 | $8.68 | $2.37 | $-192.63 | $1,193.91 | ▼ -192.63 after sell → book $9,459.08; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CYPH` | 439 | $3.04 | $5.75 | $+146.63 | $2,520.53 | ▲ +146.63 after sell → book $9,453.34; vs 09:30 mark -5.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MRLN` | 517 | $2.07 | $6.77 | $-116.83 | $3,583.95 | ▼ -116.83 after sell → book $9,446.57; vs 09:30 mark -6.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PALI` | 671 | $1.68 | $8.78 | $-64.40 | $4,702.45 | ▼ -64.40 after sell → book $9,437.79; vs 09:30 mark -8.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 664 | $1.77 | $8.69 | $-17.25 | $5,869.05 | ▼ -17.25 after sell → book $9,429.11; vs 09:30 mark -8.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $6,814.23 | ▼ -11.22 after sell → book $9,427.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `INDP` | 356 | $3.85 | $4.66 | $+186.55 | $8,180.16 | ▲ +186.55 after sell → book $9,422.42; vs 09:30 mark -4.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BTGO` | 179 | $6.94 | $2.57 | $+62.93 | $9,419.86 | ▲ +62.93 after sell → book $9,419.86; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 22 | $209.52 | $2.06 | — | $4,808.36 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $4709.93 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 136 | $34.44 | $2.40 | — | $122.12 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+14.0; leftover $4709.93 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.12 | ▼ close $9,101.72 vs 09:30 $9,461.45 (session -313.68) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.12 | ▲ 09:30 equity $9,230.12 vs yday $9,101.72 (+128.40) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 22 | $210.00 | $2.10 | $+6.40 | $4,740.02 | ▲ +6.40 after sell → book $9,228.02; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 136 | $33.00 | $2.46 | $-200.69 | $9,225.56 | ▼ -200.69 after sell → book $9,225.56; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `AEHL` | 159 | $8.26 | $2.47 | — | $7,909.76 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+7.7; leftover $1317.94 | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 32 | $40.00 | $2.09 | — | $6,627.67 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-32.2; leftover $1317.94 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 219 | $6.00 | $2.83 | — | $5,310.84 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-24.1; leftover $1317.94 | — |
| 2026-09-21 09:30 ET | **BUY** | `KDK` | 385 | $3.42 | $4.97 | — | $3,989.18 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-12.3; leftover $1317.94 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 12 | $105.72 | $2.03 | — | $2,718.51 | — | union ∩ last_red, no 🚨; gate last_red=True; list overnight; ret5=-11.5; leftover $1317.94 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 63 | $20.85 | $2.18 | — | $1,402.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list overnight; ret5=-2.5; leftover $1317.94 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 19 | $68.39 | $2.05 | — | $101.33 | — | union ∩ last_red, no 🚨; gate last_red=True; list overnight; ret5=-7.0; leftover $1317.94 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.33 | ▼ close $8,872.66 vs 09:30 $9,230.12 (session -334.31) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.33 | ▼ 09:30 equity $8,870.47 vs yday $8,872.66 (-2.19) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 219 | $5.99 | $2.87 | $-7.89 | $1,410.26 | ▼ -7.89 after sell → book $8,867.59; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 347 | $0.58 | $3.05 | — | $1,205.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $201.47 | — |
| 2026-09-22 09:30 ET | **BUY** | `MX` | 63 | $3.18 | $2.18 | — | $1,003.43 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+11.1; leftover $201.47 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,003.43 | ▼ close $8,846.27 vs 09:30 $8,870.47 (session -16.09) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,003.43 | ▲ 09:30 equity $8,889.25 vs yday $8,846.27 (+42.98) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `XENE` | 32 | $39.51 | $2.11 | $-19.87 | $2,265.65 | ▼ -19.87 after sell → book $8,887.14; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `KDK` | 385 | $3.19 | $5.04 | $-98.56 | $3,488.76 | ▼ -98.56 after sell → book $8,882.10; vs 09:30 mark -5.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABVX` | 12 | $98.30 | $2.05 | $-93.11 | $4,666.31 | ▼ -93.11 after sell → book $8,880.05; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MLKN` | 63 | $19.76 | $2.20 | $-73.05 | $5,908.99 | ▼ -73.05 after sell → book $8,877.85; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `THO` | 19 | $71.41 | $2.07 | $+53.27 | $7,263.71 | ▲ +53.27 after sell → book $8,875.79; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 347 | $0.57 | $3.10 | $-7.89 | $7,460.13 | ▼ -7.89 after sell → book $8,872.68; vs 09:30 mark -3.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MX` | 63 | $3.19 | $2.20 | $-3.75 | $7,658.90 | ▼ -3.75 after sell → book $8,870.48; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $6,605.24 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1094.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 137 | $7.95 | $2.40 | — | $5,513.69 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1094.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 54 | $20.25 | $2.15 | — | $4,418.03 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1094.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 38 | $28.30 | $2.10 | — | $3,340.53 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1094.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 9 | $116.00 | $2.02 | — | $2,294.51 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1094.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `VICR` | 4 | $266.50 | $2.00 | — | $1,226.51 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1094.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `DNA` | 119 | $9.13 | $2.35 | — | $137.69 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+13.8; leftover $1094.13 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.69 | ▼ close $8,782.44 vs 09:30 $8,889.25 (session -73.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.69 | ▼ 09:30 equity $8,702.33 vs yday $8,782.44 (-80.11) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `AEHL` | 159 | $8.21 | $2.50 | $-12.92 | $1,440.58 | ▼ -12.92 after sell → book $8,699.83; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 9 | $112.22 | $2.04 | $-45.72 | $2,448.52 | ▼ -45.72 after sell → book $8,697.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 137 | $7.38 | $2.43 | $-82.92 | $3,457.15 | ▼ -82.92 after sell → book $8,695.36; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 54 | $19.40 | $2.17 | $-50.22 | $4,502.58 | ▼ -50.22 after sell → book $8,693.19; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `MAZE` | 38 | $28.15 | $2.12 | $-9.93 | $5,570.15 | ▼ -9.93 after sell → book $8,691.06; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 9 | $112.33 | $2.04 | $-37.08 | $6,579.09 | ▼ -37.08 after sell → book $8,689.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+28.42 | $7,675.50 | ▲ +28.42 after sell → book $8,687.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DNA` | 119 | $8.50 | $2.38 | $-79.69 | $8,684.63 | ▼ -79.69 after sell → book $8,684.63; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,684.63 | ▲ close $8,684.63 vs 09:30 $8,702.33 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,721.92 | ▲ 09:30 equity $8,721.92 vs yday $8,721.92 (+0.00) | 09:30 open · cash $8,721.92 · no holdings · equity $8,721.92 vs prior close $8,721.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `REGN` | 1 | $803.87 | $1.99 | — | $7,916.06 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+0.8; leftover $1090.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 52 | $20.61 | $2.15 | — | $6,842.19 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+9.1; leftover $1090.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `NEOV` | 456 | $2.39 | $5.88 | — | $5,746.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-31.4; leftover $1090.24 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SFIX` | 495 | $2.20 | $6.39 | — | $4,651.08 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-24.1; leftover $1090.24 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `LRMR` | 328 | $3.32 | $4.23 | — | $3,557.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-12.6; leftover $1090.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ACAD` | 49 | $22.21 | $2.14 | — | $2,467.46 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-19.2; leftover $1090.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SGMT` | 119 | $9.11 | $2.35 | — | $1,381.03 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $1090.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SMWB` | 145 | $7.50 | $2.42 | — | $291.10 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-9.7; leftover $1090.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.10 | ▼ close $8,409.25 vs 09:30 $8,721.92 (session -285.13) | 16:00 close · cash $291.10 · equity $8,409.25 vs 09:30 $8,721.92 (-312.67; session marks -285.13) · 8 name(s) marked open→close (per-name table). REGN×1 09:30 $803.87 → close $788.04 -15.83; OMER×52 09:30 $20.61 → close $20.08 -27.56; NEOV×456 09:30 $2.39 → close $2.19 -91.20; SFIX×495 09:30 $2.20 → close $2.15 -22.28; LRMR×328 09:30 $3.32 → close $3.08 -80.36; ACAD×49 09:30 $22.21 → close $20.68 -74.97; SGMT×119 09:30 $9.11 → close $9.24 +15.47; SMWB×145 09:30 $7.50 → close $7.58 +11.60 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.10 | ▼ 09:30 equity $8,382.46 vs yday $8,409.25 (-26.79) | 09:30 open · cash $291.10 (unchanged overnight, no fees) · equity $8,382.46 vs prior close $8,409.25 (-26.79) · 8 name(s) re-marked at the open (per-name table). ACAD×49 yday $20.68 → 09:30 $20.53 -7.35; LRMR×328 yday $3.08 → 09:30 $3.23 +51.10; NEOV×456 yday $2.19 → 09:30 $2.20 +4.56; OMER×52 yday $20.08 → 09:30 $19.83 -13.00; REGN×1 yday $788.04 → 09:30 $771.84 -16.20; SFIX×495 yday $2.15 → 09:30 $2.12 -16.33; SGMT×119 yday $9.24 → 09:30 $9.15 -10.71; SMWB×145 yday $7.58 → 09:30 $7.45 -18.85 | — |
| 2026-09-28 09:30 ET | **SELL** | `ACAD` | 49 | $20.53 | $2.16 | $-86.61 | $1,294.91 | ▼ -86.61 after sell → book $8,380.30; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `LRMR` | 328 | $3.23 | $4.30 | $-37.78 | $2,350.32 | ▼ -37.78 after sell → book $8,376.01; vs 09:30 mark -4.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `NEOV` | 456 | $2.20 | $5.97 | $-98.49 | $3,347.55 | ▼ -98.49 after sell → book $8,370.04; vs 09:30 mark -5.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `OMER` | 52 | $19.83 | $2.17 | $-44.87 | $4,376.55 | ▼ -44.87 after sell → book $8,367.87; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `REGN` | 1 | $771.84 | $2.01 | $-36.04 | $5,146.37 | ▼ -36.04 after sell → book $8,365.86; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SFIX` | 495 | $2.12 | $6.48 | $-51.47 | $6,190.28 | ▼ -51.47 after sell → book $8,359.38; vs 09:30 mark -6.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SGMT` | 119 | $9.15 | $2.38 | $+0.04 | $7,276.75 | ▲ +0.04 after sell → book $8,357.00; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SMWB` | 145 | $7.45 | $2.46 | $-12.13 | $8,354.55 | ▼ -12.13 after sell → book $8,354.55; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,354.55 | ▲ close $8,354.55 vs 09:30 $8,382.46 (session +0.00) | 16:00 close · cash $8,354.55 · no lots left · equity $8,354.55. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,354.55 | ▲ 09:30 equity $8,354.55 vs yday $8,354.55 (+0.00) | 09:30 open · cash $8,354.55 · no holdings · equity $8,354.55 vs prior close $8,354.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `MX` | 261 | $3.99 | $3.37 | — | $7,309.79 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+18.4; leftover $1044.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TDTH` | 1173 | $0.89 | $13.96 | — | $6,251.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-36.6; leftover $1044.32 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `OCUL` | 134 | $7.76 | $2.39 | — | $5,209.63 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-23.4; leftover $1044.32 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `CYPH` | 312 | $3.34 | $4.02 | — | $4,163.53 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-2.6; leftover $1044.32 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GRML` | 99 | $10.50 | $2.29 | — | $3,121.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+25.8; leftover $1044.32 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GLND` | 262 | $3.98 | $3.38 | — | $2,075.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+55.9; leftover $1044.32 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `INDP` | 322 | $3.24 | $4.15 | — | $1,028.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=+6.5; leftover $1044.32 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AUR` | 192 | $5.32 | $2.57 | — | $4.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-18.7; leftover $1044.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.16 | ▲ close $8,643.22 vs 09:30 $8,354.55 (session +324.79) | 16:00 close · cash $4.16 · equity $8,643.22 vs 09:30 $8,354.55 (+288.67; session marks +324.79) · 8 name(s) marked open→close (per-name table). MX×261 09:30 $3.99 → close $3.89 -26.10; TDTH×1173 09:30 $0.89 → close $1.14 +293.25; OCUL×134 09:30 $7.76 → close $7.87 +14.74; CYPH×312 09:30 $3.34 → close $3.23 -34.32; GRML×99 09:30 $10.50 → close $10.38 -12.38; GLND×262 09:30 $3.98 → close $4.75 +201.74; INDP×322 09:30 $3.24 → close $2.85 -125.58; AUR×192 09:30 $5.32 → close $5.39 +13.44 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.16 | ▼ 09:30 equity $8,643.21 vs yday $8,643.22 (-0.01) | 09:30 open · cash $4.16 (unchanged overnight, no fees) · equity $8,643.21 vs prior close $8,643.22 (-0.01) · 8 name(s) re-marked at the open (per-name table). AUR×192 yday $5.39 → 09:30 $5.39 +0.00; CYPH×312 yday $3.23 → 09:30 $3.23 +0.00; GLND×262 yday $4.75 → 09:30 $4.75 +0.00; GRML×99 yday $10.38 → 09:30 $10.38 +0.00; INDP×322 yday $2.85 → 09:30 $2.85 +0.00; MX×261 yday $3.89 → 09:30 $3.89 +0.00; OCUL×134 yday $7.87 → 09:30 $7.87 +0.00; TDTH×1173 yday $1.14 → 09:30 $1.14 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.16 | ▲ close $8,643.21 vs 09:30 $8,643.21 (session +0.00) | 16:00 close · cash $4.16 · equity $8,643.21 vs 09:30 $8,643.21 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). AUR×192 09:30 $5.39 → close $5.39 +0.00; CYPH×312 09:30 $3.23 → close $3.23 +0.00; GLND×262 09:30 $4.75 → close $4.75 +0.00; GRML×99 09:30 $10.38 → close $10.38 +0.00; INDP×322 09:30 $2.85 → close $2.85 +0.00; MX×261 09:30 $3.89 → close $3.89 +0.00; OCUL×134 09:30 $7.87 → close $7.87 +0.00; TDTH×1173 09:30 $1.14 → close $1.14 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.16 | ▼ 09:30 equity $8,311.38 vs yday $8,643.21 (-331.83) | 09:30 open · cash $4.16 (unchanged overnight, no fees) · equity $8,311.38 vs prior close $8,643.21 (-331.83) · 8 name(s) re-marked at the open (per-name table). AUR×192 yday $5.39 → 09:30 $5.58 +36.48; CYPH×312 yday $3.23 → 09:30 $3.13 -31.20; GLND×262 yday $4.75 → 09:30 $5.06 +81.22; GRML×99 yday $10.38 → 09:30 $10.04 -33.17; INDP×322 yday $2.85 → 09:30 $2.28 -183.54; MX×261 yday $3.89 → 09:30 $3.78 -28.71; OCUL×134 yday $7.87 → 09:30 $7.63 -32.16; TDTH×1173 yday $1.14 → 09:30 $1.02 -140.76 | — |
| 2026-10-01 09:30 ET | **SELL** | `AUR` | 192 | $5.58 | $2.61 | $+44.75 | $1,072.91 | ▲ +44.75 after sell → book $8,308.77; vs 09:30 mark -2.61 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `CYPH` | 312 | $3.13 | $4.09 | $-73.63 | $2,045.39 | ▼ -73.63 after sell → book $8,304.69; vs 09:30 mark -4.08 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GLND` | 262 | $5.06 | $3.43 | $+276.15 | $3,367.67 | ▲ +276.15 after sell → book $8,301.25; vs 09:30 mark -3.44 | exit unpriced hold on first bar after 2 sess | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `GRML` | 99 | $10.04 | $2.31 | $-50.14 | $4,359.32 | ▼ -50.14 after sell → book $8,298.94; vs 09:30 mark -2.31 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `INDP` | 322 | $2.28 | $4.22 | $-317.49 | $5,089.26 | ▼ -317.49 after sell → book $8,294.72; vs 09:30 mark -4.22 | exit unpriced hold on first bar after 2 sess | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `MX` | 261 | $3.78 | $3.42 | $-61.60 | $6,072.42 | ▼ -61.60 after sell → book $8,291.30; vs 09:30 mark -3.42 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `OCUL` | 134 | $7.63 | $2.42 | $-22.24 | $7,092.42 | ▼ -22.24 after sell → book $8,288.88; vs 09:30 mark -2.42 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `TDTH` | 1173 | $1.02 | $15.34 | $+123.19 | $8,273.54 | ▲ +123.19 after sell → book $8,273.54; vs 09:30 mark -15.34 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 12 | $92.93 | $2.03 | — | $7,156.35 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+0.2; leftover $1181.93 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 55 | $21.28 | $2.15 | — | $5,983.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+5.7; leftover $1181.93 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `CAPR` | 126 | $9.36 | $2.37 | — | $4,802.07 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+9.2; leftover $1181.93 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `LQDA` | 49 | $23.96 | $2.14 | — | $3,625.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-55.2; leftover $1181.93 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AIB` | 844 | $1.40 | $10.89 | — | $2,433.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-8.9; leftover $1181.93 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PYXS` | 511 | $2.31 | $6.59 | — | $1,246.40 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-27.6; leftover $1181.93 | join🟢 sector🔴 gen🔴 news🔴 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `GLAS` | 210 | $5.62 | $2.71 | — | $63.50 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; ret5=-28.5; leftover $1181.93 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.50 | ▲ close $8,366.26 vs 09:30 $8,311.38 (session +121.59) | 16:00 close · cash $63.50 · equity $8,366.26 vs 09:30 $8,311.38 (+54.88; session marks +121.59) · 7 name(s) marked open→close (per-name table). KSPI×12 09:30 $92.93 → close $92.02 -10.92; RELY×55 09:30 $21.28 → close $21.48 +11.00; CAPR×126 09:30 $9.36 → close $8.94 -52.92; LQDA×49 09:30 $23.96 → close $27.99 +197.47; AIB×844 09:30 $1.40 → close $1.52 +101.28; PYXS×511 09:30 $2.31 → close $2.19 -61.32; GLAS×210 09:30 $5.62 → close $5.32 -63.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLQT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ORBS` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CNTB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HAS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BHC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ON` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SYNA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CDW` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ADBT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `XHLD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARQQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `XENE` | no_price | no 09:30 open — carry |
| 2026-09-22 | `KDK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open |
| 2026-09-22 | `BRVE` | no_price | no 09:30 open |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CGEM` | hard_red | hard-red S=-7.66 sit; no new buys |
