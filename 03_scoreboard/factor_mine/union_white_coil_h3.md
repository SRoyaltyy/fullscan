# Factor mine action — `union_white_coil_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-21.53%** ($7,847) · signal-only (no cash/fees) was -22.27%. Starts YES **0/30**. Fills 125 · skips 128 · realized $-1374.07.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $470.90.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.73 | ▲ close $10,203.79 vs 09:30 $10,000.00 (session +216.83) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $125.63 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $16.84 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 17 | $0.94 | $0.21 | — | $109.49 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $16.84 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 11 | $1.50 | $0.20 | — | $92.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $16.84 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $77.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $16.84 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 3 | $4.31 | $0.14 | — | $64.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $16.84 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 4 | $4.18 | $0.18 | — | $47.87 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $16.84 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.87 | ▲ close $10,222.63 vs 09:30 $10,217.42 (session +6.18) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.87 | ▼ 09:30 equity $10,201.44 vs yday $10,222.63 (-21.19) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $43.78 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $40.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5.98 | — |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 1 | $3.75 | $0.04 | — | $36.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $5.98 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.71 | ▲ close $10,220.48 vs 09:30 $10,201.44 (session +19.17) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.71 | ▼ 09:30 equity $10,103.21 vs yday $10,220.48 (-117.27) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 27 | $60.00 | $2.09 | $+1.24 | $1,654.62 | ▲ +1.24 after sell → book $10,101.12; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $3,309.15 | ▲ +32.50 after sell → book $10,099.01; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 33 | $49.28 | $2.11 | $-18.06 | $4,933.28 | ▼ -18.06 after sell → book $10,096.90; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 142 | $12.66 | $2.45 | $+131.45 | $6,728.55 | ▲ +131.45 after sell → book $10,094.44; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 56 | $27.85 | $2.18 | $-110.18 | $8,285.96 | ▼ -110.18 after sell → book $10,092.26; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 75 | $22.82 | $2.24 | $+56.29 | $9,995.22 | ▲ +56.29 after sell → book $10,090.02; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,995.22 | ▼ close $10,088.14 vs 09:30 $10,103.21 (session -1.88) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,995.22 | ▼ 09:30 equity $10,088.11 vs yday $10,088.14 (-0.03) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,004.02 | ▼ -0.31 after sell → book $10,088.00; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 17 | $0.88 | $0.22 | $-1.40 | $10,018.76 | ▼ -1.40 after sell → book $10,087.78; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 11 | $1.42 | $0.21 | $-1.29 | $10,034.17 | ▼ -1.29 after sell → book $10,087.57; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 1 | $13.03 | $0.15 | $-2.07 | $10,047.05 | ▼ -2.07 after sell → book $10,087.42; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 3 | $4.79 | $0.17 | $+1.13 | $10,061.25 | ▲ +1.13 after sell → book $10,087.25; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 4 | $3.87 | $0.19 | $-1.61 | $10,076.54 | ▼ -1.61 after sell → book $10,087.06; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,076.54 | ▼ close $10,087.04 vs 09:30 $10,088.11 (session -0.01) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,076.54 | ▼ 09:30 equity $10,086.96 vs yday $10,087.04 (-0.08) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,080.40 | ▼ -0.24 after sell → book $10,086.90; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,083.54 | ▼ -0.13 after sell → book $10,086.84; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `MRLN` | 1 | $3.30 | $0.06 | $-0.55 | $10,086.79 | ▼ -0.55 after sell → book $10,086.79; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,831.06 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,645.90 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $6,385.23 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,126.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,880.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 720 | $1.75 | $9.29 | — | $2,610.87 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 169 | $7.44 | $2.50 | — | $1,351.01 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1260.85 | — |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 133 | $9.46 | $2.39 | — | $90.44 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1260.85 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.44 | ▲ close $10,405.63 vs 09:30 $10,086.96 (session +344.33) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.44 | ▲ 09:30 equity $10,665.48 vs yday $10,405.63 (+259.85) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 2 | $5.43 | $0.11 | — | $79.46 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $15.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 6 | $2.43 | $0.16 | — | $64.72 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $15.07 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.72 | ▲ close $10,665.72 vs 09:30 $10,665.48 (session +0.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.72 | ▲ 09:30 equity $10,738.47 vs yday $10,665.72 (+72.75) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.72 | ▼ close $10,671.98 vs 09:30 $10,738.47 (session -66.49) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.72 | ▼ 09:30 equity $10,562.12 vs yday $10,671.98 (-109.86) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 61 | $20.32 | $2.19 | $-18.40 | $1,302.05 | ▼ -18.40 after sell → book $10,559.93; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,546.18 | ▲ +58.97 after sell → book $10,557.88; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 218 | $5.53 | $2.86 | $-57.99 | $3,748.86 | ▼ -57.99 after sell → book $10,555.02; vs 09:30 mark -2.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 64 | $21.21 | $2.20 | $+96.73 | $5,104.10 | ▲ +96.73 after sell → book $10,552.82; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.32 | $2.14 | $+108.73 | $6,459.40 | ▲ +108.73 after sell → book $10,550.68; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 720 | $1.90 | $9.42 | $+89.29 | $7,817.98 | ▲ +89.29 after sell → book $10,541.26; vs 09:30 mark -9.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 169 | $8.53 | $2.54 | $+179.18 | $9,257.01 | ▲ +179.18 after sell → book $10,538.72; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 133 | $9.45 | $2.42 | $-6.14 | $10,511.44 | ▼ -6.14 after sell → book $10,536.30; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 157 | $8.35 | $2.46 | — | $9,198.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 73 | $17.89 | $2.21 | — | $7,889.85 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-7.5; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 691 | $1.90 | $8.91 | — | $6,568.04 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 85 | $15.28 | $2.25 | — | $5,266.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 94 | $13.96 | $2.27 | — | $3,952.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.0; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 11 | $112.17 | $2.02 | — | $2,716.59 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,643.14 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1313.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZM` | 12 | $103.50 | $2.03 | — | $399.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=-0.7; leftover $1313.93 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $399.11 | ▲ close $10,561.85 vs 09:30 $10,562.12 (session +49.70) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $399.11 | ▲ 09:30 equity $10,834.28 vs yday $10,561.85 (+272.43) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `EMBC` | 2 | $4.98 | $0.13 | $-1.14 | $408.95 | ▼ -1.14 after sell → book $10,834.16; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 6 | $2.57 | $0.19 | $+0.48 | $424.18 | ▲ +0.48 after sell → book $10,833.97; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $424.18 | ▲ close $11,066.15 vs 09:30 $10,834.28 (session +232.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $424.18 | ▼ 09:30 equity $11,030.37 vs yday $11,066.15 (-35.78) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $424.18 | ▼ close $10,953.83 vs 09:30 $11,030.37 (session -76.54) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $424.18 | ▲ 09:30 equity $11,010.62 vs yday $10,953.83 (+56.79) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 157 | $8.28 | $2.50 | $-15.95 | $1,721.64 | ▼ -15.95 after sell → book $11,008.12; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ELMT` | 73 | $18.32 | $2.23 | $+26.95 | $3,056.77 | ▲ +26.95 after sell → book $11,005.89; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMTX` | 691 | $1.89 | $9.04 | $-24.86 | $4,353.72 | ▼ -24.86 after sell → book $10,996.85; vs 09:30 mark -9.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 85 | $18.15 | $2.27 | $+239.43 | $5,894.20 | ▲ +239.43 after sell → book $10,994.58; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VIPS` | 94 | $13.58 | $2.30 | $-40.29 | $7,168.42 | ▼ -40.29 after sell → book $10,992.28; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANF` | 11 | $146.07 | $2.05 | $+368.83 | $8,773.14 | ▲ +368.83 after sell → book $10,990.23; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HEI` | 3 | $339.95 | $2.02 | $-55.62 | $9,790.97 | ▼ -55.62 after sell → book $10,988.21; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZM` | 12 | $99.77 | $2.05 | $-48.83 | $10,986.17 | ▼ -48.83 after sell → book $10,986.17; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $9,633.24 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,333.59 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.49 | $2.04 | — | $6,959.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $5,637.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,478.17 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $3,158.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,055.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1373.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `OLED` | 16 | $85.02 | $2.04 | — | $693.63 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1373.27 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $693.63 | ▼ close $10,641.73 vs 09:30 $11,010.62 (session -328.26) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $693.63 | ▲ 09:30 equity $10,693.71 vs yday $10,641.73 (+51.98) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $693.63 | ▼ close $10,682.21 vs 09:30 $10,693.71 (session -11.50) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $693.63 | ▼ 09:30 equity $10,525.69 vs yday $10,682.21 (-156.52) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $693.63 | ▼ close $10,525.07 vs 09:30 $10,525.69 (session -0.62) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $693.63 | ▼ 09:30 equity $10,495.98 vs yday $10,525.07 (-29.09) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 11 | $114.22 | $2.04 | $-98.56 | $1,948.01 | ▼ -98.56 after sell → book $10,493.94; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $3,218.15 | ▼ -29.50 after sell → book $10,491.92; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 15 | $89.39 | $2.06 | $-35.59 | $4,556.94 | ▼ -35.59 after sell → book $10,489.86; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $60.37 | $2.07 | $-55.58 | $5,822.64 | ▼ -55.58 after sell → book $10,487.79; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $268.12 | $2.02 | $-89.30 | $6,893.09 | ▼ -89.30 after sell → book $10,485.76; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $112.09 | $2.04 | $-88.44 | $8,124.04 | ▼ -88.44 after sell → book $10,483.72; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MTSI` | 4 | $260.32 | $2.02 | $-63.54 | $9,163.30 | ▼ -63.54 after sell → book $10,481.70; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OLED` | 16 | $82.40 | $2.06 | $-46.02 | $10,479.64 | ▼ -46.02 after sell → book $10,479.64; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,479.64 | ▲ close $10,479.64 vs 09:30 $10,495.98 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,479.64 | ▲ 09:30 equity $10,479.64 vs yday $10,479.64 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $9,208.46 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,918.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 360 | $3.63 | $4.64 | — | $6,607.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,412.97 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $4,102.68 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 600 | $2.18 | $7.74 | — | $2,786.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 62 | $21.03 | $2.18 | — | $1,480.91 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1309.96 | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 113 | $11.54 | $2.33 | — | $174.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1309.96 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.56 | ▼ close $10,205.42 vs 09:30 $10,479.64 (session -248.95) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.56 | ▼ 09:30 equity $10,151.72 vs yday $10,205.42 (-53.70) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 17 | $2.52 | $0.48 | — | $131.24 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $43.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 6 | $6.71 | $0.42 | — | $90.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $43.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 9 | $4.78 | $0.46 | — | $47.08 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $43.64 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.08 | ▲ close $10,178.44 vs 09:30 $10,151.72 (session +28.07) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.08 | ▼ 09:30 equity $10,152.86 vs yday $10,178.44 (-25.58) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.08 | ▼ close $10,094.69 vs 09:30 $10,152.86 (session -58.17) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.08 | ▼ 09:30 equity $10,032.07 vs yday $10,094.69 (-62.62) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $1,320.84 | ▲ +2.58 after sell → book $10,029.99; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 30 | $42.01 | $2.10 | $-31.78 | $2,579.04 | ▼ -31.78 after sell → book $10,027.89; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 360 | $3.28 | $4.71 | $-135.36 | $3,755.13 | ▼ -135.36 after sell → book $10,023.18; vs 09:30 mark -4.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $4,885.02 | ▼ -64.17 after sell → book $10,021.14; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 78 | $15.46 | $2.25 | $-106.65 | $6,088.65 | ▼ -106.65 after sell → book $10,018.89; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 600 | $2.22 | $7.85 | $+8.41 | $7,412.80 | ▲ +8.41 after sell → book $10,011.04; vs 09:30 mark -7.85 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SDGR` | 62 | $19.88 | $2.20 | $-75.67 | $8,643.17 | ▼ -75.67 after sell → book $10,008.85; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 113 | $11.04 | $2.36 | $-61.19 | $9,888.33 | ▼ -61.19 after sell → book $10,006.49; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,888.33 | ▼ close $10,000.01 vs 09:30 $10,032.07 (session -6.48) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,888.33 | ▼ 09:30 equity $9,998.03 vs yday $10,000.01 (-1.98) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 17 | $2.22 | $0.45 | $-6.03 | $9,925.62 | ▼ -6.03 after sell → book $9,997.58; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 6 | $6.11 | $0.40 | $-4.43 | $9,961.87 | ▼ -4.43 after sell → book $9,997.17; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 9 | $3.92 | $0.40 | $-8.58 | $9,996.77 | ▼ -8.58 after sell → book $9,996.77; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,996.77 | ▲ close $9,996.77 vs 09:30 $9,998.03 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,996.77 | ▲ 09:30 equity $9,996.77 vs yday $9,996.77 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 63 | $52.55 | $2.18 | — | $6,683.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3332.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 329 | $10.11 | $4.24 | — | $3,353.51 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $3332.26 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 1025 | $3.25 | $13.22 | — | $9.04 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $3332.26 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.04 | ▲ close $10,068.08 vs 09:30 $9,996.77 (session +90.95) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.04 | ▼ 09:30 equity $10,020.24 vs yday $10,068.08 (-47.84) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.04 | ▼ close $9,184.99 vs 09:30 $10,020.24 (session -835.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.04 | ▲ 09:30 equity $9,227.43 vs yday $9,184.99 (+42.44) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.04 | ▼ close $8,696.81 vs 09:30 $9,227.43 (session -530.62) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.04 | ▼ 09:30 equity $8,558.65 vs yday $8,696.81 (-138.16) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 63 | $48.60 | $2.21 | $-253.24 | $3,068.62 | ▼ -253.24 after sell → book $8,556.43; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 329 | $9.39 | $4.32 | $-245.45 | $6,153.61 | ▼ -245.45 after sell → book $8,552.11; vs 09:30 mark -4.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ZSQR` | 1025 | $2.34 | $13.41 | $-959.38 | $8,538.70 | ▼ -959.38 after sell → book $8,538.70; vs 09:30 mark -13.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,538.70 | ▲ close $8,538.70 vs 09:30 $8,558.65 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,538.70 | ▲ 09:30 equity $8,538.70 vs yday $8,538.70 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,538.70 | ▲ close $8,538.70 vs 09:30 $8,538.70 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,538.70 | ▲ 09:30 equity $8,538.70 vs yday $8,538.70 (-0.00) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 408 | $20.91 | $5.26 | — | $2.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $8538.70 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.15 | ▲ close $8,647.67 vs 09:30 $8,538.70 (session +114.24) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.15 | ▲ 09:30 equity $8,835.35 vs yday $8,647.67 (+187.68) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.15 | ▼ close $8,684.39 vs 09:30 $8,835.35 (session -150.96) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.15 | ▲ 09:30 equity $8,684.39 vs yday $8,684.39 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.15 | ▲ close $8,684.39 vs 09:30 $8,684.39 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.15 | ▼ 09:30 equity $8,631.35 vs yday $8,684.39 (-53.04) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 408 | $21.15 | $5.40 | $+87.26 | $8,625.95 | ▲ +87.26 after sell → book $8,625.95; vs 09:30 mark -5.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 12 | $89.50 | $2.03 | — | $7,549.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 38 | $27.79 | $2.10 | — | $6,491.80 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 109 | $9.81 | $2.32 | — | $5,420.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 52 | $20.65 | $2.15 | — | $4,344.25 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 9 | $116.00 | $2.02 | — | $3,298.23 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 5 | $196.78 | $2.00 | — | $2,312.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 9 | $109.67 | $2.02 | — | $1,323.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $1078.24 | — |
| 2026-09-23 09:30 ET | **BUY** | `SNX` | 3 | $283.46 | $2.00 | — | $470.90 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1078.24 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $470.90 | ▼ close $8,455.59 vs 09:30 $8,631.35 (session -153.73) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $470.90 | ▼ 09:30 equity $8,348.49 vs yday $8,455.59 (-107.10) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $470.90 | ▲ close $8,408.46 vs 09:30 $8,348.49 (session +59.97) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,074.54 | ▼ 09:30 equity $7,865.20 vs yday $7,865.69 (-0.49) | 09:30 open · cash $7,074.54 (unchanged overnight, no fees) · equity $7,865.20 vs prior close $7,865.69 (-0.49) · 6 name(s) re-marked at the open (per-name table). DXCM×1 yday $87.47 → 09:30 $87.47 +0.00; FTRE×8 yday $20.02 → 09:30 $20.02 +0.00; NTSK×9 yday $18.57 → 09:30 $18.57 +0.00; PAYX×1 yday $101.59 → 09:30 $101.59 +0.00; SAIL×7 yday $22.12 → 09:30 $22.05 -0.49; TTAN×2 yday $59.98 → 09:30 $59.98 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 184 | $7.65 | $2.54 | — | $5,664.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1414.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 16 | $83.76 | $2.04 | — | $4,322.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1414.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 16 | $83.69 | $2.04 | — | $2,981.04 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1414.91 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SENS` | 137 | $10.28 | $2.40 | — | $1,570.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ⚪; ret5=+9.7; leftover $1414.91 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RGEN` | 7 | $189.92 | $2.01 | — | $238.83 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1414.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $238.83 | ▼ close $7,847.30 vs 09:30 $7,865.20 (session -6.87) | 16:00 close · cash $238.83 · equity $7,847.30 vs 09:30 $7,865.20 (-17.90; session marks -6.87) · 11 name(s) marked open→close (per-name table). DXCM×1 09:30 $87.47 → close $87.47 +0.00; FTRE×8 09:30 $20.02 → close $20.02 +0.00; NTSK×9 09:30 $18.57 → close $18.57 -0.00; PAYX×1 09:30 $101.59 → close $101.59 -0.00; SAIL×7 09:30 $22.05 → close $20.64 -9.87; TTAN×2 09:30 $59.98 → close $59.98 -0.00; MRVI×184 09:30 $7.65 → close $7.60 -9.20; TXG×16 09:30 $83.76 → close $85.71 +31.20; TEM×16 09:30 $83.69 → close $85.01 +21.04; SENS×137 09:30 $10.28 → close $10.00 -38.36; RGEN×7 09:30 $189.92 → close $189.68 -1.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 16.84 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 16.84 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.98 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.98 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.98 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.98 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.98 < 1 share @ 16.20 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MRLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MRLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 15.07 < 1 share @ 59.72 |
| 2026-08-21 | `CF` | cash | leftover split 15.07 < 1 share @ 127.43 |
| 2026-08-21 | `TXG` | cash | leftover split 15.07 < 1 share @ 64.39 |
| 2026-08-21 | `BEKE` | cash | leftover split 15.07 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EMBC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `EMBC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ELMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VIPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ELMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MTSI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OLED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MTSI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OLED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 43.64 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PAYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SNX` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DXCM` | 12 | 2026-09-23 @ $89.50 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1078.24 |
| `ARQT` | 38 | 2026-09-23 @ $27.79 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1078.24 |
| `ADMA` | 109 | 2026-09-23 @ $9.81 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1078.24 |
| `OMER` | 52 | 2026-09-23 @ $20.65 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1078.24 |
| `BLLN` | 9 | 2026-09-23 @ $116.00 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1078.24 |
| `CTAS` | 5 | 2026-09-23 @ $196.78 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1078.24 |
| `PAYX` | 9 | 2026-09-23 @ $109.67 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-3.0; leftover $1078.24 |
| `SNX` | 3 | 2026-09-23 @ $283.46 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1078.24 |
