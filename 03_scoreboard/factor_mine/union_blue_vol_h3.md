# Factor mine action — `union_blue_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-16.19%** ($8,381) · signal-only (no cash/fees) was -21.78%. Starts YES **2/30**. Fills 173 · skips 248 · realized $-1054.91.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the name is painted 🔵 (a turn higher on a still-red row).
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
- **Gate** `vol=good,blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,897.59.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▲ close $9,809.66 vs 09:30 $9,768.32 (session +41.34) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,628.11 vs yday $9,809.66 (-181.55) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,454.54 vs 09:30 $9,628.11 (session -173.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,448.22 vs yday $9,454.54 (-6.32) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,182.24 | ▼ -88.28 after sell → book $9,437.32; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,274.50 | ▼ -153.19 after sell → book $9,435.06; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,659.80 | ▲ +131.66 after sell → book $9,431.26; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,813.01 | ▼ -100.46 after sell → book $9,427.34; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,984.52 | ▼ -68.20 after sell → book $9,425.10; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $7,215.86 | ▼ -3.75 after sell → book $9,422.90; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $8,232.71 | ▼ -230.92 after sell → book $9,420.55; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $9,414.48 | ▼ -72.38 after sell → book $9,414.48; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,414.48 | ▲ close $9,414.48 vs 09:30 $9,448.22 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,414.48 | ▲ 09:30 equity $9,414.48 vs yday $9,414.48 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,240.97 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,146.82 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,988.26 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 203 | $5.77 | $2.62 | — | $4,814.33 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,654.00 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,496.32 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 672 | $1.75 | $8.67 | — | $1,311.65 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $153.32 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1176.81 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.32 | ▲ close $9,610.85 vs 09:30 $9,414.48 (session +220.29) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.32 | ▲ 09:30 equity $9,863.41 vs yday $9,610.85 (+252.56) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $135.94 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $124.70 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 7 | $2.47 | $0.19 | — | $107.21 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 9 | $1.93 | $0.20 | — | $89.64 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 14 | $1.32 | $0.23 | — | $70.94 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $19.16 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.94 | ▼ close $9,861.85 vs 09:30 $9,863.41 (session -0.65) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.94 | ▲ 09:30 equity $9,963.14 vs yday $9,861.85 (+101.29) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.94 | ▼ close $9,931.94 vs 09:30 $9,963.14 (session -31.20) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.94 | ▼ 09:30 equity $9,775.42 vs yday $9,931.94 (-156.52) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.32 | $2.18 | $-17.45 | $1,227.00 | ▼ -17.45 after sell → book $9,773.24; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.86 | $2.05 | $+54.13 | $2,375.27 | ▲ +54.13 after sell → book $9,771.19; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.47 | $2.18 | $-14.42 | $3,519.41 | ▼ -14.42 after sell → book $9,769.01; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 203 | $5.53 | $2.66 | $-54.00 | $4,639.34 | ▼ -54.00 after sell → book $9,766.35; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.21 | $2.19 | $+88.87 | $5,888.54 | ▲ +88.87 after sell → book $9,764.16; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $7,146.89 | ▲ +100.68 after sell → book $9,762.03; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 672 | $1.90 | $8.79 | $+83.34 | $8,414.90 | ▲ +83.34 after sell → book $9,753.24; vs 09:30 mark -8.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $9,664.95 | ▲ +91.71 after sell → book $9,751.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 166 | $7.25 | $2.49 | — | $8,458.96 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 88 | $13.59 | $2.25 | — | $7,260.79 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 127 | $9.49 | $2.37 | — | $6,053.19 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $4,868.38 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 265 | $4.55 | $3.42 | — | $3,659.21 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 741 | $1.63 | $9.56 | — | $2,441.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 604 | $2.00 | $7.79 | — | $1,226.03 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 317 | $3.80 | $4.09 | — | $17.34 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1208.12 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.34 | ▲ close $9,979.05 vs 09:30 $9,775.42 (session +261.90) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.34 | ▲ 09:30 equity $9,986.14 vs yday $9,979.05 (+7.09) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $33.75 | ▼ -0.96 after sell → book $9,985.95; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $48.93 | ▲ +3.93 after sell → book $9,985.78; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 7 | $2.41 | $0.21 | $-0.82 | $65.59 | ▼ -0.82 after sell → book $9,985.57; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 9 | $2.03 | $0.23 | $+0.47 | $83.63 | ▲ +0.47 after sell → book $9,985.34; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 14 | $1.60 | $0.29 | $+3.41 | $105.74 | ▲ +3.41 after sell → book $9,985.05; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 60 | $0.58 | $0.53 | — | $70.23 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-27.5; leftover $35.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 6 | $5.81 | $0.37 | — | $35.01 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $35.25 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.01 | ▲ close $10,057.75 vs 09:30 $9,986.14 (session +73.60) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.01 | ▼ 09:30 equity $10,040.58 vs yday $10,057.75 (-17.17) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.01 | ▲ close $10,160.52 vs 09:30 $10,040.58 (session +119.94) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.01 | ▼ 09:30 equity $10,077.58 vs yday $10,160.52 (-82.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 166 | $9.73 | $2.53 | $+406.66 | $1,647.66 | ▲ +406.66 after sell → book $10,075.05; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 88 | $13.05 | $2.28 | $-52.05 | $2,793.78 | ▼ -52.05 after sell → book $10,072.77; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 127 | $9.70 | $2.40 | $+21.90 | $4,023.28 | ▲ +21.90 after sell → book $10,070.37; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 32 | $39.60 | $2.11 | $+80.29 | $5,288.37 | ▲ +80.29 after sell → book $10,068.26; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 265 | $4.21 | $3.47 | $-96.99 | $6,400.55 | ▼ -96.99 after sell → book $10,064.79; vs 09:30 mark -3.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 741 | $1.69 | $9.69 | $+25.21 | $7,643.15 | ▲ +25.21 after sell → book $10,055.10; vs 09:30 mark -9.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 604 | $1.89 | $7.90 | $-82.13 | $8,776.80 | ▼ -82.13 after sell → book $10,047.19; vs 09:30 mark -7.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 317 | $3.77 | $4.15 | $-17.75 | $9,967.74 | ▼ -17.75 after sell → book $10,043.04; vs 09:30 mark -4.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 60 | $32.90 | $2.17 | — | $7,991.57 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1993.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 25 | $79.42 | $2.06 | — | $6,004.01 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1993.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,103.07 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1993.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $2,116.40 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1993.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 85 | $23.30 | $2.25 | — | $133.65 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1993.55 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.65 | ▼ close $9,837.25 vs 09:30 $10,077.58 (session -195.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.65 | ▼ 09:30 equity $9,787.50 vs yday $9,837.25 (-49.75) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 60 | $0.51 | $0.51 | $-5.42 | $163.75 | ▼ -5.42 after sell → book $9,787.00; vs 09:30 mark -0.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 6 | $6.76 | $0.44 | $+4.89 | $203.86 | ▲ +4.89 after sell → book $9,786.55; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.86 | ▲ close $9,795.59 vs 09:30 $9,787.50 (session +9.04) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.86 | ▼ 09:30 equity $9,609.63 vs yday $9,795.59 (-185.96) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.86 | ▲ close $9,711.63 vs 09:30 $9,609.63 (session +102.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.86 | ▼ 09:30 equity $9,684.51 vs yday $9,711.63 (-27.12) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 60 | $32.42 | $2.20 | $-33.17 | $2,146.87 | ▼ -33.17 after sell → book $9,682.32; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 25 | $78.84 | $2.09 | $-18.66 | $4,115.78 | ▼ -18.66 after sell → book $9,680.23; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 13 | $139.65 | $2.05 | $-87.54 | $5,929.17 | ▼ -87.54 after sell → book $9,678.17; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 14 | $133.00 | $2.06 | $-126.73 | $7,789.11 | ▼ -126.73 after sell → book $9,676.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 85 | $22.20 | $2.27 | $-98.02 | $9,673.84 | ▼ -98.02 after sell → book $9,673.84; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,673.84 | ▲ close $9,673.84 vs 09:30 $9,684.51 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,673.84 | ▲ 09:30 equity $9,673.84 vs yday $9,673.84 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,479.77 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.45 | $2.22 | — | $7,272.45 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,102.88 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 72 | $16.77 | $2.21 | — | $4,893.23 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 554 | $2.18 | $7.15 | — | $3,678.36 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 50 | $23.88 | $2.14 | — | $2,482.22 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1860 | $0.65 | $17.67 | — | $1,255.55 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1209.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 324 | $3.73 | $4.18 | — | $42.85 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1209.23 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.85 | ▼ close $9,539.80 vs 09:30 $9,673.84 (session -94.44) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.85 | ▲ 09:30 equity $9,587.40 vs yday $9,539.80 (+47.60) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 1 | $3.46 | $0.04 | — | $39.36 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $5.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 2 | $2.52 | $0.06 | — | $34.26 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $5.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 2 | $1.90 | $0.04 | — | $30.42 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $5.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 1 | $4.78 | $0.05 | — | $25.58 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $5.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 1 | $3.52 | $0.04 | — | $22.03 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $5.36 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.03 | ▼ close $9,564.11 vs 09:30 $9,587.40 (session -23.07) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.03 | ▼ 09:30 equity $9,427.17 vs yday $9,564.11 (-136.94) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.03 | ▼ close $9,350.38 vs 09:30 $9,427.17 (session -76.80) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.03 | ▼ 09:30 equity $9,333.49 vs yday $9,350.38 (-16.89) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $1,151.92 | ▼ -64.17 after sell → book $9,331.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 78 | $15.16 | $2.25 | $-27.09 | $2,332.15 | ▼ -27.09 after sell → book $9,329.20; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $3,452.48 | ▼ -49.25 after sell → book $9,327.17; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 72 | $15.46 | $2.23 | $-98.75 | $4,563.37 | ▼ -98.75 after sell → book $9,324.94; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 554 | $2.22 | $7.25 | $+7.76 | $5,786.00 | ▲ +7.76 after sell → book $9,317.69; vs 09:30 mark -7.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 50 | $23.22 | $2.16 | $-37.30 | $6,944.84 | ▼ -37.30 after sell → book $9,315.53; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DEFT` | 1860 | $0.63 | $17.54 | $-79.85 | $8,091.66 | ▼ -79.85 after sell → book $9,297.99; vs 09:30 mark -17.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CTMX` | 324 | $3.66 | $4.24 | $-31.10 | $9,273.26 | ▼ -31.10 after sell → book $9,293.75; vs 09:30 mark -4.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,273.26 | ▼ close $9,292.56 vs 09:30 $9,333.49 (session -1.19) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,273.26 | ▼ 09:30 equity $9,292.09 vs yday $9,292.56 (-0.47) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 1 | $2.85 | $0.05 | $-0.70 | $9,276.05 | ▼ -0.70 after sell → book $9,292.04; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 2 | $2.22 | $0.07 | $-0.73 | $9,280.42 | ▼ -0.73 after sell → book $9,291.97; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 2 | $1.83 | $0.06 | $-0.25 | $9,284.02 | ▼ -0.25 after sell → book $9,291.91; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 1 | $3.92 | $0.06 | $-0.97 | $9,287.88 | ▼ -0.97 after sell → book $9,291.85; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `EOSE` | 1 | $3.96 | $0.06 | $+0.34 | $9,291.78 | ▲ +0.34 after sell → book $9,291.78; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.78 | ▲ close $9,291.78 vs 09:30 $9,292.09 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.78 | ▲ 09:30 equity $9,291.78 vs yday $9,291.78 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 430 | $2.70 | $5.55 | — | $8,125.24 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 106 | $10.95 | $2.31 | — | $6,962.23 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 236 | $4.91 | $3.04 | — | $5,800.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 215 | $5.38 | $2.77 | — | $4,640.95 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 129 | $8.98 | $2.38 | — | $3,480.15 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+14.1; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `LDI` | 1366 | $0.85 | $15.71 | — | $2,303.34 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 21 | $54.91 | $2.05 | — | $1,148.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+24.3; leftover $1161.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 185 | $6.16 | $2.54 | — | $6.04 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1161.47 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.04 | ▼ close $9,174.51 vs 09:30 $9,291.78 (session -80.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.04 | ▲ 09:30 equity $9,199.09 vs yday $9,174.51 (+24.58) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.04 | ▲ close $9,378.68 vs 09:30 $9,199.09 (session +179.59) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.04 | ▲ 09:30 equity $9,400.44 vs yday $9,378.68 (+21.76) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.04 | ▼ close $9,203.76 vs 09:30 $9,400.44 (session -196.68) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.04 | ▼ 09:30 equity $9,187.10 vs yday $9,203.76 (-16.66) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `INDP` | 430 | $3.66 | $5.63 | $+401.62 | $1,574.21 | ▲ +401.62 after sell → book $9,181.47; vs 09:30 mark -5.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 106 | $10.82 | $2.34 | $-18.42 | $2,718.79 | ▼ -18.42 after sell → book $9,179.14; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 236 | $4.77 | $3.09 | $-39.18 | $3,841.42 | ▼ -39.18 after sell → book $9,176.04; vs 09:30 mark -3.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ANGX` | 215 | $5.30 | $2.82 | $-22.79 | $4,978.10 | ▼ -22.79 after sell → book $9,173.22; vs 09:30 mark -2.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TSSI` | 129 | $8.21 | $2.41 | $-104.12 | $6,034.78 | ▼ -104.12 after sell → book $9,170.82; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LDI` | 1366 | $0.73 | $14.32 | $-192.58 | $7,019.00 | ▼ -192.58 after sell → book $9,156.49; vs 09:30 mark -14.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 21 | $50.69 | $2.07 | $-92.75 | $8,081.42 | ▼ -92.75 after sell → book $9,154.42; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `IRD` | 185 | $5.80 | $2.59 | $-71.73 | $9,151.84 | ▼ -71.73 after sell → book $9,151.84; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 194 | $5.87 | $2.57 | — | $8,010.48 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $6,872.26 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $5,732.36 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 12 | $89.38 | $2.03 | — | $4,657.77 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 49 | $23.29 | $2.14 | — | $3,514.43 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 34 | $33.14 | $2.09 | — | $2,385.57 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-2.9; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 9 | $118.18 | $2.02 | — | $1,319.94 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1143.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $190.88 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1143.98 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.88 | ▼ close $9,076.32 vs 09:30 $9,187.10 (session -58.51) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.88 | ▲ 09:30 equity $9,227.01 vs yday $9,076.32 (+150.69) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $167.88 | — | combo gate; gate vol=good,blue=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $23.86 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 9 | $2.40 | $0.24 | — | $146.03 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $23.86 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 1 | $18.04 | $0.18 | — | $127.82 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $23.86 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 6 | $3.86 | $0.25 | — | $104.41 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $23.86 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 8 | $2.67 | $0.24 | — | $82.77 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-0.4; leftover $23.86 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.77 | ▲ close $9,703.74 vs 09:30 $9,227.01 (session +477.88) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.77 | ▲ 09:30 equity $9,731.54 vs yday $9,703.74 (+27.80) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 10 | $0.97 | $0.13 | — | $72.94 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $10.35 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 2 | $3.95 | $0.09 | — | $64.96 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $10.35 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 29 | $0.35 | $0.19 | — | $54.50 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $10.35 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.50 | ▼ close $9,605.04 vs 09:30 $9,731.54 (session -126.10) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.50 | ▲ 09:30 equity $9,676.36 vs yday $9,605.04 (+71.32) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 194 | $5.62 | $2.61 | $-53.69 | $1,142.17 | ▼ -53.69 after sell → book $9,673.75; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $2,225.10 | ▼ -55.30 after sell → book $9,671.70; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 42 | $28.69 | $2.14 | $+62.95 | $3,427.94 | ▲ +62.95 after sell → book $9,669.56; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 12 | $89.66 | $2.05 | $-0.71 | $4,501.81 | ▼ -0.71 after sell → book $9,667.52; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 49 | $29.43 | $2.16 | $+296.56 | $5,941.73 | ▲ +296.56 after sell → book $9,665.36; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 34 | $40.03 | $2.11 | $+230.06 | $7,300.63 | ▲ +230.06 after sell → book $9,663.25; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 9 | $118.44 | $2.04 | $-1.71 | $8,364.56 | ▼ -1.71 after sell → book $9,661.21; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RVTY` | 8 | $144.53 | $2.03 | $+25.15 | $9,518.76 | ▲ +25.15 after sell → book $9,659.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 119 | $9.99 | $2.35 | — | $8,327.61 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 91 | $13.05 | $2.26 | — | $7,137.79 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 206 | $5.75 | $2.66 | — | $5,949.60 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `FWDI` | 144 | $8.22 | $2.42 | — | $4,763.50 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `DFDV` | 182 | $6.51 | $2.54 | — | $3,576.15 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `CAN` | 2846 | $0.42 | $20.43 | — | $2,366.08 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.7; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 553 | $2.15 | $7.13 | — | $1,170.00 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1189.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 109 | $10.71 | $2.32 | — | $0.29 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1189.85 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.29 | ▼ close $9,526.30 vs 09:30 $9,676.36 (session -90.77) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.29 | ▲ 09:30 equity $9,532.87 vs yday $9,526.30 (+6.57) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 1 | $18.51 | $0.21 | $+0.08 | $18.60 | ▲ +0.08 after sell → book $9,532.67; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 8 | $3.51 | $0.32 | $+6.12 | $46.35 | ▲ +6.12 after sell → book $9,532.34; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.35 | ▲ close $9,618.59 vs 09:30 $9,532.87 (session +86.25) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.35 | ▼ 09:30 equity $9,427.35 vs yday $9,618.59 (-191.24) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 3 | $7.95 | $0.27 | $+0.58 | $69.93 | ▲ +0.58 after sell → book $9,427.08; vs 09:30 mark -0.27 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 9 | $2.24 | $0.25 | $-1.93 | $89.84 | ▼ -1.93 after sell → book $9,426.83; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EMAT` | 6 | $3.65 | $0.26 | $-1.77 | $111.49 | ▼ -1.77 after sell → book $9,426.57; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 10 | $0.89 | $0.14 | $-1.07 | $120.25 | ▼ -1.07 after sell → book $9,426.43; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 2 | $4.10 | $0.11 | $+0.11 | $128.34 | ▲ +0.11 after sell → book $9,426.33; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DCX` | 29 | $0.09 | $0.13 | $-8.01 | $130.79 | ▼ -8.01 after sell → book $9,426.19; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 4 | $3.93 | $0.17 | — | $114.90 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $16.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 1 | $15.72 | $0.16 | — | $99.02 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $16.35 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 1 | $15.55 | $0.16 | — | $83.31 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $16.35 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.31 | ▼ close $9,066.49 vs 09:30 $9,427.35 (session -359.22) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.31 | ▼ 09:30 equity $8,983.74 vs yday $9,066.49 (-82.75) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 119 | $9.80 | $2.38 | $-27.33 | $1,247.13 | ▼ -27.33 after sell → book $8,981.37; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 91 | $12.76 | $2.29 | $-30.94 | $2,406.01 | ▼ -30.94 after sell → book $8,979.08; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 206 | $5.62 | $2.70 | $-33.17 | $3,561.02 | ▼ -33.17 after sell → book $8,976.38; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FWDI` | 144 | $7.94 | $2.46 | $-45.20 | $4,701.93 | ▼ -45.20 after sell → book $8,973.92; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DFDV` | 182 | $5.77 | $2.58 | $-139.79 | $5,749.49 | ▼ -139.79 after sell → book $8,971.34; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CAN` | 2846 | $0.38 | $19.89 | $-142.78 | $6,816.77 | ▼ -142.78 after sell → book $8,951.45; vs 09:30 mark -19.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 553 | $1.88 | $7.24 | $-163.68 | $7,849.18 | ▼ -163.68 after sell → book $8,944.22; vs 09:30 mark -7.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABTC` | 109 | $9.64 | $2.35 | $-121.29 | $8,897.59 | ▼ -121.29 after sell → book $8,941.87; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,897.59 | ▲ close $8,942.40 vs 09:30 $8,983.74 (session +0.53) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,030.17 | ▼ 09:30 equity $8,316.85 vs yday $8,316.86 (-0.01) | 09:30 open · cash $8,030.17 (unchanged overnight, no fees) · equity $8,316.85 vs prior close $8,316.86 (-0.01) · 3 name(s) re-marked at the open (per-name table). INDP×1 yday $4.00 → 09:30 $4.00 +0.00; IVVD×305 yday $0.91 → 09:30 $0.91 +0.00; NMRA×6 yday $0.70 → 09:30 $0.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 38 | $26.27 | $2.10 | — | $7,029.81 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1003.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 56 | $17.91 | $2.16 | — | $6,024.69 | — | combo gate; gate vol=good,blue=True; list probable; 🔵; ret5=+3.7; leftover $1003.77 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 260 | $3.86 | $3.35 | — | $5,017.73 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1003.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 98 | $10.20 | $2.28 | — | $4,015.85 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1003.77 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $3,093.85 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1003.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 61 | $16.21 | $2.17 | — | $2,102.86 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1003.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $1,112.85 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1003.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 33 | $29.80 | $2.09 | — | $127.36 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $1003.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.36 | ▲ close $8,380.85 vs 09:30 $8,316.85 (session +82.18) | 16:00 close · cash $127.36 · equity $8,380.85 vs 09:30 $8,316.85 (+64.00; session marks +82.18) · 11 name(s) marked open→close (per-name table). INDP×1 09:30 $4.00 → close $4.00 +0.00; IVVD×305 09:30 $0.91 → close $0.91 -0.00; NMRA×6 09:30 $0.70 → close $0.70 +0.00; WRBY×38 09:30 $26.27 → close $26.71 +16.72; PL×56 09:30 $17.91 → close $17.43 -26.88; ZSQR×260 09:30 $3.86 → close $3.78 -20.80; DNA×98 09:30 $10.20 → close $10.66 +45.08; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×61 09:30 $16.21 → close $15.96 -15.25; GRAL×8 09:30 $123.50 → close $126.89 +27.12; QMCO×33 09:30 $29.80 → close $31.68 +62.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 1.28 < 1 share @ 14.66 |
| 2026-08-17 | `NU` | cash | leftover split 1.28 < 1 share @ 15.40 |
| 2026-08-17 | `INV` | cash | leftover split 1.28 < 1 share @ 1.62 |
| 2026-08-17 | `KLC` | cash | leftover split 1.28 < 1 share @ 2.62 |
| 2026-08-17 | `ENHA` | cash | leftover split 1.28 < 1 share @ 2.01 |
| 2026-08-17 | `MP` | cash | leftover split 1.28 < 1 share @ 58.01 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 19.16 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 19.16 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 19.16 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 35.25 < 1 share @ 121.87 |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BHC` | cash | leftover split 5.36 < 1 share @ 6.71 |
| 2026-09-04 | `VIR` | cash | leftover split 5.36 < 1 share @ 11.31 |
| 2026-09-04 | `DELL` | cash | leftover split 5.36 < 1 share @ 513.78 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TSSI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TSSI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ARQT` | cash | leftover split 23.86 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 23.86 < 1 share @ 170.85 |
| 2026-09-17 | `BRKR` | cash | leftover split 23.86 < 1 share @ 61.90 |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `EMAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 10.35 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 10.35 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 10.35 < 1 share @ 85.00 |
| 2026-09-18 | `BHVN` | cash | leftover split 10.35 < 1 share @ 14.07 |
| 2026-09-18 | `RARE` | cash | leftover split 10.35 < 1 share @ 14.79 |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EMAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DXCM` | cash | leftover split 16.35 < 1 share @ 89.50 |
| 2026-09-23 | `A` | cash | leftover split 16.35 < 1 share @ 166.54 |
| 2026-09-23 | `OMER` | cash | leftover split 16.35 < 1 share @ 20.65 |
| 2026-09-23 | `MAZE` | cash | leftover split 16.35 < 1 share @ 28.30 |
| 2026-09-23 | `TNGX` | cash | leftover split 16.35 < 1 share @ 25.40 |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INDP` | 4 | 2026-09-23 @ $3.93 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $16.35 |
| `SGRY` | 1 | 2026-09-23 @ $15.72 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $16.35 |
| `CLPT` | 1 | 2026-09-23 @ $15.55 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $16.35 |
