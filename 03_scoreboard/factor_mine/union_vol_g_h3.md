# Factor mine action — `union_vol_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g, no 🚨

Cash book **-8.71%** ($9,129) · signal-only (no cash/fees) was -14.17%. Starts YES **21/30**. Fills 195 · skips 286 · realized $-156.04.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,496.73.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,240.97 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,146.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,988.26 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 203 | $5.77 | $2.62 | — | $4,814.33 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,654.00 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,496.32 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 672 | $1.75 | $8.67 | — | $1,311.65 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1176.81 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $153.32 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1176.81 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.32 | ▲ close $9,610.85 vs 09:30 $9,414.48 (session +220.29) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.32 | ▲ 09:30 equity $9,863.41 vs yday $9,610.85 (+252.56) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $135.94 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $124.70 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 7 | $2.47 | $0.19 | — | $107.21 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 9 | $1.93 | $0.20 | — | $89.64 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $19.16 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 14 | $1.32 | $0.23 | — | $70.94 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $19.16 | — |
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
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 166 | $7.25 | $2.49 | — | $8,458.96 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 88 | $13.59 | $2.25 | — | $7,260.79 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 127 | $9.49 | $2.37 | — | $6,053.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $4,868.38 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 265 | $4.55 | $3.42 | — | $3,659.21 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 741 | $1.63 | $9.56 | — | $2,441.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 604 | $2.00 | $7.79 | — | $1,226.03 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1208.12 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 317 | $3.80 | $4.09 | — | $17.34 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1208.12 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.34 | ▲ close $9,979.05 vs 09:30 $9,775.42 (session +261.90) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.34 | ▲ 09:30 equity $9,986.14 vs yday $9,979.05 (+7.09) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $33.75 | ▼ -0.96 after sell → book $9,985.95; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $48.93 | ▲ +3.93 after sell → book $9,985.78; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 7 | $2.41 | $0.21 | $-0.82 | $65.59 | ▼ -0.82 after sell → book $9,985.57; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 9 | $2.03 | $0.23 | $+0.47 | $83.63 | ▲ +0.47 after sell → book $9,985.34; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 14 | $1.60 | $0.29 | $+3.41 | $105.74 | ▲ +3.41 after sell → book $9,985.05; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 60 | $0.58 | $0.53 | — | $70.23 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ret5=-27.5; leftover $35.25 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 6 | $5.81 | $0.37 | — | $35.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $35.25 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.01 | ▲ close $10,057.75 vs 09:30 $9,986.14 (session +73.60) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.01 | ▼ 09:30 equity $10,040.58 vs yday $10,057.75 (-17.17) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.01 | ▲ close $10,160.52 vs 09:30 $10,040.58 (session +119.94) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.01 | ▼ 09:30 equity $10,077.58 vs yday $10,160.52 (-82.94) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 88 | $13.05 | $2.28 | $-52.05 | $1,181.13 | ▼ -52.05 after sell → book $10,075.30; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 127 | $9.70 | $2.40 | $+21.90 | $2,410.63 | ▲ +21.90 after sell → book $10,072.90; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 32 | $39.60 | $2.11 | $+80.29 | $3,675.72 | ▲ +80.29 after sell → book $10,070.79; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 265 | $4.21 | $3.47 | $-96.99 | $4,787.90 | ▼ -96.99 after sell → book $10,067.32; vs 09:30 mark -3.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 741 | $1.69 | $9.69 | $+25.21 | $6,030.49 | ▲ +25.21 after sell → book $10,057.62; vs 09:30 mark -9.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 604 | $1.89 | $7.90 | $-82.13 | $7,164.15 | ▼ -82.13 after sell → book $10,049.72; vs 09:30 mark -7.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 317 | $3.77 | $4.15 | $-17.75 | $8,355.09 | ▼ -17.75 after sell → book $10,045.57; vs 09:30 mark -4.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $32.90 | $2.10 | — | $7,168.59 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1193.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $79.42 | $2.04 | — | $5,975.26 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1193.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $4,804.68 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1193.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 75 | $15.88 | $2.21 | — | $3,611.47 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+19.4; leftover $1193.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 65 | $18.15 | $2.19 | — | $2,429.53 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1193.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $1,293.44 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1193.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 62 | $19.25 | $2.18 | — | $97.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer; ret5=+14.1; leftover $1193.58 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.76 | ▼ close $9,775.90 vs 09:30 $10,077.58 (session -254.93) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.76 | ▼ 09:30 equity $9,734.50 vs yday $9,775.90 (-41.40) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 166 | $9.50 | $2.53 | $+368.48 | $1,672.24 | ▲ +368.48 after sell → book $9,731.98; vs 09:30 mark -2.52 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 60 | $0.51 | $0.51 | $-5.42 | $1,702.33 | ▼ -5.42 after sell → book $9,731.47; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,702.33 | ▼ close $9,715.63 vs 09:30 $9,734.50 (session -15.84) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,702.33 | ▼ 09:30 equity $9,610.45 vs yday $9,715.63 (-105.18) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `USDE` | 6 | $8.15 | $0.53 | $+13.15 | $1,750.70 | ▲ +13.15 after sell → book $9,609.92; vs 09:30 mark -0.53 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,750.70 | ▲ close $9,653.66 vs 09:30 $9,610.45 (session +43.74) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,750.70 | ▲ 09:30 equity $9,678.76 vs yday $9,653.66 (+25.10) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $32.42 | $2.12 | $-21.50 | $2,915.70 | ▼ -21.50 after sell → book $9,676.64; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $78.84 | $2.06 | $-12.79 | $4,096.25 | ▼ -12.79 after sell → book $9,674.59; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $139.65 | $2.03 | $-55.41 | $5,211.42 | ▼ -55.41 after sell → book $9,672.56; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 75 | $15.97 | $2.24 | $+2.30 | $6,406.93 | ▲ +2.30 after sell → book $9,670.32; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 65 | $17.65 | $2.21 | $-36.89 | $7,551.97 | ▼ -36.89 after sell → book $9,668.11; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $8,613.94 | ▼ -74.13 after sell → book $9,666.08; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 62 | $16.97 | $2.20 | $-145.73 | $9,663.88 | ▼ -145.73 after sell → book $9,663.88; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,663.88 | ▲ close $9,663.88 vs 09:30 $9,678.76 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,663.88 | ▲ 09:30 equity $9,663.88 vs yday $9,663.88 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,469.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.45 | $2.22 | — | $7,262.49 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,092.92 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 72 | $16.77 | $2.21 | — | $4,883.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 21 | $55.42 | $2.05 | — | $3,717.40 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-25.9; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 554 | $2.18 | $7.15 | — | $2,502.53 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 678 | $1.78 | $8.75 | — | $1,286.95 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+183.1; leftover $1207.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 66 | $18.28 | $2.19 | — | $78.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+16.5; leftover $1207.99 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.28 | ▼ close $9,185.25 vs 09:30 $9,663.88 (session -450.04) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.28 | ▲ 09:30 equity $9,282.92 vs yday $9,185.25 (+97.67) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 2 | $3.46 | $0.08 | — | $71.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $9.78 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 3 | $2.52 | $0.08 | — | $63.64 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $9.78 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $56.86 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $9.78 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 5 | $1.90 | $0.11 | — | $47.25 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $9.78 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $37.59 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $9.78 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 6 | $1.59 | $0.11 | — | $27.93 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $9.78 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 2 | $3.52 | $0.08 | — | $20.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $9.78 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.82 | ▲ close $9,505.94 vs 09:30 $9,282.92 (session +223.65) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.82 | ▼ 09:30 equity $9,420.85 vs yday $9,505.94 (-85.09) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.82 | ▼ close $9,361.37 vs 09:30 $9,420.85 (session -59.48) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.82 | ▼ 09:30 equity $9,316.89 vs yday $9,361.37 (-44.48) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $1,150.71 | ▼ -64.17 after sell → book $9,314.85; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 78 | $15.16 | $2.25 | $-27.09 | $2,330.94 | ▼ -27.09 after sell → book $9,312.60; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $3,451.27 | ▼ -49.25 after sell → book $9,310.57; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 72 | $15.46 | $2.23 | $-98.75 | $4,562.16 | ▼ -98.75 after sell → book $9,308.34; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EIX` | 21 | $59.49 | $2.07 | $+81.34 | $5,809.38 | ▲ +81.34 after sell → book $9,306.27; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 554 | $2.22 | $7.25 | $+7.76 | $7,032.01 | ▲ +7.76 after sell → book $9,299.02; vs 09:30 mark -7.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 678 | $1.45 | $8.87 | $-241.35 | $8,006.24 | ▼ -241.35 after sell → book $9,290.15; vs 09:30 mark -8.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRVO` | 66 | $18.60 | $2.21 | $+16.72 | $9,231.63 | ▲ +16.72 after sell → book $9,287.94; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,231.63 | ▼ close $9,285.22 vs 09:30 $9,316.89 (session -2.72) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,231.63 | ▼ 09:30 equity $9,284.20 vs yday $9,285.22 (-1.02) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 2 | $2.85 | $0.08 | $-1.38 | $9,237.25 | ▼ -1.38 after sell → book $9,284.12; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 3 | $2.22 | $0.10 | $-1.08 | $9,243.81 | ▼ -1.08 after sell → book $9,284.03; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $9,249.84 | ▼ -0.75 after sell → book $9,283.94; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 5 | $1.83 | $0.13 | $-0.59 | $9,258.86 | ▼ -0.59 after sell → book $9,283.82; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $9,266.60 | ▼ -1.92 after sell → book $9,283.71; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 6 | $1.53 | $0.13 | $-0.60 | $9,275.65 | ▼ -0.60 after sell → book $9,283.58; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `EOSE` | 2 | $3.96 | $0.11 | $+0.71 | $9,283.48 | ▲ +0.71 after sell → book $9,283.48; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,283.48 | ▲ close $9,283.48 vs 09:30 $9,284.20 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,283.48 | ▲ 09:30 equity $9,283.48 vs yday $9,283.48 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,130.46 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 49 | $23.63 | $2.14 | — | $6,970.45 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-6.3; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $5,808.46 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+2.5; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 429 | $2.70 | $5.53 | — | $4,644.63 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 370 | $3.13 | $4.77 | — | $3,481.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+24.2; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 105 | $10.95 | $2.31 | — | $2,329.70 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 196 | $5.91 | $2.58 | — | $1,168.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1160.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 236 | $4.91 | $3.04 | — | $6.96 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1160.43 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.96 | ▼ close $9,142.80 vs 09:30 $9,283.48 (session -116.26) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.96 | ▲ 09:30 equity $9,206.19 vs yday $9,142.80 (+63.39) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.96 | ▲ close $9,730.78 vs 09:30 $9,206.19 (session +524.59) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.96 | ▲ 09:30 equity $9,766.18 vs yday $9,730.78 (+35.40) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.96 | ▲ close $9,822.15 vs 09:30 $9,766.18 (session +55.97) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.96 | ▼ 09:30 equity $9,736.40 vs yday $9,822.15 (-85.75) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 7 | $140.03 | $2.03 | $-174.84 | $985.14 | ▼ -174.84 after sell → book $9,734.37; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 49 | $25.58 | $2.16 | $+91.26 | $2,236.40 | ▲ +91.26 after sell → book $9,732.21; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 15 | $76.75 | $2.06 | $-12.79 | $3,385.60 | ▼ -12.79 after sell → book $9,730.16; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 370 | $3.48 | $4.84 | $+119.88 | $4,668.35 | ▲ +119.88 after sell → book $9,725.31; vs 09:30 mark -4.85 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 105 | $10.82 | $2.33 | $-18.29 | $5,802.12 | ▼ -18.29 after sell → book $9,722.98; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 196 | $6.25 | $2.62 | $+61.44 | $7,024.50 | ▲ +61.44 after sell → book $9,720.36; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 236 | $4.77 | $3.09 | $-39.18 | $8,147.12 | ▼ -39.18 after sell → book $9,717.26; vs 09:30 mark -3.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $6,988.29 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1163.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 198 | $5.87 | $2.58 | — | $5,823.45 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1163.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 427 | $2.72 | $5.51 | — | $4,656.50 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-0.4; leftover $1163.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $3,518.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1163.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $2,378.37 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1163.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 90 | $12.89 | $2.26 | — | $1,216.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=-18.2; leftover $1163.87 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $52.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1163.87 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.04 | ▼ close $9,473.29 vs 09:30 $9,736.40 (session -225.41) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.04 | ▲ 09:30 equity $9,590.10 vs yday $9,473.29 (+116.81) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 429 | $3.30 | $5.62 | $+246.25 | $1,462.13 | ▲ +246.25 after sell → book $9,584.49; vs 09:30 mark -5.61 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $1,313.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; ret5=+17.7; leftover $182.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 24 | $7.59 | $1.89 | — | $1,128.98 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $182.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 1075 | $0.17 | $5.05 | — | $941.18 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $182.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 11 | $15.87 | $1.78 | — | $764.83 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $182.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 7 | $25.95 | $1.84 | — | $581.35 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $182.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $408.78 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $182.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 76 | $2.40 | $2.05 | — | $224.33 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $182.77 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.33 | ▲ close $9,769.58 vs 09:30 $9,590.10 (session +200.90) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.33 | ▲ 09:30 equity $9,804.02 vs yday $9,769.58 (+34.44) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 28 | $0.97 | $0.36 | — | $196.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $28.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 7 | $3.95 | $0.30 | — | $168.87 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $28.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $154.65 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $28.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $139.71 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $28.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 79 | $0.35 | $0.52 | — | $111.23 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $28.04 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.23 | ▼ close $9,648.70 vs 09:30 $9,804.02 (session -153.86) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.23 | ▲ 09:30 equity $9,699.36 vs yday $9,648.70 (+50.66) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $1,253.23 | ▼ -16.84 after sell → book $9,697.31; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 198 | $5.62 | $2.63 | $-54.71 | $2,363.36 | ▼ -54.71 after sell → book $9,694.68; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 427 | $3.13 | $5.59 | $+163.97 | $3,694.28 | ▲ +163.97 after sell → book $9,689.09; vs 09:30 mark -5.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $4,777.21 | ▼ -55.30 after sell → book $9,687.04; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 42 | $28.69 | $2.14 | $+62.95 | $5,980.05 | ▲ +62.95 after sell → book $9,684.90; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HQ` | 90 | $13.41 | $2.28 | $+42.26 | $7,184.67 | ▲ +42.26 after sell → book $9,682.62; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 13 | $89.66 | $2.05 | $-0.44 | $8,348.20 | ▼ -0.44 after sell → book $9,680.57; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,398.97 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; ret5=+6.5; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 11 | $88.83 | $2.02 | — | $6,419.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; ret5=+7.6; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 112 | $9.31 | $2.33 | — | $5,374.77 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 77 | $13.47 | $2.22 | — | $4,334.98 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 104 | $9.99 | $2.30 | — | $3,293.72 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 102 | $10.13 | $2.30 | — | $2,257.65 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+4.9; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 61 | $16.91 | $2.17 | — | $1,223.97 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+50.5; leftover $1043.53 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 79 | $13.05 | $2.23 | — | $190.79 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1043.53 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.79 | ▼ close $9,649.20 vs 09:30 $9,699.36 (session -13.79) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.79 | ▲ 09:30 equity $9,671.99 vs yday $9,649.20 (+22.79) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 1075 | $0.16 | $5.13 | $-20.94 | $357.66 | ▼ -20.94 after sell → book $9,666.86; vs 09:30 mark -5.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 77 | $0.58 | $0.68 | — | $312.32 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $44.71 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 4 | $9.11 | $0.38 | — | $275.50 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+44.4; leftover $44.71 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 44 | $1.01 | $0.58 | — | $230.49 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $44.71 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 6 | $7.23 | $0.45 | — | $186.65 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+36.6; leftover $44.71 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.65 | ▲ close $9,732.18 vs 09:30 $9,671.99 (session +67.42) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.65 | ▼ 09:30 equity $9,712.73 vs yday $9,732.18 (-19.45) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $327.61 | ▼ -8.14 after sell → book $9,711.28; vs 09:30 mark -1.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 24 | $7.95 | $2.00 | $+4.75 | $516.41 | ▲ +4.75 after sell → book $9,709.28; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 11 | $17.10 | $1.93 | $+9.82 | $702.57 | ▲ +9.82 after sell → book $9,707.35; vs 09:30 mark -1.93 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 7 | $27.79 | $1.99 | $+9.06 | $895.12 | ▲ +9.06 after sell → book $9,705.36; vs 09:30 mark -1.99 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 1 | $174.50 | $1.77 | $+0.17 | $1,067.85 | ▲ +0.17 after sell → book $9,703.59; vs 09:30 mark -1.77 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 76 | $2.24 | $1.95 | $-16.16 | $1,236.14 | ▼ -16.16 after sell → book $9,701.64; vs 09:30 mark -1.95 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 28 | $0.89 | $0.35 | $-2.95 | $1,260.70 | ▼ -2.95 after sell → book $9,701.29; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 7 | $4.10 | $0.33 | $+0.42 | $1,289.07 | ▲ +0.42 after sell → book $9,700.96; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $1,303.74 | ▲ +0.45 after sell → book $9,700.79; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 1 | $15.40 | $0.18 | $+0.28 | $1,318.97 | ▲ +0.28 after sell → book $9,700.61; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DCX` | 79 | $0.09 | $0.33 | $-21.78 | $1,325.67 | ▼ -21.78 after sell → book $9,700.28; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $1,207.65 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $220.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 10 | $20.65 | $2.02 | — | $999.13 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $220.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 56 | $3.93 | $2.16 | — | $776.89 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $220.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 7 | $28.30 | $2.00 | — | $576.79 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $220.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 14 | $15.72 | $2.03 | — | $354.67 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $220.94 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 8 | $25.40 | $2.01 | — | $149.46 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $220.94 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.46 | ▲ close $9,813.91 vs 09:30 $9,712.73 (session +125.02) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.46 | ▼ 09:30 equity $9,785.96 vs yday $9,813.91 (-27.95) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $1,131.13 | ▲ +32.44 after sell → book $9,783.93; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 11 | $87.67 | $2.04 | $-16.77 | $2,093.51 | ▼ -16.77 after sell → book $9,781.88; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 112 | $8.67 | $2.35 | $-76.36 | $3,062.20 | ▼ -76.36 after sell → book $9,779.53; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 77 | $12.26 | $2.24 | $-98.02 | $4,003.98 | ▼ -98.02 after sell → book $9,777.29; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 104 | $9.80 | $2.33 | $-24.39 | $5,020.85 | ▼ -24.39 after sell → book $9,774.96; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 102 | $9.89 | $2.32 | $-29.61 | $6,027.30 | ▼ -29.61 after sell → book $9,772.63; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `TJGC` | 61 | $24.03 | $2.19 | $+429.95 | $7,490.94 | ▲ +429.95 after sell → book $9,770.44; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 79 | $12.76 | $2.25 | $-27.39 | $8,496.73 | ▼ -27.39 after sell → book $9,768.19; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,496.73 | ▲ close $9,772.03 vs 09:30 $9,785.96 (session +3.84) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,794.75 | ▼ 09:30 equity $9,223.03 vs yday $9,223.78 (-0.75) | 09:30 open · cash $7,794.75 (unchanged overnight, no fees) · equity $9,223.03 vs prior close $9,223.78 (-0.75) · 11 name(s) re-marked at the open (per-name table). DEFT×336 yday $0.53 → 09:30 $0.53 +0.00; EU×187 yday $1.22 → 09:30 $1.22 +0.00; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; HELP×14 yday $12.59 → 09:30 $12.59 +0.00; INDP×12 yday $4.00 → 09:30 $4.00 +0.00; IVVD×196 yday $0.91 → 09:30 $0.91 +0.00; MX×62 yday $3.18 → 09:30 $3.18 +0.00; NMRA×63 yday $0.70 → 09:30 $0.70 +0.00; NUAI×27 yday $6.94 → 09:30 $6.94 +0.00; OMER×2 yday $20.13 → 09:30 $20.61 +0.96; TNGX×1 yday $24.63 → 09:30 $24.63 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 1 | $123.50 | $1.26 | $+14.42 | $7,916.99 | ▲ +14.42 after sell → book $9,221.77; vs 09:30 mark -1.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 8 | $115.36 | $2.01 | — | $6,992.10 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+5.1; leftover $989.62 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 37 | $26.27 | $2.10 | — | $6,018.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $989.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 55 | $17.91 | $2.15 | — | $5,030.80 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable; 🔵; ret5=+3.7; leftover $989.62 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 11 | $83.69 | $2.02 | — | $4,108.13 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $989.62 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 163 | $6.06 | $2.48 | — | $3,117.88 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+342.1; leftover $989.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 256 | $3.86 | $3.30 | — | $2,126.41 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $989.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 97 | $10.20 | $2.28 | — | $1,134.73 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $989.62 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $212.73 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $989.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.73 | ▼ close $9,128.55 vs 09:30 $9,223.03 (session -74.86) | 16:00 close · cash $212.73 · equity $9,128.55 vs 09:30 $9,223.03 (-94.48; session marks -74.86) · 18 name(s) marked open→close (per-name table). DEFT×336 09:30 $0.53 → close $0.53 +0.00; EU×187 09:30 $1.22 → close $1.22 +0.00; HELP×14 09:30 $12.59 → close $12.59 +0.00; INDP×12 09:30 $4.00 → close $4.00 +0.00; IVVD×196 09:30 $0.91 → close $0.91 -0.00; MX×62 09:30 $3.18 → close $3.18 +0.00; NMRA×63 09:30 $0.70 → close $0.70 +0.00; NUAI×27 09:30 $6.94 → close $6.94 +0.00; OMER×2 09:30 $20.61 → close $20.08 -1.06; TNGX×1 09:30 $24.63 → close $24.63 -0.00; HALO×8 09:30 $115.36 → close $113.90 -11.68; WRBY×37 09:30 $26.27 → close $26.71 +16.28; PL×55 09:30 $17.91 → close $17.43 -26.40; TEM×11 09:30 $83.69 → close $85.01 +14.47; GLND×163 09:30 $6.06 → close $5.54 -84.76; ZSQR×256 09:30 $3.86 → close $3.78 -20.48; DNA×97 09:30 $10.20 → close $10.66 +44.62; TWST×5 09:30 $184.00 → close $182.83 -5.85 | — |

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
| 2026-08-17 | `CDNL` | cash | leftover split 1.28 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 1.28 < 1 share @ 31.30 |
| 2026-08-17 | `CAPR` | cash | leftover split 1.28 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 1.28 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 1.28 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 1.28 < 1 share @ 1.92 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 35.01 < 1 share @ 128.73 |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GPRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIR` | cash | leftover split 9.78 < 1 share @ 11.31 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 182.77 < 1 share @ 233.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 28.04 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 28.04 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 28.04 < 1 share @ 85.00 |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 77 | 2026-09-22 @ $0.58 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $44.71 |
| `CRML` | 4 | 2026-09-22 @ $9.11 | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+44.4; leftover $44.71 |
| `IVVD` | 44 | 2026-09-22 @ $1.01 | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $44.71 |
| `NUAI` | 6 | 2026-09-22 @ $7.23 | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+36.6; leftover $44.71 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $220.94 |
| `OMER` | 10 | 2026-09-23 @ $20.65 | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $220.94 |
| `INDP` | 56 | 2026-09-23 @ $3.93 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $220.94 |
| `MAZE` | 7 | 2026-09-23 @ $28.30 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $220.94 |
| `SGRY` | 14 | 2026-09-23 @ $15.72 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $220.94 |
| `TNGX` | 8 | 2026-09-23 @ $25.40 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $220.94 |
