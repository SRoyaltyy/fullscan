# Factor mine action — `union_vol_g_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g hold 5, no 🚨

Cash book **-12.38%** ($8,762) · signal-only (no cash/fees) was +4.97%. Starts YES **4/30**. Fills 152 · skips 401 · realized $-981.98.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $249.95.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▲ close $9,809.66 vs 09:30 $9,768.32 (session +41.34) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,628.11 vs yday $9,809.66 (-181.55) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,454.54 vs 09:30 $9,628.11 (session -173.57) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,448.22 vs yday $9,454.54 (-6.32) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,275.27 vs 09:30 $9,448.22 (session -172.95) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,267.56 vs yday $9,275.27 (-7.71) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,115.98 vs 09:30 $9,267.56 (session -151.58) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▲ 09:30 equity $9,241.45 vs yday $9,115.98 (+125.47) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 833 | $1.66 | $10.89 | $+111.64 | $1,382.16 | ▲ +111.64 after sell → book $9,230.55; vs 09:30 mark -10.90 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 84 | $11.73 | $2.27 | $-262.39 | $2,365.22 | ▼ -262.39 after sell → book $9,228.29; vs 09:30 mark -2.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $3,646.12 | ▲ +27.26 after sell → book $9,224.49; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $4,664.78 | ▼ -235.01 after sell → book $9,220.57; vs 09:30 mark -3.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $5,862.54 | ▼ -41.95 after sell → book $9,218.33; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,093.25 | ▼ -4.38 after sell → book $9,216.13; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRO` | 112 | $8.39 | $2.35 | $-310.44 | $8,030.58 | ▼ -310.44 after sell → book $9,213.78; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NCMI` | 464 | $2.55 | $6.07 | $-77.02 | $9,207.71 | ▼ -77.02 after sell → book $9,207.71; vs 09:30 mark -6.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,130.82 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 66 | $17.20 | $2.19 | — | $6,993.43 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,909.93 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 103 | $11.13 | $2.30 | — | $4,761.24 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 465 | $2.47 | $6.00 | — | $3,606.69 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 596 | $1.93 | $7.69 | — | $2,448.72 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $1,311.99 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1150.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 871 | $1.32 | $11.24 | — | $151.04 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1150.96 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.04 | ▲ close $9,403.10 vs 09:30 $9,241.45 (session +230.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.04 | ▲ 09:30 equity $9,734.05 vs yday $9,403.10 (+330.95) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.04 | ▼ close $9,639.72 vs 09:30 $9,734.05 (session -94.32) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.04 | ▼ 09:30 equity $9,522.23 vs yday $9,639.72 (-117.49) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 2 | $7.25 | $0.15 | — | $136.39 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $18.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 1 | $13.59 | $0.14 | — | $122.66 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $18.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 1 | $9.49 | $0.10 | — | $113.07 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $18.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 4 | $4.55 | $0.19 | — | $94.68 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $18.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 11 | $1.63 | $0.21 | — | $76.53 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $18.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 9 | $2.00 | $0.21 | — | $58.33 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $18.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 4 | $3.80 | $0.16 | — | $42.96 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $18.88 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.96 | ▲ close $9,991.13 vs 09:30 $9,522.23 (session +470.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.96 | ▼ 09:30 equity $9,872.22 vs yday $9,991.13 (-118.91) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 24 | $0.58 | $0.21 | — | $28.76 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_mover; 🔵; ret5=-27.5; leftover $14.32 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 2 | $5.81 | $0.12 | — | $17.02 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $14.32 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.02 | ▲ close $9,911.06 vs 09:30 $9,872.22 (session +39.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.02 | ▲ 09:30 equity $9,959.63 vs yday $9,911.06 (+48.57) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.02 | ▲ close $10,151.94 vs 09:30 $9,959.63 (session +192.31) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.02 | ▼ 09:30 equity $9,994.47 vs yday $10,151.94 (-157.47) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 9 | $119.19 | $2.04 | $-6.21 | $1,087.69 | ▼ -6.21 after sell → book $9,992.43; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 66 | $16.44 | $2.21 | $-54.56 | $2,170.52 | ▼ -54.56 after sell → book $9,990.23; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 5 | $216.31 | $2.02 | $-3.98 | $3,250.05 | ▼ -3.98 after sell → book $9,988.20; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 103 | $15.43 | $2.33 | $+438.27 | $4,837.01 | ▲ +438.27 after sell → book $9,985.87; vs 09:30 mark -2.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 465 | $2.35 | $6.09 | $-67.88 | $5,923.67 | ▼ -67.88 after sell → book $9,979.79; vs 09:30 mark -6.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 596 | $2.06 | $7.80 | $+61.99 | $7,143.63 | ▲ +61.99 after sell → book $9,971.99; vs 09:30 mark -7.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 19 | $58.22 | $2.07 | $-32.61 | $8,247.75 | ▼ -32.61 after sell → book $9,969.92; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 871 | $1.82 | $11.39 | $+412.87 | $9,821.57 | ▲ +412.87 after sell → book $9,958.53; vs 09:30 mark -11.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $8,437.66 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1403.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $7,085.48 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1403.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $5,768.83 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1403.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 88 | $15.88 | $2.25 | — | $4,369.14 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+19.4; leftover $1403.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 77 | $18.15 | $2.22 | — | $2,969.37 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1403.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,691.51 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1403.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 72 | $19.25 | $2.21 | — | $303.30 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer; ret5=+14.1; leftover $1403.08 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $303.30 | ▼ close $9,677.82 vs 09:30 $9,994.47 (session -265.83) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $303.30 | ▼ 09:30 equity $9,646.88 vs yday $9,677.82 (-30.94) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $303.30 | ▼ close $9,620.08 vs 09:30 $9,646.88 (session -26.80) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $303.30 | ▼ 09:30 equity $9,504.06 vs yday $9,620.08 (-116.02) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 2 | $10.77 | $0.24 | $+6.65 | $324.60 | ▲ +6.65 after sell → book $9,503.82; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `KURA` | 1 | $12.54 | $0.15 | $-1.34 | $336.99 | ▼ -1.34 after sell → book $9,503.67; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CCOI` | 1 | $9.42 | $0.12 | $-0.29 | $346.30 | ▼ -0.29 after sell → book $9,503.55; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZIP` | 4 | $4.14 | $0.20 | $-2.03 | $362.66 | ▼ -2.03 after sell → book $9,503.35; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 11 | $1.68 | $0.24 | $+0.10 | $380.90 | ▲ +0.10 after sell → book $9,503.11; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 9 | $1.75 | $0.20 | $-2.66 | $396.45 | ▼ -2.66 after sell → book $9,502.91; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `PUSA` | 4 | $3.85 | $0.19 | $-0.15 | $411.66 | ▼ -0.15 after sell → book $9,502.72; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $411.66 | ▲ close $9,553.49 vs 09:30 $9,504.06 (session +50.77) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $411.66 | ▲ 09:30 equity $9,582.56 vs yday $9,553.49 (+29.07) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SLQT` | 24 | $0.53 | $0.22 | $-1.68 | $424.18 | ▼ -1.68 after sell → book $9,582.34; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `USDE` | 2 | $8.07 | $0.19 | $+4.21 | $440.14 | ▲ +4.21 after sell → book $9,582.16; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $440.14 | ▼ close $9,531.56 vs 09:30 $9,582.56 (session -50.60) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $440.14 | ▲ 09:30 equity $9,561.47 vs yday $9,531.56 (+29.91) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 3 | $15.45 | $0.47 | — | $393.31 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $55.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 3 | $16.77 | $0.51 | — | $342.49 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $55.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 25 | $2.18 | $0.62 | — | $287.37 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $55.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 30 | $1.78 | $0.62 | — | $233.35 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+183.1; leftover $55.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 3 | $18.28 | $0.56 | — | $177.95 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+16.5; leftover $55.02 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.95 | ▲ close $9,579.66 vs 09:30 $9,561.47 (session +20.98) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.95 | ▼ 09:30 equity $9,569.57 vs yday $9,579.66 (-10.09) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 42 | $33.86 | $2.14 | $+36.07 | $1,597.93 | ▲ +36.07 after sell → book $9,567.43; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 17 | $79.55 | $2.06 | $-1.89 | $2,948.22 | ▼ -1.89 after sell → book $9,565.37; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 9 | $144.89 | $2.04 | $-14.67 | $4,250.19 | ▼ -14.67 after sell → book $9,563.33; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 88 | $15.52 | $2.28 | $-36.21 | $5,613.67 | ▼ -36.21 after sell → book $9,561.05; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 77 | $16.91 | $2.24 | $-99.95 | $6,913.50 | ▼ -99.95 after sell → book $9,558.81; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $138.71 | $2.04 | $-31.50 | $8,159.85 | ▼ -31.50 after sell → book $9,556.77; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ERAS` | 72 | $16.04 | $2.23 | $-235.55 | $9,312.51 | ▼ -235.55 after sell → book $9,554.55; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 336 | $3.46 | $4.33 | — | $8,145.61 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 461 | $2.52 | $5.95 | — | $6,977.94 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 173 | $6.71 | $2.51 | — | $5,814.61 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 612 | $1.90 | $7.89 | — | $4,643.91 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 243 | $4.78 | $3.13 | — | $3,479.24 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 732 | $1.59 | $9.44 | — | $2,305.91 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 102 | $11.31 | $2.30 | — | $1,150.00 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1164.06 | — |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 325 | $3.52 | $4.19 | — | $1.80 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1164.06 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.80 | ▲ close $9,607.68 vs 09:30 $9,569.57 (session +92.89) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.80 | ▼ 09:30 equity $9,542.04 vs yday $9,607.68 (-65.64) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.80 | ▼ close $9,513.02 vs 09:30 $9,542.04 (session -29.02) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.80 | ▼ 09:30 equity $9,444.40 vs yday $9,513.02 (-68.62) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.80 | ▼ close $9,015.36 vs 09:30 $9,444.40 (session -429.05) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.80 | ▼ 09:30 equity $8,835.53 vs yday $9,015.36 (-179.83) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.80 | ▼ close $8,652.13 vs 09:30 $8,835.53 (session -183.39) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.80 | ▲ 09:30 equity $8,762.53 vs yday $8,652.13 (+110.40) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `CRK` | 3 | $15.03 | $0.48 | $-2.21 | $46.41 | ▼ -2.21 after sell → book $8,762.05; vs 09:30 mark -0.48 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 3 | $14.06 | $0.45 | $-9.09 | $88.14 | ▼ -9.09 after sell → book $8,761.60; vs 09:30 mark -0.45 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 25 | $2.03 | $0.60 | $-4.97 | $138.29 | ▼ -4.97 after sell → book $8,761.00; vs 09:30 mark -0.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `GPRO` | 30 | $1.40 | $0.53 | $-12.55 | $179.76 | ▼ -12.55 after sell → book $8,760.47; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `FRVO` | 3 | $15.88 | $0.51 | $-8.26 | $226.90 | ▼ -8.26 after sell → book $8,759.97; vs 09:30 mark -0.50 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 1 | $23.63 | $0.24 | — | $203.03 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-6.3; leftover $28.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 10 | $2.70 | $0.30 | — | $175.73 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $28.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 9 | $3.13 | $0.31 | — | $147.25 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+24.2; leftover $28.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 2 | $10.95 | $0.23 | — | $125.12 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $28.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 4 | $5.91 | $0.25 | — | $101.23 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $28.36 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 5 | $4.91 | $0.26 | — | $76.42 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $28.36 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.42 | ▼ close $8,713.70 vs 09:30 $8,762.53 (session -44.68) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.42 | ▼ 09:30 equity $8,696.10 vs yday $8,713.70 (-17.60) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CABA` | 336 | $2.72 | $4.40 | $-257.37 | $985.94 | ▼ -257.37 after sell → book $8,691.70; vs 09:30 mark -4.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 461 | $2.15 | $6.03 | $-182.55 | $1,971.06 | ▼ -182.55 after sell → book $8,685.67; vs 09:30 mark -6.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 173 | $5.93 | $2.55 | $-140.00 | $2,994.40 | ▼ -140.00 after sell → book $8,683.12; vs 09:30 mark -2.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 612 | $1.72 | $8.01 | $-129.12 | $4,035.98 | ▼ -129.12 after sell → book $8,675.12; vs 09:30 mark -8.00 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 243 | $4.13 | $3.19 | $-164.27 | $5,036.38 | ▼ -164.27 after sell → book $8,671.93; vs 09:30 mark -3.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 732 | $1.59 | $9.57 | $-19.02 | $6,190.69 | ▼ -19.02 after sell → book $8,662.36; vs 09:30 mark -9.57 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 102 | $10.73 | $2.32 | $-63.78 | $7,282.82 | ▼ -63.78 after sell → book $8,660.03; vs 09:30 mark -2.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `EOSE` | 325 | $3.77 | $4.26 | $+72.80 | $8,503.82 | ▲ +72.80 after sell → book $8,655.78; vs 09:30 mark -4.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,503.82 | ▲ close $8,666.82 vs 09:30 $8,696.10 (session +11.04) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,503.82 | ▲ 09:30 equity $8,667.94 vs yday $8,666.82 (+1.12) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,503.82 | ▲ close $8,669.16 vs 09:30 $8,667.94 (session +1.22) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,503.82 | ▼ 09:30 equity $8,667.81 vs yday $8,669.16 (-1.35) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $7,344.98 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1214.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 206 | $5.87 | $2.66 | — | $6,133.11 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1214.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 446 | $2.72 | $5.75 | — | $4,914.23 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-0.4; leftover $1214.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $3,776.00 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1214.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 44 | $27.09 | $2.12 | — | $2,581.92 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1214.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 94 | $12.89 | $2.27 | — | $1,367.99 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=-18.2; leftover $1214.83 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $204.02 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1214.83 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.02 | ▼ close $8,612.99 vs 09:30 $8,667.81 (session -35.92) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.02 | ▲ 09:30 equity $8,696.62 vs yday $8,612.99 (+83.63) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 3 | $7.59 | $0.24 | — | $181.01 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $25.50 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 150 | $0.17 | $0.70 | — | $154.81 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $25.50 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 1 | $15.87 | $0.16 | — | $138.78 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $25.50 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 10 | $2.40 | $0.27 | — | $114.51 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $25.50 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.51 | ▲ close $8,898.17 vs 09:30 $8,696.62 (session +202.92) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.51 | ▲ 09:30 equity $8,915.52 vs yday $8,898.17 (+17.35) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `TYRA` | 1 | $24.58 | $0.27 | $+0.44 | $138.82 | ▲ +0.44 after sell → book $8,915.25; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `INDP` | 10 | $3.85 | $0.43 | $+10.76 | $176.88 | ▲ +10.76 after sell → book $8,914.81; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CMRC` | 9 | $3.41 | $0.35 | $+1.86 | $207.22 | ▲ +1.86 after sell → book $8,914.46; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `WLTH` | 2 | $10.10 | $0.23 | $-2.15 | $227.19 | ▼ -2.15 after sell → book $8,914.23; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `DBI` | 4 | $6.07 | $0.27 | $+0.12 | $251.20 | ▲ +0.12 after sell → book $8,913.96; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BNC` | 5 | $5.83 | $0.33 | $+4.01 | $280.02 | ▲ +4.01 after sell → book $8,913.63; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 36 | $0.97 | $0.46 | — | $244.64 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $35.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 8 | $3.95 | $0.34 | — | $212.70 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $35.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 2 | $14.07 | $0.29 | — | $184.27 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $35.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $154.39 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $35.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 98 | $0.35 | $0.64 | — | $119.06 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $35.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.06 | ▼ close $8,783.04 vs 09:30 $8,915.52 (session -128.56) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.06 | ▲ 09:30 equity $8,813.57 vs yday $8,783.04 (+30.53) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 1 | $9.31 | $0.10 | — | $109.65 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $14.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 1 | $13.47 | $0.14 | — | $96.04 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $14.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 1 | $9.99 | $0.10 | — | $85.95 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $14.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 1 | $10.13 | $0.10 | — | $75.71 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+4.9; leftover $14.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 1 | $13.05 | $0.13 | — | $62.53 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $14.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.53 | ▼ close $8,798.30 vs 09:30 $8,813.57 (session -14.69) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.53 | ▼ 09:30 equity $8,779.27 vs yday $8,798.30 (-19.03) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 13 | $0.58 | $0.11 | — | $54.87 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $7.82 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 7 | $1.01 | $0.09 | — | $47.71 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $7.82 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 1 | $7.23 | $0.08 | — | $40.40 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+36.6; leftover $7.82 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.40 | ▲ close $8,807.99 vs 09:30 $8,779.27 (session +29.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.40 | ▲ 09:30 equity $9,014.82 vs yday $8,807.99 (+206.83) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 15 | $73.61 | $2.06 | $-56.74 | $1,142.50 | ▼ -56.74 after sell → book $9,012.77; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 206 | $5.53 | $2.70 | $-75.40 | $2,278.98 | ▼ -75.40 after sell → book $9,010.06; vs 09:30 mark -2.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 446 | $3.17 | $5.84 | $+189.11 | $3,686.96 | ▲ +189.11 after sell → book $9,004.23; vs 09:30 mark -5.83 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `VAL` | 13 | $83.39 | $2.05 | $-56.21 | $4,768.98 | ▼ -56.21 after sell → book $9,002.18; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ADPT` | 44 | $27.74 | $2.14 | $+24.34 | $5,987.40 | ▲ +24.34 after sell → book $9,000.03; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `HQ` | 94 | $16.40 | $2.30 | $+325.37 | $7,526.70 | ▲ +325.37 after sell → book $8,997.73; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `SWKS` | 13 | $90.21 | $2.05 | $+6.71 | $8,697.38 | ▲ +6.71 after sell → book $8,995.69; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 12 | $89.50 | $2.03 | — | $7,621.35 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 6 | $166.54 | $2.01 | — | $6,620.10 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $5,566.44 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 52 | $20.65 | $2.15 | — | $4,490.49 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 276 | $3.93 | $3.56 | — | $3,402.25 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `MAZE` | 38 | $28.30 | $2.10 | — | $2,324.75 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 69 | $15.72 | $2.20 | — | $1,237.87 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1087.17 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 42 | $25.40 | $2.12 | — | $168.95 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1087.17 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.95 | ▼ close $8,732.93 vs 09:30 $9,014.82 (session -244.58) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.95 | ▼ 09:30 equity $8,679.48 vs yday $8,732.93 (-53.45) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 3 | $7.38 | $0.25 | $-1.12 | $190.84 | ▼ -1.12 after sell → book $8,679.23; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 150 | $0.15 | $0.71 | $-4.41 | $212.63 | ▼ -4.41 after sell → book $8,678.52; vs 09:30 mark -0.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BRUN` | 1 | $16.07 | $0.18 | $-0.15 | $228.52 | ▼ -0.15 after sell → book $8,678.34; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `SABR` | 10 | $2.17 | $0.27 | $-2.84 | $249.95 | ▼ -2.84 after sell → book $8,678.07; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.95 | ▲ close $8,738.71 vs 09:30 $8,679.48 (session +60.64) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,375.09 | ▲ 09:30 equity $8,823.78 vs yday $8,801.83 (+21.95) | 09:30 open · cash $1,375.09 (unchanged overnight, no fees) · equity $8,823.78 vs prior close $8,801.83 (+21.95) · 14 name(s) re-marked at the open (per-name table). A×6 yday $172.84 → 09:30 $171.98 -5.16; CYPH×1 yday $4.08 → 09:30 $4.00 -0.07; DCX×15 yday $0.06 → 09:30 $0.06 +0.00; DEFT×6 yday $0.53 → 09:30 $0.53 +0.00; DXCM×12 yday $87.47 → 09:30 $87.47 +0.00; EU×3 yday $1.22 → 09:30 $1.22 +0.00; EYPT×1 yday $3.65 → 09:30 $3.65 +0.00; HALO×9 yday $115.22 → 09:30 $115.36 +1.26; IVVD×3 yday $0.91 → 09:30 $0.91 +0.00; MX×1 yday $3.18 → 09:30 $3.18 +0.00; NMRA×1467 yday $0.70 → 09:30 $0.70 +0.00; OMER×54 yday $20.13 → 09:30 $20.61 +25.92; TNGX×44 yday $24.63 → 09:30 $24.63 +0.00; TTAN×18 yday $59.98 → 09:30 $59.98 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 7 | $26.27 | $1.86 | — | $1,189.34 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $196.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 10 | $17.91 | $1.82 | — | $1,008.42 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable; 🔵; ret5=+3.7; leftover $196.44 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 2 | $83.69 | $1.68 | — | $839.35 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $196.44 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 32 | $6.06 | $2.04 | — | $643.39 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+342.1; leftover $196.44 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 50 | $3.86 | $2.08 | — | $448.31 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $196.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 19 | $10.20 | $2.00 | — | $252.52 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $196.44 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 1 | $184.00 | $1.84 | — | $66.68 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $196.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.68 | ▼ close $8,761.52 vs 09:30 $8,823.78 (session -48.94) | 16:00 close · cash $66.68 · equity $8,761.52 vs 09:30 $8,823.78 (-62.26; session marks -48.94) · 21 name(s) marked open→close (per-name table). A×6 09:30 $171.98 → close $172.79 +4.86; CYPH×1 09:30 $4.00 → close $4.12 +0.12; DCX×15 09:30 $0.06 → close $0.06 -0.00; DEFT×6 09:30 $0.53 → close $0.53 +0.00; DXCM×12 09:30 $87.47 → close $87.47 +0.00; EU×3 09:30 $1.22 → close $1.22 +0.00; EYPT×1 09:30 $3.65 → close $3.65 +0.00; HALO×9 09:30 $115.36 → close $113.90 -13.14; IVVD×3 09:30 $0.91 → close $0.91 -0.00; MX×1 09:30 $3.18 → close $3.18 +0.00; NMRA×1467 09:30 $0.70 → close $0.70 +0.00; OMER×54 09:30 $20.61 → close $20.08 -28.62; TNGX×44 09:30 $24.63 → close $24.63 -0.00; TTAN×18 09:30 $59.98 → close $59.98 -0.00; WRBY×7 09:30 $26.27 → close $26.71 +3.08; PL×10 09:30 $17.91 → close $17.43 -4.80; TEM×2 09:30 $83.69 → close $85.01 +2.63; GLND×32 09:30 $6.06 → close $5.54 -16.64; ZSQR×50 09:30 $3.86 → close $3.78 -4.00; DNA×19 09:30 $10.20 → close $10.66 +8.74; TWST×1 09:30 $184.00 → close $182.83 -1.17 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `CDNL` | cash | leftover split 1.28 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 1.28 < 1 share @ 31.30 |
| 2026-08-17 | `CAPR` | cash | leftover split 1.28 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 1.28 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 1.28 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 1.28 < 1 share @ 1.92 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NCMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NCMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AG` | cash | leftover split 1.28 < 1 share @ 20.55 |
| 2026-08-20 | `BHP` | cash | leftover split 1.28 < 1 share @ 91.01 |
| 2026-08-20 | `CDE` | cash | leftover split 1.28 < 1 share @ 20.65 |
| 2026-08-20 | `HDSN` | cash | leftover split 1.28 < 1 share @ 5.77 |
| 2026-08-20 | `IAG` | cash | leftover split 1.28 < 1 share @ 19.63 |
| 2026-08-20 | `KGC` | cash | leftover split 1.28 < 1 share @ 29.63 |
| 2026-08-20 | `NFGC` | cash | leftover split 1.28 < 1 share @ 1.75 |
| 2026-08-20 | `WPM` | cash | leftover split 1.28 < 1 share @ 144.54 |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `LIFE` | cash | leftover split 18.88 < 1 share @ 36.96 |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 14.32 < 1 share @ 121.87 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 17.02 < 1 share @ 128.73 |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CCOI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZIP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `PUSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CCOI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZIP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `PUSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SLQT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SLQT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `USDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `URBN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ERAS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `URBN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ERAS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `RVTY` | cash | leftover split 55.02 < 1 share @ 132.45 |
| 2026-09-03 | `MRNA` | cash | leftover split 55.02 < 1 share @ 145.94 |
| 2026-09-03 | `EIX` | cash | leftover split 55.02 < 1 share @ 55.42 |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `GPRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `FRVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `GPRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `FRVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `GPRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `FRVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `EOSE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `EOSE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ORCL` | cash | leftover split 28.36 < 1 share @ 164.43 |
| 2026-09-11 | `VIST` | cash | leftover split 28.36 < 1 share @ 77.33 |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `INDP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `INDP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TRX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `TYRA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CMRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `WLTH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `DBI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `TYRA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `INDP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CMRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `WLTH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `DBI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `HQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 25.50 < 1 share @ 233.85 |
| 2026-09-17 | `RVTY` | cash | leftover split 25.50 < 1 share @ 147.61 |
| 2026-09-17 | `ARQT` | cash | leftover split 25.50 < 1 share @ 25.95 |
| 2026-09-17 | `SMTC` | cash | leftover split 25.50 < 1 share @ 170.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `HQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `GNRC` | cash | leftover split 35.00 < 1 share @ 209.52 |
| 2026-09-18 | `VICR` | cash | leftover split 35.00 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 35.00 < 1 share @ 85.00 |
| 2026-09-21 | `RDNT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `VAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ADPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `HQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `SWKS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DCX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 14.88 < 1 share @ 157.87 |
| 2026-09-21 | `DXCM` | cash | leftover split 14.88 < 1 share @ 88.83 |
| 2026-09-21 | `TJGC` | cash | leftover split 14.88 < 1 share @ 16.91 |
| 2026-09-22 | `RDNT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `VAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ADPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `HQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `SWKS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BRUN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `SABR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DCX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 7.82 < 1 share @ 9.11 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-23 | `PGEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BRUN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `SABR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `EYPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DCX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `EYPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DCX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SGML` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `USDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `MAZE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LU` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TLSA` | 36 | 2026-09-18 @ $0.97 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $35.00 |
| `EYPT` | 8 | 2026-09-18 @ $3.95 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $35.00 |
| `BHVN` | 2 | 2026-09-18 @ $14.07 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $35.00 |
| `RARE` | 2 | 2026-09-18 @ $14.79 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $35.00 |
| `DCX` | 98 | 2026-09-18 @ $0.35 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $35.00 |
| `BKKT` | 1 | 2026-09-21 @ $9.31 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $14.88 |
| `BTDR` | 1 | 2026-09-21 @ $13.47 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $14.88 |
| `SBET` | 1 | 2026-09-21 @ $9.99 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $14.88 |
| `SGML` | 1 | 2026-09-21 @ $10.13 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+4.9; leftover $14.88 |
| `USDE` | 1 | 2026-09-21 @ $13.05 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $14.88 |
| `DEFT` | 13 | 2026-09-22 @ $0.58 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $7.82 |
| `IVVD` | 7 | 2026-09-22 @ $1.01 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $7.82 |
| `NUAI` | 1 | 2026-09-22 @ $7.23 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+36.6; leftover $7.82 |
| `DXCM` | 12 | 2026-09-23 @ $89.50 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1087.17 |
| `A` | 6 | 2026-09-23 @ $166.54 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1087.17 |
| `HALO` | 9 | 2026-09-23 @ $116.85 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1087.17 |
| `OMER` | 52 | 2026-09-23 @ $20.65 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1087.17 |
| `INDP` | 276 | 2026-09-23 @ $3.93 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1087.17 |
| `MAZE` | 38 | 2026-09-23 @ $28.30 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.2; leftover $1087.17 |
| `SGRY` | 69 | 2026-09-23 @ $15.72 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1087.17 |
| `TNGX` | 42 | 2026-09-23 @ $25.40 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1087.17 |
