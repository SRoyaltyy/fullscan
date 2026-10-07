# Factor mine action — `union_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-16.45%** ($8,355) · signal-only (no cash/fees) was -4.91%. Starts YES **5/30**. Fills 328 · skips 78 · realized $-2.02.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the last finished bar was green (closed up).
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,998.02.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▲ +38.98 after sell → book $9,727.96; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ -13.54 after sell → book $9,725.42; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,527.84 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,312.49 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $6,120.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,923.24 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,716.79 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $2,493.26 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 81 | $14.94 | $2.23 | — | $1,280.89 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1215.68 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 110 | $10.97 | $2.32 | — | $71.87 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1215.68 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.87 | ▼ close $9,428.97 vs 09:30 $9,759.50 (session -272.98) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.87 | ▼ 09:30 equity $9,316.08 vs yday $9,428.97 (-112.89) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,316.87 | ▲ +47.42 after sell → book $9,313.98; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,515.44 | ▼ -16.78 after sell → book $9,311.56; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $3,703.09 | ▼ -3.85 after sell → book $9,309.43; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $4,904.50 | ▲ +3.66 after sell → book $9,307.34; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $5,960.21 | ▼ -150.74 after sell → book $9,305.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $7,028.02 | ▼ -155.71 after sell → book $9,296.93; vs 09:30 mark -8.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 81 | $14.01 | $2.26 | $-79.82 | $8,160.58 | ▼ -79.82 after sell → book $9,294.68; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 110 | $10.31 | $2.35 | $-77.27 | $9,292.33 | ▼ -77.27 after sell → book $9,292.33; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,292.33 | ▲ close $9,292.33 vs 09:30 $9,316.08 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,292.33 | ▲ 09:30 equity $9,292.33 vs yday $9,292.33 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,292.33 | ▲ close $9,292.33 vs 09:30 $9,292.33 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,292.33 | ▲ 09:30 equity $9,292.33 vs yday $9,292.33 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,139.37 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,980.81 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,818.45 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,658.11 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,500.43 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,331.63 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,173.29 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $9.13 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.54 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.13 | ▲ close $9,420.74 vs 09:30 $9,292.33 (session +153.21) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.13 | ▲ 09:30 equity $9,748.43 vs yday $9,420.74 (+327.69) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,233.35 | ▲ +71.26 after sell → book $9,746.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,449.17 | ▲ +57.26 after sell → book $9,744.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,586.20 | ▼ -25.34 after sell → book $9,741.43; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,833.05 | ▲ +86.51 after sell → book $9,739.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,085.55 | ▲ +94.83 after sell → book $9,737.12; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,263.65 | ▲ +9.29 after sell → book $9,728.45; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,499.21 | ▲ +77.23 after sell → book $9,726.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,723.32 | ▲ +59.94 after sell → book $9,723.32; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,527.00 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,320.80 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,237.29 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,021.81 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,795.54 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,570.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $1,945.72 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1215.41 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 390 | $3.11 | $5.03 | — | $727.79 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+7.1; leftover $1215.41 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $727.79 | ▲ close $9,969.77 vs 09:30 $9,748.43 (session +283.33) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $727.79 | ▲ 09:30 equity $10,350.15 vs yday $9,969.77 (+380.38) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,930.85 | ▲ +6.74 after sell → book $10,348.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.57 | $2.22 | $-48.52 | $3,088.53 | ▼ -48.52 after sell → book $10,345.89; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $4,171.66 | ▼ -0.38 after sell → book $10,343.87; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $5,622.28 | ▲ +235.14 after sell → book $10,341.52; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $7,293.84 | ▲ +445.30 after sell → book $10,329.48; vs 09:30 mark -12.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $8,418.87 | ▼ -99.54 after sell → book $10,319.91; vs 09:30 mark -9.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $9,069.90 | ▲ +25.77 after sell → book $10,317.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 390 | $3.20 | $5.11 | $+24.96 | $10,312.79 | ▲ +24.96 after sell → book $10,312.79; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,312.79 | ▲ close $10,312.79 vs 09:30 $10,350.15 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,312.79 | ▲ 09:30 equity $10,312.79 vs yday $10,312.79 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 94 | $13.59 | $2.27 | — | $9,033.06 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 135 | $9.49 | $2.40 | — | $7,749.51 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 34 | $36.96 | $2.09 | — | $6,490.78 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 283 | $4.55 | $3.65 | — | $5,199.48 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 790 | $1.63 | $10.19 | — | $3,901.59 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 644 | $2.00 | $8.31 | — | $2,605.28 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 246 | $5.24 | $3.17 | — | $1,313.07 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1289.10 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 146 | $8.79 | $2.43 | — | $27.30 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1289.10 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.30 | ▲ close $10,407.63 vs 09:30 $10,312.79 (session +129.35) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.30 | ▼ 09:30 equity $10,388.95 vs yday $10,407.63 (-18.68) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 94 | $13.63 | $2.30 | $-0.81 | $1,306.22 | ▼ -0.81 after sell → book $10,386.65; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 135 | $9.89 | $2.43 | $+49.18 | $2,638.95 | ▲ +49.18 after sell → book $10,384.23; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 34 | $38.24 | $2.11 | $+39.32 | $3,936.99 | ▲ +39.32 after sell → book $10,382.11; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 283 | $4.31 | $3.71 | $-75.28 | $5,153.02 | ▼ -75.28 after sell → book $10,378.41; vs 09:30 mark -3.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 790 | $1.75 | $10.33 | $+78.23 | $6,529.13 | ▲ +78.23 after sell → book $10,368.07; vs 09:30 mark -10.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 644 | $1.93 | $8.42 | $-61.81 | $7,763.63 | ▼ -61.81 after sell → book $10,359.65; vs 09:30 mark -8.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 246 | $4.98 | $3.22 | $-70.36 | $8,985.48 | ▼ -70.36 after sell → book $10,356.42; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 146 | $9.39 | $2.46 | $+82.71 | $10,353.96 | ▲ +82.71 after sell → book $10,353.96; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1778 | $5.81 | $22.94 | — | $0.84 | — | combo gate; gate vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10353.96 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.84 | ▲ close $10,633.28 vs 09:30 $10,388.95 (session +302.26) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▲ 09:30 equity $11,557.84 vs yday $10,633.28 (+924.56) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1778 | $6.50 | $23.32 | $+1180.56 | $11,534.52 | ▲ +1,180.56 after sell → book $11,534.52; vs 09:30 mark -23.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 89 | $128.73 | $2.26 | — | $75.29 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=-32.2; leftover $11534.52 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.29 | ▲ close $11,802.82 vs 09:30 $11,557.84 (session +270.56) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.29 | ▲ 09:30 equity $11,894.49 vs yday $11,802.82 (+91.67) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 89 | $132.80 | $2.37 | $+357.61 | $11,892.13 | ▲ +357.61 after sell → book $11,892.13; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 11 | $146.07 | $2.02 | — | $10,283.33 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1698.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 174 | $9.73 | $2.51 | — | $8,587.80 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1698.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 88 | $19.25 | $2.25 | — | $6,891.55 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+14.1; leftover $1698.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 18 | $91.75 | $2.04 | — | $5,238.00 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=-13.2; leftover $1698.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 72 | $23.30 | $2.21 | — | $3,558.20 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1698.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 89 | $19.00 | $2.26 | — | $1,864.94 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $1698.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 68 | $24.69 | $2.19 | — | $183.83 | — | combo gate; gate vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $1698.88 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.83 | ▼ close $11,601.10 vs 09:30 $11,894.49 (session -275.54) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.83 | ▼ 09:30 equity $11,449.70 vs yday $11,601.10 (-151.40) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 11 | $148.03 | $2.05 | $+17.49 | $1,810.11 | ▲ +17.49 after sell → book $11,447.66; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 174 | $9.50 | $2.55 | $-45.09 | $3,460.56 | ▼ -45.09 after sell → book $11,445.10; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 88 | $17.87 | $2.28 | $-125.98 | $5,030.84 | ▼ -125.98 after sell → book $11,442.82; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 18 | $89.15 | $2.07 | $-50.91 | $6,633.47 | ▼ -50.91 after sell → book $11,440.75; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 72 | $22.66 | $2.23 | $-50.52 | $8,262.76 | ▼ -50.52 after sell → book $11,438.52; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 89 | $18.12 | $2.28 | $-82.42 | $9,873.60 | ▼ -82.42 after sell → book $11,436.24; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 68 | $22.98 | $2.22 | $-120.69 | $11,434.02 | ▼ -120.69 after sell → book $11,434.02; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,434.02 | ▲ close $11,434.02 vs 09:30 $11,449.70 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,434.02 | ▲ 09:30 equity $11,434.02 vs yday $11,434.02 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,434.02 | ▲ close $11,434.02 vs 09:30 $11,434.02 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,434.02 | ▲ 09:30 equity $11,434.02 vs yday $11,434.02 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,434.02 | ▲ close $11,434.02 vs 09:30 $11,434.02 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,434.02 | ▲ 09:30 equity $11,434.02 vs yday $11,434.02 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $10,107.50 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 85 | $16.77 | $2.25 | — | $8,679.81 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 655 | $2.18 | $8.45 | — | $7,243.46 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 802 | $1.78 | $10.35 | — | $5,805.55 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $23.88 | $2.17 | — | $4,394.46 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 137 | $10.42 | $2.40 | — | $2,964.52 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 740 | $1.93 | $9.55 | — | $1,526.78 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1429.25 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $161.54 | $2.01 | — | $232.44 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1429.25 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $232.44 | ▼ close $10,898.02 vs 09:30 $11,434.02 (session -496.81) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.44 | ▲ 09:30 equity $10,972.09 vs yday $10,898.02 (+74.07) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $1,530.70 | ▼ -28.26 after sell → book $10,970.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 85 | $15.61 | $2.27 | $-103.11 | $2,855.28 | ▼ -103.11 after sell → book $10,967.78; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 655 | $2.16 | $8.57 | $-30.12 | $4,261.51 | ▼ -30.12 after sell → book $10,959.21; vs 09:30 mark -8.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GPRO` | 802 | $1.48 | $10.49 | $-261.43 | $5,437.98 | ▼ -261.43 after sell → book $10,948.72; vs 09:30 mark -10.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.84 | $2.19 | $-6.72 | $6,842.36 | ▼ -6.72 after sell → book $10,946.54; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 137 | $10.50 | $2.44 | $+6.12 | $8,278.42 | ▲ +6.12 after sell → book $10,944.10; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 740 | $1.90 | $9.68 | $-41.43 | $9,674.74 | ▼ -41.43 after sell → book $10,934.42; vs 09:30 mark -9.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $157.46 | $2.03 | $-36.69 | $10,932.39 | ▼ -36.69 after sell → book $10,932.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $9,902.83 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $8,577.59 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 544 | $2.51 | $7.02 | — | $7,205.13 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $6,068.12 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=-12.7; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 54 | $25.18 | $2.15 | — | $4,706.24 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 236 | $5.79 | $3.04 | — | $3,336.76 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 199 | $6.84 | $2.59 | — | $1,973.01 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+13.2; leftover $1366.55 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 36 | $37.44 | $2.10 | — | $623.07 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1366.55 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $623.07 | ▲ close $11,185.34 vs 09:30 $10,972.09 (session +275.89) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $623.07 | ▼ 09:30 equity $11,072.84 vs yday $11,185.34 (-112.50) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,663.36 | ▲ +10.73 after sell → book $11,070.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $3,096.02 | ▲ +107.42 after sell → book $11,068.77; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 544 | $2.66 | $7.12 | $+67.46 | $4,535.94 | ▲ +67.46 after sell → book $11,061.65; vs 09:30 mark -7.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $5,616.17 | ▼ -56.79 after sell → book $11,059.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 54 | $26.44 | $2.17 | $+63.71 | $7,041.76 | ▲ +63.71 after sell → book $11,057.46; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 236 | $5.81 | $3.09 | $-1.42 | $8,409.82 | ▼ -1.42 after sell → book $11,054.36; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 199 | $6.46 | $2.63 | $-80.84 | $9,692.73 | ▼ -80.84 after sell → book $11,051.73; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 36 | $37.75 | $2.12 | $+6.94 | $11,049.61 | ▲ +6.94 after sell → book $11,049.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,049.61 | ▲ close $11,049.61 vs 09:30 $11,072.84 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,049.61 | ▲ 09:30 equity $11,049.61 vs yday $11,049.61 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,049.61 | ▲ close $11,049.61 vs 09:30 $11,049.61 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,049.61 | ▲ 09:30 equity $11,049.61 vs yday $11,049.61 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,049.61 | ▲ close $11,049.61 vs 09:30 $11,049.61 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,049.61 | ▲ 09:30 equity $11,049.61 vs yday $11,049.61 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 58 | $23.63 | $2.16 | — | $9,676.91 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 17 | $77.33 | $2.04 | — | $8,360.26 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+2.5; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 511 | $2.70 | $6.59 | — | $6,973.97 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 441 | $3.13 | $5.69 | — | $5,587.95 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+24.2; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 126 | $10.95 | $2.37 | — | $4,205.88 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 233 | $5.91 | $3.01 | — | $2,825.84 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 281 | $4.91 | $3.62 | — | $1,442.51 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1381.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 16 | $84.27 | $2.04 | — | $92.15 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1381.20 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.15 | ▲ close $11,067.98 vs 09:30 $11,049.61 (session +45.89) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.15 | ▲ 09:30 equity $11,179.47 vs yday $11,067.98 (+111.49) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 58 | $23.20 | $2.18 | $-29.29 | $1,435.57 | ▼ -29.29 after sell → book $11,177.29; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 17 | $77.10 | $2.06 | $-8.01 | $2,744.20 | ▼ -8.01 after sell → book $11,175.22; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 126 | $10.29 | $2.40 | $-87.93 | $4,038.35 | ▼ -87.93 after sell → book $11,172.83; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 233 | $5.86 | $3.06 | $-17.71 | $5,400.67 | ▼ -17.71 after sell → book $11,169.77; vs 09:30 mark -3.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 281 | $5.03 | $3.68 | $+26.41 | $6,810.42 | ▲ +26.41 after sell → book $11,166.09; vs 09:30 mark -3.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 16 | $86.06 | $2.06 | $+24.54 | $8,185.32 | ▲ +24.54 after sell → book $11,164.03; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,185.32 | ▲ close $11,395.10 vs 09:30 $11,179.47 (session +231.07) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,185.32 | ▲ 09:30 equity $11,527.96 vs yday $11,395.10 (+132.86) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `INDP` | 511 | $3.40 | $6.69 | $+344.42 | $9,916.03 | ▲ +344.42 after sell → book $11,521.27; vs 09:30 mark -6.69 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 441 | $3.64 | $5.77 | $+213.45 | $11,515.49 | ▲ +213.45 after sell → book $11,515.49; vs 09:30 mark -5.78 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,515.49 | ▲ close $11,515.49 vs 09:30 $11,527.96 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,515.49 | ▲ 09:30 equity $11,515.49 vs yday $11,515.49 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 18 | $77.12 | $2.04 | — | $10,125.29 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 245 | $5.87 | $3.16 | — | $8,683.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 529 | $2.72 | $6.82 | — | $7,238.27 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-0.4; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 16 | $87.40 | $2.04 | — | $5,837.84 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 53 | $27.09 | $2.15 | — | $4,399.92 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 111 | $12.89 | $2.32 | — | $2,966.80 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=-18.2; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `INDP` | 393 | $3.66 | $5.07 | — | $1,523.35 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+96.8; leftover $1439.44 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 16 | $89.38 | $2.04 | — | $91.24 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1439.44 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.24 | ▼ close $11,274.93 vs 09:30 $11,515.49 (session -214.92) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.24 | ▲ 09:30 equity $11,407.13 vs yday $11,274.93 (+132.20) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 18 | $76.44 | $2.06 | $-16.35 | $1,465.09 | ▼ -16.35 after sell → book $11,405.06; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 245 | $5.58 | $3.21 | $-77.42 | $2,828.98 | ▼ -77.42 after sell → book $11,401.85; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 529 | $2.94 | $6.92 | $+102.63 | $4,377.31 | ▲ +102.63 after sell → book $11,394.92; vs 09:30 mark -6.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 16 | $83.20 | $2.06 | $-71.30 | $5,706.46 | ▼ -71.30 after sell → book $11,392.87; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 53 | $28.23 | $2.17 | $+56.10 | $7,200.47 | ▲ +56.10 after sell → book $11,390.69; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 111 | $13.56 | $2.35 | $+69.69 | $8,703.28 | ▲ +69.69 after sell → book $11,388.34; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 393 | $3.30 | $5.15 | $-151.70 | $9,995.04 | ▼ -151.70 after sell → book $11,383.20; vs 09:30 mark -5.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 16 | $86.76 | $2.06 | $-46.02 | $11,381.14 | ▼ -46.02 after sell → book $11,381.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 6 | $233.85 | $2.01 | — | $9,976.03 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 9 | $147.61 | $2.02 | — | $8,645.52 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 187 | $7.59 | $2.55 | — | $7,223.64 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 8368 | $0.17 | $39.33 | — | $5,761.75 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 89 | $15.87 | $2.26 | — | $4,347.06 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 54 | $25.95 | $2.15 | — | $2,943.61 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $1,574.80 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1422.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 592 | $2.40 | $7.64 | — | $146.36 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1422.64 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.36 | ▲ close $11,460.90 vs 09:30 $11,407.13 (session +139.73) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.36 | ▲ 09:30 equity $11,652.50 vs yday $11,460.90 (+191.60) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 6 | $249.13 | $2.03 | $+87.64 | $1,639.11 | ▲ +87.64 after sell → book $11,650.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 9 | $146.50 | $2.04 | $-14.04 | $2,955.57 | ▼ -14.04 after sell → book $11,648.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 187 | $7.98 | $2.59 | $+67.79 | $4,445.24 | ▲ +67.79 after sell → book $11,645.84; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `DVLT` | 8368 | $0.17 | $40.73 | $-80.06 | $5,827.07 | ▼ -80.06 after sell → book $11,605.11; vs 09:30 mark -40.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRUN` | 89 | $17.44 | $2.28 | $+135.19 | $7,376.95 | ▲ +135.19 after sell → book $11,602.83; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 54 | $26.14 | $2.17 | $+5.93 | $8,786.33 | ▲ +5.93 after sell → book $11,600.65; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $10,242.94 | ▲ +87.79 after sell → book $11,598.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 592 | $2.29 | $7.75 | $-80.50 | $11,590.87 | ▼ -80.50 after sell → book $11,590.87; vs 09:30 mark -7.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $10,271.14 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 17 | $85.00 | $2.04 | — | $8,824.10 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1493 | $0.97 | $18.96 | — | $7,356.93 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 366 | $3.95 | $4.72 | — | $5,906.51 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 102 | $14.07 | $2.30 | — | $4,469.07 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 97 | $14.79 | $2.28 | — | $3,032.16 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 4092 | $0.35 | $26.76 | — | $1,556.83 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $1448.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 49 | $29.32 | $2.14 | — | $118.02 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1448.86 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.02 | ▼ close $10,429.14 vs 09:30 $11,652.50 (session -1,100.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.02 | ▲ 09:30 equity $10,578.57 vs yday $10,429.14 (+149.43) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $1,497.49 | ▲ +59.74 after sell → book $10,576.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 17 | $82.83 | $2.06 | $-40.99 | $2,903.53 | ▼ -40.99 after sell → book $10,574.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TLSA` | 1493 | $0.94 | $18.77 | $-82.52 | $4,288.18 | ▼ -82.52 after sell → book $10,555.70; vs 09:30 mark -18.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 366 | $3.87 | $4.79 | $-38.79 | $5,699.81 | ▼ -38.79 after sell → book $10,550.91; vs 09:30 mark -4.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 102 | $13.90 | $2.32 | $-21.96 | $7,115.28 | ▼ -21.96 after sell → book $10,548.59; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 97 | $14.58 | $2.31 | $-24.96 | $8,527.24 | ▼ -24.96 after sell → book $10,546.28; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `DCX` | 4092 | $0.14 | $18.73 | $-917.09 | $9,085.47 | ▼ -917.09 after sell → book $10,527.54; vs 09:30 mark -18.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 49 | $29.43 | $2.16 | $+1.09 | $10,525.38 | ▲ +1.09 after sell → book $10,525.38; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,260.41 | — | combo gate; gate vol=good,last_green=True; list flatten; ret5=+6.5; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 14 | $88.83 | $2.03 | — | $8,014.76 | — | combo gate; gate vol=good,last_green=True; list flatten; ret5=+7.6; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 141 | $9.31 | $2.41 | — | $6,699.64 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 97 | $13.47 | $2.28 | — | $5,390.28 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 131 | $9.99 | $2.38 | — | $4,079.21 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 129 | $10.13 | $2.38 | — | $2,769.41 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+4.9; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 77 | $16.91 | $2.22 | — | $1,465.12 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1315.67 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 100 | $13.05 | $2.29 | — | $157.83 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1315.67 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.83 | ▲ close $10,510.92 vs 09:30 $10,578.57 (session +3.55) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.83 | ▲ 09:30 equity $10,540.75 vs yday $10,510.92 (+29.83) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 131 | $9.91 | $2.42 | $-15.28 | $1,453.63 | ▼ -15.28 after sell → book $10,538.34; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 100 | $12.99 | $2.32 | $-10.61 | $2,750.31 | ▼ -10.61 after sell → book $10,536.02; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 37 | $9.11 | $2.10 | — | $2,411.14 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $343.79 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 340 | $1.01 | $4.39 | — | $2,063.35 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $343.79 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 47 | $7.23 | $2.13 | — | $1,721.41 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $343.79 | — |
| 2026-09-22 09:30 ET | **BUY** | `SECZ` | 26 | $12.96 | $2.07 | — | $1,382.39 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+64.4; leftover $343.79 | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $1,060.98 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; leftover $343.79 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,060.98 | ▼ close $10,489.46 vs 09:30 $10,540.75 (session -33.89) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,060.98 | ▼ 09:30 equity $10,476.68 vs yday $10,489.46 (-12.78) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 141 | $9.50 | $2.45 | $+21.93 | $2,398.04 | ▲ +21.93 after sell → book $10,474.24; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 97 | $12.84 | $2.31 | $-66.18 | $3,641.21 | ▼ -66.18 after sell → book $10,471.93; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SGML` | 129 | $10.26 | $2.41 | $+11.34 | $4,962.34 | ▲ +11.34 after sell → book $10,469.52; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 77 | $16.92 | $2.24 | $-3.70 | $6,262.93 | ▼ -3.70 after sell → book $10,467.27; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 37 | $8.39 | $2.12 | $-30.86 | $6,571.24 | ▼ -30.86 after sell → book $10,465.15; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 340 | $0.95 | $4.32 | $-29.10 | $6,889.93 | ▼ -29.10 after sell → book $10,460.84; vs 09:30 mark -4.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 47 | $6.83 | $2.15 | $-23.08 | $7,208.79 | ▼ -23.08 after sell → book $10,458.69; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 26 | $12.80 | $2.09 | $-8.32 | $7,539.50 | ▼ -8.32 after sell → book $10,456.60; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $7,869.27 | ▲ +8.36 after sell → book $10,454.59; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 63 | $20.65 | $2.18 | — | $6,566.14 | — | combo gate; gate vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1311.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 333 | $3.93 | $4.30 | — | $5,253.15 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1311.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 83 | $15.72 | $2.24 | — | $3,946.15 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1311.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 51 | $25.40 | $2.14 | — | $2,648.61 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1311.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 84 | $15.55 | $2.24 | — | $1,340.17 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $1311.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1707 | $0.77 | $18.23 | — | $10.96 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1311.54 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.96 | ▼ close $10,084.50 vs 09:30 $10,476.68 (session -338.76) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.96 | ▼ 09:30 equity $10,033.51 vs yday $10,084.50 (-50.99) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,320.53 | ▲ +44.59 after sell → book $10,031.48; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 14 | $87.67 | $2.05 | $-20.25 | $2,545.92 | ▼ -20.25 after sell → book $10,029.43; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 63 | $20.52 | $2.20 | $-12.57 | $3,836.48 | ▼ -12.57 after sell → book $10,027.23; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 333 | $3.77 | $4.36 | $-61.94 | $5,087.53 | ▼ -61.94 after sell → book $10,022.86; vs 09:30 mark -4.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 83 | $14.38 | $2.26 | $-115.72 | $6,278.81 | ▼ -115.72 after sell → book $10,020.60; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 51 | $23.99 | $2.16 | $-76.22 | $7,500.14 | ▼ -76.22 after sell → book $10,018.44; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CLPT` | 84 | $14.82 | $2.27 | $-65.83 | $8,742.75 | ▼ -65.83 after sell → book $10,016.17; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1707 | $0.75 | $18.15 | $-73.93 | $9,998.02 | ▼ -73.93 after sell → book $9,998.02; vs 09:30 mark -18.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,998.02 | ▲ close $9,998.02 vs 09:30 $10,033.51 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,260.77 | ▲ 09:30 equity $8,260.77 vs yday $8,260.77 (+0.00) | 09:30 open · cash $8,260.77 · no holdings · equity $8,260.77 vs prior close $8,260.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 39 | $26.27 | $2.11 | — | $7,234.13 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1032.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 57 | $17.91 | $2.16 | — | $6,211.10 | — | combo gate; gate vol=good,last_green=True; list probable; 🔵; ret5=+3.7; leftover $1032.60 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $5,204.74 | — | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1032.60 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 170 | $6.06 | $2.50 | — | $4,172.04 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+342.1; leftover $1032.60 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 267 | $3.86 | $3.44 | — | $3,137.97 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1032.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 101 | $10.20 | $2.29 | — | $2,105.48 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1032.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $1,183.47 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1032.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 63 | $16.21 | $2.18 | — | $160.06 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1032.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.06 | ▼ close $8,162.73 vs 09:30 $8,260.77 (session -79.32) | 16:00 close · cash $160.06 · equity $8,162.73 vs 09:30 $8,260.77 (-98.04; session marks -79.32) · 8 name(s) marked open→close (per-name table). WRBY×39 09:30 $26.27 → close $26.71 +17.16; PL×57 09:30 $17.91 → close $17.43 -27.36; TEM×12 09:30 $83.69 → close $85.01 +15.78; GLND×170 09:30 $6.06 → close $5.54 -88.40; ZSQR×267 09:30 $3.86 → close $3.78 -21.36; DNA×101 09:30 $10.20 → close $10.66 +46.46; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×63 09:30 $16.21 → close $15.96 -15.75 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.06 | ▼ 09:30 equity $7,984.85 vs yday $8,162.73 (-177.88) | 09:30 open · cash $160.06 (unchanged overnight, no fees) · equity $7,984.85 vs prior close $8,162.73 (-177.88) · 8 name(s) re-marked at the open (per-name table). DNA×101 yday $10.66 → 09:30 $10.38 -28.79; GLND×170 yday $5.54 → 09:30 $5.14 -68.34; PL×57 yday $17.43 → 09:30 $17.17 -14.82; SECZ×63 yday $15.96 → 09:30 $16.00 +2.52; TEM×12 yday $85.01 → 09:30 $83.57 -17.28; TWST×5 yday $182.83 → 09:30 $181.87 -4.80; WRBY×39 yday $26.71 → 09:30 $26.00 -27.69; ZSQR×267 yday $3.78 → 09:30 $3.71 -18.69 | — |
| 2026-09-28 09:30 ET | **SELL** | `DNA` | 101 | $10.38 | $2.32 | $+13.06 | $1,205.62 | ▲ +13.06 after sell → book $7,982.53; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `GLND` | 170 | $5.14 | $2.54 | $-161.78 | $2,076.54 | ▼ -161.78 after sell → book $7,979.99; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `PL` | 57 | $17.17 | $2.18 | $-46.52 | $3,053.05 | ▼ -46.52 after sell → book $7,977.81; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SECZ` | 63 | $16.00 | $2.20 | $-17.61 | $4,058.85 | ▼ -17.61 after sell → book $7,975.61; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TEM` | 12 | $83.57 | $2.05 | $-5.57 | $5,059.64 | ▼ -5.57 after sell → book $7,973.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `TWST` | 5 | $181.87 | $2.02 | $-14.68 | $5,966.97 | ▼ -14.68 after sell → book $7,971.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `WRBY` | 39 | $26.00 | $2.13 | $-14.76 | $6,978.84 | ▼ -14.76 after sell → book $7,969.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 267 | $3.71 | $3.50 | $-46.99 | $7,965.91 | ▼ -46.99 after sell → book $7,965.91; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,965.91 | ▲ close $7,965.91 vs 09:30 $7,984.85 (session +0.00) | 16:00 close · cash $7,965.91 · no lots left · equity $7,965.91. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,965.91 | ▲ 09:30 equity $7,965.91 vs yday $7,965.91 (+0.00) | 09:30 open · cash $7,965.91 · no holdings · equity $7,965.91 vs prior close $7,965.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `SFIX` | 403 | $2.47 | $5.20 | — | $6,965.30 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-13.3; leftover $995.74 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BUR` | 252 | $3.95 | $3.25 | — | $5,966.65 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; ret5=-0.3; leftover $995.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `KOD` | 11 | $87.50 | $2.02 | — | $5,002.13 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+177.4; leftover $995.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `QNC` | 483 | $2.06 | $6.23 | — | $4,000.92 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+24.3; leftover $995.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `NAUT` | 840 | $1.19 | $10.84 | — | $2,994.68 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+16.2; leftover $995.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AMC` | 299 | $3.32 | $3.86 | — | $1,998.14 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.8; leftover $995.74 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MGNX` | 243 | $4.09 | $3.13 | — | $1,001.14 | — | combo gate; gate last_green=True,vol=good; list yday_gainer; ret5=-1.5; leftover $995.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `XERS` | 101 | $9.77 | $2.29 | — | $12.08 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $995.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.08 | ▼ close $7,853.65 vs 09:30 $7,965.91 (session -75.44) | 16:00 close · cash $12.08 · equity $7,853.65 vs 09:30 $7,965.91 (-112.26; session marks -75.44) · 8 name(s) marked open→close (per-name table). SFIX×403 09:30 $2.47 → close $2.53 +24.18; BUR×252 09:30 $3.95 → close $3.90 -12.60; KOD×11 09:30 $87.50 → close $91.12 +39.82; QNC×483 09:30 $2.06 → close $1.75 -149.73; NAUT×840 09:30 $1.19 → close $1.36 +151.20; AMC×299 09:30 $3.32 → close $3.08 -71.76; MGNX×243 09:30 $4.09 → close $3.77 -77.76; XERS×101 09:30 $9.77 → close $9.98 +21.21 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.08 | ▲ 09:30 equity $7,853.65 vs yday $7,853.65 (+0.00) | 09:30 open · cash $12.08 (unchanged overnight, no fees) · equity $7,853.65 vs prior close $7,853.65 (+0.00) · 8 name(s) re-marked at the open (per-name table). AMC×299 yday $3.08 → 09:30 $3.08 +0.00; BUR×252 yday $3.90 → 09:30 $3.90 +0.00; KOD×11 yday $91.12 → 09:30 $91.12 +0.00; MGNX×243 yday $3.77 → 09:30 $3.77 +0.00; NAUT×840 yday $1.36 → 09:30 $1.36 +0.00; QNC×483 yday $1.75 → 09:30 $1.75 +0.00; SFIX×403 yday $2.53 → 09:30 $2.53 +0.00; XERS×101 yday $9.98 → 09:30 $9.98 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.08 | ▲ close $7,853.65 vs 09:30 $7,853.65 (session +0.00) | 16:00 close · cash $12.08 · equity $7,853.65 vs 09:30 $7,853.65 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). AMC×299 09:30 $3.08 → close $3.08 +0.00; BUR×252 09:30 $3.90 → close $3.90 +0.00; KOD×11 09:30 $91.12 → close $91.12 +0.00; MGNX×243 09:30 $3.77 → close $3.77 +0.00; NAUT×840 09:30 $1.36 → close $1.36 +0.00; QNC×483 09:30 $1.75 → close $1.75 +0.00; SFIX×403 09:30 $2.53 → close $2.53 +0.00; XERS×101 09:30 $9.98 → close $9.98 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.08 | ▼ 09:30 equity $7,814.60 vs yday $7,853.65 (-39.05) | 09:30 open · cash $12.08 (unchanged overnight, no fees) · equity $7,814.60 vs prior close $7,853.65 (-39.05) · 8 name(s) re-marked at the open (per-name table). AMC×299 yday $3.08 → 09:30 $2.98 -29.90; BUR×252 yday $3.90 → 09:30 $3.88 -5.04; KOD×11 yday $91.12 → 09:30 $95.41 +47.19; MGNX×243 yday $3.77 → 09:30 $3.85 +19.44; NAUT×840 yday $1.36 → 09:30 $1.43 +54.60; QNC×483 yday $1.75 → 09:30 $1.57 -86.94; SFIX×403 yday $2.53 → 09:30 $2.55 +8.06; XERS×101 yday $9.98 → 09:30 $9.52 -46.46 | — |
| 2026-10-01 09:30 ET | **SELL** | `AMC` | 299 | $2.98 | $3.92 | $-109.43 | $899.18 | ▼ -109.43 after sell → book $7,810.68; vs 09:30 mark -3.92 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `BUR` | 252 | $3.88 | $3.30 | $-24.19 | $1,873.64 | ▼ -24.19 after sell → book $7,807.38; vs 09:30 mark -3.30 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `KOD` | 11 | $95.41 | $2.04 | $+82.94 | $2,921.11 | ▲ +82.94 after sell → book $7,805.34; vs 09:30 mark -2.04 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MGNX` | 243 | $3.85 | $3.19 | $-64.64 | $3,853.47 | ▼ -64.64 after sell → book $7,802.15; vs 09:30 mark -3.19 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `NAUT` | 840 | $1.43 | $10.99 | $+183.98 | $5,043.69 | ▲ +183.98 after sell → book $7,791.17; vs 09:30 mark -10.98 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `QNC` | 483 | $1.57 | $6.32 | $-249.22 | $5,795.68 | ▼ -249.22 after sell → book $7,784.85; vs 09:30 mark -6.32 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SFIX` | 403 | $2.55 | $5.28 | $+21.77 | $6,818.05 | ▲ +21.77 after sell → book $7,779.57; vs 09:30 mark -5.28 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `XERS` | 101 | $9.52 | $2.32 | $-29.86 | $7,777.25 | ▼ -29.86 after sell → book $7,777.25; vs 09:30 mark -2.32 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 68 | $14.27 | $2.19 | — | $6,804.70 | — | combo gate; gate last_green=True,vol=good; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $972.16 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `TLSA` | 875 | $1.11 | $11.29 | — | $5,822.16 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+5.7; leftover $972.16 | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IVA` | 288 | $3.38 | $3.72 | — | $4,846.44 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; ret5=+7.0; leftover $972.16 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `CDNL` | 32 | $30.30 | $2.09 | — | $3,874.76 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; ret5=+6.7; leftover $972.16 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `CDZI` | 249 | $3.89 | $3.21 | — | $2,902.94 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; ret5=+2.6; leftover $972.16 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `SWMR` | 55 | $17.50 | $2.15 | — | $1,938.28 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; 🔵; ret5=-21.6; leftover $972.16 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PACB` | 415 | $2.34 | $5.35 | — | $961.83 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+68.1; leftover $972.16 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PMVP` | 568 | $1.68 | $7.33 | — | $0.26 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+21.9; leftover $972.16 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.26 | ▲ close $7,761.21 vs 09:30 $7,814.60 (session +21.29) | 16:00 close · cash $0.26 · equity $7,761.21 vs 09:30 $7,814.60 (-53.39; session marks +21.29) · 8 name(s) marked open→close (per-name table). AVPT×68 09:30 $14.27 → close $14.08 -12.92; TLSA×875 09:30 $1.11 → close $1.14 +26.25; IVA×288 09:30 $3.38 → close $3.46 +24.48; CDNL×32 09:30 $30.30 → close $29.74 -17.92; CDZI×249 09:30 $3.89 → close $3.70 -47.31; SWMR×55 09:30 $17.50 → close $16.38 -61.60; PACB×415 09:30 $2.34 → close $2.51 +70.55; PMVP×568 09:30 $1.68 → close $1.75 +39.76 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.26 | ▲ 09:30 equity $7,830.41 vs yday $7,761.21 (+69.20) | 09:30 open · cash $0.26 (unchanged overnight, no fees) · equity $7,830.41 vs prior close $7,761.21 (+69.20) · 8 name(s) re-marked at the open (per-name table). AVPT×68 yday $14.08 → 09:30 $14.22 +9.52; CDNL×32 yday $29.74 → 09:30 $30.29 +17.60; CDZI×249 yday $3.70 → 09:30 $3.75 +12.45; IVA×288 yday $3.46 → 09:30 $3.57 +31.68; PACB×415 yday $2.51 → 09:30 $2.50 -4.15; PMVP×568 yday $1.75 → 09:30 $1.75 +0.00; SWMR×55 yday $16.38 → 09:30 $16.10 -15.40; TLSA×875 yday $1.14 → 09:30 $1.16 +17.50 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 68 | $14.22 | $2.22 | $-7.81 | $965.00 | ▼ -7.81 after sell → book $7,828.19; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `CDNL` | 32 | $30.29 | $2.11 | $-4.51 | $1,932.18 | ▼ -4.51 after sell → book $7,826.09; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `CDZI` | 249 | $3.75 | $3.26 | $-41.34 | $2,862.67 | ▼ -41.34 after sell → book $7,822.83; vs 09:30 mark -3.26 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `IVA` | 288 | $3.57 | $3.77 | $+48.67 | $3,887.05 | ▲ +48.67 after sell → book $7,819.05; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PACB` | 415 | $2.50 | $5.43 | $+55.61 | $4,919.12 | ▲ +55.61 after sell → book $7,813.62; vs 09:30 mark -5.43 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PMVP` | 568 | $1.75 | $7.43 | $+25.00 | $5,905.69 | ▲ +25.00 after sell → book $7,806.19; vs 09:30 mark -7.43 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `SWMR` | 55 | $16.10 | $2.17 | $-81.33 | $6,789.01 | ▼ -81.33 after sell → book $7,804.01; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `TLSA` | 875 | $1.16 | $11.44 | $+21.02 | $7,792.57 | ▲ +21.02 after sell → book $7,792.57; vs 09:30 mark -11.44 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 14 | $66.33 | $2.03 | — | $6,861.92 | — | combo gate; gate last_green=True,vol=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $974.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 18 | $52.42 | $2.04 | — | $5,916.31 | — | combo gate; gate last_green=True,vol=good; list flatten; 🔵; ⚪; ret5=-12.6; leftover $974.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `QNC` | 576 | $1.69 | $7.43 | — | $4,935.44 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer,yday_mover; ret5=-7.9; leftover $974.07 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `AIB` | 671 | $1.45 | $8.66 | — | $3,953.84 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer,yday_mover; ret5=+7.0; leftover $974.07 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `COHR` | 3 | $316.56 | $2.00 | — | $3,002.14 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; 🔵; ret5=+9.8; leftover $974.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `INOD` | 13 | $73.05 | $2.03 | — | $2,050.47 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=-0.0; leftover $974.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SDEV` | 192 | $5.06 | $2.57 | — | $1,076.38 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+183.7; leftover $974.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SES` | 1132 | $0.86 | $13.13 | — | $89.73 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+54.3; leftover $974.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.73 | ▲ close $8,402.26 vs 09:30 $7,830.41 (session +649.58) | 16:00 close · cash $89.73 · equity $8,402.26 vs 09:30 $7,830.41 (+571.85; session marks +649.58) · 8 name(s) marked open→close (per-name table). CDNA×14 09:30 $66.33 → close $67.15 +11.48; ETON×18 09:30 $52.42 → close $55.36 +52.92; QNC×576 09:30 $1.69 → close $1.59 -57.60; AIB×671 09:30 $1.45 → close $1.64 +127.49; COHR×3 09:30 $316.56 → close $337.04 +61.43; INOD×13 09:30 $73.05 → close $70.07 -38.74; SDEV×192 09:30 $5.06 → close $7.48 +464.64; SES×1132 09:30 $0.86 → close $0.88 +27.96 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.73 | ▲ 09:30 equity $8,880.40 vs yday $8,402.26 (+478.14) | 09:30 open · cash $89.73 (unchanged overnight, no fees) · equity $8,880.40 vs prior close $8,402.26 (+478.14) · 8 name(s) re-marked at the open (per-name table). AIB×671 yday $1.64 → 09:30 $1.62 -13.42; CDNA×14 yday $67.15 → 09:30 $66.90 -3.50; COHR×3 yday $337.04 → 09:30 $340.93 +11.65; ETON×18 yday $55.36 → 09:30 $55.85 +8.82; INOD×13 yday $70.07 → 09:30 $70.98 +11.83; QNC×576 yday $1.59 → 09:30 $1.62 +17.28; SDEV×192 yday $7.48 → 09:30 $9.71 +428.16; SES×1132 yday $0.88 → 09:30 $0.90 +17.32 | — |
| 2026-10-05 09:30 ET | **SELL** | `AIB` | 671 | $1.62 | $8.78 | $+96.64 | $1,167.97 | ▲ +96.64 after sell → book $8,871.63; vs 09:30 mark -8.77 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 14 | $66.90 | $2.05 | $+3.90 | $2,102.52 | ▲ +3.90 after sell → book $8,869.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `COHR` | 3 | $340.93 | $2.02 | $+69.06 | $3,123.28 | ▲ +69.06 after sell → book $8,867.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ETON` | 18 | $55.85 | $2.06 | $+57.63 | $4,126.51 | ▲ +57.63 after sell → book $8,865.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `INOD` | 13 | $70.98 | $2.05 | $-30.99 | $5,047.20 | ▼ -30.99 after sell → book $8,863.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `QNC` | 576 | $1.62 | $7.54 | $-55.29 | $5,972.79 | ▼ -55.29 after sell → book $8,855.91; vs 09:30 mark -7.53 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SDEV` | 192 | $9.71 | $2.61 | $+887.62 | $7,834.49 | ▲ +887.62 after sell → book $8,853.29; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `SES` | 1132 | $0.90 | $13.78 | $+18.37 | $8,839.51 | ▲ +18.37 after sell → book $8,839.51; vs 09:30 mark -13.78 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `WNC` | 78 | $14.02 | $2.22 | — | $7,743.73 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+7.8; leftover $1104.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `EVGO` | 800 | $1.38 | $10.32 | — | $6,629.41 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; ret5=+0.0; leftover $1104.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `NNBR` | 240 | $4.59 | $3.10 | — | $5,524.71 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+22.9; leftover $1104.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `NEOV` | 359 | $3.07 | $4.63 | — | $4,417.95 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+39.3; leftover $1104.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `FEAM` | 284 | $3.88 | $3.66 | — | $3,312.37 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+38.6; leftover $1104.94 | join🟡 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `WOLF` | 32 | $34.04 | $2.09 | — | $2,221.00 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+28.0; leftover $1104.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `AZTA` | 27 | $39.72 | $2.07 | — | $1,146.49 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+13.9; leftover $1104.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VSH` | 29 | $37.64 | $2.08 | — | $52.85 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1104.94 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.85 | ▼ close $8,720.16 vs 09:30 $8,880.40 (session -89.18) | 16:00 close · cash $52.85 · equity $8,720.16 vs 09:30 $8,880.40 (-160.24; session marks -89.18) · 8 name(s) marked open→close (per-name table). WNC×78 09:30 $14.02 → close $13.99 -2.34; EVGO×800 09:30 $1.38 → close $1.35 -24.00; NNBR×240 09:30 $4.59 → close $4.24 -84.00; NEOV×359 09:30 $3.07 → close $2.96 -39.49; FEAM×284 09:30 $3.88 → close $3.92 +11.36; WOLF×32 09:30 $34.04 → close $33.53 -16.32; AZTA×27 09:30 $39.72 → close $42.73 +81.27; VSH×29 09:30 $37.64 → close $37.10 -15.66 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.85 | ▲ 09:30 equity $8,800.72 vs yday $8,720.16 (+80.56) | 09:30 open · cash $52.85 (unchanged overnight, no fees) · equity $8,800.72 vs prior close $8,720.16 (+80.56) · 8 name(s) re-marked at the open (per-name table). AZTA×27 yday $42.73 → 09:30 $43.81 +29.16; EVGO×800 yday $1.35 → 09:30 $1.36 +7.28; FEAM×284 yday $3.92 → 09:30 $3.96 +11.36; NEOV×359 yday $2.96 → 09:30 $2.97 +3.59; NNBR×240 yday $4.24 → 09:30 $4.24 +0.00; VSH×29 yday $37.10 → 09:30 $37.13 +0.87; WNC×78 yday $13.99 → 09:30 $14.16 +13.26; WOLF×32 yday $33.53 → 09:30 $34.00 +15.04 | — |
| 2026-10-06 09:30 ET | **SELL** | `AZTA` | 27 | $43.81 | $2.09 | $+106.27 | $1,233.63 | ▲ +106.27 after sell → book $8,798.63; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `EVGO` | 800 | $1.36 | $10.46 | $-37.50 | $2,310.45 | ▼ -37.50 after sell → book $8,788.17; vs 09:30 mark -10.46 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `FEAM` | 284 | $3.96 | $3.72 | $+15.34 | $3,431.37 | ▲ +15.34 after sell → book $8,784.45; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `NEOV` | 359 | $2.97 | $4.70 | $-45.23 | $4,492.89 | ▼ -45.23 after sell → book $8,779.74; vs 09:30 mark -4.71 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `NNBR` | 240 | $4.24 | $3.15 | $-90.24 | $5,507.35 | ▼ -90.24 after sell → book $8,776.60; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `VSH` | 29 | $37.13 | $2.10 | $-18.96 | $6,582.02 | ▼ -18.96 after sell → book $8,774.50; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `WNC` | 78 | $14.16 | $2.25 | $+6.45 | $7,684.26 | ▲ +6.45 after sell → book $8,772.26; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `WOLF` | 32 | $34.00 | $2.11 | $-5.47 | $8,770.15 | ▼ -5.47 after sell → book $8,770.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **BUY** | `AVPT` | 74 | $14.77 | $2.21 | — | $7,674.96 | — | combo gate; gate last_green=True,vol=good; list flatten; ⚪; ret5=+7.2; leftover $1096.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `EBS` | 153 | $7.16 | $2.45 | — | $6,577.03 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer,yday_mover; ret5=+5.5; leftover $1096.27 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ALVO` | 180 | $6.07 | $2.53 | — | $5,481.90 | — | combo gate; gate last_green=True,vol=good; list probable,yday_gainer; ret5=-0.7; leftover $1096.27 | join🔴 sector🟡 gen🟡 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XERS` | 107 | $10.23 | $2.31 | — | $4,384.98 | — | combo gate; gate last_green=True,vol=good; list probable; ret5=+3.1; leftover $1096.27 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `ZNTL` | 390 | $2.81 | $5.03 | — | $3,284.05 | — | combo gate; gate last_green=True,vol=good; list probable; ret5=+6.9; leftover $1096.27 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `QTEX` | 642 | $1.71 | $8.28 | — | $2,181.15 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+123.8; leftover $1096.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XP` | 37 | $29.20 | $2.10 | — | $1,098.65 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; ret5=+39.1; leftover $1096.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PAGS` | 100 | $10.96 | $2.29 | — | $0.36 | — | combo gate; gate last_green=True,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+23.0; leftover $1096.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.36 | ▼ close $8,560.15 vs 09:30 $8,800.72 (session -182.79) | 16:00 close · cash $0.36 · equity $8,560.15 vs 09:30 $8,800.72 (-240.57; session marks -182.79) · 8 name(s) marked open→close (per-name table). AVPT×74 09:30 $14.77 → close $14.59 -13.32; EBS×153 09:30 $7.16 → close $6.95 -32.13; ALVO×180 09:30 $6.07 → close $6.14 +12.60; XERS×107 09:30 $10.23 → close $10.07 -17.12; ZNTL×390 09:30 $2.81 → close $2.67 -54.60; QTEX×642 09:30 $1.71 → close $1.59 -73.83; XP×37 09:30 $29.20 → close $29.73 +19.61; PAGS×100 09:30 $10.96 → close $10.72 -24.00 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.36 | ▼ 09:30 equity $8,382.48 vs yday $8,560.15 (-177.67) | 09:30 open · cash $0.36 (unchanged overnight, no fees) · equity $8,382.48 vs prior close $8,560.15 (-177.67) · 8 name(s) re-marked at the open (per-name table). ALVO×180 yday $6.14 → 09:30 $6.10 -7.20; AVPT×74 yday $14.59 → 09:30 $14.51 -5.92; EBS×153 yday $6.95 → 09:30 $7.05 +15.30; PAGS×100 yday $10.72 → 09:30 $10.64 -8.00; QTEX×642 yday $1.59 → 09:30 $1.38 -138.03; XERS×107 yday $10.07 → 09:30 $10.00 -7.49; XP×37 yday $29.73 → 09:30 $29.44 -10.73; ZNTL×390 yday $2.67 → 09:30 $2.63 -15.60 | — |
| 2026-10-07 09:30 ET | **SELL** | `ALVO` | 180 | $6.10 | $2.57 | $+0.30 | $1,095.79 | ▲ +0.30 after sell → book $8,379.91; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `AVPT` | 74 | $14.51 | $2.23 | $-23.69 | $2,167.30 | ▼ -23.69 after sell → book $8,377.68; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `EBS` | 153 | $7.05 | $2.48 | $-21.76 | $3,243.46 | ▼ -21.76 after sell → book $8,375.19; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `PAGS` | 100 | $10.64 | $2.32 | $-36.61 | $4,305.14 | ▼ -36.61 after sell → book $8,372.87; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `QTEX` | 642 | $1.38 | $8.40 | $-228.54 | $5,179.50 | ▼ -228.54 after sell → book $8,364.48; vs 09:30 mark -8.39 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `XERS` | 107 | $10.00 | $2.34 | $-29.26 | $6,247.16 | ▼ -29.26 after sell → book $8,362.14; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `XP` | 37 | $29.44 | $2.12 | $+4.66 | $7,334.32 | ▲ +4.66 after sell → book $8,360.02; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 09:30 ET | **SELL** | `ZNTL` | 390 | $2.63 | $5.11 | $-80.34 | $8,354.91 | ▼ -80.34 after sell → book $8,354.91; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,354.91 | ▲ close $8,354.91 vs 09:30 $8,382.48 (session +0.00) | 16:00 close · cash $8,354.91 · no lots left · equity $8,354.91. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CHRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SGML` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `EU` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
