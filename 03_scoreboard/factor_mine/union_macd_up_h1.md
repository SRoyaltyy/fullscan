# Factor mine action — `union_macd_up_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_up, no 🚨

Cash book **-14.26%** ($8,574) · signal-only (no cash/fees) was +22.07%. Starts YES **11/30**. Fills 246 · skips 104 · realized $+158.57.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior MACD histogram is above zero (momentum still up).
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
- **Gate** `macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,158.59.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $8,020.74 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $6,044.33 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 67 | $29.74 | $2.19 | — | $4,049.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $2,064.26 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 90 | $22.01 | $2.26 | — | $81.10 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+0.3; leftover $2000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.10 | ▲ close $10,125.70 vs 09:30 $10,000.00 (session +136.63) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.10 | ▲ 09:30 equity $10,134.23 vs yday $10,125.70 (+8.53) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $1,974.83 | ▼ -85.53 after sell → book $10,132.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $4,129.00 | ▲ +177.76 after sell → book $10,129.95; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 67 | $29.15 | $2.22 | $-43.94 | $6,079.84 | ▼ -43.94 after sell → book $10,127.74; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $8,025.76 | ▼ -39.37 after sell → book $10,125.46; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 90 | $23.33 | $2.29 | $+114.25 | $10,123.17 | ▲ +114.25 after sell → book $10,123.17; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,041.68 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+5.9; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $7,829.82 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 843 | $1.50 | $10.87 | — | $6,554.44 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 302 | $4.18 | $3.90 | — | $5,288.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $4,031.97 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $2,796.22 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable; 🔵; ret5=+3.9; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $1,541.55 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1265.40 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 113 | $11.12 | $2.33 | — | $282.67 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1265.40 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.67 | ▼ close $9,904.78 vs 09:30 $10,134.23 (session -190.77) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.67 | ▼ 09:30 equity $9,866.97 vs yday $9,904.78 (-37.81) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,384.29 | ▲ +20.13 after sell → book $9,864.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $2,544.98 | ▼ -51.17 after sell → book $9,862.87; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 843 | $1.52 | $11.02 | $-5.04 | $3,815.32 | ▼ -5.04 after sell → book $9,851.85; vs 09:30 mark -11.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 302 | $4.10 | $3.96 | $-32.01 | $5,049.56 | ▼ -32.01 after sell → book $9,847.89; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $6,242.80 | ▼ -62.98 after sell → book $9,845.65; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $7,509.67 | ▲ +31.11 after sell → book $9,843.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $8,759.94 | ▼ -4.38 after sell → book $9,841.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 113 | $9.57 | $2.36 | $-179.84 | $9,839.00 | ▼ -179.84 after sell → book $9,839.00; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $8,636.25 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+6.7; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $7,418.04 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+8.3; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 303 | $4.05 | $3.91 | — | $6,186.98 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 145 | $8.46 | $2.42 | — | $4,957.86 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $3,778.81 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=-7.2; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 379 | $3.24 | $4.89 | — | $2,545.96 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+0.3; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 242 | $5.07 | $3.12 | — | $1,315.90 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=-4.7; leftover $1229.87 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $105.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ret5=-0.8; leftover $1229.87 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.00 | ▼ close $9,785.85 vs 09:30 $9,866.97 (session -30.67) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.00 | ▼ 09:30 equity $9,695.56 vs yday $9,785.85 (-90.29) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $1,350.91 | ▲ +43.16 after sell → book $9,693.47; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $2,602.46 | ▲ +33.34 after sell → book $9,691.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 303 | $3.72 | $3.97 | $-107.87 | $3,725.65 | ▼ -107.87 after sell → book $9,687.47; vs 09:30 mark -3.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 145 | $8.55 | $2.46 | $+8.17 | $4,962.94 | ▲ +8.17 after sell → book $9,685.01; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $6,175.62 | ▲ +33.62 after sell → book $9,682.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 379 | $3.11 | $4.96 | $-59.12 | $7,349.34 | ▼ -59.12 after sell → book $9,678.00; vs 09:30 mark -4.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 242 | $4.66 | $3.17 | $-105.51 | $8,473.89 | ▼ -105.51 after sell → book $9,674.83; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $9,672.78 | ▼ -12.01 after sell → book $9,672.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.78 | ▲ close $9,672.78 vs 09:30 $9,695.56 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,672.78 | ▲ 09:30 equity $9,672.78 vs yday $9,672.78 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.78 | ▲ close $9,672.78 vs 09:30 $9,672.78 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,672.78 | ▲ 09:30 equity $9,672.78 vs yday $9,672.78 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,478.72 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,293.56 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $6,093.70 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,894.09 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,706.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 690 | $1.75 | $8.90 | — | $2,490.38 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,332.05 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1209.10 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 245 | $4.92 | $3.16 | — | $123.49 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1209.10 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.49 | ▲ close $9,879.93 vs 09:30 $9,672.78 (session +231.86) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.49 | ▲ 09:30 equity $10,224.42 vs yday $9,879.93 (+344.49) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,391.50 | ▲ +73.95 after sell → book $10,222.23; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,633.81 | ▲ +57.15 after sell → book $10,220.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $3,893.13 | ▲ +59.45 after sell → book $10,218.00; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,182.31 | ▲ +89.57 after sell → book $10,215.81; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,466.98 | ▲ +97.36 after sell → book $10,213.68; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 690 | $1.79 | $9.03 | $+9.67 | $7,693.05 | ▲ +9.67 after sell → book $10,204.65; vs 09:30 mark -9.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,928.62 | ▲ +77.23 after sell → book $10,202.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 245 | $5.20 | $3.21 | $+62.23 | $10,199.40 | ▲ +62.23 after sell → book $10,199.40; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,003.08 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 74 | $17.20 | $2.21 | — | $7,728.07 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,644.57 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 114 | $11.13 | $2.33 | — | $5,373.42 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 516 | $2.47 | $6.66 | — | $4,092.24 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 660 | $1.93 | $8.51 | — | $2,809.93 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,553.75 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1274.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 965 | $1.32 | $12.45 | — | $267.50 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1274.93 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.50 | ▲ close $10,416.36 vs 09:30 $10,224.42 (session +255.20) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.50 | ▲ 09:30 equity $10,782.45 vs yday $10,416.36 (+366.09) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,470.56 | ▲ +6.74 after sell → book $10,780.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 74 | $16.57 | $2.23 | $-51.07 | $2,694.51 | ▼ -51.07 after sell → book $10,778.18; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,777.63 | ▼ -0.38 after sell → book $10,776.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 114 | $13.33 | $2.36 | $+246.10 | $5,294.89 | ▲ +246.10 after sell → book $10,773.79; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 516 | $2.40 | $6.75 | $-49.53 | $6,526.54 | ▼ -49.53 after sell → book $10,767.04; vs 09:30 mark -6.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 660 | $1.88 | $8.63 | $-50.15 | $7,758.71 | ▼ -50.15 after sell → book $10,758.41; vs 09:30 mark -8.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $8,990.38 | ▼ -24.50 after sell → book $10,756.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 965 | $1.83 | $12.62 | $+467.08 | $10,743.71 | ▲ +467.08 after sell → book $10,743.71; vs 09:30 mark -12.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,743.71 | ▲ close $10,743.71 vs 09:30 $10,782.45 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,743.71 | ▲ 09:30 equity $10,743.71 vs yday $10,743.71 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 56 | $23.77 | $2.16 | — | $9,410.43 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+13.0; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 122 | $10.98 | $2.36 | — | $8,068.52 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+1.2; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $6,781.47 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+7.4; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 160 | $8.35 | $2.47 | — | $5,443.00 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 271 | $4.94 | $3.50 | — | $4,100.77 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+7.1; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,817.86 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+6.0; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $7.25 | $2.54 | — | $1,474.06 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1342.96 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.59 | $2.28 | — | $139.96 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1342.96 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.96 | ▲ close $10,987.97 vs 09:30 $10,743.71 (session +263.62) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.96 | ▲ 09:30 equity $10,994.45 vs yday $10,987.97 (+6.48) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 21 | $60.07 | $2.07 | $-27.65 | $1,399.36 | ▼ -27.65 after sell → book $10,992.38; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 185 | $8.29 | $2.59 | $+187.27 | $2,930.42 | ▲ +187.27 after sell → book $10,989.79; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 98 | $13.63 | $2.31 | $-0.67 | $4,263.85 | ▼ -0.67 after sell → book $10,987.48; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 45 | $31.21 | $2.12 | — | $2,857.27 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1421.28 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 127 | $11.12 | $2.37 | — | $1,442.66 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1421.28 | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 144 | $9.83 | $2.42 | — | $24.72 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1421.28 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.72 | ▼ close $10,898.12 vs 09:30 $10,994.45 (session -82.44) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.72 | ▲ 09:30 equity $10,914.29 vs yday $10,898.12 (+16.17) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 122 | $10.63 | $2.39 | $-47.44 | $1,319.19 | ▼ -47.44 after sell → book $10,911.90; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 160 | $8.49 | $2.51 | $+17.42 | $2,675.09 | ▲ +17.42 after sell → book $10,909.40; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 271 | $5.07 | $3.55 | $+28.18 | $4,045.50 | ▲ +28.18 after sell → book $10,905.84; vs 09:30 mark -3.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-11.10 | $5,317.31 | ▼ -11.10 after sell → book $10,903.82; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $3,989.15 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+3.1; leftover $1329.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 92 | $14.42 | $2.27 | — | $2,660.24 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+7.1; leftover $1329.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 511 | $2.60 | $6.59 | — | $1,325.05 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; ret5=+13.0; leftover $1329.33 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 101 | $12.98 | $2.29 | — | $11.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1329.33 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.78 | ▲ close $10,962.21 vs 09:30 $10,914.29 (session +71.62) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.78 | ▼ 09:30 equity $10,950.01 vs yday $10,962.21 (-12.20) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AVBP` | 45 | $30.53 | $2.15 | $-34.87 | $1,383.48 | ▼ -34.87 after sell → book $10,947.86; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 127 | $11.27 | $2.40 | $+14.28 | $2,812.37 | ▲ +14.28 after sell → book $10,945.46; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABX` | 144 | $9.88 | $2.46 | $+2.32 | $4,232.63 | ▲ +2.32 after sell → book $10,943.00; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 101 | $13.05 | $2.32 | $+2.46 | $5,548.36 | ▲ +2.46 after sell → book $10,940.68; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $4,164.44 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1387.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 88 | $15.66 | $2.25 | — | $2,784.11 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1387.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,431.93 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1387.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $168.72 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1387.09 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.72 | ▼ close $10,629.65 vs 09:30 $10,950.01 (session -302.61) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.72 | ▲ 09:30 equity $10,676.61 vs yday $10,629.65 (+46.96) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 56 | $23.68 | $2.18 | $-9.38 | $1,492.63 | ▼ -9.38 after sell → book $10,674.44; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,834.52 | ▲ +13.73 after sell → book $10,672.33; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 92 | $14.54 | $2.29 | $+6.48 | $4,169.91 | ▲ +6.48 after sell → book $10,670.04; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 511 | $2.58 | $6.69 | $-23.50 | $5,481.60 | ▼ -23.50 after sell → book $10,663.35; vs 09:30 mark -6.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $6,787.76 | ▼ -77.75 after sell → book $10,661.21; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 88 | $14.44 | $2.28 | $-111.89 | $8,056.20 | ▼ -111.89 after sell → book $10,658.93; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $9,421.62 | ▲ +13.24 after sell → book $10,656.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $10,654.85 | ▼ -29.98 after sell → book $10,654.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,654.85 | ▲ close $10,654.85 vs 09:30 $10,676.61 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,654.85 | ▲ 09:30 equity $10,654.85 vs yday $10,654.85 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,654.85 | ▲ close $10,654.85 vs 09:30 $10,654.85 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,654.85 | ▲ 09:30 equity $10,654.85 vs yday $10,654.85 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,654.85 | ▲ close $10,654.85 vs 09:30 $10,654.85 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,654.85 | ▲ 09:30 equity $10,654.85 vs yday $10,654.85 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,330.78 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $7,997.87 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 366 | $3.63 | $4.72 | — | $6,664.57 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $8.03 | $2.48 | — | $5,337.13 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,010.61 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.45 | $2.25 | — | $2,679.66 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,364.14 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1331.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.77 | $2.23 | — | $37.09 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1331.86 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.09 | ▼ close $10,397.70 vs 09:30 $10,654.85 (session -237.28) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.09 | ▲ 09:30 equity $10,401.92 vs yday $10,397.70 (+4.22) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $1,321.48 | ▼ -48.52 after sell → book $10,399.81; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 165 | $7.91 | $2.52 | $-24.81 | $2,624.11 | ▼ -24.81 after sell → book $10,397.29; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $3,922.37 | ▼ -28.26 after sell → book $10,395.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.00 | $2.27 | $-43.22 | $5,210.10 | ▼ -43.22 after sell → book $10,392.98; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $6,590.64 | ▲ +65.02 after sell → book $10,390.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 79 | $15.61 | $2.25 | $-96.12 | $7,821.58 | ▼ -96.12 after sell → book $10,388.69; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 517 | $2.52 | $6.67 | — | $6,512.07 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1303.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 686 | $1.90 | $8.85 | — | $5,199.82 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1303.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 272 | $4.78 | $3.51 | — | $3,896.15 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1303.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 819 | $1.59 | $10.57 | — | $2,583.38 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1303.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 115 | $11.31 | $2.33 | — | $1,280.39 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1303.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $224.95 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1303.60 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.95 | ▼ close $10,314.48 vs 09:30 $10,401.92 (session -40.28) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.95 | ▼ 09:30 equity $10,250.29 vs yday $10,314.48 (-64.19) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+31.60 | $1,580.61 | ▲ +31.60 after sell → book $10,248.20; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 366 | $3.43 | $4.79 | $-82.71 | $2,831.20 | ▼ -82.71 after sell → book $10,243.41; vs 09:30 mark -4.79 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 517 | $2.38 | $6.77 | $-85.81 | $4,054.90 | ▼ -85.81 after sell → book $10,236.65; vs 09:30 mark -6.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 686 | $2.00 | $8.97 | $+50.78 | $5,417.92 | ▲ +50.78 after sell → book $10,227.67; vs 09:30 mark -8.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 272 | $4.30 | $3.56 | $-137.63 | $6,583.96 | ▼ -137.63 after sell → book $10,224.11; vs 09:30 mark -3.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 819 | $1.63 | $10.71 | $+11.48 | $7,908.22 | ▲ +11.48 after sell → book $10,213.40; vs 09:30 mark -10.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 115 | $11.22 | $2.36 | $-15.05 | $9,196.15 | ▼ -15.05 after sell → book $10,211.03; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $10,209.01 | ▼ -42.58 after sell → book $10,209.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,209.01 | ▲ close $10,209.01 vs 09:30 $10,250.29 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,209.01 | ▲ 09:30 equity $10,209.01 vs yday $10,209.01 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,209.01 | ▲ close $10,209.01 vs 09:30 $10,209.01 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,209.01 | ▲ 09:30 equity $10,209.01 vs yday $10,209.01 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,209.01 | ▲ close $10,209.01 vs 09:30 $10,209.01 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,209.01 | ▲ 09:30 equity $10,209.01 vs yday $10,209.01 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $8,965.96 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+8.3; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $7,812.94 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 8 | $157.78 | $2.01 | — | $6,548.69 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+4.7; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 22 | $56.09 | $2.06 | — | $5,312.65 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+19.6; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 268 | $4.75 | $3.46 | — | $4,036.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 601 | $2.12 | $7.75 | — | $2,754.32 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 110 | $11.55 | $2.32 | — | $1,481.50 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1276.13 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 8 | $157.55 | $2.01 | — | $219.09 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1276.13 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.09 | ▲ close $10,194.15 vs 09:30 $10,209.01 (session +8.77) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.09 | ▼ 09:30 equity $9,872.50 vs yday $10,194.15 (-321.65) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `SANM` | 6 | $206.50 | $2.03 | $-6.08 | $1,456.06 | ▼ -6.08 after sell → book $9,870.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $2,443.97 | ▼ -165.11 after sell → book $9,868.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COHU` | 22 | $52.23 | $2.08 | $-89.05 | $3,590.95 | ▼ -89.05 after sell → book $9,866.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 268 | $4.82 | $3.51 | $+11.79 | $4,879.20 | ▲ +11.79 after sell → book $9,862.85; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 601 | $2.05 | $7.86 | $-57.69 | $6,103.39 | ▼ -57.69 after sell → book $9,854.99; vs 09:30 mark -7.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `FUBO` | 110 | $11.56 | $2.35 | $-3.57 | $7,372.64 | ▼ -3.57 after sell → book $9,852.64; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RDDT` | 8 | $160.00 | $2.03 | $+15.55 | $8,650.60 | ▲ +15.55 after sell → book $9,850.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,650.60 | ▼ close $9,823.72 vs 09:30 $9,872.50 (session -26.88) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,650.60 | ▲ 09:30 equity $9,859.56 vs yday $9,823.72 (+35.84) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `NVT` | 8 | $151.12 | $2.03 | $-57.33 | $9,857.53 | ▼ -57.33 after sell → book $9,857.53; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,857.53 | ▲ close $9,857.53 vs 09:30 $9,859.56 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,857.53 | ▲ 09:30 equity $9,857.53 vs yday $9,857.53 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 453 | $2.72 | $5.84 | — | $8,619.53 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; ret5=-0.4; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 684 | $1.80 | $8.82 | — | $7,379.50 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `INDP` | 336 | $3.66 | $4.33 | — | $6,145.41 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+96.8; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $4,981.44 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 62 | $19.75 | $2.18 | — | $3,754.76 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $2,541.54 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 37 | $33.14 | $2.10 | — | $1,313.26 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer; 🔵; ret5=-2.9; leftover $1232.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 10 | $118.18 | $2.02 | — | $129.44 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1232.19 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.44 | ▲ close $9,966.06 vs 09:30 $9,857.53 (session +138.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.44 | ▲ 09:30 equity $10,155.36 vs yday $9,966.06 (+189.30) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QTRX` | 453 | $2.94 | $5.93 | $+87.89 | $1,455.33 | ▲ +87.89 after sell → book $10,149.43; vs 09:30 mark -5.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HLP` | 684 | $2.10 | $8.95 | $+187.43 | $2,882.78 | ▲ +187.43 after sell → book $10,140.48; vs 09:30 mark -8.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 336 | $3.30 | $4.40 | $-129.69 | $3,987.18 | ▼ -129.69 after sell → book $10,136.08; vs 09:30 mark -4.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $5,113.01 | ▼ -38.14 after sell → book $10,134.03; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 62 | $20.31 | $2.20 | $+30.35 | $6,370.03 | ▲ +30.35 after sell → book $10,131.83; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 52 | $24.09 | $2.17 | $+37.29 | $7,620.55 | ▲ +37.29 after sell → book $10,129.67; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 37 | $36.76 | $2.12 | $+129.72 | $8,978.54 | ▲ +129.72 after sell → book $10,127.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $-36.86 | $10,125.50 | ▼ -36.86 after sell → book $10,125.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $8,942.61 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 123 | $10.25 | $2.36 | — | $7,679.50 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 36 | $34.93 | $2.10 | — | $6,419.92 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+1.6; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $5,172.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $3,974.23 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 527 | $2.40 | $6.80 | — | $2,702.63 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 70 | $18.04 | $2.20 | — | $1,437.98 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1265.69 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 327 | $3.86 | $4.22 | — | $171.54 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $1265.69 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $171.54 | ▼ close $10,062.03 vs 09:30 $10,155.36 (session -39.64) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.54 | ▲ 09:30 equity $10,113.07 vs yday $10,062.03 (+51.04) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $1,341.51 | ▼ -12.93 after sell → book $10,111.04; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 123 | $10.12 | $2.39 | $-20.74 | $2,583.88 | ▼ -20.74 after sell → book $10,108.65; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 36 | $34.52 | $2.12 | $-18.98 | $3,824.48 | ▼ -18.98 after sell → book $10,106.53; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 48 | $26.14 | $2.15 | $+4.83 | $5,077.05 | ▲ +4.83 after sell → book $10,104.38; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $6,351.33 | ▲ +76.32 after sell → book $10,102.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 527 | $2.29 | $6.90 | $-71.66 | $7,551.26 | ▼ -71.66 after sell → book $10,095.45; vs 09:30 mark -6.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 70 | $17.80 | $2.22 | $-20.87 | $8,795.04 | ▼ -20.87 after sell → book $10,093.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EMAT` | 327 | $3.97 | $4.28 | $+27.47 | $10,088.94 | ▲ +27.47 after sell → book $10,088.94; vs 09:30 mark -4.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $8,892.87 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ⚪; ret5=+21.3; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $7,633.74 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $6,533.64 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $5,341.61 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 319 | $3.95 | $4.12 | — | $4,077.44 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 216 | $5.83 | $2.79 | — | $2,815.38 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 43 | $29.32 | $2.12 | — | $1,552.50 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1261.12 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 415 | $3.04 | $5.35 | — | $287.62 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1261.12 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $287.62 | ▲ close $10,270.66 vs 09:30 $10,113.07 (session +204.15) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $287.62 | ▲ 09:30 equity $10,587.42 vs yday $10,270.66 (+316.76) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $1,468.84 | ▼ -14.85 after sell → book $10,585.37; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $2,726.82 | ▼ -1.16 after sell → book $10,583.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $3,876.04 | ▲ +49.12 after sell → book $10,581.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $5,033.61 | ▼ -34.46 after sell → book $10,579.27; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 319 | $3.87 | $4.18 | $-33.81 | $6,263.96 | ▼ -33.81 after sell → book $10,575.09; vs 09:30 mark -4.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 216 | $6.42 | $2.83 | $+120.74 | $7,646.77 | ▲ +120.74 after sell → book $10,572.26; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 43 | $29.43 | $2.14 | $+0.47 | $8,910.12 | ▲ +0.47 after sell → book $10,570.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 415 | $4.00 | $5.44 | $+389.69 | $10,564.68 | ▲ +389.69 after sell → book $10,564.68; vs 09:30 mark -5.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $9,299.71 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+6.5; leftover $1320.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 98 | $13.47 | $2.28 | — | $7,976.88 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1320.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 723 | $1.82 | $9.33 | — | $6,648.07 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1320.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 78 | $16.91 | $2.22 | — | $5,326.87 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1320.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 229 | $5.75 | $2.95 | — | $4,006.02 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1320.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 113 | $11.67 | $2.33 | — | $2,684.98 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+31.3; leftover $1320.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 534 | $2.47 | $6.89 | — | $1,359.11 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+73.6; leftover $1320.59 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,359.11 | ▲ close $10,850.98 vs 09:30 $10,587.42 (session +314.33) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,359.11 | ▼ 09:30 equity $10,786.77 vs yday $10,850.98 (-64.21) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `BTBT` | 723 | $1.79 | $9.46 | $-40.47 | $2,647.44 | ▼ -40.47 after sell → book $10,777.32; vs 09:30 mark -9.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 229 | $6.05 | $3.00 | $+62.74 | $4,031.03 | ▲ +62.74 after sell → book $10,774.31; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 63 | $9.11 | $2.18 | — | $3,454.92 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $575.86 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 570 | $1.01 | $7.35 | — | $2,871.87 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $575.86 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 79 | $7.23 | $2.23 | — | $2,298.47 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $575.86 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,298.47 | ▼ close $10,684.80 vs 09:30 $10,786.77 (session -77.76) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,298.47 | ▲ 09:30 equity $10,824.19 vs yday $10,684.80 (+139.39) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 98 | $12.84 | $2.31 | $-66.82 | $3,554.48 | ▼ -66.82 after sell → book $10,821.88; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 78 | $16.92 | $2.25 | $-3.69 | $4,872.00 | ▼ -3.69 after sell → book $10,819.64; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 113 | $12.80 | $2.36 | $+123.00 | $6,316.04 | ▲ +123.00 after sell → book $10,817.28; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FEAM` | 534 | $2.92 | $6.99 | $+226.42 | $7,868.33 | ▲ +226.42 after sell → book $10,810.29; vs 09:30 mark -6.99 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 63 | $8.39 | $2.20 | $-49.74 | $8,394.70 | ▼ -49.74 after sell → book $10,808.09; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 570 | $0.95 | $7.23 | $-48.78 | $8,928.97 | ▼ -48.78 after sell → book $10,800.86; vs 09:30 mark -7.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 79 | $6.83 | $2.25 | $-36.08 | $9,466.29 | ▼ -36.08 after sell → book $10,798.61; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 48 | $27.79 | $2.13 | — | $8,130.23 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1352.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 170 | $7.95 | $2.50 | — | $6,776.23 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1352.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 66 | $20.25 | $2.19 | — | $5,437.55 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1352.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 344 | $3.93 | $4.44 | — | $4,081.19 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1352.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 53 | $25.40 | $2.15 | — | $2,732.84 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1352.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 11 | $116.00 | $2.02 | — | $1,454.82 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1352.33 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 32 | $41.76 | $2.09 | — | $116.41 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1352.33 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.41 | ▼ close $10,442.48 vs 09:30 $10,824.19 (session -338.61) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.41 | ▼ 09:30 equity $10,178.35 vs yday $10,442.48 (-264.13) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,425.98 | ▲ +44.59 after sell → book $10,176.32; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 48 | $26.22 | $2.15 | $-79.65 | $2,682.38 | ▼ -79.65 after sell → book $10,174.16; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 170 | $7.38 | $2.54 | $-101.94 | $3,934.44 | ▼ -101.94 after sell → book $10,171.62; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 66 | $19.40 | $2.21 | $-60.50 | $5,212.63 | ▼ -60.50 after sell → book $10,169.41; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 344 | $3.77 | $4.51 | $-63.98 | $6,505.01 | ▼ -63.98 after sell → book $10,164.91; vs 09:30 mark -4.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 53 | $23.99 | $2.17 | $-79.05 | $7,774.31 | ▼ -79.05 after sell → book $10,162.74; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 11 | $112.33 | $2.04 | $-44.44 | $9,007.90 | ▼ -44.44 after sell → book $10,160.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 32 | $36.02 | $2.11 | $-187.71 | $10,158.59 | ▼ -187.71 after sell → book $10,158.59; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.59 | ▲ close $10,158.59 vs 09:30 $10,178.35 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,582.60 | ▲ 09:30 equity $8,582.60 vs yday $8,582.60 (+0.00) | 09:30 open · cash $8,582.60 · no holdings · equity $8,582.60 vs prior close $8,582.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 52 | $20.61 | $2.15 | — | $7,508.73 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ret5=+9.1; leftover $1072.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 27 | $38.51 | $2.07 | — | $6,466.89 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; ret5=+4.7; leftover $1072.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 140 | $7.65 | $2.41 | — | $5,393.48 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1072.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 40 | $26.27 | $2.11 | — | $4,340.57 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1072.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $3,333.43 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1072.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 487 | $2.20 | $6.28 | — | $2,255.74 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $1072.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 178 | $6.00 | $2.52 | — | $1,185.22 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $1072.83 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 59 | $17.91 | $2.17 | — | $126.36 | — | union ∩ macd_up, no 🚨; gate macd_up=True; list probable; 🔵; ret5=+3.7; leftover $1072.83 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.36 | ▲ close $8,573.57 vs 09:30 $8,582.60 (session +12.71) | 16:00 close · cash $126.36 · equity $8,573.57 vs 09:30 $8,582.60 (-9.03; session marks +12.71) · 8 name(s) marked open→close (per-name table). OMER×52 09:30 $20.61 → close $20.08 -27.56; BLFS×27 09:30 $38.51 → close $38.49 -0.54; MRVI×140 09:30 $7.65 → close $7.60 -7.00; WRBY×40 09:30 $26.27 → close $26.71 +17.60; TXG×12 09:30 $83.76 → close $85.71 +23.40; HLP×487 09:30 $2.20 → close $2.21 +4.87; SATL×178 09:30 $6.00 → close $6.17 +30.26; PL×59 09:30 $17.91 → close $17.43 -28.32 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CMRC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INDP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1320.59 < 1 share @ 1826.00 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
