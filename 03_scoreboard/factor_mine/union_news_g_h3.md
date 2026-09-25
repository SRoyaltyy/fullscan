# Factor mine action — `union_news_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **-19.96%** ($8,004) · signal-only (no cash/fees) was +177.31%. Starts YES **1/30**. Fills 160 · skips 234 · realized $-2132.89.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
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
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,650.90.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 6 | $46.18 | $2.01 | — | $1,281.40 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $993.87 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $789.17 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 3 | $92.99 | $2.00 | — | $508.20 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $212.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $312.10 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.20 | ▼ close $10,059.83 vs 09:30 $10,211.68 (session -141.85) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▼ 09:30 equity $10,014.18 vs yday $10,059.83 (-45.65) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.20 | ▼ close $9,819.43 vs 09:30 $10,014.18 (session -194.75) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▲ 09:30 equity $9,839.65 vs yday $9,819.43 (+20.22) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,173.18 | ▼ -120.51 after sell → book $9,837.64; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,297.06 | ▼ -53.33 after sell → book $9,835.60; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,457.02 | ▼ -42.06 after sell → book $9,833.56; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,688.36 | ▼ -3.75 after sell → book $9,831.36; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $5,882.99 | ▼ -54.24 after sell → book $9,829.07; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $7,093.29 | ▼ -30.89 after sell → book $9,826.77; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,093.29 | ▼ close $9,745.25 vs 09:30 $9,839.65 (session -81.52) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,093.29 | ▲ 09:30 equity $9,751.60 vs yday $9,745.25 (+6.35) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $8,414.79 | ▲ +67.86 after sell → book $9,747.80; vs 09:30 mark -3.80 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 6 | $49.02 | $2.03 | $+13.00 | $8,706.89 | ▲ +13.00 after sell → book $9,745.78; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,007.77 | ▲ +13.35 after sell → book $9,743.76; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,219.27 | ▲ +6.80 after sell → book $9,741.75; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CELC` | 3 | $92.90 | $2.02 | $-4.29 | $9,495.95 | ▼ -4.29 after sell → book $9,739.73; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 6 | $40.63 | $2.03 | $-54.26 | $9,737.70 | ▼ -54.26 after sell → book $9,737.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,552.54 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,349.41 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1721 | $0.71 | $17.33 | — | $6,115.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 184 | $6.61 | $2.54 | — | $4,897.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 76 | $16.00 | $2.22 | — | $3,679.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $2,481.47 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $1,304.82 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1217.21 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $94.23 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1217.21 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.23 | ▼ close $9,522.09 vs 09:30 $9,751.60 (session -183.23) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.23 | ▲ 09:30 equity $9,762.08 vs yday $9,522.09 (+239.99) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $81.74 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $13.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $69.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $13.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 1 | $11.10 | $0.11 | — | $58.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $13.46 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $45.61 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $13.46 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.61 | ▲ close $9,794.99 vs 09:30 $9,762.08 (session +33.42) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.61 | ▲ 09:30 equity $9,825.15 vs yday $9,794.99 (+30.16) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.61 | ▼ close $9,809.86 vs 09:30 $9,825.15 (session -15.29) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.61 | ▼ 09:30 equity $9,762.67 vs yday $9,809.86 (-47.19) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,289.75 | ▲ +58.97 after sell → book $9,760.62; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $2,435.71 | ▼ -57.17 after sell → book $9,758.58; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1721 | $0.66 | $16.87 | $-109.92 | $3,559.87 | ▼ -109.92 after sell → book $9,741.72; vs 09:30 mark -16.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 184 | $6.75 | $2.58 | $+21.56 | $4,799.28 | ▲ +21.56 after sell → book $9,739.13; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 76 | $19.04 | $2.24 | $+226.58 | $6,244.08 | ▲ +226.58 after sell → book $9,736.89; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $26.04 | $2.15 | $-28.12 | $7,413.74 | ▼ -28.12 after sell → book $9,734.75; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $8,570.27 | ▼ -20.12 after sell → book $9,732.68; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $9,685.43 | ▼ -95.42 after sell → book $9,730.58; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 146 | $9.42 | $2.43 | — | $8,307.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1383.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 39 | $35.05 | $2.11 | — | $6,938.63 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1383.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.11 | $2.16 | — | $5,562.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; leftover $1383.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 47 | $28.86 | $2.13 | — | $4,203.65 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; leftover $1383.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 158 | $8.72 | $2.46 | — | $2,823.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; leftover $1383.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $1,517.68 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1383.63 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $204.43 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; leftover $1383.63 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $204.43 | ▲ close $10,164.79 vs 09:30 $9,762.67 (session +449.56) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $204.43 | ▼ 09:30 equity $9,991.96 vs yday $10,164.79 (-172.83) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 5 | $2.41 | $0.16 | $-0.59 | $216.32 | ▼ -0.59 after sell → book $9,991.80; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $227.75 | ▼ -0.40 after sell → book $9,991.67; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTDR` | 1 | $11.05 | $0.13 | $-0.29 | $238.66 | ▼ -0.29 after sell → book $9,991.53; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $250.31 | ▼ -1.45 after sell → book $9,991.38; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 3 | $11.12 | $0.34 | — | $216.61 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $41.72 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 5 | $8.29 | $0.43 | — | $174.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $41.72 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 2 | $17.41 | $0.35 | — | $139.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; leftover $41.72 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 3 | $11.22 | $0.35 | — | $105.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; leftover $41.72 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.55 | ▼ close $9,868.41 vs 09:30 $9,991.96 (session -121.50) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.55 | ▼ 09:30 equity $9,824.60 vs yday $9,868.41 (-43.81) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.55 | ▼ close $9,726.36 vs 09:30 $9,824.60 (session -98.24) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.55 | ▼ 09:30 equity $9,695.78 vs yday $9,726.36 (-30.58) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 146 | $9.30 | $2.46 | $-22.41 | $1,460.89 | ▼ -22.41 after sell → book $9,693.32; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 39 | $34.50 | $2.13 | $-25.68 | $2,804.26 | ▼ -25.68 after sell → book $9,691.19; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 57 | $23.40 | $2.18 | $-44.81 | $4,135.88 | ▼ -44.81 after sell → book $9,689.01; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EOLS` | 158 | $8.84 | $2.50 | $+13.99 | $5,530.10 | ▲ +13.99 after sell → book $9,686.51; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $119.19 | $2.04 | $+3.30 | $6,839.14 | ▲ +3.30 after sell → book $9,684.46; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.57 | $2.06 | $+20.38 | $8,172.77 | ▲ +20.38 after sell → book $9,682.40; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 32 | $41.74 | $2.09 | — | $6,835.01 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+2.4; leftover $1362.13 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $5,483.99 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1362.13 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 158 | $8.61 | $2.46 | — | $4,121.15 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; leftover $1362.13 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $2,843.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1362.13 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 70 | $19.25 | $2.20 | — | $1,493.59 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; leftover $1362.13 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 72 | $18.75 | $2.21 | — | $141.39 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; leftover $1362.13 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.39 | ▼ close $9,407.27 vs 09:30 $9,695.78 (session -262.05) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.39 | ▼ 09:30 equity $9,404.54 vs yday $9,407.27 (-2.73) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 47 | $28.06 | $2.15 | $-41.88 | $1,458.05 | ▼ -41.88 after sell → book $9,402.38; vs 09:30 mark -2.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 3 | $10.82 | $0.35 | $-1.60 | $1,490.16 | ▼ -1.60 after sell → book $9,402.03; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 5 | $9.50 | $0.51 | $+5.11 | $1,537.15 | ▲ +5.11 after sell → book $9,401.52; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FWRD` | 2 | $17.03 | $0.37 | $-1.48 | $1,570.84 | ▼ -1.48 after sell → book $9,401.15; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 3 | $11.80 | $0.38 | $+1.01 | $1,605.86 | ▲ +1.01 after sell → book $9,400.77; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,605.86 | ▼ close $9,376.56 vs 09:30 $9,404.54 (session -24.21) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,605.86 | ▼ 09:30 equity $9,239.84 vs yday $9,376.56 (-136.72) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,605.86 | ▼ close $9,148.54 vs 09:30 $9,239.84 (session -91.30) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,605.86 | ▼ 09:30 equity $9,138.20 vs yday $9,148.54 (-10.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 32 | $42.10 | $2.11 | $+7.33 | $2,950.95 | ▲ +7.33 after sell → book $9,136.09; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 41 | $32.42 | $2.13 | $-23.93 | $4,278.04 | ▼ -23.93 after sell → book $9,133.96; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 158 | $7.25 | $2.50 | $-219.84 | $5,421.04 | ▼ -219.84 after sell → book $9,131.46; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $6,616.00 | ▼ -82.89 after sell → book $9,129.42; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 70 | $16.97 | $2.22 | $-164.02 | $7,801.68 | ▼ -164.02 after sell → book $9,127.20; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 72 | $18.41 | $2.23 | $-28.91 | $9,124.97 | ▼ -28.91 after sell → book $9,124.97; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,124.97 | ▲ close $9,124.97 vs 09:30 $9,138.20 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,124.97 | ▲ 09:30 equity $9,124.97 vs yday $9,124.97 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 47 | $23.88 | $2.13 | — | $8,000.48 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 34 | $32.88 | $2.09 | — | $6,880.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 150 | $7.59 | $2.44 | — | $5,739.53 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $5,034.29 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 71 | $15.87 | $2.20 | — | $3,905.31 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $2,848.09 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $1,782.63 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; leftover $1140.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 23 | $47.60 | $2.06 | — | $685.77 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1140.62 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $685.77 | ▲ close $9,257.44 vs 09:30 $9,124.97 (session +149.38) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $685.77 | ▼ 09:30 equity $9,218.16 vs yday $9,257.44 (-39.28) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 70 | $1.94 | $1.57 | — | $548.40 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $137.15 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 1 | $75.65 | $0.76 | — | $471.99 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $137.15 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $471.99 | ▼ close $9,090.37 vs 09:30 $9,218.16 (session -125.46) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $471.99 | ▲ 09:30 equity $9,157.55 vs yday $9,090.37 (+67.18) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $471.99 | ▼ close $9,138.58 vs 09:30 $9,157.55 (session -18.97) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $471.99 | ▲ 09:30 equity $9,143.89 vs yday $9,138.58 (+5.31) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 47 | $23.22 | $2.15 | $-35.30 | $1,561.18 | ▼ -35.30 after sell → book $9,141.74; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNXC` | 34 | $28.13 | $2.11 | $-165.70 | $2,515.49 | ▼ -165.70 after sell → book $9,139.63; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OPTX` | 150 | $7.72 | $2.47 | $+14.59 | $3,671.01 | ▲ +14.59 after sell → book $9,137.15; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $4,350.32 | ▼ -25.94 after sell → book $9,135.14; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 71 | $15.96 | $2.22 | $+1.96 | $5,481.25 | ▲ +1.96 after sell → book $9,132.91; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $6,577.92 | ▲ +39.45 after sell → book $9,130.89; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 3 | $341.90 | $2.02 | $-41.79 | $7,601.61 | ▼ -41.79 after sell → book $9,128.88; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 23 | $56.94 | $2.08 | $+210.68 | $8,909.15 | ▲ +210.68 after sell → book $9,126.80; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,909.15 | ▼ close $9,122.77 vs 09:30 $9,143.89 (session -4.03) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,909.15 | ▼ 09:30 equity $9,122.05 vs yday $9,122.77 (-0.72) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 70 | $1.97 | $1.61 | $-1.08 | $9,045.44 | ▼ -1.08 after sell → book $9,120.44; vs 09:30 mark -1.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 1 | $75.00 | $0.77 | $-2.18 | $9,119.66 | ▼ -2.18 after sell → book $9,119.66; vs 09:30 mark -0.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,119.66 | ▲ close $9,119.66 vs 09:30 $9,122.05 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,119.66 | ▲ 09:30 equity $9,119.66 vs yday $9,119.66 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $7,637.78 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1519.94 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 745 | $2.04 | $9.61 | — | $6,108.36 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1519.94 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 716 | $2.12 | $9.24 | — | $4,581.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1519.94 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 101 | $15.01 | $2.29 | — | $3,062.91 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; leftover $1519.94 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $1,607.88 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; leftover $1519.94 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 11 | $135.71 | $2.02 | — | $113.04 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; leftover $1519.94 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.04 | ▼ close $8,936.25 vs 09:30 $9,119.66 (session -156.22) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.04 | ▼ 09:30 equity $8,867.40 vs yday $8,936.25 (-68.85) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.04 | ▲ close $8,908.69 vs 09:30 $8,867.40 (session +41.29) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.04 | ▼ 09:30 equity $8,865.04 vs yday $8,908.69 (-43.65) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.04 | ▼ close $8,781.87 vs 09:30 $8,865.04 (session -83.17) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.04 | ▼ 09:30 equity $8,568.42 vs yday $8,781.87 (-213.45) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 9 | $140.03 | $2.04 | $-223.65 | $1,371.28 | ▼ -223.65 after sell → book $8,566.39; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 745 | $1.89 | $9.75 | $-131.11 | $2,769.58 | ▼ -131.11 after sell → book $8,556.64; vs 09:30 mark -9.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 716 | $1.84 | $9.37 | $-219.08 | $4,077.66 | ▼ -219.08 after sell → book $8,547.28; vs 09:30 mark -9.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 6 | $253.34 | $2.03 | $+62.98 | $5,595.67 | ▲ +62.98 after sell → book $8,545.25; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 11 | $125.55 | $2.04 | $-115.83 | $6,974.67 | ▼ -115.83 after sell → book $8,543.20; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 66 | $26.27 | $2.19 | — | $5,238.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; leftover $1743.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 250 | $6.95 | $3.23 | — | $3,497.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; leftover $1743.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 43 | $39.99 | $2.12 | — | $1,776.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; leftover $1743.67 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 9 | $189.17 | $2.02 | — | $71.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; leftover $1743.67 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.70 | ▼ close $8,467.69 vs 09:30 $8,568.42 (session -65.96) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.70 | ▲ 09:30 equity $8,564.33 vs yday $8,467.69 (+96.64) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 6 | $1.77 | $0.12 | — | $60.96 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; leftover $11.95 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.96 | ▼ close $8,506.32 vs 09:30 $8,564.33 (session -57.89) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.96 | ▲ 09:30 equity $8,540.62 vs yday $8,506.32 (+34.30) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 101 | $15.87 | $2.32 | $+82.24 | $1,661.51 | ▲ +82.24 after sell → book $8,538.30; vs 09:30 mark -2.32 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 23 | $14.07 | $2.06 | — | $1,335.84 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $332.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 22 | $14.79 | $2.06 | — | $1,008.40 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $332.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 44 | $7.54 | $2.12 | — | $674.74 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; leftover $332.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 14 | $22.90 | $2.03 | — | $352.11 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $332.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 15 | $20.91 | $2.04 | — | $36.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $332.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.42 | ▼ close $8,008.74 vs 09:30 $8,540.62 (session -519.25) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.42 | ▲ 09:30 equity $8,036.61 vs yday $8,008.74 (+27.87) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 66 | $25.94 | $2.21 | $-26.18 | $1,746.25 | ▼ -26.18 after sell → book $8,034.40; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 43 | $35.91 | $2.14 | $-179.70 | $3,288.24 | ▼ -179.70 after sell → book $8,032.26; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 9 | $180.61 | $2.04 | $-81.10 | $4,911.69 | ▼ -81.10 after sell → book $8,030.22; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 37 | $25.95 | $2.10 | — | $3,949.44 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $982.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 456 | $2.15 | $5.88 | — | $2,963.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $982.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 70 | $13.94 | $2.20 | — | $1,985.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $982.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $1,031.65 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; leftover $982.34 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 4 | $230.25 | $2.00 | — | $108.65 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; leftover $982.34 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.65 | ▼ close $7,861.07 vs 09:30 $8,036.61 (session -154.96) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.65 | ▲ 09:30 equity $7,865.69 vs yday $7,861.07 (+4.62) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 250 | $5.99 | $3.28 | $-246.50 | $1,602.87 | ▼ -246.50 after sell → book $7,862.41; vs 09:30 mark -3.28 | dropped from list after 4 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 264 | $1.01 | $3.41 | — | $1,332.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $267.14 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 1 | $168.50 | $1.69 | — | $1,162.64 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; leftover $267.14 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 62 | $4.30 | $2.18 | — | $893.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $267.14 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $893.86 | ▲ close $7,903.21 vs 09:30 $7,865.69 (session +48.07) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $893.86 | ▲ 09:30 equity $8,084.65 vs yday $7,903.21 (+181.44) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BAK` | 6 | $1.68 | $0.14 | $-0.80 | $903.80 | ▼ -0.80 after sell → book $8,084.52; vs 09:30 mark -0.13 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 23 | $14.84 | $2.08 | $+13.57 | $1,243.04 | ▲ +13.57 after sell → book $8,082.44; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 22 | $15.40 | $2.08 | $+9.29 | $1,579.77 | ▲ +9.29 after sell → book $8,080.36; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FLNC` | 44 | $7.52 | $2.14 | $-4.92 | $1,908.50 | ▼ -4.92 after sell → book $8,078.22; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GME` | 14 | $23.94 | $2.05 | $+10.48 | $2,241.61 | ▲ +10.48 after sell → book $8,076.17; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 15 | $21.15 | $2.06 | $-0.49 | $2,556.81 | ▼ -0.49 after sell → book $8,074.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 53 | $7.95 | $2.15 | — | $2,133.31 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $426.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 27 | $15.72 | $2.07 | — | $1,706.80 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $426.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 349 | $1.22 | $4.50 | — | $1,276.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; leftover $426.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 327 | $1.30 | $4.22 | — | $847.20 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $426.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 10 | $40.00 | $2.02 | — | $445.18 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; leftover $426.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 2 | $196.78 | $2.00 | — | $49.62 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $426.13 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.62 | ▼ close $7,874.10 vs 09:30 $8,084.65 (session -183.05) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.62 | ▼ 09:30 equity $7,729.70 vs yday $7,874.10 (-144.40) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 37 | $25.00 | $2.12 | $-39.56 | $972.31 | ▼ -39.56 after sell → book $7,727.57; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 456 | $1.88 | $5.97 | $-134.97 | $1,823.63 | ▼ -134.97 after sell → book $7,721.61; vs 09:30 mark -5.96 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 70 | $13.07 | $2.22 | $-65.32 | $2,736.30 | ▼ -65.32 after sell → book $7,719.38; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 5 | $164.04 | $2.02 | $-135.33 | $3,554.48 | ▼ -135.33 after sell → book $7,717.36; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+173.42 | $4,650.90 | ▲ +173.42 after sell → book $7,715.34; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,650.90 | ▲ close $7,782.47 vs 09:30 $7,729.70 (session +67.14) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,802.97 | ▲ 09:30 equity $8,049.38 vs yday $8,026.72 (+22.66) | 09:30 open · cash $5,802.97 (unchanged overnight, no fees) · equity $8,049.38 vs prior close $8,026.72 (+22.66) · 8 name(s) re-marked at the open (per-name table). CMPX×33 yday $1.13 → 09:30 $1.13 +0.00; DGXX×118 yday $4.53 → 09:30 $4.78 +29.50; GRAL×4 yday $125.21 → 09:30 $123.50 -6.84; IVVD×502 yday $0.91 → 09:30 $0.91 +0.00; MRNA×3 yday $194.82 → 09:30 $194.82 +0.00; PGEN×5 yday $7.70 → 09:30 $7.70 +0.00; SGRY×2 yday $14.20 → 09:30 $14.20 +0.00; VERI×31 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `DGXX` | 118 | $4.78 | $2.37 | $+54.28 | $6,364.64 | ▲ +54.28 after sell → book $8,047.01; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 4 | $123.50 | $2.02 | $+62.98 | $6,856.61 | ▲ +62.98 after sell → book $8,044.99; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 355 | $3.86 | $4.58 | — | $5,481.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1371.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 84 | $16.21 | $2.24 | — | $4,117.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1371.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 5 | $272.16 | $2.00 | — | $2,755.05 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1371.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 18 | $74.15 | $2.04 | — | $1,418.30 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; leftover $1371.32 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $529.31 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; leftover $1371.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $529.31 | ▼ close $8,004.09 vs 09:30 $8,049.38 (session -28.04) | 16:00 close · cash $529.31 · equity $8,004.09 vs 09:30 $8,049.38 (-45.29; session marks -28.04) · 11 name(s) marked open→close (per-name table). CMPX×33 09:30 $1.14 → close $1.14 -0.00; IVVD×502 09:30 $0.91 → close $0.91 -0.00; MRNA×3 09:30 $194.82 → close $194.82 +0.00; PGEN×5 09:30 $7.70 → close $7.70 -0.00; SGRY×2 09:30 $14.20 → close $14.20 -0.00; VERI×31 09:30 $1.33 → close $1.33 +0.00; ZSQR×355 09:30 $3.86 → close $3.78 -28.40; SECZ×84 09:30 $16.21 → close $15.96 -21.00; ILMN×5 09:30 $272.16 → close $270.00 -10.80; RKLB×18 09:30 $74.15 → close $73.95 -3.60; COST×1 09:30 $887.00 → close $922.76 +35.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 13.46 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 13.46 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 13.46 < 1 share @ 623.26 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EOLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 41.72 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 41.72 < 1 share @ 118.50 |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EOLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 26.39 < 1 share @ 41.44 |
| 2026-08-27 | `AXTI` | cash | leftover split 26.39 < 1 share @ 70.30 |
| 2026-08-27 | `SRRK` | cash | leftover split 26.39 < 1 share @ 60.00 |
| 2026-08-27 | `CM` | cash | leftover split 26.39 < 1 share @ 118.77 |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 137.15 < 1 share @ 263.36 |
| 2026-09-04 | `MSTR` | cash | leftover split 137.15 < 1 share @ 137.35 |
| 2026-09-04 | `BE` | cash | leftover split 137.15 < 1 share @ 236.82 |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 11.95 < 1 share @ 170.85 |
| 2026-09-17 | `LITE` | cash | leftover split 11.95 < 1 share @ 934.88 |
| 2026-09-17 | `TNDM` | cash | leftover split 11.95 < 1 share @ 17.72 |
| 2026-09-17 | `JBHT` | cash | leftover split 11.95 < 1 share @ 238.60 |
| 2026-09-17 | `GME` | cash | leftover split 11.95 < 1 share @ 22.12 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BAK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DGXX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `IVVD` | 264 | 2026-09-22 @ $1.01 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $267.14 |
| `MRNA` | 1 | 2026-09-22 @ $168.50 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; leftover $267.14 |
| `DGXX` | 62 | 2026-09-22 @ $4.30 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $267.14 |
| `PGEN` | 53 | 2026-09-23 @ $7.95 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $426.13 |
| `SGRY` | 27 | 2026-09-23 @ $15.72 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $426.13 |
| `CMPX` | 349 | 2026-09-23 @ $1.22 | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; leftover $426.13 |
| `VERI` | 327 | 2026-09-23 @ $1.30 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $426.13 |
| `BLSH` | 10 | 2026-09-23 @ $40.00 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; leftover $426.13 |
| `CTAS` | 2 | 2026-09-23 @ $196.78 | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $426.13 |
