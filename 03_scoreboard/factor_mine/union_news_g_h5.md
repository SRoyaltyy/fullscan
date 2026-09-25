# Factor mine action — `union_news_g_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g hold 5, no 🚨

Cash book **-15.93%** ($8,407) · signal-only (no cash/fees) was +99.58%. Starts YES **1/30**. Fills 131 · skips 344 · realized $-1589.16.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $78.53.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 6 | $46.18 | $2.01 | — | $1,281.40 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $993.87 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $789.17 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 3 | $92.99 | $2.00 | — | $508.20 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $312.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $212.20 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $312.10 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.20 | ▼ close $10,059.83 vs 09:30 $10,211.68 (session -141.85) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▼ 09:30 equity $10,014.18 vs yday $10,059.83 (-45.65) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.20 | ▼ close $9,819.43 vs 09:30 $10,014.18 (session -194.75) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▲ 09:30 equity $9,839.65 vs yday $9,819.43 (+20.22) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.20 | ▼ close $9,805.29 vs 09:30 $9,839.65 (session -34.36) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▼ 09:30 equity $9,773.30 vs yday $9,805.29 (-31.99) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 37 | $0.71 | $0.37 | — | $185.66 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $26.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 4 | $6.61 | $0.28 | — | $158.97 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $26.52 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 1 | $16.00 | $0.16 | — | $142.80 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $26.52 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.80 | ▼ close $9,583.34 vs 09:30 $9,773.30 (session -189.14) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.80 | ▲ 09:30 equity $9,653.60 vs yday $9,583.34 (+70.26) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `TLN` | 3 | $318.52 | $2.02 | $-127.95 | $1,096.35 | ▼ -127.95 after sell → book $9,651.58; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 8 | $139.99 | $2.03 | $-59.33 | $2,214.23 | ▼ -59.33 after sell → book $9,649.55; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 10 | $116.58 | $2.04 | $-38.26 | $3,377.99 | ▼ -38.26 after sell → book $9,647.51; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $4,658.89 | ▲ +27.26 after sell → book $9,643.71; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,889.60 | ▼ -4.38 after sell → book $9,641.51; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MH` | 92 | $12.87 | $2.29 | $-67.12 | $7,071.35 | ▼ -67.12 after sell → book $9,639.22; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HLIT` | 94 | $12.48 | $2.30 | $-70.37 | $8,242.17 | ▼ -70.37 after sell → book $9,636.92; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 8 | $119.43 | $2.01 | — | $7,284.72 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 417 | $2.47 | $5.38 | — | $6,249.35 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 17 | $59.72 | $2.04 | — | $5,232.07 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 8 | $115.18 | $2.01 | — | $4,308.62 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $3,683.36 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 88 | $11.70 | $2.25 | — | $2,651.51 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 92 | $11.10 | $2.27 | — | $1,628.50 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1030.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 317 | $3.24 | $4.09 | — | $597.33 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1030.27 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $597.33 | ▼ close $9,609.45 vs 09:30 $9,653.60 (session -5.42) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $597.33 | ▼ 09:30 equity $9,552.17 vs yday $9,609.45 (-57.28) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 6 | $48.89 | $2.03 | $+12.22 | $888.64 | ▲ +12.22 after sell → book $9,550.14; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 2 | $152.07 | $2.02 | $+14.59 | $1,190.77 | ▲ +14.59 after sell → book $9,548.12; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $210.00 | $2.01 | $+3.29 | $1,398.76 | ▲ +3.29 after sell → book $9,546.11; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CELC` | 3 | $93.47 | $2.02 | $-2.58 | $1,677.15 | ▼ -2.58 after sell → book $9,544.09; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `OUST` | 6 | $37.37 | $2.03 | $-73.85 | $1,899.31 | ▼ -73.85 after sell → book $9,542.06; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,899.31 | ▼ close $9,349.85 vs 09:30 $9,552.17 (session -192.21) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,899.31 | ▲ 09:30 equity $9,359.73 vs yday $9,349.85 (+9.88) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 33 | $9.42 | $2.09 | — | $1,586.36 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $316.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 9 | $35.05 | $2.02 | — | $1,268.89 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $316.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 13 | $24.11 | $2.03 | — | $953.43 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; ret5=+891.7; leftover $316.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 10 | $28.86 | $2.02 | — | $662.81 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; leftover $316.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 36 | $8.72 | $2.10 | — | $346.80 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; leftover $316.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 4 | $77.13 | $2.00 | — | $36.27 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; leftover $316.55 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.27 | ▲ close $9,766.35 vs 09:30 $9,359.73 (session +418.88) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.27 | ▼ 09:30 equity $9,607.93 vs yday $9,766.35 (-158.42) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.27 | ▼ close $9,451.81 vs 09:30 $9,607.93 (session -156.13) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.27 | ▲ 09:30 equity $9,537.98 vs yday $9,451.81 (+86.17) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `HUMA` | 37 | $0.70 | $0.39 | $-1.02 | $61.78 | ▼ -1.02 after sell → book $9,537.59; vs 09:30 mark -0.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTGO` | 4 | $7.14 | $0.32 | $+1.55 | $90.03 | ▲ +1.55 after sell → book $9,537.28; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 1 | $22.45 | $0.25 | $+6.04 | $112.23 | ▲ +6.04 after sell → book $9,537.03; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `FLNC` | 1 | $11.52 | $0.12 | — | $100.59 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-8.2; leftover $14.03 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 1 | $9.19 | $0.09 | — | $91.31 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $14.03 | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 1 | $11.38 | $0.12 | — | $79.81 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+13.3; leftover $14.03 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.81 | ▲ close $9,580.63 vs 09:30 $9,537.98 (session +43.93) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.81 | ▼ 09:30 equity $9,487.17 vs yday $9,580.63 (-93.46) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 8 | $119.19 | $2.03 | $-5.97 | $1,031.29 | ▼ -5.97 after sell → book $9,485.13; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 417 | $2.35 | $5.46 | $-60.88 | $2,005.79 | ▼ -60.88 after sell → book $9,479.68; vs 09:30 mark -5.45 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 17 | $58.22 | $2.06 | $-29.60 | $2,993.47 | ▼ -29.60 after sell → book $9,477.62; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `FUTU` | 8 | $124.27 | $2.03 | $+68.67 | $3,985.59 | ▲ +68.67 after sell → book $9,475.58; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 1 | $626.50 | $2.01 | $-0.77 | $4,610.08 | ▼ -0.77 after sell → book $9,473.57; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MARA` | 88 | $11.53 | $2.28 | $-19.49 | $5,622.44 | ▼ -19.49 after sell → book $9,471.29; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BTDR` | 92 | $11.15 | $2.29 | $+0.50 | $6,645.95 | ▲ +0.50 after sell → book $9,469.00; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `HIVE` | 317 | $2.99 | $4.15 | $-87.49 | $7,589.63 | ▼ -87.49 after sell → book $9,464.85; vs 09:30 mark -4.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 30 | $41.74 | $2.08 | — | $6,335.35 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; ret5=+2.4; leftover $1264.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $5,083.04 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1264.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 146 | $8.61 | $2.43 | — | $3,823.55 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; leftover $1264.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $2,687.46 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1264.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 65 | $19.25 | $2.19 | — | $1,434.03 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; leftover $1264.94 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 67 | $18.75 | $2.19 | — | $175.58 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; leftover $1264.94 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.58 | ▼ close $9,173.91 vs 09:30 $9,487.17 (session -277.93) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.58 | ▼ 09:30 equity $9,171.20 vs yday $9,173.91 (-2.71) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.58 | ▼ close $9,139.98 vs 09:30 $9,171.20 (session -31.22) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.58 | ▼ 09:30 equity $8,988.75 vs yday $9,139.98 (-151.23) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RUM` | 33 | $8.69 | $2.11 | $-28.29 | $460.25 | ▼ -28.29 after sell → book $8,986.64; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `EZPW` | 9 | $31.97 | $2.04 | $-31.77 | $745.94 | ▼ -31.77 after sell → book $8,984.60; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `REAX` | 13 | $18.70 | $2.05 | $-74.41 | $986.99 | ▼ -74.41 after sell → book $8,982.55; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 10 | $29.32 | $2.04 | $+0.54 | $1,278.15 | ▲ +0.54 after sell → book $8,980.51; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `EOLS` | 36 | $9.04 | $2.12 | $+7.30 | $1,601.47 | ▲ +7.30 after sell → book $8,978.39; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `FCX` | 4 | $73.55 | $2.02 | $-18.34 | $1,893.65 | ▼ -18.34 after sell → book $8,976.37; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,893.65 | ▼ close $8,889.75 vs 09:30 $8,988.75 (session -86.62) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,893.65 | ▼ 09:30 equity $8,879.86 vs yday $8,889.75 (-9.89) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,893.65 | ▲ close $8,967.17 vs 09:30 $8,879.86 (session +87.31) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,893.65 | ▼ 09:30 equity $8,961.48 vs yday $8,967.17 (-5.69) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `FLNC` | 1 | $10.01 | $0.12 | $-1.75 | $1,903.54 | ▼ -1.75 after sell → book $8,961.36; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CAPR` | 1 | $9.83 | $0.12 | $+0.42 | $1,913.25 | ▲ +0.42 after sell → book $8,961.24; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `TRLV` | 1 | $11.89 | $0.14 | $+0.25 | $1,925.00 | ▲ +0.25 after sell → book $8,961.10; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 11 | $23.88 | $2.02 | — | $1,660.29 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $275.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 8 | $32.88 | $2.01 | — | $1,395.24 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; leftover $275.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 17 | $15.87 | $2.04 | — | $1,123.41 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $275.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 5 | $47.60 | $2.00 | — | $883.40 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $275.00 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $883.40 | ▲ close $9,067.49 vs 09:30 $8,961.48 (session +114.48) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $883.40 | ▼ 09:30 equity $9,048.17 vs yday $9,067.49 (-19.32) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RRC` | 30 | $42.36 | $2.10 | $+14.42 | $2,152.10 | ▲ +14.42 after sell → book $9,046.07; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 38 | $33.86 | $2.12 | $+32.25 | $3,436.66 | ▲ +32.25 after sell → book $9,043.95; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 146 | $7.79 | $2.46 | $-124.61 | $4,571.54 | ▼ -124.61 after sell → book $9,041.49; vs 09:30 mark -2.46 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 8 | $138.71 | $2.03 | $-28.45 | $5,679.18 | ▼ -28.45 after sell → book $9,039.45; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ERAS` | 65 | $16.04 | $2.21 | $-213.04 | $6,719.58 | ▼ -213.04 after sell → book $9,037.25; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BBWI` | 67 | $18.62 | $2.21 | $-13.11 | $7,964.90 | ▼ -13.11 after sell → book $9,035.03; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $6,382.74 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1592.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 821 | $1.94 | $10.59 | — | $4,779.40 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $1592.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 11 | $137.35 | $2.02 | — | $3,266.53 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; leftover $1592.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $1,843.60 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; leftover $1592.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 21 | $75.65 | $2.05 | — | $252.90 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1592.98 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.90 | ▲ close $9,142.40 vs 09:30 $9,048.17 (session +126.05) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.90 | ▲ 09:30 equity $9,198.93 vs yday $9,142.40 (+56.53) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.90 | ▼ close $9,134.55 vs 09:30 $9,198.93 (session -64.38) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.90 | ▲ 09:30 equity $9,248.94 vs yday $9,134.55 (+114.39) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.90 | ▼ close $9,029.90 vs 09:30 $9,248.94 (session -219.04) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.90 | ▼ 09:30 equity $8,913.64 vs yday $9,029.90 (-116.26) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.90 | ▲ close $8,954.57 vs 09:30 $8,913.64 (session +40.93) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.90 | ▲ 09:30 equity $9,075.56 vs yday $8,954.57 (+120.99) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `MMED` | 11 | $23.03 | $2.04 | $-13.42 | $504.19 | ▼ -13.42 after sell → book $9,073.52; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CNXC` | 8 | $27.13 | $2.03 | $-50.05 | $719.19 | ▼ -50.05 after sell → book $9,071.48; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `FRNM` | 17 | $14.85 | $2.06 | $-21.44 | $969.58 | ▼ -21.44 after sell → book $9,069.42; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HPE` | 5 | $56.37 | $2.02 | $+39.82 | $1,249.41 | ▲ +39.82 after sell → book $9,067.40; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 1 | $164.43 | $1.65 | — | $1,083.33 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $249.88 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 122 | $2.04 | $2.36 | — | $832.09 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $249.88 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 16 | $15.01 | $2.04 | — | $589.90 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; leftover $249.88 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 1 | $242.17 | $1.99 | — | $345.73 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list earn_react; ret5=-11.1; leftover $249.88 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 1 | $135.71 | $1.36 | — | $208.66 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list earn_react; ret5=-9.2; leftover $249.88 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.66 | ▼ close $9,049.59 vs 09:30 $9,075.56 (session -8.41) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.66 | ▼ 09:30 equity $8,925.76 vs yday $9,049.59 (-123.83) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CRM` | 6 | $255.75 | $2.03 | $-49.70 | $1,741.13 | ▼ -49.70 after sell → book $8,923.73; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 821 | $2.05 | $10.74 | $+68.98 | $3,413.44 | ▲ +68.98 after sell → book $8,912.99; vs 09:30 mark -10.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `MSTR` | 11 | $130.90 | $2.04 | $-75.02 | $4,851.30 | ▼ -75.02 after sell → book $8,910.95; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `MRX` | 21 | $71.61 | $2.08 | $-88.97 | $6,353.03 | ▼ -88.97 after sell → book $8,908.87; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,353.03 | ▲ close $8,920.35 vs 09:30 $8,925.76 (session +11.48) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,353.03 | ▲ 09:30 equity $8,936.74 vs yday $8,920.35 (+16.39) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BE` | 6 | $261.00 | $2.03 | $+141.04 | $7,917.00 | ▲ +141.04 after sell → book $8,934.71; vs 09:30 mark -2.03 | dropped from list after 6 sess (min 5) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,917.00 | ▼ close $8,920.00 vs 09:30 $8,936.74 (session -14.71) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,917.00 | ▼ 09:30 equity $8,914.98 vs yday $8,920.00 (-5.02) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 75 | $26.27 | $2.21 | — | $5,944.54 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; leftover $1979.25 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 284 | $6.95 | $3.66 | — | $3,967.07 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; ret5=-5.8; leftover $1979.25 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 49 | $39.99 | $2.14 | — | $2,005.43 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; leftover $1979.25 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 10 | $189.17 | $2.02 | — | $111.71 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; leftover $1979.25 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.71 | ▼ close $8,825.30 vs 09:30 $8,914.98 (session -79.65) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.71 | ▲ 09:30 equity $8,927.38 vs yday $8,825.30 (+102.08) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 1 | $17.72 | $0.18 | — | $93.81 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; leftover $18.62 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 10 | $1.77 | $0.21 | — | $75.90 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; ret5=-10.2; leftover $18.62 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.90 | ▼ close $8,850.23 vs 09:30 $8,927.38 (session -76.76) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.90 | ▲ 09:30 equity $8,888.36 vs yday $8,850.23 (+38.13) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 1 | $150.47 | $1.53 | $-17.14 | $224.84 | ▼ -17.14 after sell → book $8,886.83; vs 09:30 mark -1.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 122 | $1.90 | $2.39 | $-21.82 | $454.26 | ▼ -21.82 after sell → book $8,884.45; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 16 | $15.87 | $2.06 | $+9.66 | $706.12 | ▲ +9.66 after sell → book $8,882.39; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ADBE` | 1 | $251.58 | $2.01 | $+5.40 | $955.68 | ▲ +5.40 after sell → book $8,880.37; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `RH` | 1 | $126.50 | $1.29 | $-11.86 | $1,080.90 | ▼ -11.86 after sell → book $8,879.09; vs 09:30 mark -1.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 15 | $14.07 | $2.04 | — | $867.81 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $216.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 14 | $14.79 | $2.03 | — | $658.72 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $216.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 28 | $7.54 | $2.07 | — | $445.67 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; leftover $216.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 9 | $22.90 | $2.02 | — | $237.55 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $216.18 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 10 | $20.91 | $2.02 | — | $26.43 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $216.18 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.43 | ▼ close $8,294.23 vs 09:30 $8,888.36 (session -574.68) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.43 | ▲ 09:30 equity $8,316.45 vs yday $8,294.23 (+22.22) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 2 | $2.15 | $0.05 | — | $22.08 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $5.29 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.08 | ▲ close $8,381.44 vs 09:30 $8,316.45 (session +65.04) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.08 | ▼ 09:30 equity $8,367.58 vs yday $8,381.44 (-13.86) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 3 | $1.01 | $0.04 | — | $19.01 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $3.68 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.01 | ▲ close $8,448.92 vs 09:30 $8,367.58 (session +81.38) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.01 | ▼ 09:30 equity $8,444.94 vs yday $8,448.92 (-3.98) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `WAY` | 75 | $25.51 | $2.24 | $-61.46 | $1,930.02 | ▼ -61.46 after sell → book $8,442.70; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `SION` | 284 | $6.03 | $3.72 | $-268.67 | $3,638.81 | ▼ -268.67 after sell → book $8,438.97; vs 09:30 mark -3.73 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `SM` | 49 | $34.43 | $2.16 | $-276.74 | $5,323.72 | ▼ -276.74 after sell → book $8,436.81; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QCOM` | 10 | $199.61 | $2.05 | $+100.33 | $7,317.78 | ▲ +100.33 after sell → book $8,434.77; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 131 | $7.95 | $2.38 | — | $6,273.94 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1045.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 66 | $15.72 | $2.19 | — | $5,234.24 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1045.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 856 | $1.22 | $11.04 | — | $4,178.87 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; leftover $1045.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 804 | $1.30 | $10.37 | — | $3,123.30 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1045.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `DGXX` | 243 | $4.30 | $3.13 | — | $2,075.27 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $1045.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 26 | $40.00 | $2.07 | — | $1,033.20 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; leftover $1045.40 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 5 | $196.78 | $2.00 | — | $47.29 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1045.40 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.29 | ▼ close $8,143.50 vs 09:30 $8,444.94 (session -258.07) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.29 | ▼ 09:30 equity $8,047.94 vs yday $8,143.50 (-95.56) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `TNDM` | 1 | $16.43 | $0.19 | $-1.66 | $63.54 | ▼ -1.66 after sell → book $8,047.76; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BAK` | 10 | $1.52 | $0.20 | $-2.91 | $78.53 | ▼ -2.91 after sell → book $8,047.55; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.53 | ▲ close $8,245.91 vs 09:30 $8,047.94 (session +198.36) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.47 | ▲ 09:30 equity $8,407.61 vs yday $8,407.11 (+0.50) | 09:30 open · cash $68.47 (unchanged overnight, no fees) · equity $8,407.61 vs prior close $8,407.11 (+0.50) · 10 name(s) re-marked at the open (per-name table). AMTX×4 yday $1.77 → 09:30 $1.77 +0.00; BHVN×3 yday $13.19 → 09:30 $13.19 +0.00; CMPX×1764 yday $1.13 → 09:30 $1.13 +0.00; DGXX×2 yday $4.53 → 09:30 $4.78 +0.50; FLNC×6 yday $7.46 → 09:30 $7.46 +0.00; IVVD×12 yday $0.91 → 09:30 $0.91 +0.00; PGEN×270 yday $7.70 → 09:30 $7.70 +0.00; RARE×3 yday $14.77 → 09:30 $14.77 +0.00; SGRY×136 yday $14.20 → 09:30 $14.20 +0.00; VERI×1632 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 3 | $3.86 | $0.12 | — | $56.77 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $13.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.77 | ▼ close $8,406.86 vs 09:30 $8,407.61 (session -0.62) | 16:00 close · cash $56.77 · equity $8,406.86 vs 09:30 $8,407.61 (-0.75; session marks -0.62) · 11 name(s) marked open→close (per-name table). AMTX×4 09:30 $1.77 → close $1.77 -0.00; BHVN×3 09:30 $13.19 → close $13.19 -0.00; CMPX×1764 09:30 $1.14 → close $1.14 -0.00; DGXX×2 09:30 $4.78 → close $4.59 -0.38; FLNC×6 09:30 $7.46 → close $7.46 +0.00; IVVD×12 09:30 $0.91 → close $0.91 -0.00; PGEN×270 09:30 $7.70 → close $7.70 -0.00; RARE×3 09:30 $14.77 → close $14.77 +0.00; SGRY×136 09:30 $14.20 → close $14.20 -0.00; VERI×1632 09:30 $1.33 → close $1.33 +0.00; ZSQR×3 09:30 $3.86 → close $3.78 -0.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `VST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NRG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HLIT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `TLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `VST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NRG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HLIT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `CELC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `OUST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BHP` | cash | leftover split 26.52 < 1 share @ 91.01 |
| 2026-08-20 | `MRNA` | cash | leftover split 26.52 < 1 share @ 150.14 |
| 2026-08-20 | `ZLAB` | cash | leftover split 26.52 < 1 share @ 26.57 |
| 2026-08-20 | `CRSP` | cash | leftover split 26.52 < 1 share @ 58.73 |
| 2026-08-20 | `APA` | cash | leftover split 26.52 < 1 share @ 44.76 |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `CELC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `OUST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `HUMA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BTGO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ASST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `FUTU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `HUMA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BTGO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `FUTU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HIVE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `EOLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `FLNC` | cash | leftover split 6.05 < 1 share @ 11.12 |
| 2026-08-26 | `CAPR` | cash | leftover split 6.05 < 1 share @ 8.29 |
| 2026-08-26 | `FWRD` | cash | leftover split 6.05 < 1 share @ 17.41 |
| 2026-08-26 | `TRLV` | cash | leftover split 6.05 < 1 share @ 11.22 |
| 2026-08-26 | `FNV` | cash | leftover split 6.05 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 6.05 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `FUTU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BTDR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `HIVE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `EOLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 14.03 < 1 share @ 41.44 |
| 2026-08-27 | `AXTI` | cash | leftover split 14.03 < 1 share @ 70.30 |
| 2026-08-27 | `FWRD` | cash | leftover split 14.03 < 1 share @ 17.60 |
| 2026-08-27 | `SRRK` | cash | leftover split 14.03 < 1 share @ 60.00 |
| 2026-08-27 | `CM` | cash | leftover split 14.03 < 1 share @ 118.77 |
| 2026-08-28 | `RUM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `EZPW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `REAX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `EOLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `FCX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RUM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `EZPW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `REAX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `EOLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `FCX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `TRLV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `OPTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ERAS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BBWI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ERAS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BBWI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `DE` | cash | leftover split 275.00 < 1 share @ 703.25 |
| 2026-09-03 | `AVGO` | cash | leftover split 275.00 < 1 share @ 351.74 |
| 2026-09-03 | `CIEN` | cash | leftover split 275.00 < 1 share @ 354.49 |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `MMED` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CNXC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `FRNM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HPE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `MMED` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CNXC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `FRNM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BAK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `MSTR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `MRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `CRM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `MSTR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `MRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `ORCL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ADBE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `RH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `ORCL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ADBE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `SION` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 18.62 < 1 share @ 170.85 |
| 2026-09-17 | `LITE` | cash | leftover split 18.62 < 1 share @ 934.88 |
| 2026-09-17 | `JBHT` | cash | leftover split 18.62 < 1 share @ 238.60 |
| 2026-09-17 | `GME` | cash | leftover split 18.62 < 1 share @ 22.12 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `WAY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `SM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QCOM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BAK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `GLXY` | cash | leftover split 5.29 < 1 share @ 25.95 |
| 2026-09-21 | `MARA` | cash | leftover split 5.29 < 1 share @ 13.94 |
| 2026-09-21 | `SMTC` | cash | leftover split 5.29 < 1 share @ 190.30 |
| 2026-09-21 | `VICR` | cash | leftover split 5.29 < 1 share @ 230.25 |
| 2026-09-22 | `WAY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `SM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QCOM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BAK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `MRNA` | cash | leftover split 3.68 < 1 share @ 168.50 |
| 2026-09-22 | `DGXX` | cash | leftover split 3.68 < 1 share @ 4.30 |
| 2026-09-23 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BAK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `FLNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `GME` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `FLNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `GME` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `DGXX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `BHVN` | 15 | 2026-09-18 @ $14.07 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $216.18 |
| `RARE` | 14 | 2026-09-18 @ $14.79 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $216.18 |
| `FLNC` | 28 | 2026-09-18 @ $7.54 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; leftover $216.18 |
| `GME` | 9 | 2026-09-18 @ $22.90 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $216.18 |
| `TH` | 10 | 2026-09-18 @ $20.91 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $216.18 |
| `AMTX` | 2 | 2026-09-21 @ $2.15 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $5.29 |
| `IVVD` | 3 | 2026-09-22 @ $1.01 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $3.68 |
| `PGEN` | 131 | 2026-09-23 @ $7.95 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1045.40 |
| `SGRY` | 66 | 2026-09-23 @ $15.72 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1045.40 |
| `CMPX` | 856 | 2026-09-23 @ $1.22 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; leftover $1045.40 |
| `VERI` | 804 | 2026-09-23 @ $1.30 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1045.40 |
| `DGXX` | 243 | 2026-09-23 @ $4.30 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $1045.40 |
| `BLSH` | 26 | 2026-09-23 @ $40.00 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; leftover $1045.40 |
| `CTAS` | 5 | 2026-09-23 @ $196.78 | union ∩ news_g hold 5, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1045.40 |
