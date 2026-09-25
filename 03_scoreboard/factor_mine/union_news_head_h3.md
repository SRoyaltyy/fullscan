# Factor mine action — `union_news_head_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-20.85%** ($7,915) · signal-only (no cash/fees) was +184.18%. Starts YES **1/30**. Fills 138 · skips 192 · realized $-2398.84.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the prior-export headline is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `headline=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,882.37.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $2,534.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $1,392.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,392.75 | ▲ close $10,019.55 vs 09:30 $10,000.00 (session +36.39) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,392.75 | ▲ 09:30 equity $10,071.64 vs yday $10,019.55 (+52.09) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 14 | $49.00 | $2.03 | — | $704.71 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $696.37 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 7 | $92.99 | $2.01 | — | $51.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $696.37 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.77 | ▼ close $9,911.33 vs 09:30 $10,071.64 (session -156.27) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.77 | ▼ 09:30 equity $9,793.79 vs yday $9,911.33 (-117.54) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.77 | ▲ close $9,817.46 vs 09:30 $9,793.79 (session +23.67) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.77 | ▼ 09:30 equity $9,813.18 vs yday $9,817.46 (-4.28) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $1,262.07 | ▼ -30.89 after sell → book $9,810.88; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $2,493.41 | ▼ -3.75 after sell → book $9,808.68; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $3,688.04 | ▼ -54.24 after sell → book $9,806.39; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VELO` | 81 | $14.51 | $2.26 | $-74.96 | $4,861.10 | ▼ -74.96 after sell → book $9,804.14; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `S` | 52 | $22.37 | $2.17 | $-77.37 | $6,022.17 | ▼ -77.37 after sell → book $9,801.97; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ZS` | 6 | $186.70 | $2.03 | $-23.84 | $7,140.34 | ▼ -23.84 after sell → book $9,799.94; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,140.34 | ▼ close $9,690.73 vs 09:30 $9,813.18 (session -109.21) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,140.34 | ▼ 09:30 equity $9,684.76 vs yday $9,690.73 (-5.97) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $8,461.84 | ▲ +67.86 after sell → book $9,680.96; vs 09:30 mark -3.80 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 14 | $40.63 | $2.05 | $-121.26 | $9,028.61 | ▼ -121.26 after sell → book $9,678.91; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CELC` | 7 | $92.90 | $2.03 | $-4.67 | $9,676.88 | ▼ -4.67 after sell → book $9,676.88; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,491.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 489 | $2.47 | $6.31 | — | $7,277.58 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $6,100.93 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 75 | $16.00 | $2.21 | — | $4,898.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,695.58 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $2,497.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 6 | $173.90 | $2.01 | — | $1,452.40 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1209.61 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1710 | $0.71 | $17.22 | — | $226.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1209.61 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.21 | ▼ close $9,469.92 vs 09:30 $9,684.76 (session -170.99) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.21 | ▲ 09:30 equity $9,641.29 vs yday $9,469.92 (+171.37) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 4 | $8.66 | $0.36 | — | $191.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $37.70 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 11 | $3.24 | $0.39 | — | $155.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $37.70 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 3 | $11.70 | $0.36 | — | $119.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $37.70 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 3 | $11.10 | $0.34 | — | $86.10 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+19.1; leftover $37.70 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.10 | ▲ close $9,675.59 vs 09:30 $9,641.29 (session +35.74) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.10 | ▲ 09:30 equity $9,695.57 vs yday $9,675.59 (+19.98) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.10 | ▼ close $9,671.51 vs 09:30 $9,695.57 (session -24.06) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.10 | ▲ 09:30 equity $9,691.43 vs yday $9,671.51 (+19.92) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,330.23 | ▲ +58.97 after sell → book $9,689.38; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 489 | $2.38 | $6.40 | $-56.72 | $2,487.65 | ▼ -56.72 after sell → book $9,682.98; vs 09:30 mark -6.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $3,644.18 | ▼ -20.12 after sell → book $9,680.91; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 75 | $19.04 | $2.24 | $+223.55 | $5,069.94 | ▲ +223.55 after sell → book $9,678.67; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $6,215.91 | ▼ -57.17 after sell → book $9,676.64; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $26.04 | $2.15 | $-28.12 | $7,385.56 | ▼ -28.12 after sell → book $9,674.49; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEAM` | 6 | $170.64 | $2.03 | $-23.60 | $8,407.37 | ▼ -23.60 after sell → book $9,672.46; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1710 | $0.66 | $16.76 | $-109.22 | $9,524.34 | ▼ -109.22 after sell → book $9,655.70; vs 09:30 mark -16.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 54 | $35.05 | $2.15 | — | $7,629.49 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1904.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 202 | $9.42 | $2.61 | — | $5,724.04 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1904.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 66 | $28.86 | $2.19 | — | $3,817.10 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1904.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 79 | $24.11 | $2.23 | — | $1,910.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+891.7; leftover $1904.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 218 | $8.72 | $2.81 | — | $6.41 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1904.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.41 | ▲ close $10,130.37 vs 09:30 $9,691.43 (session +486.65) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.41 | ▼ 09:30 equity $9,956.62 vs yday $10,130.37 (-173.75) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 4 | $8.84 | $0.39 | $-0.02 | $41.38 | ▼ -0.02 after sell → book $9,956.23; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 11 | $2.95 | $0.38 | $-3.96 | $73.45 | ▼ -3.96 after sell → book $9,955.85; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 3 | $11.56 | $0.38 | $-1.16 | $107.76 | ▼ -1.16 after sell → book $9,955.48; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTDR` | 3 | $11.05 | $0.36 | $-0.84 | $140.55 | ▼ -0.84 after sell → book $9,955.12; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 2 | $11.22 | $0.23 | — | $117.88 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $28.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 3 | $8.29 | $0.26 | — | $92.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $28.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 1 | $17.41 | $0.18 | — | $75.16 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-9.2; leftover $28.11 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 2 | $11.12 | $0.23 | — | $52.69 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $28.11 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.69 | ▼ close $9,816.55 vs 09:30 $9,956.62 (session -137.67) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.69 | ▼ 09:30 equity $9,770.39 vs yday $9,816.55 (-46.16) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.69 | ▼ close $9,625.76 vs 09:30 $9,770.39 (session -144.63) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.69 | ▼ 09:30 equity $9,569.50 vs yday $9,625.76 (-56.26) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 54 | $34.50 | $2.18 | $-34.03 | $1,913.52 | ▼ -34.03 after sell → book $9,567.33; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 202 | $9.30 | $2.65 | $-29.50 | $3,789.46 | ▼ -29.50 after sell → book $9,564.67; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 66 | $28.91 | $2.21 | $-1.10 | $5,695.31 | ▼ -1.10 after sell → book $9,562.46; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 79 | $23.40 | $2.25 | $-60.57 | $7,541.65 | ▼ -60.57 after sell → book $9,560.20; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EOLS` | 218 | $8.84 | $2.86 | $+20.48 | $9,465.91 | ▲ +20.48 after sell → book $9,557.34; vs 09:30 mark -2.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,188.05 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1352.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $6,837.04 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1352.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 280 | $4.82 | $3.61 | — | $5,483.83 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1352.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 71 | $19.00 | $2.20 | — | $4,132.62 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1352.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 157 | $8.61 | $2.46 | — | $2,778.39 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1352.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 72 | $18.75 | $2.21 | — | $1,426.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=-5.0; leftover $1352.27 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 70 | $19.25 | $2.20 | — | $76.49 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+14.1; leftover $1352.27 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.49 | ▼ close $9,277.82 vs 09:30 $9,569.50 (session -262.71) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.49 | ▼ 09:30 equity $9,243.32 vs yday $9,277.82 (-34.50) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 2 | $11.80 | $0.26 | $+0.67 | $99.83 | ▲ +0.67 after sell → book $9,243.06; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 3 | $9.50 | $0.31 | $+3.06 | $128.01 | ▲ +3.06 after sell → book $9,242.75; vs 09:30 mark -0.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FWRD` | 1 | $17.03 | $0.19 | $-0.75 | $144.85 | ▼ -0.75 after sell → book $9,242.55; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `FLNC` | 2 | $10.82 | $0.24 | $-1.07 | $166.25 | ▼ -1.07 after sell → book $9,242.31; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.25 | ▲ close $9,271.04 vs 09:30 $9,243.32 (session +28.72) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.25 | ▼ 09:30 equity $9,095.76 vs yday $9,271.04 (-175.28) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.25 | ▼ close $8,949.42 vs 09:30 $9,095.76 (session -146.34) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.25 | ▼ 09:30 equity $8,945.12 vs yday $8,949.42 (-4.30) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $1,361.21 | ▼ -82.89 after sell → book $8,943.08; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 41 | $32.42 | $2.13 | $-23.93 | $2,688.29 | ▼ -23.93 after sell → book $8,940.94; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 280 | $4.73 | $3.67 | $-32.48 | $4,009.03 | ▼ -32.48 after sell → book $8,937.28; vs 09:30 mark -3.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 71 | $17.98 | $2.23 | $-76.85 | $5,283.38 | ▼ -76.85 after sell → book $8,935.05; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 157 | $7.25 | $2.50 | $-218.48 | $6,419.13 | ▼ -218.48 after sell → book $8,932.55; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 72 | $18.41 | $2.23 | $-28.91 | $7,742.42 | ▼ -28.91 after sell → book $8,930.32; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 70 | $16.97 | $2.22 | $-164.02 | $8,928.10 | ▼ -164.02 after sell → book $8,928.10; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,928.10 | ▲ close $8,928.10 vs 09:30 $8,945.12 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,928.10 | ▲ 09:30 equity $8,928.10 vs yday $8,928.10 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 46 | $32.31 | $2.13 | — | $7,439.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1488.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 93 | $15.87 | $2.27 | — | $5,961.54 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1488.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 62 | $23.88 | $2.18 | — | $4,478.80 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1488.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,070.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1488.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 45 | $32.88 | $2.12 | — | $1,588.58 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1488.02 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 196 | $7.59 | $2.58 | — | $98.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1488.02 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.36 | ▲ close $9,084.53 vs 09:30 $8,928.10 (session +169.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.36 | ▼ 09:30 equity $9,013.30 vs yday $9,084.53 (-71.23) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 25 | $1.94 | $0.56 | — | $49.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $49.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.30 | ▼ close $8,972.00 vs 09:30 $9,013.30 (session -40.74) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.30 | ▲ 09:30 equity $8,984.47 vs yday $8,972.00 (+12.47) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.30 | ▼ close $8,797.08 vs 09:30 $8,984.47 (session -187.39) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.30 | ▼ 09:30 equity $8,779.35 vs yday $8,797.08 (-17.73) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 46 | $35.09 | $2.15 | $+123.60 | $1,661.29 | ▲ +123.60 after sell → book $8,777.20; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 93 | $15.96 | $2.30 | $+3.80 | $3,143.27 | ▲ +3.80 after sell → book $8,774.90; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 62 | $23.22 | $2.20 | $-45.29 | $4,580.72 | ▼ -45.29 after sell → book $8,772.70; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $5,941.34 | ▼ -47.87 after sell → book $8,770.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNXC` | 45 | $28.13 | $2.15 | $-218.02 | $7,205.04 | ▼ -218.02 after sell → book $8,768.54; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OPTX` | 196 | $7.72 | $2.62 | $+20.28 | $8,715.54 | ▲ +20.28 after sell → book $8,765.92; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,715.54 | ▼ close $8,764.79 vs 09:30 $8,779.35 (session -1.13) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,715.54 | ▲ 09:30 equity $8,764.79 vs yday $8,764.79 (+0.00) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 25 | $1.97 | $0.59 | $-0.40 | $8,764.20 | ▼ -0.40 after sell → book $8,764.20; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,764.20 | ▲ close $8,764.20 vs 09:30 $8,764.79 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,764.20 | ▲ 09:30 equity $8,764.20 vs yday $8,764.20 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $7,446.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1460.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $5,991.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1460.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 97 | $15.01 | $2.28 | — | $4,533.47 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1460.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 689 | $2.12 | $8.89 | — | $3,063.90 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1460.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 716 | $2.04 | $9.24 | — | $1,594.03 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1460.70 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 10 | $135.71 | $2.02 | — | $234.91 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1460.70 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.91 | ▼ close $8,600.08 vs 09:30 $8,764.20 (session -137.68) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.91 | ▼ 09:30 equity $8,543.33 vs yday $8,600.08 (-56.75) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.91 | ▲ close $8,580.14 vs 09:30 $8,543.33 (session +36.81) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.91 | ▼ 09:30 equity $8,538.49 vs yday $8,580.14 (-41.65) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.91 | ▼ close $8,464.71 vs 09:30 $8,538.49 (session -73.78) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.91 | ▼ 09:30 equity $8,258.10 vs yday $8,464.71 (-206.61) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 8 | $140.03 | $2.03 | $-199.25 | $1,353.11 | ▼ -199.25 after sell → book $8,256.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 6 | $253.34 | $2.03 | $+62.98 | $2,871.12 | ▲ +62.98 after sell → book $8,254.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 689 | $1.84 | $9.01 | $-210.82 | $4,129.87 | ▼ -210.82 after sell → book $8,245.02; vs 09:30 mark -9.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 716 | $1.89 | $9.37 | $-126.00 | $5,473.74 | ▼ -126.00 after sell → book $8,235.65; vs 09:30 mark -9.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RH` | 10 | $125.55 | $2.04 | $-105.66 | $6,727.20 | ▼ -105.66 after sell → book $8,233.61; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 128 | $26.27 | $2.37 | — | $3,362.27 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3363.60 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 482 | $6.95 | $6.22 | — | $6.15 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-5.8; leftover $3363.60 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.15 | ▲ close $8,317.12 vs 09:30 $8,258.10 (session +92.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.15 | ▲ 09:30 equity $8,437.14 vs yday $8,317.12 (+120.02) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.15 | ▼ close $8,398.61 vs 09:30 $8,437.14 (session -38.53) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.15 | ▲ 09:30 equity $8,426.98 vs yday $8,398.61 (+28.37) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 97 | $15.87 | $2.31 | $+78.83 | $1,543.23 | ▲ +78.83 after sell → book $8,424.67; vs 09:30 mark -2.31 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 14 | $20.91 | $2.03 | — | $1,248.46 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $308.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 13 | $22.90 | $2.03 | — | $948.73 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $308.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 20 | $14.79 | $2.05 | — | $650.88 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $308.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 21 | $14.07 | $2.05 | — | $353.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $308.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 40 | $7.54 | $2.11 | — | $49.85 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $308.65 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.85 | ▼ close $7,667.05 vs 09:30 $8,426.98 (session -747.35) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.85 | ▲ 09:30 equity $7,739.31 vs yday $7,667.05 (+72.26) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 128 | $25.94 | $2.42 | $-47.04 | $3,367.75 | ▼ -47.04 after sell → book $7,736.89; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 2 | $230.25 | $2.00 | — | $2,905.25 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $673.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 3 | $190.30 | $2.00 | — | $2,332.35 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $673.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 25 | $25.95 | $2.06 | — | $1,681.54 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $673.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 48 | $13.94 | $2.13 | — | $1,010.28 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $673.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 313 | $2.15 | $4.04 | — | $333.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $673.55 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $333.30 | ▼ close $7,629.59 vs 09:30 $7,739.31 (session -95.07) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $333.30 | ▲ 09:30 equity $7,633.79 vs yday $7,629.59 (+4.20) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 482 | $5.99 | $6.32 | $-475.26 | $3,214.15 | ▼ -475.26 after sell → book $7,627.46; vs 09:30 mark -6.33 | dropped from list after 4 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $2,706.66 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $535.69 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 530 | $1.01 | $6.84 | — | $2,164.52 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $535.69 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 124 | $4.30 | $2.36 | — | $1,628.96 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+16.9; leftover $535.69 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,628.96 | ▲ close $7,662.94 vs 09:30 $7,633.79 (session +46.67) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,628.96 | ▲ 09:30 equity $7,764.40 vs yday $7,662.94 (+101.46) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 14 | $21.15 | $2.05 | $-0.72 | $1,923.00 | ▼ -0.72 after sell → book $7,762.35; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GME` | 13 | $23.94 | $2.05 | $+9.44 | $2,232.18 | ▲ +9.44 after sell → book $7,760.30; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 20 | $15.40 | $2.07 | $+8.08 | $2,538.11 | ▲ +8.08 after sell → book $7,758.23; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 21 | $14.84 | $2.07 | $+12.04 | $2,847.67 | ▲ +12.04 after sell → book $7,756.16; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FLNC` | 40 | $7.52 | $2.13 | $-4.84 | $3,146.34 | ▼ -4.84 after sell → book $7,754.03; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 79 | $7.95 | $2.23 | — | $2,516.07 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $629.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 40 | $15.72 | $2.11 | — | $1,885.16 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $629.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 484 | $1.30 | $6.24 | — | $1,249.71 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $629.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 515 | $1.22 | $6.64 | — | $614.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $629.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 15 | $40.00 | $2.04 | — | $12.73 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $629.27 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.73 | ▼ close $7,524.81 vs 09:30 $7,764.40 (session -209.95) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.73 | ▼ 09:30 equity $7,410.13 vs yday $7,524.81 (-114.68) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 2 | $274.61 | $2.02 | $+84.71 | $559.94 | ▲ +84.71 after sell → book $7,408.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 3 | $164.04 | $2.02 | $-82.80 | $1,050.04 | ▼ -82.80 after sell → book $7,406.09; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 25 | $25.00 | $2.08 | $-28.03 | $1,672.83 | ▼ -28.03 after sell → book $7,404.01; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 48 | $13.07 | $2.15 | $-46.05 | $2,298.03 | ▼ -46.05 after sell → book $7,401.85; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMTX` | 313 | $1.88 | $4.10 | $-92.65 | $2,882.37 | ▼ -92.65 after sell → book $7,397.75; vs 09:30 mark -4.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,882.37 | ▲ close $7,512.50 vs 09:30 $7,410.13 (session +114.75) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,558.58 | ▲ 09:30 equity $8,010.34 vs yday $7,985.39 (+24.95) | 09:30 open · cash $5,558.58 (unchanged overnight, no fees) · equity $8,010.34 vs prior close $7,985.39 (+24.95) · 8 name(s) re-marked at the open (per-name table). CMPX×22 yday $1.13 → 09:30 $1.13 +0.00; DGXX×134 yday $4.53 → 09:30 $4.78 +33.50; GRAL×5 yday $125.21 → 09:30 $123.50 -8.55; IVVD×570 yday $0.91 → 09:30 $0.91 +0.00; MRNA×3 yday $194.82 → 09:30 $194.82 +0.00; PGEN×3 yday $7.70 → 09:30 $7.70 +0.00; SGRY×1 yday $14.20 → 09:30 $14.20 +0.00; VERI×20 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `DGXX` | 134 | $4.78 | $2.42 | $+62.18 | $6,196.68 | ▲ +62.18 after sell → book $8,007.92; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 5 | $123.50 | $2.02 | $+79.72 | $6,812.15 | ▲ +79.72 after sell → book $8,005.89; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 441 | $3.86 | $5.69 | — | $5,104.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1703.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,469.23 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1703.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 105 | $16.21 | $2.31 | — | $1,764.88 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1703.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 22 | $74.15 | $2.06 | — | $131.52 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+8.5; leftover $1703.04 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.52 | ▼ close $7,914.94 vs 09:30 $8,010.34 (session -78.89) | 16:00 close · cash $131.52 · equity $7,914.94 vs 09:30 $8,010.34 (-95.40; session marks -78.89) · 10 name(s) marked open→close (per-name table). CMPX×22 09:30 $1.14 → close $1.14 -0.00; IVVD×570 09:30 $0.91 → close $0.91 -0.00; MRNA×3 09:30 $194.82 → close $194.82 +0.00; PGEN×3 09:30 $7.70 → close $7.70 -0.00; SGRY×1 09:30 $14.20 → close $14.20 -0.00; VERI×20 09:30 $1.33 → close $1.33 +0.00; ZSQR×441 09:30 $3.86 → close $3.78 -35.28; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×105 09:30 $16.21 → close $15.96 -26.25; RKLB×22 09:30 $74.15 → close $73.95 -4.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ZS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ZS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | cash | leftover split 37.70 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 37.70 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EOLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 28.11 < 1 share @ 267.02 |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EOLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASML` | cash | leftover split 17.56 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 17.56 < 1 share @ 29.83 |
| 2026-08-27 | `SRRK` | cash | leftover split 17.56 < 1 share @ 60.00 |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FWRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRX` | cash | leftover split 49.18 < 1 share @ 75.65 |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVTR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TXG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 1.23 < 1 share @ 170.85 |
| 2026-09-17 | `GME` | cash | leftover split 1.23 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 1.23 < 1 share @ 238.60 |
| 2026-09-17 | `TNDM` | cash | leftover split 1.23 < 1 share @ 17.72 |
| 2026-09-17 | `BAK` | cash | leftover split 1.23 < 1 share @ 1.77 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `FLNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FLNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DGXX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEWP` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MRNA` | 3 | 2026-09-22 @ $168.50 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $535.69 |
| `IVVD` | 530 | 2026-09-22 @ $1.01 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $535.69 |
| `DGXX` | 124 | 2026-09-22 @ $4.30 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+16.9; leftover $535.69 |
| `PGEN` | 79 | 2026-09-23 @ $7.95 | prior-export headline🟢 only; gate headline=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $629.27 |
| `SGRY` | 40 | 2026-09-23 @ $15.72 | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $629.27 |
| `VERI` | 484 | 2026-09-23 @ $1.30 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $629.27 |
| `CMPX` | 515 | 2026-09-23 @ $1.22 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $629.27 |
| `BLSH` | 15 | 2026-09-23 @ $40.00 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $629.27 |
