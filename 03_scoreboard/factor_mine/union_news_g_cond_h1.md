# Factor mine action — `union_news_g_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢, rank +G−R

Cash book **-14.80%** ($8,520) · signal-only (no cash/fees) was +8.94%. Starts YES **0/30**. Fills 205 · skips 79 · realized $-430.09.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,569.93.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 43 | $46.18 | $2.12 | — | $8,150.46 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,149.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,120.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $2,109.52 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $154.67 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2027.66 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.67 | ▲ close $10,223.75 vs 09:30 $10,155.37 (session +95.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.67 | ▼ 09:30 equity $10,169.20 vs yday $10,223.75 (-54.55) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 43 | $48.00 | $2.15 | $+74.00 | $2,216.53 | ▲ +74.00 after sell → book $10,167.06; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,287.03 | ▲ +69.69 after sell → book $10,165.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,374.28 | ▲ +58.23 after sell → book $10,162.95; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $8,220.84 | ▼ -164.56 after sell → book $10,160.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $10,158.74 | ▼ -16.94 after sell → book $10,158.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.74 | ▲ close $10,158.74 vs 09:30 $10,169.20 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.74 | ▲ 09:30 equity $10,158.74 vs yday $10,158.74 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.74 | ▲ close $10,158.74 vs 09:30 $10,158.74 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.74 | ▲ 09:30 equity $10,158.74 vs yday $10,158.74 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,973.58 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,718.22 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 514 | $2.47 | $6.63 | — | $6,442.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,206.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,940.40 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,737.27 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,486.35 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1796 | $0.71 | $18.09 | — | $198.49 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1269.84 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.49 | ▼ close $9,930.41 vs 09:30 $10,158.74 (session -191.09) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.49 | ▲ 09:30 equity $10,117.39 vs yday $9,930.41 (+186.98) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,440.80 | ▲ +57.15 after sell → book $10,115.34; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,685.27 | ▼ -10.89 after sell → book $10,113.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,078.16 | ▲ +126.66 after sell → book $10,110.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,141.00 | ▼ -140.29 after sell → book $10,108.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,372.60 | ▼ -19.32 after sell → book $10,106.81; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1796 | $0.67 | $17.80 | $-95.15 | $7,565.30 | ▼ -95.15 after sell → book $10,089.00; vs 09:30 mark -17.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,368.98 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1260.88 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,215.16 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1260.88 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $4,029.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1260.88 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 145 | $8.66 | $2.42 | — | $2,771.80 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1260.88 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 389 | $3.24 | $5.02 | — | $1,506.43 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1260.88 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 107 | $11.70 | $2.31 | — | $252.22 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1260.88 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.22 | ▼ close $9,915.50 vs 09:30 $10,117.39 (session -157.68) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.22 | ▼ 09:30 equity $9,881.02 vs yday $9,915.50 (-34.48) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 514 | $2.40 | $6.73 | $-49.34 | $1,479.09 | ▼ -49.34 after sell → book $9,874.29; vs 09:30 mark -6.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,682.15 | ▲ +6.74 after sell → book $9,872.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,890.11 | ▲ +54.14 after sell → book $9,870.21; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $5,116.10 | ▲ +40.76 after sell → book $9,868.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 145 | $8.00 | $2.46 | $-100.58 | $6,273.65 | ▼ -100.58 after sell → book $9,865.70; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 389 | $2.99 | $5.09 | $-107.36 | $7,431.66 | ▼ -107.36 after sell → book $9,860.60; vs 09:30 mark -5.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 107 | $11.17 | $2.34 | $-61.36 | $8,624.51 | ▼ -61.36 after sell → book $9,858.26; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,624.51 | ▼ close $9,823.09 vs 09:30 $9,881.02 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,624.51 | ▲ 09:30 equity $9,841.04 vs yday $9,823.09 (+17.95) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,838.97 | ▼ -20.93 after sell → book $9,838.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,533.23 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1405.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $7,142.84 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1405.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $5,738.73 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1405.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 149 | $9.42 | $2.44 | — | $4,332.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1405.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 48 | $28.86 | $2.13 | — | $2,945.30 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1405.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 58 | $24.11 | $2.16 | — | $1,544.76 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=+891.7; leftover $1405.57 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 161 | $8.72 | $2.47 | — | $138.37 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1405.57 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.37 | ▲ close $10,279.98 vs 09:30 $9,841.04 (session +456.39) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.37 | ▼ 09:30 equity $10,105.44 vs yday $10,279.98 (-174.54) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,454.12 | ▲ +10.01 after sell → book $10,103.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $2,880.18 | ▲ +35.67 after sell → book $10,101.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,306.05 | ▲ +21.76 after sell → book $10,099.20; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 149 | $10.07 | $2.47 | $+91.94 | $5,804.00 | ▲ +91.94 after sell → book $10,096.72; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 48 | $27.56 | $2.15 | $-66.69 | $7,124.73 | ▼ -66.69 after sell → book $10,094.57; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 58 | $26.61 | $2.19 | $+140.65 | $8,665.92 | ▲ +140.65 after sell → book $10,092.38; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 161 | $8.86 | $2.51 | $+17.56 | $10,089.87 | ▲ +17.56 after sell → book $10,089.87; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 6 | $267.02 | $2.01 | — | $8,485.74 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1681.64 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 14 | $118.50 | $2.03 | — | $6,824.71 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1681.64 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 149 | $11.22 | $2.44 | — | $5,150.49 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1681.64 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 202 | $8.29 | $2.61 | — | $3,473.31 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1681.64 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 96 | $17.41 | $2.28 | — | $1,799.67 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-9.2; leftover $1681.64 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 151 | $11.12 | $2.44 | — | $118.11 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1681.64 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.11 | ▲ close $10,336.48 vs 09:30 $10,105.44 (session +260.41) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.11 | ▲ 09:30 equity $10,365.39 vs yday $10,336.48 (+28.91) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 6 | $267.23 | $2.03 | $-2.78 | $1,719.46 | ▼ -2.78 after sell → book $10,363.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 149 | $11.38 | $2.48 | $+18.93 | $3,412.60 | ▲ +18.93 after sell → book $10,360.88; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 202 | $9.19 | $2.65 | $+176.54 | $5,266.33 | ▲ +176.54 after sell → book $10,358.23; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 96 | $17.60 | $2.31 | $+13.65 | $6,953.62 | ▲ +13.65 after sell → book $10,355.92; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 151 | $11.52 | $2.48 | $+55.48 | $8,690.66 | ▲ +55.48 after sell → book $10,353.44; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $7,463.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1241.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,494.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1241.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 41 | $29.83 | $2.11 | — | $5,269.72 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1241.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $4,311.09 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1241.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $3,194.78 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1241.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $2,146.90 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $1241.52 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,146.90 | ▼ close $10,325.86 vs 09:30 $10,365.39 (session -15.43) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,146.90 | ▼ 09:30 equity $10,260.51 vs yday $10,325.86 (-65.35) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 14 | $115.66 | $2.06 | $-43.85 | $3,764.08 | ▼ -43.85 after sell → book $10,258.45; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $4,951.08 | ▼ -39.79 after sell → book $10,256.40; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,868.36 | ▼ -51.73 after sell → book $10,254.39; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 41 | $30.50 | $2.13 | $+23.22 | $7,116.72 | ▲ +23.22 after sell → book $10,252.25; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $8,068.79 | ▼ -6.57 after sell → book $10,250.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $9,203.57 | ▲ +18.47 after sell → book $10,248.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $7,903.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1314.80 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $6,626.07 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1314.80 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $5,422.81 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1314.80 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,114.79 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1314.80 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $2,911.68 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1314.80 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $1,626.48 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1314.80 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 272 | $4.82 | $3.51 | — | $311.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1314.80 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $311.93 | ▼ close $9,919.33 vs 09:30 $10,260.51 (session -313.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $311.93 | ▼ 09:30 equity $9,913.64 vs yday $9,919.33 (-5.69) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-19.06 | $1,340.75 | ▼ -19.06 after sell → book $9,911.62; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,628.68 | ▼ -11.70 after sell → book $9,909.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,817.35 | ▼ -89.19 after sell → book $9,907.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,950.65 | ▼ -69.96 after sell → book $9,905.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,210.53 | ▼ -48.14 after sell → book $9,903.53; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $7,378.33 | ▼ -35.31 after sell → book $9,901.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $8,591.06 | ▼ -72.48 after sell → book $9,899.38; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 272 | $4.81 | $3.56 | $-9.79 | $9,895.81 | ▼ -9.79 after sell → book $9,895.81; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.81 | ▲ close $9,895.81 vs 09:30 $9,913.64 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.81 | ▲ 09:30 equity $9,895.81 vs yday $9,895.81 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.81 | ▲ close $9,895.81 vs 09:30 $9,895.81 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.81 | ▲ 09:30 equity $9,895.81 vs yday $9,895.81 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.81 | ▲ close $9,895.81 vs 09:30 $9,895.81 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.81 | ▲ 09:30 equity $9,895.81 vs yday $9,895.81 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,838.59 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,863.98 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 38 | $32.31 | $2.10 | — | $6,634.09 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 77 | $15.87 | $2.22 | — | $5,409.88 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $4,189.86 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,484.62 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $47.60 | $2.06 | — | $2,292.55 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1236.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 37 | $32.88 | $2.10 | — | $1,073.89 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1236.98 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,073.89 | ▲ close $10,245.23 vs 09:30 $9,895.81 (session +366.04) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,073.89 | ▼ 09:30 equity $10,170.71 vs yday $10,245.23 (-74.52) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,150.97 | ▲ +19.86 after sell → book $10,168.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,176.52 | ▲ +50.93 after sell → book $10,166.68; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 38 | $33.46 | $2.12 | $+39.47 | $4,445.87 | ▲ +39.47 after sell → book $10,164.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.84 | $2.16 | $-6.35 | $5,659.55 | ▼ -6.35 after sell → book $10,162.39; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,349.57 | ▼ -15.23 after sell → book $10,160.38; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $53.85 | $2.09 | $+152.10 | $7,693.73 | ▲ +152.10 after sell → book $10,158.29; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 37 | $32.48 | $2.12 | $-19.02 | $8,893.37 | ▼ -19.02 after sell → book $10,156.17; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,311.20 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1778.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 23 | $75.65 | $2.06 | — | $5,569.19 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1778.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $3,909.44 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1778.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 916 | $1.94 | $11.82 | — | $2,120.58 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1778.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 12 | $137.35 | $2.03 | — | $470.36 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $1778.67 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $470.36 | ▲ close $10,296.75 vs 09:30 $10,170.71 (session +160.50) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $470.36 | ▲ 09:30 equity $10,397.78 vs yday $10,296.75 (+101.03) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 77 | $16.74 | $2.24 | $+62.52 | $1,757.09 | ▲ +62.52 after sell → book $10,395.53; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,277.38 | ▼ -61.88 after sell → book $10,393.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $5,149.67 | ▲ +212.53 after sell → book $10,391.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 916 | $1.94 | $11.98 | $-23.80 | $6,914.72 | ▼ -23.80 after sell → book $10,379.48; vs 09:30 mark -11.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,914.72 | ▼ close $10,317.29 vs 09:30 $10,397.78 (session -62.19) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,914.72 | ▲ 09:30 equity $10,378.36 vs yday $10,317.29 (+61.07) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 23 | $76.60 | $2.08 | $+17.71 | $8,674.44 | ▲ +17.71 after sell → book $10,376.28; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 12 | $141.82 | $2.05 | $+49.56 | $10,374.23 | ▲ +49.56 after sell → book $10,374.23; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,374.23 | ▲ close $10,374.23 vs 09:30 $10,378.36 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,374.23 | ▲ 09:30 equity $10,374.23 vs yday $10,374.23 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,374.23 | ▲ close $10,374.23 vs 09:30 $10,374.23 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,374.23 | ▲ 09:30 equity $10,374.23 vs yday $10,374.23 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $8,727.91 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1729.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 7 | $242.17 | $2.01 | — | $7,030.71 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1729.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 115 | $15.01 | $2.33 | — | $5,302.23 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1729.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 815 | $2.12 | $10.51 | — | $3,563.91 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1729.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 847 | $2.04 | $10.93 | — | $1,825.11 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1729.04 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 12 | $135.71 | $2.03 | — | $194.56 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; ret5=-9.2; leftover $1729.04 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.56 | ▼ close $10,172.63 vs 09:30 $10,374.23 (session -171.77) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.56 | ▼ 09:30 equity $10,099.40 vs yday $10,172.63 (-73.23) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 10 | $141.42 | $2.04 | $-234.16 | $1,606.72 | ▼ -234.16 after sell → book $10,097.36; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 7 | $261.51 | $2.04 | $+131.33 | $3,435.25 | ▲ +131.33 after sell → book $10,095.32; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 815 | $2.05 | $10.66 | $-78.23 | $5,095.34 | ▼ -78.23 after sell → book $10,084.66; vs 09:30 mark -10.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 847 | $2.01 | $11.08 | $-47.42 | $6,786.73 | ▼ -47.42 after sell → book $10,073.58; vs 09:30 mark -11.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 12 | $131.40 | $2.05 | $-55.79 | $8,361.48 | ▼ -55.79 after sell → book $10,071.53; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,361.48 | ▲ close $10,104.88 vs 09:30 $10,099.40 (session +33.35) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,361.48 | ▲ 09:30 equity $10,111.78 vs yday $10,104.88 (+6.90) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,361.48 | ▲ close $10,137.08 vs 09:30 $10,111.78 (session +25.30) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,361.48 | ▲ 09:30 equity $10,147.43 vs yday $10,137.08 (+10.35) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 79 | $26.27 | $2.23 | — | $6,283.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2090.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 11 | $189.17 | $2.02 | — | $4,201.03 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2090.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 52 | $39.99 | $2.15 | — | $2,119.41 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2090.37 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 300 | $6.95 | $3.87 | — | $30.54 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-5.8; leftover $2090.37 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.54 | ▼ close $10,055.86 vs 09:30 $10,147.43 (session -81.31) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.54 | ▲ 09:30 equity $10,171.47 vs yday $10,055.86 (+115.61) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 79 | $26.51 | $2.26 | $+14.48 | $2,122.57 | ▲ +14.48 after sell → book $10,169.21; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 11 | $190.35 | $2.05 | $+8.91 | $4,214.37 | ▲ +8.91 after sell → book $10,167.16; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 52 | $37.57 | $2.17 | $-130.16 | $6,165.84 | ▼ -130.16 after sell → book $10,164.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 300 | $7.27 | $3.94 | $+88.19 | $8,342.90 | ▲ +88.19 after sell → book $10,161.05; vs 09:30 mark -3.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $6,974.09 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1390.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 62 | $22.12 | $2.18 | — | $5,600.47 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1390.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $4,405.47 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-11.6; leftover $1390.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $3,468.59 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; ret5=-7.0; leftover $1390.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 78 | $17.72 | $2.22 | — | $2,084.21 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1390.48 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 785 | $1.77 | $10.13 | — | $684.63 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-10.2; leftover $1390.48 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $684.63 | ▲ close $10,172.49 vs 09:30 $10,171.47 (session +31.98) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $684.63 | ▲ 09:30 equity $10,213.37 vs yday $10,172.49 (+40.88) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 115 | $15.87 | $2.37 | $+94.20 | $2,507.31 | ▲ +94.20 after sell → book $10,211.00; vs 09:30 mark -2.37 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $3,963.92 | ▲ +87.79 after sell → book $10,208.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $5,145.89 | ▼ -13.03 after sell → book $10,206.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $6,059.54 | ▼ -23.23 after sell → book $10,204.93; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 78 | $17.13 | $2.25 | $-50.49 | $7,393.43 | ▼ -50.49 after sell → book $10,202.68; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 785 | $1.77 | $10.27 | $-20.39 | $8,772.61 | ▼ -20.39 after sell → book $10,192.41; vs 09:30 mark -10.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 104 | $20.91 | $2.30 | — | $6,595.67 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2193.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 148 | $14.79 | $2.43 | — | $4,404.32 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2193.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 155 | $14.07 | $2.46 | — | $2,221.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2193.15 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 291 | $7.54 | $3.75 | — | $24.57 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $2193.15 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.57 | ▼ close $10,020.71 vs 09:30 $10,213.37 (session -160.75) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.57 | ▲ 09:30 equity $10,142.63 vs yday $10,020.71 (+121.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 62 | $22.78 | $2.20 | $+36.55 | $1,434.74 | ▲ +36.55 after sell → book $10,140.44; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 104 | $21.65 | $2.34 | $+72.32 | $3,684.00 | ▲ +72.32 after sell → book $10,138.10; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 148 | $14.58 | $2.48 | $-35.99 | $5,839.36 | ▼ -35.99 after sell → book $10,135.62; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 155 | $13.90 | $2.50 | $-31.30 | $7,991.37 | ▼ -31.30 after sell → book $10,133.13; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 291 | $7.36 | $3.82 | $-58.50 | $10,129.31 | ▼ -58.50 after sell → book $10,129.31; vs 09:30 mark -3.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $8,515.55 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $1688.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 8 | $190.30 | $2.01 | — | $6,991.13 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1688.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 65 | $25.95 | $2.19 | — | $5,302.20 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1688.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 121 | $13.94 | $2.35 | — | $3,613.10 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1688.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 281 | $6.00 | $3.62 | — | $1,923.48 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; ret5=-24.1; leftover $1688.22 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 785 | $2.15 | $10.13 | — | $225.60 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1688.22 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.60 | ▼ close $9,839.94 vs 09:30 $10,142.63 (session -267.05) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.60 | ▼ 09:30 equity $9,818.98 vs yday $9,839.94 (-20.96) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 121 | $13.13 | $2.39 | $-102.75 | $1,811.95 | ▼ -102.75 after sell → book $9,816.60; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 281 | $5.99 | $3.69 | $-10.12 | $3,491.45 | ▼ -10.12 after sell → book $9,812.91; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $2,983.95 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $581.91 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 576 | $1.01 | $7.43 | — | $2,394.76 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $581.91 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 135 | $4.30 | $2.40 | — | $1,811.87 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+16.9; leftover $581.91 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,811.87 | ▲ close $9,805.97 vs 09:30 $9,818.98 (session +4.88) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,811.87 | ▲ 09:30 equity $10,119.63 vs yday $9,805.97 (+313.66) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 7 | $266.50 | $2.04 | $+249.70 | $3,675.33 | ▲ +249.70 after sell → book $10,117.60; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 8 | $174.50 | $2.04 | $-130.45 | $5,069.30 | ▼ -130.45 after sell → book $10,115.56; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 65 | $26.58 | $2.21 | $+36.56 | $6,794.79 | ▲ +36.56 after sell → book $10,113.35; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 785 | $2.09 | $10.27 | $-67.50 | $8,425.17 | ▼ -67.50 after sell → book $10,103.08; vs 09:30 mark -10.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 3 | $183.41 | $2.02 | $+40.70 | $8,973.36 | ▲ +40.70 after sell → book $10,101.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 576 | $0.95 | $7.31 | $-49.30 | $9,513.26 | ▼ -49.30 after sell → book $10,093.76; vs 09:30 mark -7.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 8 | $196.78 | $2.01 | — | $7,937.00 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1585.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 199 | $7.95 | $2.59 | — | $6,352.37 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1585.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 100 | $15.72 | $2.29 | — | $4,778.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1585.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1219 | $1.30 | $15.73 | — | $3,177.65 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1585.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1299 | $1.22 | $16.76 | — | $1,576.11 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $1585.54 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 39 | $40.00 | $2.11 | — | $14.01 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $1585.54 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.01 | ▼ close $9,712.66 vs 09:30 $10,119.63 (session -339.62) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.01 | ▼ 09:30 equity $9,614.40 vs yday $9,712.66 (-98.26) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 135 | $4.12 | $2.43 | $-29.12 | $567.78 | ▼ -29.12 after sell → book $9,611.97; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 8 | $192.26 | $2.04 | $-40.21 | $2,103.82 | ▼ -40.21 after sell → book $9,609.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 199 | $7.38 | $2.63 | $-118.65 | $3,569.81 | ▼ -118.65 after sell → book $9,607.30; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 100 | $14.38 | $2.32 | $-138.61 | $5,005.49 | ▼ -138.61 after sell → book $9,604.98; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1219 | $1.27 | $15.94 | $-68.23 | $6,537.68 | ▼ -68.23 after sell → book $9,589.04; vs 09:30 mark -15.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1299 | $1.17 | $16.98 | $-98.69 | $8,040.53 | ▼ -98.69 after sell → book $9,572.06; vs 09:30 mark -16.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 39 | $39.27 | $2.13 | $-32.71 | $9,569.93 | ▼ -32.71 after sell → book $9,569.93; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,569.93 | ▲ close $9,569.93 vs 09:30 $9,614.40 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,577.47 | ▲ 09:30 equity $8,577.47 vs yday $8,577.47 (+0.00) | 09:30 open · cash $8,577.47 · no holdings · equity $8,577.47 vs prior close $8,577.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 444 | $3.86 | $5.73 | — | $6,857.90 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $5,222.93 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 105 | $16.21 | $2.31 | — | $3,518.58 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,629.59 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 23 | $74.15 | $2.06 | — | $922.08 | — | merged news🟢, rank +G−R; gate news=good; rank cond; list ohlc_hot; ret5=+8.5; leftover $1715.49 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $922.08 | ▼ close $8,519.81 vs 09:30 $8,577.47 (session -43.57) | 16:00 close · cash $922.08 · equity $8,519.81 vs 09:30 $8,577.47 (-57.66; session marks -43.57) · 5 name(s) marked open→close (per-name table). ZSQR×444 09:30 $3.86 → close $3.78 -35.52; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×105 09:30 $16.21 → close $15.96 -26.25; COST×1 09:30 $887.00 → close $922.76 +35.76; RKLB×23 09:30 $74.15 → close $73.95 -4.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1241.52 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
