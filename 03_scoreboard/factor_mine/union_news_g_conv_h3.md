# Factor mine action — `union_news_g_conv_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5

Cash book **-14.55%** ($8,545) · signal-only (no cash/fees) was +282.84%. Starts YES **10/30**. Fills 84 · skips 137 · realized $-233.27.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `news=good` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,577.14.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 8 | $46.18 | $2.01 | — | $619.99 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $396.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $332.45 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $297.43 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 2 | $49.00 | $0.99 | — | $233.47 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $99.14 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.47 | ▼ close $10,223.18 vs 09:30 $10,405.75 (session -177.58) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.47 | ▼ 09:30 equity $9,978.91 vs yday $10,223.18 (-244.27) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.47 | ▼ close $9,881.88 vs 09:30 $9,978.91 (session -97.03) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.47 | ▲ 09:30 equity $9,964.71 vs yday $9,881.88 (+82.83) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 531 | $12.90 | $6.99 | $-162.52 | $7,076.38 | ▼ -162.52 after sell → book $9,957.72; vs 09:30 mark -6.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 51 | $19.58 | $2.16 | $-3.80 | $8,072.79 | ▼ -3.80 after sell → book $9,955.55; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,072.79 | ▼ close $9,904.59 vs 09:30 $9,964.71 (session -50.96) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,072.79 | ▲ 09:30 equity $9,909.35 vs yday $9,904.59 (+4.76) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 232 | $4.57 | $3.04 | $+54.29 | $9,129.99 | ▲ +54.29 after sell → book $9,906.31; vs 09:30 mark -3.04 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 8 | $49.02 | $2.03 | $+18.67 | $9,520.12 | ▲ +18.67 after sell → book $9,904.28; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,821.00 | ▲ +13.35 after sell → book $9,902.26; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 2 | $40.63 | $0.84 | $-18.56 | $9,901.42 | ▼ -18.56 after sell → book $9,901.42; vs 09:30 mark -0.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 76 | $91.01 | $2.22 | — | $2,982.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $6931.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 22 | $44.76 | $2.06 | — | $1,995.67 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $990.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 400 | $2.47 | $5.16 | — | $1,002.51 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $990.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 16 | $58.73 | $2.04 | — | $60.79 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $990.14 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.79 | ▲ close $10,067.17 vs 09:30 $9,909.35 (session +177.22) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.79 | ▲ 09:30 equity $10,258.47 vs yday $10,067.17 (+191.30) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.79 | ▲ close $10,305.65 vs 09:30 $10,258.47 (session +47.18) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.79 | ▼ 09:30 equity $10,300.81 vs yday $10,305.65 (-4.84) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.79 | ▼ close $10,236.99 vs 09:30 $10,300.81 (session -63.82) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.79 | ▼ 09:30 equity $10,135.39 vs yday $10,236.99 (-101.60) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 76 | $95.86 | $2.29 | $+364.09 | $7,343.86 | ▲ +364.09 after sell → book $10,133.10; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 22 | $41.38 | $2.08 | $-78.49 | $8,252.15 | ▼ -78.49 after sell → book $10,131.03; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 400 | $2.38 | $5.24 | $-46.40 | $9,198.91 | ▼ -46.40 after sell → book $10,125.79; vs 09:30 mark -5.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 16 | $57.93 | $2.06 | $-16.90 | $10,123.73 | ▼ -16.90 after sell → book $10,123.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 59 | $118.52 | $2.17 | — | $3,128.88 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7086.61 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 13 | $77.13 | $2.03 | — | $2,124.17 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1012.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 28 | $35.05 | $2.07 | — | $1,140.69 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1012.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 107 | $9.42 | $2.31 | — | $130.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1012.37 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.44 | ▲ close $10,530.33 vs 09:30 $10,135.39 (session +415.18) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.44 | ▼ 09:30 equity $10,307.15 vs yday $10,530.33 (-223.18) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 1 | $11.22 | $0.12 | — | $119.11 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $13.04 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 1 | $8.29 | $0.09 | — | $110.73 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $13.04 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.73 | ▼ close $10,079.33 vs 09:30 $10,307.15 (session -227.62) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.73 | ▼ 09:30 equity $10,038.85 vs yday $10,079.33 (-40.48) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.73 | ▲ close $10,109.37 vs 09:30 $10,038.85 (session +70.52) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.73 | ▲ 09:30 equity $10,146.18 vs yday $10,109.37 (+36.81) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 59 | $119.19 | $2.23 | $+35.13 | $7,140.71 | ▲ +35.13 after sell → book $10,143.95; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 13 | $78.57 | $2.05 | $+14.64 | $8,160.07 | ▲ +14.64 after sell → book $10,141.90; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 28 | $34.50 | $2.09 | $-19.57 | $9,123.97 | ▼ -19.57 after sell → book $10,139.80; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 107 | $9.30 | $2.34 | $-17.49 | $10,116.73 | ▼ -17.49 after sell → book $10,137.46; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 21 | $324.41 | $2.05 | — | $3,302.07 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7081.71 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,307.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1011.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,504.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1011.67 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,504.90 | ▼ close $9,920.75 vs 09:30 $10,146.18 (session -210.65) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,504.90 | ▲ 09:30 equity $9,981.47 vs yday $9,920.75 (+60.72) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 1 | $11.80 | $0.14 | $+0.32 | $1,516.56 | ▲ +0.32 after sell → book $9,981.33; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 1 | $9.50 | $0.12 | $+1.01 | $1,525.95 | ▲ +1.01 after sell → book $9,981.22; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,525.95 | ▲ close $9,998.97 vs 09:30 $9,981.47 (session +17.75) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,525.95 | ▼ 09:30 equity $9,924.01 vs yday $9,998.97 (-74.96) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,525.95 | ▼ close $9,877.17 vs 09:30 $9,924.01 (session -46.84) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,525.95 | ▼ 09:30 equity $9,850.29 vs yday $9,877.17 (-26.88) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 21 | $318.04 | $2.12 | $-137.94 | $8,202.67 | ▼ -137.94 after sell → book $9,848.17; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 7 | $133.00 | $2.03 | $-65.36 | $9,131.64 | ▼ -65.36 after sell → book $9,846.14; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 2 | $357.25 | $2.02 | $-90.35 | $9,844.12 | ▼ -90.35 after sell → book $9,844.12; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,844.12 | ▲ close $9,844.12 vs 09:30 $9,850.29 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,844.12 | ▲ 09:30 equity $9,844.12 vs yday $9,844.12 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 19 | $351.74 | $2.05 | — | $3,159.01 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $6890.89 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,184.40 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $984.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 30 | $32.31 | $2.08 | — | $1,213.02 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $984.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 62 | $15.87 | $2.18 | — | $226.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $984.41 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.90 | ▲ close $10,103.32 vs 09:30 $9,844.12 (session +267.50) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.90 | ▲ 09:30 equity $10,109.36 vs yday $10,103.32 (+6.04) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.90 | ▲ close $10,127.80 vs 09:30 $10,109.36 (session +18.44) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.90 | ▲ 09:30 equity $10,251.70 vs yday $10,127.80 (+123.90) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.90 | ▲ close $10,340.18 vs 09:30 $10,251.70 (session +88.48) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.90 | ▼ 09:30 equity $10,304.43 vs yday $10,340.18 (-35.75) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 19 | $366.23 | $2.11 | $+271.15 | $7,183.16 | ▲ +271.15 after sell → book $10,302.32; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $8,258.08 | ▲ +100.31 after sell → book $10,300.30; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 30 | $35.09 | $2.10 | $+79.22 | $9,308.68 | ▲ +79.22 after sell → book $10,298.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 62 | $15.96 | $2.20 | $+1.21 | $10,296.01 | ▲ +1.21 after sell → book $10,296.01; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,296.01 | ▲ close $10,296.01 vs 09:30 $10,304.43 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.01 | ▲ 09:30 equity $10,296.01 vs yday $10,296.01 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,296.01 | ▲ close $10,296.01 vs 09:30 $10,296.01 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,296.01 | ▲ 09:30 equity $10,296.01 vs yday $10,296.01 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 43 | $164.43 | $2.12 | — | $3,223.40 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7207.21 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $2,252.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1029.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 68 | $15.01 | $2.19 | — | $1,229.84 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1029.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 485 | $2.12 | $6.26 | — | $195.39 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1029.60 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.39 | ▼ close $9,682.23 vs 09:30 $10,296.01 (session -601.21) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.39 | ▼ 09:30 equity $9,327.90 vs yday $9,682.23 (-354.33) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.39 | ▲ close $9,489.49 vs 09:30 $9,327.90 (session +161.59) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.39 | ▼ 09:30 equity $9,425.63 vs yday $9,489.49 (-63.86) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.39 | ▼ close $9,329.90 vs 09:30 $9,425.63 (session -95.73) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.39 | ▼ 09:30 equity $9,178.48 vs yday $9,329.90 (-151.42) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 43 | $140.03 | $2.18 | $-1053.50 | $6,214.50 | ▼ -1,053.50 after sell → book $9,176.30; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 4 | $253.34 | $2.02 | $+40.66 | $7,225.84 | ▲ +40.66 after sell → book $9,174.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 485 | $1.84 | $6.35 | $-148.40 | $8,111.89 | ▼ -148.40 after sell → book $9,167.93; vs 09:30 mark -6.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 216 | $26.27 | $2.79 | — | $2,434.78 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $5678.32 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $1,297.76 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1216.78 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 30 | $39.99 | $2.08 | — | $95.98 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1216.78 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.98 | ▼ close $9,154.74 vs 09:30 $9,178.48 (session -6.32) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.98 | ▲ 09:30 equity $9,166.42 vs yday $9,154.74 (+11.68) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.98 | ▼ close $9,141.98 vs 09:30 $9,166.42 (session -24.44) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.98 | ▲ 09:30 equity $9,250.48 vs yday $9,141.98 (+108.50) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 68 | $15.87 | $2.22 | $+54.07 | $1,172.92 | ▲ +54.07 after sell → book $9,248.26; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 39 | $20.91 | $2.11 | — | $355.32 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $821.04 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 5 | $22.90 | $1.16 | — | $239.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $117.29 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 7 | $14.79 | $1.06 | — | $135.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $117.29 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 8 | $14.07 | $1.15 | — | $21.37 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $117.29 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.37 | ▼ close $8,889.49 vs 09:30 $9,250.48 (session -353.30) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.37 | ▲ 09:30 equity $8,956.88 vs yday $8,889.49 (+67.39) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 216 | $25.94 | $2.87 | $-76.93 | $5,621.54 | ▼ -76.93 after sell → book $8,954.01; vs 09:30 mark -2.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 6 | $180.61 | $2.03 | $-55.40 | $6,703.17 | ▼ -55.40 after sell → book $8,951.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 30 | $35.91 | $2.10 | $-126.58 | $7,778.37 | ▼ -126.58 after sell → book $8,949.88; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 23 | $230.25 | $2.06 | — | $2,480.56 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $5444.86 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 4 | $190.30 | $2.00 | — | $1,717.36 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $777.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 29 | $25.95 | $2.08 | — | $962.73 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $777.84 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 55 | $13.94 | $2.15 | — | $193.88 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $777.84 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.88 | ▼ close $8,699.52 vs 09:30 $8,956.88 (session -242.07) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.88 | ▼ 09:30 equity $8,695.88 vs yday $8,699.52 (-3.64) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.88 | ▲ close $8,731.87 vs 09:30 $8,695.88 (session +36.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.88 | ▲ 09:30 equity $9,703.02 vs yday $8,731.87 (+971.15) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 39 | $21.15 | $2.13 | $+5.13 | $1,016.60 | ▲ +5.13 after sell → book $9,700.89; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `GME` | 5 | $23.94 | $1.23 | $+2.81 | $1,135.07 | ▲ +2.81 after sell → book $9,699.66; vs 09:30 mark -1.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 7 | $15.40 | $1.12 | $+2.09 | $1,241.75 | ▲ +2.09 after sell → book $9,698.54; vs 09:30 mark -1.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 8 | $14.84 | $1.23 | $+3.78 | $1,359.24 | ▲ +3.78 after sell → book $9,697.31; vs 09:30 mark -1.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $570.12 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $951.47 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 17 | $7.95 | $1.40 | — | $433.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $135.92 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 8 | $15.72 | $1.28 | — | $306.52 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $135.92 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 104 | $1.30 | $1.66 | — | $169.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $135.92 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.66 | ▲ close $9,986.82 vs 09:30 $9,703.02 (session +295.86) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.66 | ▼ 09:30 equity $9,727.18 vs yday $9,986.82 (-259.64) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 23 | $274.61 | $2.12 | $+1016.10 | $6,483.57 | ▲ +1,016.10 after sell → book $9,725.06; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 4 | $164.04 | $2.02 | $-109.06 | $7,137.71 | ▼ -109.06 after sell → book $9,723.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 29 | $25.00 | $2.10 | $-31.87 | $7,860.47 | ▼ -31.87 after sell → book $9,720.94; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 55 | $13.07 | $2.17 | $-52.18 | $8,577.14 | ▼ -52.18 after sell → book $9,718.76; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,577.14 | ▲ close $9,750.68 vs 09:30 $9,727.18 (session +31.92) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,480.96 | ▲ 09:30 equity $8,712.48 vs yday $8,711.23 (+1.25) | 09:30 open · cash $8,480.96 (unchanged overnight, no fees) · equity $8,712.48 vs prior close $8,711.23 (+1.25) · 6 name(s) re-marked at the open (per-name table). CMPX×14 yday $1.13 → 09:30 $1.13 +0.00; DGXX×5 yday $4.53 → 09:30 $4.78 +1.25; IVVD×49 yday $0.91 → 09:30 $0.91 +0.00; PGEN×15 yday $7.70 → 09:30 $7.70 +0.00; SGRY×1 yday $14.20 → 09:30 $14.20 +0.00; VERI×13 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `DGXX` | 5 | $4.78 | $0.27 | $+2.00 | $8,504.59 | ▲ +2.00 after sell → book $8,712.20; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 1542 | $3.86 | $19.89 | — | $2,532.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $5953.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 3 | $272.16 | $2.00 | — | $1,714.10 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $850.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 52 | $16.21 | $2.15 | — | $869.03 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $850.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $869.03 | ▼ close $8,545.33 vs 09:30 $8,712.48 (session -142.84) | 16:00 close · cash $869.03 · equity $8,545.33 vs 09:30 $8,712.48 (-167.15; session marks -142.84) · 8 name(s) marked open→close (per-name table). CMPX×14 09:30 $1.14 → close $1.14 -0.00; IVVD×49 09:30 $0.91 → close $0.91 -0.00; PGEN×15 09:30 $7.70 → close $7.70 -0.00; SGRY×1 09:30 $14.20 → close $14.20 -0.00; VERI×13 09:30 $1.33 → close $1.33 +0.00; ZSQR×1542 09:30 $3.86 → close $3.78 -123.36; ILMN×3 09:30 $272.16 → close $270.00 -6.48; SECZ×52 09:30 $16.21 → close $15.96 -13.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FANG` | cash | leftover split 198.29 < 1 share @ 202.70 |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 42.55 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 18.24 < 1 share @ 115.18 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 91.31 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 13.04 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 77.51 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 11.07 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 11.07 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 11.07 < 1 share @ 118.77 |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MPWR` | cash | leftover split 1011.67 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 158.83 < 1 share @ 263.36 |
| 2026-09-04 | `MRX` | cash | leftover split 34.04 < 1 share @ 75.65 |
| 2026-09-04 | `BE` | cash | leftover split 34.04 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 67.18 < 1 share @ 170.85 |
| 2026-09-17 | `GME` | cash | leftover split 14.40 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 14.40 < 1 share @ 238.60 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `MRNA` | cash | leftover split 38.78 < 1 share @ 168.50 |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CTAS` | 4 | 2026-09-23 @ $196.78 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $951.47 |
| `PGEN` | 17 | 2026-09-23 @ $7.95 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $135.92 |
| `SGRY` | 8 | 2026-09-23 @ $15.72 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $135.92 |
| `VERI` | 104 | 2026-09-23 @ $1.30 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $135.92 |
