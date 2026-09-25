# Factor mine action — `union_news_or_net5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 5

Cash book **-19.10%** ($8,090) · signal-only (no cash/fees) was -10.75%. Starts YES **1/30**. Fills 108 · skips 18 · realized $-1136.92.

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
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 5.
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
- **Gate** `news_or_headline=True,cam_net_min=5` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,863.07.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 379 | $13.18 | $4.89 | — | $4,999.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $5000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 3 | $1646.93 | $2.00 | — | $57.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $5000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,256.11 vs 09:30 $10,000.00 (session +263.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $10,404.70 vs yday $10,256.11 (+148.59) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 379 | $13.84 | $4.99 | $+240.26 | $5,297.47 | ▲ +240.26 after sell → book $10,399.70; vs 09:30 mark -5.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 3 | $1700.74 | $2.05 | $+157.40 | $10,397.65 | ▲ +157.40 after sell → book $10,397.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,404.70 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,397.65 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,397.65 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 28 | $91.01 | $2.07 | — | $7,847.30 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2599.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 58 | $44.76 | $2.16 | — | $5,249.05 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2599.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 1052 | $2.47 | $13.57 | — | $2,637.04 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2599.41 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 44 | $58.73 | $2.12 | — | $50.80 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2599.41 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.80 | ▲ close $10,392.26 vs 09:30 $10,397.65 (session +14.54) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.80 | ▲ 09:30 equity $10,539.24 vs yday $10,392.26 (+146.98) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 28 | $95.72 | $2.11 | $+127.70 | $2,728.86 | ▲ +127.70 after sell → book $10,537.14; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 58 | $44.52 | $2.19 | $-18.28 | $5,308.82 | ▼ -18.28 after sell → book $10,534.94; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 7 | $119.43 | $2.01 | — | $4,470.80 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $884.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 7 | $115.18 | $2.01 | — | $3,662.53 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $884.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 11 | $78.88 | $2.02 | — | $2,792.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $884.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 102 | $8.66 | $2.30 | — | $1,907.21 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $884.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 273 | $3.24 | $3.52 | — | $1,019.17 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $884.80 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 75 | $11.70 | $2.21 | — | $139.45 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $884.80 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.45 | ▼ close $10,362.28 vs 09:30 $10,539.24 (session -158.58) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.45 | ▼ 09:30 equity $10,310.41 vs yday $10,362.28 (-51.87) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 1052 | $2.40 | $13.77 | $-100.98 | $2,650.49 | ▼ -100.98 after sell → book $10,296.65; vs 09:30 mark -13.76 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 7 | $120.51 | $2.03 | $+3.52 | $3,492.03 | ▲ +3.52 after sell → book $10,294.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 7 | $121.00 | $2.03 | $+36.70 | $4,337.00 | ▲ +36.70 after sell → book $10,292.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 11 | $81.87 | $2.04 | $+28.82 | $5,235.52 | ▲ +28.82 after sell → book $10,290.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 102 | $8.00 | $2.32 | $-71.94 | $6,049.20 | ▼ -71.94 after sell → book $10,288.22; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 273 | $2.99 | $3.58 | $-75.35 | $6,861.89 | ▼ -75.35 after sell → book $10,284.64; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 75 | $11.17 | $2.24 | $-44.20 | $7,697.41 | ▼ -44.20 after sell → book $10,282.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,697.41 | ▼ close $10,208.71 vs 09:30 $10,310.41 (session -73.70) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,697.41 | ▲ 09:30 equity $10,246.33 vs yday $10,208.71 (+37.62) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 44 | $57.93 | $2.15 | $-39.47 | $10,244.17 | ▼ -39.47 after sell → book $10,244.17; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 28 | $118.52 | $2.07 | — | $6,923.54 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3414.72 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 44 | $77.13 | $2.12 | — | $3,527.70 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3414.72 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 97 | $35.05 | $2.28 | — | $125.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3414.72 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.57 | ▲ close $10,513.84 vs 09:30 $10,246.33 (session +276.14) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.57 | ▼ 09:30 equity $10,433.83 vs yday $10,513.84 (-80.01) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 28 | $119.80 | $2.11 | $+31.66 | $3,477.86 | ▲ +31.66 after sell → book $10,431.72; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 44 | $79.34 | $2.16 | $+92.96 | $6,966.66 | ▲ +92.96 after sell → book $10,429.56; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 97 | $35.70 | $2.32 | $+58.44 | $10,427.23 | ▲ +58.44 after sell → book $10,427.23; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 19 | $267.02 | $2.05 | — | $5,351.80 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5213.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 43 | $118.50 | $2.12 | — | $254.19 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5213.62 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.19 | ▼ close $10,416.82 vs 09:30 $10,433.83 (session -6.25) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $254.19 | ▲ 09:30 equity $10,438.67 vs yday $10,416.82 (+21.85) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 19 | $267.23 | $2.10 | $-0.15 | $5,329.46 | ▼ -0.15 after sell → book $10,436.57; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 9 | $81.65 | $2.02 | — | $4,592.59 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $761.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 25 | $29.83 | $2.06 | — | $3,844.78 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $761.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,205.02 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $761.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,534.44 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $761.35 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $2,009.50 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $761.35 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,009.50 | ▼ close $10,296.79 vs 09:30 $10,438.67 (session -129.70) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,009.50 | ▲ 09:30 equity $10,299.27 vs yday $10,296.79 (+2.48) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 43 | $115.66 | $2.17 | $-126.41 | $6,980.72 | ▼ -126.41 after sell → book $10,297.11; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 9 | $79.27 | $2.04 | $-25.47 | $7,692.11 | ▼ -25.47 after sell → book $10,295.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 25 | $30.50 | $2.08 | $+12.60 | $8,452.52 | ▲ +12.60 after sell → book $10,292.98; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $9,086.57 | ▼ -5.71 after sell → book $10,290.97; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,766.63 | ▲ +9.48 after sell → book $10,288.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,142.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1627.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,581.19 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1627.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $4,977.51 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1627.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,669.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1627.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,226.16 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1627.77 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 49 | $32.90 | $2.14 | — | $611.92 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1627.77 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $611.92 | ▼ close $9,906.95 vs 09:30 $10,299.27 (session -369.83) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $611.92 | ▼ 09:30 equity $9,900.89 vs yday $9,906.95 (-6.06) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $1,125.33 | ▼ -11.53 after sell → book $9,898.88; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $2,735.75 | ▼ -13.63 after sell → book $9,896.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $132.30 | $2.04 | $-108.13 | $4,189.00 | ▼ -108.13 after sell → book $9,894.80; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $5,700.74 | ▼ -91.95 after sell → book $9,892.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,960.63 | ▼ -48.14 after sell → book $9,890.77; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 6 | $233.97 | $2.03 | $-41.57 | $8,362.39 | ▼ -41.57 after sell → book $9,888.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 49 | $31.15 | $2.16 | $-90.05 | $9,886.58 | ▼ -90.05 after sell → book $9,886.58; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.58 | ▲ close $9,886.58 vs 09:30 $9,900.89 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.58 | ▲ 09:30 equity $9,886.58 vs yday $9,886.58 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.58 | ▲ close $9,886.58 vs 09:30 $9,886.58 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.58 | ▲ 09:30 equity $9,886.58 vs yday $9,886.58 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.58 | ▲ close $9,886.58 vs 09:30 $9,886.58 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.58 | ▲ 09:30 equity $9,886.58 vs yday $9,886.58 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,477.61 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1647.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,016.69 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1647.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 50 | $32.31 | $2.14 | — | $5,399.05 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1647.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 103 | $15.87 | $2.30 | — | $3,762.14 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1647.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 69 | $23.88 | $2.20 | — | $2,112.22 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1647.76 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $703.72 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1647.76 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $703.72 | ▲ close $10,139.01 vs 09:30 $9,886.58 (session +265.07) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $703.72 | ▼ 09:30 equity $10,075.08 vs yday $10,139.01 (-63.93) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $2,140.50 | ▲ +27.81 after sell → book $10,073.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $3,679.82 | ▲ +78.39 after sell → book $10,071.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 50 | $33.46 | $2.16 | $+53.20 | $5,350.66 | ▲ +53.20 after sell → book $10,068.88; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 69 | $23.84 | $2.22 | $-7.18 | $6,993.39 | ▼ -7.18 after sell → book $10,066.65; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $8,375.44 | ▼ -26.45 after sell → book $10,064.64; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 10 | $263.36 | $2.02 | — | $5,739.82 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2791.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 36 | $75.65 | $2.10 | — | $3,014.32 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2791.81 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 11 | $236.82 | $2.02 | — | $407.28 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+8.1; leftover $2791.81 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $407.28 | ▲ close $10,278.80 vs 09:30 $10,075.08 (session +220.30) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $407.28 | ▲ 09:30 equity $10,452.30 vs yday $10,278.80 (+173.50) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 103 | $16.74 | $2.33 | $+84.98 | $2,129.17 | ▲ +84.98 after sell → book $10,449.97; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 10 | $253.72 | $2.05 | $-100.47 | $4,664.32 | ▼ -100.47 after sell → book $10,447.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 36 | $78.84 | $2.13 | $+110.61 | $7,500.43 | ▲ +110.61 after sell → book $10,445.79; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 11 | $267.76 | $2.06 | $+336.26 | $10,443.73 | ▲ +336.26 after sell → book $10,443.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.73 | ▲ close $10,443.73 vs 09:30 $10,452.30 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.73 | ▲ 09:30 equity $10,443.73 vs yday $10,443.73 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.73 | ▲ close $10,443.73 vs 09:30 $10,443.73 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.73 | ▲ 09:30 equity $10,443.73 vs yday $10,443.73 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.73 | ▲ close $10,443.73 vs 09:30 $10,443.73 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.73 | ▲ 09:30 equity $10,443.73 vs yday $10,443.73 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 63 | $164.43 | $2.18 | — | $82.46 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10443.73 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.46 | ▼ close $9,550.10 vs 09:30 $10,443.73 (session -891.45) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.46 | ▼ 09:30 equity $8,991.92 vs yday $9,550.10 (-558.18) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 63 | $141.42 | $2.26 | $-1454.07 | $8,989.66 | ▼ -1,454.07 after sell → book $8,989.66; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,989.66 | ▲ close $8,989.66 vs 09:30 $8,991.92 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,989.66 | ▲ 09:30 equity $8,989.66 vs yday $8,989.66 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,989.66 | ▲ close $8,989.66 vs 09:30 $8,989.66 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,989.66 | ▲ 09:30 equity $8,989.66 vs yday $8,989.66 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 114 | $26.27 | $2.33 | — | $5,992.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2996.55 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $3,152.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2996.55 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 74 | $39.99 | $2.21 | — | $191.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2996.55 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.49 | ▼ close $8,819.19 vs 09:30 $8,989.66 (session -163.89) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.49 | ▲ 09:30 equity $8,849.06 vs yday $8,819.19 (+29.87) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 114 | $26.51 | $2.38 | $+22.65 | $3,211.25 | ▲ +22.65 after sell → book $8,846.68; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 15 | $190.35 | $2.07 | $+13.60 | $6,064.44 | ▲ +13.60 after sell → book $8,844.62; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 74 | $37.57 | $2.25 | $-183.54 | $8,842.37 | ▼ -183.54 after sell → book $8,842.37; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 17 | $170.85 | $2.04 | — | $5,935.88 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $2947.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 186 | $15.81 | $2.55 | — | $2,992.67 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2947.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 133 | $22.12 | $2.39 | — | $48.32 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $2947.46 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.32 | ▲ close $9,055.92 vs 09:30 $8,849.06 (session +220.53) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.32 | ▲ 09:30 equity $9,145.45 vs yday $9,055.92 (+89.53) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 17 | $182.33 | $2.08 | $+191.04 | $3,145.86 | ▲ +191.04 after sell → book $9,143.38; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 186 | $15.87 | $2.60 | $+6.01 | $6,095.07 | ▲ +6.01 after sell → book $9,140.77; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 97 | $20.91 | $2.28 | — | $4,064.52 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2031.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 137 | $14.79 | $2.40 | — | $2,035.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2031.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 144 | $14.07 | $2.42 | — | $7.39 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2031.69 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.39 | ▼ close $9,023.09 vs 09:30 $9,145.45 (session -110.58) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.39 | ▲ 09:30 equity $9,136.24 vs yday $9,023.09 (+113.15) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 133 | $22.78 | $2.44 | $+82.96 | $3,034.69 | ▲ +82.96 after sell → book $9,133.80; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 97 | $21.65 | $2.31 | $+67.19 | $5,132.43 | ▲ +67.19 after sell → book $9,131.49; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 137 | $14.58 | $2.44 | $-33.61 | $7,127.45 | ▼ -33.61 after sell → book $9,129.05; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 144 | $13.90 | $2.46 | $-29.36 | $9,126.59 | ▼ -29.36 after sell → book $9,126.59; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 19 | $230.25 | $2.05 | — | $4,749.79 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+12.5; leftover $4563.29 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 23 | $190.30 | $2.06 | — | $370.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+10.6; leftover $4563.29 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.83 | ▼ close $8,704.44 vs 09:30 $9,136.24 (session -418.04) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.83 | ▲ 09:30 equity $8,704.44 vs yday $8,704.44 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.83 | ▲ close $8,704.44 vs 09:30 $8,704.44 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.83 | ▲ 09:30 equity $9,447.83 vs yday $8,704.44 (+743.39) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 19 | $266.50 | $2.10 | $+684.61 | $5,432.24 | ▲ +684.61 after sell → book $9,445.74; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 23 | $174.50 | $2.10 | $-367.56 | $9,443.63 | ▼ -367.56 after sell → book $9,443.63; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 15 | $196.78 | $2.04 | — | $6,489.90 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $3147.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 395 | $7.95 | $5.10 | — | $3,344.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $3147.88 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 200 | $15.72 | $2.59 | — | $197.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $3147.88 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $197.96 | ▼ close $8,928.31 vs 09:30 $9,447.83 (session -505.60) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $197.96 | ▼ 09:30 equity $8,872.96 vs yday $8,928.31 (-55.35) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 15 | $192.26 | $2.07 | $-71.90 | $3,079.80 | ▼ -71.90 after sell → book $8,870.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 395 | $7.38 | $5.18 | $-235.43 | $5,989.71 | ▼ -235.43 after sell → book $8,865.71; vs 09:30 mark -5.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 200 | $14.38 | $2.65 | $-273.24 | $8,863.07 | ▼ -273.24 after sell → book $8,863.07; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,863.07 | ▲ close $8,863.07 vs 09:30 $8,872.96 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,120.06 | ▲ 09:30 equity $8,120.06 vs yday $8,120.06 (+0.00) | 09:30 open · cash $8,120.06 · no holdings · equity $8,120.06 vs prior close $8,120.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 525 | $3.86 | $6.77 | — | $6,086.79 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $2030.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 7 | $272.16 | $2.01 | — | $4,179.66 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $2030.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 125 | $16.21 | $2.37 | — | $2,151.04 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $2030.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $375.05 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $2030.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $375.05 | ▼ close $8,090.08 vs 09:30 $8,120.06 (session -16.84) | 16:00 close · cash $375.05 · equity $8,090.08 vs 09:30 $8,120.06 (-29.98; session marks -16.84) · 4 name(s) marked open→close (per-name table). ZSQR×525 09:30 $3.86 → close $3.78 -42.00; ILMN×7 09:30 $272.16 → close $270.00 -15.12; SECZ×125 09:30 $16.21 → close $15.96 -31.25; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 761.35 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 761.35 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
