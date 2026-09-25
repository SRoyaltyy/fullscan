# Factor mine action — `union_news_or_net4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-23.73%** ($7,627) · signal-only (no cash/fees) was -11.70%. Starts YES **2/30**. Fills 95 · skips 143 · realized $-1899.45.

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
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,950.77.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 126 | $13.18 | $2.37 | — | $8,336.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $6,688.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 386 | $4.31 | $4.98 | — | $5,019.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 85 | $19.57 | $2.25 | — | $3,353.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 123 | $13.55 | $2.36 | — | $1,684.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 108 | $15.38 | $2.31 | — | $21.33 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1666.67 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▲ close $10,124.06 vs 09:30 $10,000.00 (session +140.32) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▲ 09:30 equity $10,257.05 vs yday $10,124.06 (+132.99) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▼ close $10,234.29 vs 09:30 $10,257.05 (session -22.75) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▼ 09:30 equity $10,030.28 vs yday $10,234.29 (-204.01) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▼ close $9,979.59 vs 09:30 $10,030.28 (session -50.69) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▲ 09:30 equity $10,009.69 vs yday $9,979.59 (+30.10) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 126 | $12.90 | $2.40 | $-40.05 | $1,644.33 | ▼ -40.05 after sell → book $10,007.28; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $3,324.72 | ▲ +31.47 after sell → book $10,005.27; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 386 | $4.79 | $5.06 | $+175.24 | $5,168.60 | ▲ +175.24 after sell → book $10,000.21; vs 09:30 mark -5.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 85 | $19.58 | $2.27 | $-3.67 | $6,830.63 | ▼ -3.67 after sell → book $9,997.94; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 123 | $13.01 | $2.39 | $-71.17 | $8,428.47 | ▼ -71.17 after sell → book $9,995.55; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VELO` | 108 | $14.51 | $2.34 | $-98.62 | $9,993.20 | ▼ -98.62 after sell → book $9,993.20; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,993.20 | ▲ close $9,993.20 vs 09:30 $10,009.69 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,993.20 | ▲ 09:30 equity $9,993.20 vs yday $9,993.20 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 15 | $91.01 | $2.04 | — | $8,626.02 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1427.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 31 | $44.76 | $2.08 | — | $7,236.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1427.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 577 | $2.47 | $7.44 | — | $5,803.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1427.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 24 | $58.73 | $2.06 | — | $4,392.16 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1427.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 89 | $16.00 | $2.26 | — | $2,965.90 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1427.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 9 | $150.14 | $2.02 | — | $1,612.62 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1427.60 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 53 | $26.57 | $2.15 | — | $202.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1427.60 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.26 | ▼ close $9,811.61 vs 09:30 $9,993.20 (session -161.54) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.26 | ▲ 09:30 equity $10,037.63 vs yday $9,811.61 (+226.02) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 3 | $8.66 | $0.27 | — | $176.02 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $33.71 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 10 | $3.24 | $0.35 | — | $143.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $33.71 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $119.62 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $33.71 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.62 | ▲ close $10,121.62 vs 09:30 $10,037.63 (session +84.85) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.62 | ▼ 09:30 equity $10,082.87 vs yday $10,121.62 (-38.75) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.62 | ▼ close $10,070.09 vs 09:30 $10,082.87 (session -12.78) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.62 | ▼ 09:30 equity $10,046.76 vs yday $10,070.09 (-23.33) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 15 | $95.86 | $2.06 | $+68.66 | $1,555.47 | ▲ +68.66 after sell → book $10,044.71; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 31 | $41.38 | $2.10 | $-108.97 | $2,836.14 | ▼ -108.97 after sell → book $10,042.60; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 577 | $2.38 | $7.55 | $-66.92 | $4,201.85 | ▼ -66.92 after sell → book $10,035.05; vs 09:30 mark -7.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 24 | $57.93 | $2.08 | $-23.35 | $5,590.09 | ▼ -23.35 after sell → book $10,032.97; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 89 | $19.04 | $2.29 | $+266.02 | $7,282.36 | ▲ +266.02 after sell → book $10,030.68; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 9 | $143.50 | $2.04 | $-63.81 | $8,571.83 | ▼ -63.81 after sell → book $10,028.65; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 53 | $26.04 | $2.17 | $-32.41 | $9,949.78 | ▼ -32.41 after sell → book $10,026.48; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,747.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3316.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 43 | $77.13 | $2.12 | — | $3,428.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3316.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 94 | $35.05 | $2.27 | — | $131.98 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3316.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.98 | ▲ close $10,292.84 vs 09:30 $10,046.76 (session +272.83) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.98 | ▼ 09:30 equity $10,213.14 vs yday $10,292.84 (-79.70) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 3 | $8.84 | $0.29 | $-0.02 | $158.21 | ▼ -0.02 after sell → book $10,212.85; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 10 | $2.95 | $0.34 | $-3.60 | $187.36 | ▼ -3.60 after sell → book $10,212.50; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $210.23 | ▼ -0.78 after sell → book $10,212.25; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.23 | ▼ close $9,982.80 vs 09:30 $10,213.14 (session -229.45) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.23 | ▼ 09:30 equity $9,918.99 vs yday $9,982.80 (-63.81) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.23 | ▲ close $10,013.63 vs 09:30 $9,918.99 (session +94.64) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.23 | ▲ 09:30 equity $10,049.87 vs yday $10,013.63 (+36.24) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 27 | $119.19 | $2.11 | $+13.91 | $3,426.25 | ▲ +13.91 after sell → book $10,047.76; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 43 | $78.57 | $2.16 | $+57.64 | $6,802.60 | ▲ +57.64 after sell → book $10,045.60; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 94 | $34.50 | $2.31 | $-56.29 | $10,043.29 | ▼ -56.29 after sell → book $10,043.29; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,068.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1255.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,931.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1255.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,728.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1255.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,525.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1255.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,478.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+7.8; leftover $1255.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $3,226.66 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1255.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 260 | $4.82 | $3.35 | — | $1,970.10 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1255.41 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,970.10 | ▼ close $9,781.21 vs 09:30 $10,049.87 (session -246.60) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,970.10 | ▼ 09:30 equity $9,766.26 vs yday $9,781.21 (-14.95) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,970.10 | ▲ close $9,846.40 vs 09:30 $9,766.26 (session +80.14) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,970.10 | ▼ 09:30 equity $9,715.80 vs yday $9,846.40 (-130.60) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,970.10 | ▼ close $9,638.84 vs 09:30 $9,715.80 (session -76.96) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,970.10 | ▼ 09:30 equity $9,605.83 vs yday $9,638.84 (-33.01) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $2,922.21 | ▼ -23.13 after sell → book $9,603.82; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $3,984.17 | ▼ -74.13 after sell → book $9,601.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $5,053.90 | ▼ -133.53 after sell → book $9,599.76; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,149.18 | ▼ -107.83 after sell → book $9,597.74; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $7,133.96 | ▼ -61.86 after sell → book $9,595.72; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $32.42 | $2.12 | $-22.47 | $8,363.79 | ▼ -22.47 after sell → book $9,593.59; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 260 | $4.73 | $3.41 | $-30.16 | $9,590.18 | ▼ -30.16 after sell → book $9,590.18; vs 09:30 mark -3.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,590.18 | ▲ close $9,590.18 vs 09:30 $9,605.83 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,590.18 | ▲ 09:30 equity $9,590.18 vs yday $9,590.18 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,532.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1370.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,558.35 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1370.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 42 | $32.31 | $2.12 | — | $6,199.21 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1370.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 86 | $15.87 | $2.25 | — | $4,832.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1370.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $23.88 | $2.16 | — | $3,468.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1370.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,763.58 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1370.03 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $1,428.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1370.03 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,428.71 | ▲ close $9,977.70 vs 09:30 $9,590.18 (session +402.10) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,428.71 | ▼ 09:30 equity $9,909.80 vs yday $9,977.70 (-67.90) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $1,163.35 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $357.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 4 | $75.65 | $2.00 | — | $858.75 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $357.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $619.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $357.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 184 | $1.94 | $2.54 | — | $260.44 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $357.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.44 | ▼ close $9,892.90 vs 09:30 $9,909.80 (session -8.37) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.44 | ▲ 09:30 equity $9,947.25 vs yday $9,892.90 (+54.35) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.44 | ▲ close $10,046.39 vs 09:30 $9,947.25 (session +99.14) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.44 | ▲ 09:30 equity $10,081.52 vs yday $10,046.39 (+35.13) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,357.11 | ▲ +39.45 after sell → book $10,079.50; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,432.03 | ▲ +100.31 after sell → book $10,077.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 42 | $35.09 | $2.14 | $+112.51 | $3,903.67 | ▲ +112.51 after sell → book $10,075.34; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 86 | $15.96 | $2.27 | $+3.22 | $5,273.96 | ▲ +3.22 after sell → book $10,073.07; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 57 | $23.22 | $2.18 | $-41.96 | $6,595.32 | ▼ -41.96 after sell → book $10,070.89; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $7,274.63 | ▼ -25.94 after sell → book $10,068.88; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 28 | $56.94 | $2.10 | $+257.35 | $8,866.85 | ▲ +257.35 after sell → book $10,066.78; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,866.85 | ▼ close $10,045.65 vs 09:30 $10,081.52 (session -21.13) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,866.85 | ▼ 09:30 equity $10,035.39 vs yday $10,045.65 (-10.26) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $9,110.19 | ▼ -22.02 after sell → book $10,033.37; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 4 | $75.00 | $2.02 | $-6.62 | $9,408.16 | ▼ -6.62 after sell → book $10,031.35; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $9,666.86 | ▲ +19.88 after sell → book $10,029.34; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 184 | $1.97 | $2.58 | $+0.40 | $10,026.76 | ▲ +0.40 after sell → book $10,026.76; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,026.76 | ▲ close $10,026.76 vs 09:30 $10,035.39 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,026.76 | ▲ 09:30 equity $10,026.76 vs yday $10,026.76 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 60 | $164.43 | $2.17 | — | $158.79 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10026.76 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▼ close $9,175.59 vs 09:30 $10,026.76 (session -849.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▼ 09:30 equity $8,643.99 vs yday $9,175.59 (-531.60) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▲ close $8,846.19 vs 09:30 $8,643.99 (session +202.20) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▼ 09:30 equity $8,766.39 vs yday $8,846.19 (-79.80) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▼ close $8,579.79 vs 09:30 $8,766.39 (session -186.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▼ 09:30 equity $8,560.59 vs yday $8,579.79 (-19.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 60 | $140.03 | $2.25 | $-1468.42 | $8,558.34 | ▼ -1,468.42 after sell → book $8,558.34; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 108 | $26.27 | $2.31 | — | $5,718.87 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2852.78 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $2,879.28 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2852.78 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 71 | $39.99 | $2.20 | — | $37.79 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2852.78 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.79 | ▼ close $8,391.47 vs 09:30 $8,560.59 (session -160.32) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.79 | ▲ 09:30 equity $8,423.59 vs yday $8,391.47 (+32.12) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.79 | ▼ close $8,356.39 vs 09:30 $8,423.59 (session -67.20) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.79 | ▲ 09:30 equity $8,436.26 vs yday $8,356.39 (+79.87) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.79 | ▼ close $8,099.74 vs 09:30 $8,436.26 (session -336.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.79 | ▼ 09:30 equity $8,098.07 vs yday $8,099.74 (-1.67) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 108 | $25.94 | $2.35 | $-40.31 | $2,836.95 | ▼ -40.31 after sell → book $8,095.71; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 15 | $180.61 | $2.07 | $-132.50 | $5,544.04 | ▼ -132.50 after sell → book $8,093.65; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 71 | $35.91 | $2.24 | $-294.12 | $8,091.41 | ▼ -294.12 after sell → book $8,091.41; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 11 | $230.25 | $2.02 | — | $5,556.64 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $2697.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 14 | $190.30 | $2.03 | — | $2,890.41 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $2697.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 103 | $25.95 | $2.30 | — | $215.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $2697.14 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.26 | ▼ close $7,846.55 vs 09:30 $8,098.07 (session -238.51) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.26 | ▲ 09:30 equity $7,846.55 vs yday $7,846.55 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.26 | ▲ close $7,846.55 vs 09:30 $7,846.55 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.26 | ▲ 09:30 equity $8,327.50 vs yday $7,846.55 (+480.95) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 6 | $7.95 | $0.49 | — | $167.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $53.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 3 | $15.72 | $0.48 | — | $119.42 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $53.81 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 41 | $1.30 | $0.66 | — | $65.47 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $53.81 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.47 | ▲ close $8,348.43 vs 09:30 $8,327.50 (session +22.56) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.47 | ▼ 09:30 equity $8,096.71 vs yday $8,348.43 (-251.72) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 11 | $274.61 | $2.06 | $+483.88 | $3,084.12 | ▲ +483.88 after sell → book $8,094.65; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 14 | $164.04 | $2.06 | $-371.73 | $5,378.62 | ▼ -371.73 after sell → book $8,092.59; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 103 | $25.00 | $2.34 | $-103.00 | $7,950.77 | ▼ -103.00 after sell → book $8,090.26; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,950.77 | ▲ close $8,094.10 vs 09:30 $8,096.71 (session +3.84) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,167.56 | ▲ 09:30 equity $7,645.49 vs yday $7,645.49 (-0.00) | 09:30 open · cash $7,167.56 (unchanged overnight, no fees) · equity $7,645.49 vs prior close $7,645.49 (-0.00) · 3 name(s) re-marked at the open (per-name table). PGEN×21 yday $7.70 → 09:30 $7.70 +0.00; SGRY×10 yday $14.20 → 09:30 $14.20 +0.00; VERI×131 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 464 | $3.86 | $5.99 | — | $5,370.53 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,735.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 110 | $16.21 | $2.32 | — | $1,950.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $174.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.15 | ▼ close $7,627.13 vs 09:30 $7,645.49 (session -6.05) | 16:00 close · cash $174.15 · equity $7,627.13 vs 09:30 $7,645.49 (-18.36; session marks -6.05) · 7 name(s) marked open→close (per-name table). PGEN×21 09:30 $7.70 → close $7.70 -0.00; SGRY×10 09:30 $14.20 → close $14.20 -0.00; VERI×131 09:30 $1.33 → close $1.33 +0.00; ZSQR×464 09:30 $3.86 → close $3.78 -37.12; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×110 09:30 $16.21 → close $15.96 -27.50; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.11 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.11 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.11 < 1 share @ 202.70 |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 33.71 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 33.71 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 33.71 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 105.11 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 105.11 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 26.28 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 26.28 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 26.28 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 26.28 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 26.28 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 26.28 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 26.28 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 26.28 < 1 share @ 261.47 |
| 2026-08-28 | `MPWR` | cash | leftover split 1255.41 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 9.45 < 1 share @ 170.85 |
| 2026-09-17 | `AVTR` | cash | leftover split 9.45 < 1 share @ 15.81 |
| 2026-09-17 | `GME` | cash | leftover split 9.45 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 9.45 < 1 share @ 238.60 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 9.45 < 1 share @ 20.91 |
| 2026-09-18 | `GME` | cash | leftover split 9.45 < 1 share @ 22.90 |
| 2026-09-18 | `RARE` | cash | leftover split 9.45 < 1 share @ 14.79 |
| 2026-09-18 | `BHVN` | cash | leftover split 9.45 < 1 share @ 14.07 |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CTAS` | cash | leftover split 53.81 < 1 share @ 196.78 |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 6 | 2026-09-23 @ $7.95 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $53.81 |
| `SGRY` | 3 | 2026-09-23 @ $15.72 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $53.81 |
| `VERI` | 41 | 2026-09-23 @ $1.30 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $53.81 |
