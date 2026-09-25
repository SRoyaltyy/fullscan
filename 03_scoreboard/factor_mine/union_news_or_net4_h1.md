# Factor mine action — `union_news_or_net4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-20.95%** ($7,906) · signal-only (no cash/fees) was -6.67%. Starts YES **0/30**. Fills 140 · skips 35 · realized $-1049.49.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,950.49.

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
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 126 | $13.84 | $2.40 | $+78.39 | $1,762.77 | ▲ +78.39 after sell → book $10,254.64; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $3,461.50 | ▲ +49.81 after sell → book $10,252.63; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 386 | $4.60 | $5.06 | $+101.90 | $5,232.04 | ▲ +101.90 after sell → book $10,247.57; vs 09:30 mark -5.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 85 | $19.57 | $2.27 | $-4.52 | $6,893.22 | ▼ -4.52 after sell → book $10,245.30; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 123 | $13.16 | $2.39 | $-52.72 | $8,509.50 | ▼ -52.72 after sell → book $10,242.90; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 108 | $16.05 | $2.35 | $+67.70 | $10,240.56 | ▲ +67.70 after sell → book $10,240.56; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 73 | $46.18 | $2.21 | — | $6,867.21 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3413.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 23 | $142.77 | $2.06 | — | $3,581.44 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3413.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $336.20 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3413.52 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.20 | ▲ close $10,470.90 vs 09:30 $10,257.05 (session +236.65) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.20 | ▲ 09:30 equity $10,588.00 vs yday $10,470.90 (+117.10) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 73 | $48.00 | $2.25 | $+128.40 | $3,837.95 | ▲ +128.40 after sell → book $10,585.75; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 23 | $148.04 | $2.10 | $+117.05 | $7,240.78 | ▲ +117.05 after sell → book $10,583.66; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,581.58 | ▲ +95.57 after sell → book $10,581.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,581.58 | ▲ close $10,581.58 vs 09:30 $10,588.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,581.58 | ▲ 09:30 equity $10,581.58 vs yday $10,581.58 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,581.58 | ▲ close $10,581.58 vs 09:30 $10,581.58 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,581.58 | ▲ 09:30 equity $10,581.58 vs yday $10,581.58 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 16 | $91.01 | $2.04 | — | $9,123.38 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1511.65 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 33 | $44.76 | $2.09 | — | $7,644.22 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1511.65 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 612 | $2.47 | $7.89 | — | $6,124.68 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1511.65 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 25 | $58.73 | $2.06 | — | $4,654.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1511.65 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 94 | $16.00 | $2.27 | — | $3,148.09 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1511.65 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 10 | $150.14 | $2.02 | — | $1,644.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1511.65 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 56 | $26.57 | $2.16 | — | $154.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1511.65 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.60 | ▼ close $10,382.61 vs 09:30 $10,581.58 (session -178.44) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.60 | ▲ 09:30 equity $10,621.06 vs yday $10,382.61 (+238.45) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 16 | $95.72 | $2.06 | $+71.26 | $1,684.06 | ▲ +71.26 after sell → book $10,619.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 33 | $44.52 | $2.11 | $-12.12 | $3,151.10 | ▼ -12.12 after sell → book $10,616.88; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 94 | $17.66 | $2.30 | $+151.47 | $4,808.84 | ▲ +151.47 after sell → book $10,614.58; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 10 | $133.11 | $2.04 | $-174.36 | $6,137.90 | ▼ -174.36 after sell → book $10,612.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 56 | $26.25 | $2.18 | $-22.26 | $7,605.72 | ▼ -22.26 after sell → book $10,610.36; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,409.40 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1267.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,140.40 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1267.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,876.28 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1267.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $2,609.49 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1267.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 391 | $3.24 | $5.04 | — | $1,337.61 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1267.62 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 108 | $11.70 | $2.31 | — | $71.70 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1267.62 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.70 | ▼ close $10,437.59 vs 09:30 $10,621.06 (session -156.91) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.70 | ▼ 09:30 equity $10,398.72 vs yday $10,437.59 (-38.87) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 612 | $2.40 | $8.01 | $-58.74 | $1,532.49 | ▼ -58.74 after sell → book $10,390.71; vs 09:30 mark -8.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,735.55 | ▲ +6.74 after sell → book $10,388.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $4,064.50 | ▲ +59.95 after sell → book $10,386.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,372.37 | ▲ +43.74 after sell → book $10,384.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 146 | $8.00 | $2.46 | $-101.25 | $6,537.90 | ▼ -101.25 after sell → book $10,382.10; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 391 | $2.99 | $5.12 | $-107.91 | $7,701.88 | ▼ -107.91 after sell → book $10,376.99; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 108 | $11.17 | $2.34 | $-61.90 | $8,905.89 | ▼ -61.90 after sell → book $10,374.64; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,905.89 | ▼ close $10,332.77 vs 09:30 $10,398.72 (session -41.87) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,905.89 | ▲ 09:30 equity $10,354.14 vs yday $10,332.77 (+21.37) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 25 | $57.93 | $2.09 | $-24.15 | $10,352.06 | ▼ -24.15 after sell → book $10,352.06; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 29 | $118.52 | $2.08 | — | $6,912.90 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3450.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 44 | $77.13 | $2.12 | — | $3,517.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3450.69 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 98 | $35.05 | $2.28 | — | $79.87 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3450.69 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.87 | ▲ close $10,626.76 vs 09:30 $10,354.14 (session +281.19) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.87 | ▼ 09:30 equity $10,543.63 vs yday $10,626.76 (-83.13) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 29 | $119.80 | $2.11 | $+32.93 | $3,551.96 | ▲ +32.93 after sell → book $10,541.52; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 44 | $79.34 | $2.16 | $+92.96 | $7,040.76 | ▲ +92.96 after sell → book $10,539.36; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 98 | $35.70 | $2.33 | $+59.09 | $10,537.03 | ▲ +59.09 after sell → book $10,537.03; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 19 | $267.02 | $2.05 | — | $5,461.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5268.52 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 44 | $118.50 | $2.12 | — | $245.48 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5268.52 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.48 | ▼ close $10,526.31 vs 09:30 $10,543.63 (session -6.55) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.48 | ▲ 09:30 equity $10,548.73 vs yday $10,526.31 (+22.42) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 19 | $267.23 | $2.10 | $-0.15 | $5,320.75 | ▼ -0.15 after sell → book $10,546.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 9 | $81.65 | $2.02 | — | $4,583.89 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $760.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 25 | $29.83 | $2.06 | — | $3,836.07 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $760.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,196.32 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $760.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,525.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $760.11 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $2,000.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $760.11 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,000.80 | ▼ close $10,402.93 vs 09:30 $10,548.73 (session -133.63) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,000.80 | ▲ 09:30 equity $10,406.23 vs yday $10,402.93 (+3.30) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 44 | $115.66 | $2.17 | $-129.25 | $7,087.67 | ▼ -129.25 after sell → book $10,404.06; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 9 | $79.27 | $2.04 | $-25.47 | $7,799.06 | ▼ -25.47 after sell → book $10,402.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 25 | $30.50 | $2.08 | $+12.60 | $8,559.48 | ▲ +12.60 after sell → book $10,399.94; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $9,193.52 | ▼ -5.71 after sell → book $10,397.92; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,873.58 | ▲ +9.48 after sell → book $10,395.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,573.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1410.51 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,296.08 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1410.51 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,092.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1410.51 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,784.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1410.51 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,581.70 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1410.51 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $2,197.78 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1410.51 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 292 | $4.82 | $3.77 | — | $786.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1410.51 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $786.57 | ▼ close $10,062.68 vs 09:30 $10,406.23 (session -317.32) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $786.57 | ▼ 09:30 equity $10,062.52 vs yday $10,062.68 (-0.16) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $1,299.98 | ▼ -11.53 after sell → book $10,060.50; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,587.91 | ▼ -11.70 after sell → book $10,058.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,776.58 | ▼ -89.19 after sell → book $10,056.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,909.88 | ▼ -69.96 after sell → book $10,054.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,169.77 | ▼ -48.14 after sell → book $10,052.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $7,337.57 | ▼ -35.31 after sell → book $10,050.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $8,643.73 | ▼ -77.75 after sell → book $10,048.25; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 292 | $4.81 | $3.83 | $-10.51 | $10,044.42 | ▼ -10.51 after sell → book $10,044.42; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.42 | ▲ close $10,044.42 vs 09:30 $10,062.52 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.42 | ▲ 09:30 equity $10,044.42 vs yday $10,044.42 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.42 | ▲ close $10,044.42 vs 09:30 $10,044.42 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.42 | ▲ 09:30 equity $10,044.42 vs yday $10,044.42 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.42 | ▲ close $10,044.42 vs 09:30 $10,044.42 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.42 | ▲ 09:30 equity $10,044.42 vs yday $10,044.42 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,635.46 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1434.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,660.84 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1434.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 44 | $32.31 | $2.12 | — | $6,237.08 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1434.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 90 | $15.87 | $2.26 | — | $4,806.52 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1434.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 60 | $23.88 | $2.17 | — | $3,371.55 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1434.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $1,963.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1434.92 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 30 | $47.60 | $2.08 | — | $532.98 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1434.92 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $532.98 | ▲ close $10,448.86 vs 09:30 $10,044.42 (session +419.06) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $532.98 | ▼ 09:30 equity $10,377.54 vs yday $10,448.86 (-71.32) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $1,969.75 | ▲ +27.81 after sell → book $10,375.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $2,995.30 | ▲ +50.93 after sell → book $10,373.50; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 44 | $33.46 | $2.14 | $+46.33 | $4,465.39 | ▲ +46.33 after sell → book $10,371.35; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 60 | $23.84 | $2.19 | $-6.76 | $5,893.60 | ▼ -6.76 after sell → book $10,369.16; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $7,275.64 | ▼ -26.45 after sell → book $10,367.14; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 30 | $53.85 | $2.10 | $+183.32 | $8,889.04 | ▲ +183.32 after sell → book $10,365.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,780.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2222.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 29 | $75.65 | $2.08 | — | $4,584.22 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2222.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,450.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $2222.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1145 | $1.94 | $14.77 | — | $214.75 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2222.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $214.75 | ▲ close $10,466.20 vs 09:30 $10,377.54 (session +122.04) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $214.75 | ▲ 09:30 equity $10,668.61 vs yday $10,466.20 (+202.41) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 90 | $16.74 | $2.29 | $+73.75 | $1,719.07 | ▲ +73.75 after sell → book $10,666.33; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,746.79 | ▼ -81.17 after sell → book $10,664.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 29 | $78.84 | $2.11 | $+88.33 | $6,031.04 | ▲ +88.33 after sell → book $10,662.18; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $8,438.83 | ▲ +274.40 after sell → book $10,660.13; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1145 | $1.94 | $14.98 | $-29.75 | $10,645.16 | ▼ -29.75 after sell → book $10,645.16; vs 09:30 mark -14.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,645.16 | ▲ close $10,645.16 vs 09:30 $10,668.61 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,645.16 | ▲ 09:30 equity $10,645.16 vs yday $10,645.16 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,645.16 | ▲ close $10,645.16 vs 09:30 $10,645.16 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,645.16 | ▲ 09:30 equity $10,645.16 vs yday $10,645.16 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,645.16 | ▲ close $10,645.16 vs 09:30 $10,645.16 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,645.16 | ▲ 09:30 equity $10,645.16 vs yday $10,645.16 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 64 | $164.43 | $2.18 | — | $119.45 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10645.16 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.45 | ▼ close $9,737.37 vs 09:30 $10,645.16 (session -905.60) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.45 | ▼ 09:30 equity $9,170.33 vs yday $9,737.37 (-567.04) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 64 | $141.42 | $2.27 | $-1477.09 | $9,168.07 | ▼ -1,477.09 after sell → book $9,168.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,168.07 | ▲ close $9,168.07 vs 09:30 $9,170.33 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,168.07 | ▲ 09:30 equity $9,168.07 vs yday $9,168.07 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,168.07 | ▲ close $9,168.07 vs 09:30 $9,168.07 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,168.07 | ▲ 09:30 equity $9,168.07 vs yday $9,168.07 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 116 | $26.27 | $2.34 | — | $6,118.41 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3056.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 16 | $189.17 | $2.04 | — | $3,089.65 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $3056.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 76 | $39.99 | $2.22 | — | $48.19 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3056.02 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.19 | ▼ close $8,990.23 vs 09:30 $9,168.07 (session -171.24) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.19 | ▲ 09:30 equity $9,024.27 vs yday $8,990.23 (+34.04) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 116 | $26.51 | $2.38 | $+23.12 | $3,120.97 | ▲ +23.12 after sell → book $9,021.89; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 16 | $190.35 | $2.07 | $+14.77 | $6,164.50 | ▲ +14.77 after sell → book $9,019.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 76 | $37.57 | $2.25 | $-188.39 | $9,017.57 | ▼ -188.39 after sell → book $9,017.57; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 13 | $170.85 | $2.03 | — | $6,794.49 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $2254.39 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 142 | $15.81 | $2.42 | — | $4,547.05 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2254.39 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 101 | $22.12 | $2.29 | — | $2,310.64 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $2254.39 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 9 | $238.60 | $2.02 | — | $161.22 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; ret5=-11.6; leftover $2254.39 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.22 | ▲ close $9,160.78 vs 09:30 $9,024.27 (session +151.97) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.22 | ▲ 09:30 equity $9,229.15 vs yday $9,160.78 (+68.37) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 13 | $182.33 | $2.06 | $+145.15 | $2,529.45 | ▲ +145.15 after sell → book $9,227.09; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 142 | $15.87 | $2.46 | $+3.65 | $4,780.54 | ▲ +3.65 after sell → book $9,224.64; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 9 | $236.80 | $2.04 | $-20.26 | $6,909.69 | ▼ -20.26 after sell → book $9,222.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 110 | $20.91 | $2.32 | — | $4,607.27 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2303.23 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 155 | $14.79 | $2.46 | — | $2,312.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2303.23 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 163 | $14.07 | $2.48 | — | $16.48 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2303.23 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.48 | ▼ close $9,103.13 vs 09:30 $9,229.15 (session -112.21) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.48 | ▲ 09:30 equity $9,224.36 vs yday $9,103.13 (+121.23) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 101 | $22.78 | $2.33 | $+62.04 | $2,314.93 | ▲ +62.04 after sell → book $9,222.03; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 110 | $21.65 | $2.36 | $+76.72 | $4,694.07 | ▲ +76.72 after sell → book $9,219.67; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 155 | $14.58 | $2.50 | $-37.50 | $6,951.47 | ▼ -37.50 after sell → book $9,217.17; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 163 | $13.90 | $2.52 | $-32.71 | $9,214.65 | ▼ -32.71 after sell → book $9,214.65; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 13 | $230.25 | $2.03 | — | $6,219.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $3071.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 16 | $190.30 | $2.04 | — | $3,172.53 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $3071.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 118 | $25.95 | $2.34 | — | $108.09 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $3071.55 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.09 | ▼ close $8,932.97 vs 09:30 $9,224.36 (session -275.27) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.09 | ▲ 09:30 equity $8,932.97 vs yday $8,932.97 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.09 | ▲ close $8,932.97 vs 09:30 $8,932.97 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.09 | ▲ 09:30 equity $9,501.03 vs yday $8,932.97 (+568.06) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 13 | $266.50 | $2.07 | $+467.15 | $3,570.52 | ▲ +467.15 after sell → book $9,498.96; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 16 | $174.50 | $2.07 | $-256.91 | $6,360.45 | ▼ -256.91 after sell → book $9,496.89; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 118 | $26.58 | $2.39 | $+69.61 | $9,494.50 | ▲ +69.61 after sell → book $9,494.50; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 12 | $196.78 | $2.03 | — | $7,131.12 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $2373.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 298 | $7.95 | $3.84 | — | $4,758.17 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $2373.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 150 | $15.72 | $2.44 | — | $2,397.73 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $2373.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1825 | $1.30 | $23.54 | — | $1.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $2373.63 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.69 | ▼ close $9,060.70 vs 09:30 $9,501.03 (session -401.95) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.69 | ▼ 09:30 equity $8,982.80 vs yday $9,060.70 (-77.90) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 12 | $192.26 | $2.05 | $-58.32 | $2,306.76 | ▼ -58.32 after sell → book $8,980.75; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 298 | $7.38 | $3.91 | $-177.62 | $4,502.08 | ▼ -177.62 after sell → book $8,976.83; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 150 | $14.38 | $2.48 | $-205.92 | $6,656.60 | ▼ -205.92 after sell → book $8,974.35; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1825 | $1.27 | $23.86 | $-102.16 | $8,950.49 | ▼ -102.16 after sell → book $8,950.49; vs 09:30 mark -23.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,950.49 | ▲ close $8,950.49 vs 09:30 $8,982.80 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,933.64 | ▲ 09:30 equity $7,933.64 vs yday $7,933.64 (+0.00) | 09:30 open · cash $7,933.64 · no holdings · equity $7,933.64 vs prior close $7,933.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 513 | $3.86 | $6.62 | — | $5,946.84 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1983.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 7 | $272.16 | $2.01 | — | $4,039.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1983.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 122 | $16.21 | $2.36 | — | $2,059.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1983.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $283.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1983.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.74 | ▼ close $7,905.53 vs 09:30 $7,933.64 (session -15.13) | 16:00 close · cash $283.74 · equity $7,905.53 vs 09:30 $7,933.64 (-28.11; session marks -15.13) · 4 name(s) marked open→close (per-name table). ZSQR×513 09:30 $3.86 → close $3.78 -41.04; ILMN×7 09:30 $272.16 → close $270.00 -15.12; SECZ×122 09:30 $16.21 → close $15.96 -30.50; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 760.11 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 760.11 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
