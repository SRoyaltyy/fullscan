# Factor mine action — `union_news_or_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢

Cash book **-14.80%** ($8,520) · signal-only (no cash/fees) was +9.53%. Starts YES **0/30**. Fills 205 · skips 79 · realized $-327.36.

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
- **Gate** `news_or_headline=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,672.66.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 43 | $46.18 | $2.12 | — | $8,150.46 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,149.65 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,120.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $2,109.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2027.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $154.67 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $2027.66 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,973.58 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,718.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 514 | $2.47 | $6.63 | — | $6,442.01 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,206.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,940.40 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,737.27 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,486.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1269.84 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $267.04 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1269.84 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.04 | ▼ close $10,000.25 vs 09:30 $10,158.74 (session -137.32) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.04 | ▲ 09:30 equity $10,194.97 vs yday $10,000.25 (+194.72) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,509.35 | ▲ +57.15 after sell → book $10,192.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,753.81 | ▼ -10.89 after sell → book $10,190.82; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,146.70 | ▲ +126.66 after sell → book $10,188.57; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,209.55 | ▼ -140.29 after sell → book $10,186.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,441.15 | ▼ -19.32 after sell → book $10,184.39; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,658.66 | ▼ -1.80 after sell → book $10,182.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,462.34 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1276.44 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,193.33 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1276.44 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,929.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1276.44 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 147 | $8.66 | $2.43 | — | $2,653.77 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1276.44 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 393 | $3.24 | $5.07 | — | $1,375.38 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1276.44 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $97.76 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1276.44 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.76 | ▼ close $10,014.72 vs 09:30 $10,194.97 (session -151.74) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.76 | ▼ 09:30 equity $9,979.73 vs yday $10,014.72 (-34.99) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 514 | $2.40 | $6.73 | $-49.34 | $1,324.63 | ▼ -49.34 after sell → book $9,973.00; vs 09:30 mark -6.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,527.69 | ▲ +6.74 after sell → book $9,970.96; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,856.65 | ▲ +59.95 after sell → book $9,968.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,164.51 | ▲ +43.74 after sell → book $9,966.86; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 147 | $8.00 | $2.47 | $-101.92 | $6,338.05 | ▼ -101.92 after sell → book $9,964.40; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 393 | $2.99 | $5.14 | $-108.46 | $7,507.97 | ▼ -108.46 after sell → book $9,959.25; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $8,723.16 | ▼ -62.43 after sell → book $9,956.91; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,723.16 | ▼ close $9,921.73 vs 09:30 $9,979.73 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,723.16 | ▲ 09:30 equity $9,939.69 vs yday $9,921.73 (+17.96) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,937.61 | ▼ -20.93 after sell → book $9,937.61; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $118.52 | $2.02 | — | $8,631.87 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1419.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $7,241.49 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1419.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 40 | $35.05 | $2.11 | — | $5,837.38 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1419.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 150 | $9.42 | $2.44 | — | $4,421.94 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1419.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 49 | $28.86 | $2.14 | — | $3,005.66 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1419.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 58 | $24.11 | $2.16 | — | $1,605.11 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=+891.7; leftover $1419.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 162 | $8.72 | $2.48 | — | $190.00 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1419.66 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.00 | ▲ close $10,378.29 vs 09:30 $9,939.69 (session +456.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.00 | ▼ 09:30 equity $10,203.56 vs yday $10,378.29 (-174.73) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $+10.01 | $1,505.75 | ▲ +10.01 after sell → book $10,201.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $2,931.81 | ▲ +35.67 after sell → book $10,199.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 40 | $35.70 | $2.13 | $+21.76 | $4,357.68 | ▲ +21.76 after sell → book $10,197.32; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 150 | $10.07 | $2.48 | $+92.58 | $5,865.70 | ▲ +92.58 after sell → book $10,194.84; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 49 | $27.56 | $2.16 | $-67.99 | $7,213.98 | ▼ -67.99 after sell → book $10,192.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 58 | $26.61 | $2.19 | $+140.65 | $8,755.18 | ▲ +140.65 after sell → book $10,190.50; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 162 | $8.86 | $2.51 | $+17.69 | $10,187.98 | ▲ +17.69 after sell → book $10,187.98; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 6 | $267.02 | $2.01 | — | $8,583.85 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1698.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 14 | $118.50 | $2.03 | — | $6,922.82 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1698.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 151 | $11.22 | $2.44 | — | $5,226.16 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1698.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 204 | $8.29 | $2.63 | — | $3,532.37 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1698.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 97 | $17.41 | $2.28 | — | $1,841.32 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-9.2; leftover $1698.00 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 152 | $11.12 | $2.45 | — | $148.63 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1698.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.63 | ▲ close $10,437.29 vs 09:30 $10,203.56 (session +263.15) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.63 | ▲ 09:30 equity $10,466.17 vs yday $10,437.29 (+28.88) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 6 | $267.23 | $2.03 | $-2.78 | $1,749.98 | ▼ -2.78 after sell → book $10,464.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 151 | $11.38 | $2.48 | $+19.24 | $3,465.88 | ▲ +19.24 after sell → book $10,461.66; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 204 | $9.19 | $2.68 | $+178.29 | $5,337.96 | ▲ +178.29 after sell → book $10,458.98; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 97 | $17.60 | $2.31 | $+13.84 | $7,042.85 | ▲ +13.84 after sell → book $10,456.67; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 152 | $11.52 | $2.49 | $+55.87 | $8,791.40 | ▲ +55.87 after sell → book $10,454.18; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $7,564.62 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1255.91 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $6,595.61 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1255.91 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 42 | $29.83 | $2.12 | — | $5,340.64 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1255.91 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $4,382.00 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1255.91 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $3,265.69 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1255.91 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $2,217.81 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $1255.91 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,217.81 | ▼ close $10,427.27 vs 09:30 $10,466.17 (session -14.76) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,217.81 | ▼ 09:30 equity $10,361.92 vs yday $10,427.27 (-65.35) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 14 | $115.66 | $2.06 | $-43.85 | $3,835.00 | ▼ -43.85 after sell → book $10,359.87; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $5,021.99 | ▼ -39.79 after sell → book $10,357.81; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,939.27 | ▼ -51.73 after sell → book $10,355.80; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 42 | $30.50 | $2.14 | $+23.89 | $7,218.13 | ▲ +23.89 after sell → book $10,353.66; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $8,170.20 | ▼ -6.57 after sell → book $10,351.64; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $9,304.98 | ▲ +18.47 after sell → book $10,349.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,005.34 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1329.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $6,727.48 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1329.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $5,524.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1329.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,216.20 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1329.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,013.09 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1329.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $1,694.98 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1329.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 275 | $4.82 | $3.55 | — | $365.94 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1329.28 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $365.94 | ▼ close $10,019.12 vs 09:30 $10,361.92 (session -314.83) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $365.94 | ▼ 09:30 equity $10,013.23 vs yday $10,019.12 (-5.89) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-19.06 | $1,394.75 | ▼ -19.06 after sell → book $10,011.21; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,682.69 | ▼ -11.70 after sell → book $10,009.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,871.35 | ▼ -89.19 after sell → book $10,007.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,004.65 | ▼ -69.96 after sell → book $10,005.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,264.54 | ▼ -48.14 after sell → book $10,003.12; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $7,432.34 | ▼ -35.31 after sell → book $10,001.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $8,676.21 | ▼ -74.24 after sell → book $9,998.96; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 275 | $4.81 | $3.60 | $-9.90 | $9,995.36 | ▼ -9.90 after sell → book $9,995.36; vs 09:30 mark -3.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,995.36 | ▲ close $9,995.36 vs 09:30 $10,013.23 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,995.36 | ▲ 09:30 equity $9,995.36 vs yday $9,995.36 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,995.36 | ▲ close $9,995.36 vs 09:30 $9,995.36 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,995.36 | ▲ 09:30 equity $9,995.36 vs yday $9,995.36 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,995.36 | ▲ close $9,995.36 vs 09:30 $9,995.36 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,995.36 | ▲ 09:30 equity $9,995.36 vs yday $9,995.36 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,938.14 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,963.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 38 | $32.31 | $2.10 | — | $6,733.64 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 78 | $15.87 | $2.22 | — | $5,493.56 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,249.65 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,544.41 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $2,304.74 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1249.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 37 | $32.88 | $2.10 | — | $1,086.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1249.42 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,086.08 | ▲ close $10,352.60 vs 09:30 $9,995.36 (session +373.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,086.08 | ▼ 09:30 equity $10,276.99 vs yday $10,352.60 (-75.61) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,163.16 | ▲ +19.86 after sell → book $10,274.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,188.70 | ▲ +50.93 after sell → book $10,272.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 38 | $33.46 | $2.12 | $+39.47 | $4,458.06 | ▲ +39.47 after sell → book $10,270.83; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $5,695.57 | ▼ -6.39 after sell → book $10,268.66; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,385.59 | ▼ -15.23 after sell → book $10,266.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $7,783.60 | ▲ +158.34 after sell → book $10,264.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 37 | $32.48 | $2.12 | $-19.02 | $8,983.24 | ▼ -19.02 after sell → book $10,262.44; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,401.07 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1796.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 23 | $75.65 | $2.06 | — | $5,659.06 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1796.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $3,999.31 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+8.1; leftover $1796.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 926 | $1.94 | $11.95 | — | $2,190.93 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $1796.65 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $403.35 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+5.4; leftover $1796.65 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $403.35 | ▲ close $10,407.75 vs 09:30 $10,276.99 (session +165.36) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $403.35 | ▲ 09:30 equity $10,504.53 vs yday $10,407.75 (+96.78) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+63.39 | $1,706.82 | ▲ +63.39 after sell → book $10,502.28; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,227.11 | ▼ -61.88 after sell → book $10,500.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $5,099.39 | ▲ +212.53 after sell → book $10,498.21; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 926 | $1.94 | $12.11 | $-24.06 | $6,883.72 | ▼ -24.06 after sell → book $10,486.10; vs 09:30 mark -12.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,883.72 | ▼ close $10,422.81 vs 09:30 $10,504.53 (session -63.29) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,883.72 | ▲ 09:30 equity $10,489.18 vs yday $10,422.81 (+66.37) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 23 | $76.60 | $2.08 | $+17.71 | $8,643.44 | ▲ +17.71 after sell → book $10,487.10; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $10,485.04 | ▲ +54.03 after sell → book $10,485.04; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,485.04 | ▲ close $10,485.04 vs 09:30 $10,489.18 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,485.04 | ▲ 09:30 equity $10,485.04 vs yday $10,485.04 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,485.04 | ▲ close $10,485.04 vs 09:30 $10,485.04 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,485.04 | ▲ 09:30 equity $10,485.04 vs yday $10,485.04 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $8,838.72 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1747.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 7 | $242.17 | $2.01 | — | $7,141.52 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-11.1; leftover $1747.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 116 | $15.01 | $2.34 | — | $5,398.02 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1747.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 824 | $2.12 | $10.63 | — | $3,640.51 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1747.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 856 | $2.04 | $11.04 | — | $1,883.23 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1747.51 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 12 | $135.71 | $2.03 | — | $252.69 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; ret5=-9.2; leftover $1747.51 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $252.69 | ▼ close $10,282.38 vs 09:30 $10,485.04 (session -172.60) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $252.69 | ▼ 09:30 equity $10,208.94 vs yday $10,282.38 (-73.44) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 10 | $141.42 | $2.04 | $-234.16 | $1,664.84 | ▼ -234.16 after sell → book $10,206.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 7 | $261.51 | $2.04 | $+131.33 | $3,493.38 | ▲ +131.33 after sell → book $10,204.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 824 | $2.05 | $10.78 | $-79.09 | $5,171.80 | ▼ -79.09 after sell → book $10,194.08; vs 09:30 mark -10.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 856 | $2.01 | $11.20 | $-47.92 | $6,881.16 | ▼ -47.92 after sell → book $10,182.88; vs 09:30 mark -11.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 12 | $131.40 | $2.05 | $-55.79 | $8,455.91 | ▼ -55.79 after sell → book $10,180.83; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,455.91 | ▲ close $10,214.47 vs 09:30 $10,208.94 (session +33.64) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,455.91 | ▲ 09:30 equity $10,221.43 vs yday $10,214.47 (+6.96) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,455.91 | ▲ close $10,246.95 vs 09:30 $10,221.43 (session +25.52) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,455.91 | ▲ 09:30 equity $10,257.39 vs yday $10,246.95 (+10.44) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 80 | $26.27 | $2.23 | — | $6,352.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2113.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 11 | $189.17 | $2.02 | — | $4,269.19 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2113.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 52 | $39.99 | $2.15 | — | $2,187.56 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2113.98 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 304 | $6.95 | $3.92 | — | $70.84 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-5.8; leftover $2113.98 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.84 | ▼ close $10,166.52 vs 09:30 $10,257.39 (session -80.55) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.84 | ▲ 09:30 equity $10,283.17 vs yday $10,166.52 (+116.65) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 80 | $26.51 | $2.26 | $+14.71 | $2,189.38 | ▲ +14.71 after sell → book $10,280.91; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 11 | $190.35 | $2.05 | $+8.91 | $4,281.18 | ▲ +8.91 after sell → book $10,278.86; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 52 | $37.57 | $2.17 | $-130.16 | $6,232.65 | ▼ -130.16 after sell → book $10,276.69; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 304 | $7.27 | $3.99 | $+89.37 | $8,438.74 | ▲ +89.37 after sell → book $10,272.70; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $7,069.93 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1406.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 63 | $22.12 | $2.18 | — | $5,674.19 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1406.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $4,479.18 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-11.6; leftover $1406.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $3,542.31 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; ret5=-7.0; leftover $1406.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 79 | $17.72 | $2.23 | — | $2,140.20 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1406.46 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 794 | $1.77 | $10.24 | — | $724.58 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-10.2; leftover $1406.46 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $724.58 | ▲ close $10,284.41 vs 09:30 $10,283.17 (session +32.37) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $724.58 | ▲ 09:30 equity $10,325.15 vs yday $10,284.41 (+40.74) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 116 | $15.87 | $2.37 | $+95.05 | $2,563.13 | ▲ +95.05 after sell → book $10,322.78; vs 09:30 mark -2.37 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $4,019.73 | ▲ +87.79 after sell → book $10,320.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $5,201.71 | ▼ -13.03 after sell → book $10,318.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $6,115.35 | ▼ -23.23 after sell → book $10,316.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 79 | $17.13 | $2.25 | $-51.09 | $7,466.37 | ▼ -51.09 after sell → book $10,314.45; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 794 | $1.77 | $10.39 | $-20.63 | $8,861.37 | ▼ -20.63 after sell → book $10,304.07; vs 09:30 mark -10.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 105 | $20.91 | $2.31 | — | $6,663.51 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2215.34 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 149 | $14.79 | $2.44 | — | $4,457.36 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2215.34 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 157 | $14.07 | $2.46 | — | $2,245.91 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2215.34 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 294 | $7.54 | $3.79 | — | $26.83 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $2215.34 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.83 | ▼ close $10,130.51 vs 09:30 $10,325.15 (session -162.56) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.83 | ▲ 09:30 equity $10,253.78 vs yday $10,130.51 (+123.27) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 63 | $22.78 | $2.20 | $+37.20 | $1,459.77 | ▲ +37.20 after sell → book $10,251.58; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 105 | $21.65 | $2.34 | $+73.05 | $3,730.68 | ▲ +73.05 after sell → book $10,249.24; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 149 | $14.58 | $2.48 | $-36.21 | $5,900.62 | ▼ -36.21 after sell → book $10,246.76; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 157 | $13.90 | $2.50 | $-31.66 | $8,080.42 | ▼ -31.66 after sell → book $10,244.26; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 294 | $7.36 | $3.86 | $-59.10 | $10,240.40 | ▼ -59.10 after sell → book $10,240.40; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $8,626.64 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+12.5; leftover $1706.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 8 | $190.30 | $2.01 | — | $7,102.22 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+10.6; leftover $1706.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 65 | $25.95 | $2.19 | — | $5,413.29 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1706.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 122 | $13.94 | $2.36 | — | $3,710.25 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1706.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 284 | $6.00 | $3.66 | — | $2,002.59 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; ret5=-24.1; leftover $1706.73 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 793 | $2.15 | $10.23 | — | $287.41 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1706.73 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $287.41 | ▼ close $9,949.75 vs 09:30 $10,253.78 (session -268.19) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $287.41 | ▼ 09:30 equity $9,928.61 vs yday $9,949.75 (-21.14) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 122 | $13.13 | $2.39 | $-103.57 | $1,886.88 | ▼ -103.57 after sell → book $9,926.22; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 284 | $5.99 | $3.72 | $-10.23 | $3,584.31 | ▼ -10.23 after sell → book $9,922.49; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $3,076.82 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+17.9; leftover $597.39 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 591 | $1.01 | $7.62 | — | $2,472.28 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $597.39 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 138 | $4.30 | $2.40 | — | $1,876.48 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+16.9; leftover $597.39 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,876.48 | ▲ close $9,914.42 vs 09:30 $9,928.61 (session +3.95) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,876.48 | ▲ 09:30 equity $10,228.11 vs yday $9,914.42 (+313.69) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 7 | $266.50 | $2.04 | $+249.70 | $3,739.94 | ▲ +249.70 after sell → book $10,226.08; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 8 | $174.50 | $2.04 | $-130.45 | $5,133.91 | ▼ -130.45 after sell → book $10,224.04; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 65 | $26.58 | $2.21 | $+36.56 | $6,859.40 | ▲ +36.56 after sell → book $10,221.83; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 793 | $2.09 | $10.37 | $-68.18 | $8,506.39 | ▼ -68.18 after sell → book $10,211.46; vs 09:30 mark -10.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 3 | $183.41 | $2.02 | $+40.70 | $9,054.59 | ▲ +40.70 after sell → book $10,209.44; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 591 | $0.95 | $7.50 | $-50.58 | $9,608.54 | ▼ -50.58 after sell → book $10,201.94; vs 09:30 mark -7.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 8 | $196.78 | $2.01 | — | $8,032.29 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1601.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 201 | $7.95 | $2.60 | — | $6,431.74 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1601.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 101 | $15.72 | $2.29 | — | $4,841.73 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1601.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1231 | $1.30 | $15.88 | — | $3,225.55 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1601.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1312 | $1.22 | $16.92 | — | $1,607.98 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $1601.42 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 40 | $40.00 | $2.11 | — | $5.87 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $1601.42 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.87 | ▼ close $9,817.42 vs 09:30 $10,228.11 (session -342.70) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.87 | ▼ 09:30 equity $9,717.48 vs yday $9,817.42 (-99.94) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 138 | $4.12 | $2.44 | $-29.68 | $572.00 | ▼ -29.68 after sell → book $9,715.05; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 8 | $192.26 | $2.04 | $-40.21 | $2,108.04 | ▼ -40.21 after sell → book $9,713.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 201 | $7.38 | $2.64 | $-119.81 | $3,588.78 | ▼ -119.81 after sell → book $9,710.37; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 101 | $14.38 | $2.32 | $-139.95 | $5,038.84 | ▼ -139.95 after sell → book $9,708.05; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1231 | $1.27 | $16.10 | $-68.91 | $6,586.11 | ▼ -68.91 after sell → book $9,691.95; vs 09:30 mark -16.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1312 | $1.17 | $17.15 | $-99.68 | $8,103.99 | ▼ -99.68 after sell → book $9,674.79; vs 09:30 mark -17.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 40 | $39.27 | $2.13 | $-33.44 | $9,672.66 | ▼ -33.44 after sell → book $9,672.66; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.66 | ▲ close $9,672.66 vs 09:30 $9,717.48 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,577.47 | ▲ 09:30 equity $8,577.47 vs yday $8,577.47 (+0.00) | 09:30 open · cash $8,577.47 · no holdings · equity $8,577.47 vs prior close $8,577.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 444 | $3.86 | $5.73 | — | $6,857.90 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $5,222.93 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 105 | $16.21 | $2.31 | — | $3,518.58 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,629.59 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1715.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 23 | $74.15 | $2.06 | — | $922.08 | — | packet🟢 OR headline🟢; gate news_or_headline=True; rank cond; list ohlc_hot; ret5=+8.5; leftover $1715.49 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
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
| 2026-08-27 | `ASML` | cash | leftover split 1255.91 < 1 share @ 1746.53 |
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
