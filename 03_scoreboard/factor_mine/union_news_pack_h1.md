# Factor mine action — `union_news_pack_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **-9.67%** ($9,033) · signal-only (no cash/fees) was +6.91%. Starts YES **21/30**. Fills 71 · skips 29 · realized $+1396.12.

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
- Must-have: the morning news packet box is green.
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
- **Gate** `news_box=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,396.10.

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
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 27 | $127.40 | $2.11 | $+195.62 | $3,721.28 | ▲ +195.62 after sell → book $10,318.34; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 9 | $367.88 | $2.05 | $+68.38 | $7,030.14 | ▲ +68.38 after sell → book $10,316.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 22 | $149.37 | $2.09 | $+50.19 | $10,314.19 | ▲ +50.19 after sell → book $10,314.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 74 | $46.18 | $2.21 | — | $6,894.66 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3438.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $3,466.12 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3438.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $220.88 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3438.06 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.88 | ▲ close $10,549.30 vs 09:30 $10,320.45 (session +241.42) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.88 | ▲ 09:30 equity $10,668.72 vs yday $10,549.30 (+119.42) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 74 | $48.00 | $2.25 | $+130.22 | $3,770.63 | ▲ +130.22 after sell → book $10,666.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $7,321.49 | ▲ +122.32 after sell → book $10,664.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,662.29 | ▲ +95.57 after sell → book $10,662.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,668.72 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,662.29 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 238 | $44.76 | $3.07 | — | $6.34 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $10662.29 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.34 | ▼ close $10,571.16 vs 09:30 $10,662.29 (session -88.06) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.34 | ▲ 09:30 equity $10,602.10 vs yday $10,571.16 (+30.94) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 238 | $44.52 | $3.19 | $-63.38 | $10,598.91 | ▼ -63.38 after sell → book $10,598.91; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 44 | $119.43 | $2.12 | — | $5,341.87 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $5299.45 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 8 | $623.26 | $2.01 | — | $353.77 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $5299.45 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $353.77 | ▲ close $10,867.21 vs 09:30 $10,602.10 (session +272.44) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $353.77 | ▲ 09:30 equity $10,880.53 vs yday $10,867.21 (+13.32) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 44 | $120.51 | $2.17 | $+43.22 | $5,654.04 | ▲ +43.22 after sell → book $10,878.36; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 8 | $653.04 | $2.07 | $+234.16 | $10,876.29 | ▲ +234.16 after sell → book $10,876.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,876.29 | ▲ close $10,876.29 vs 09:30 $10,880.53 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,876.29 | ▲ 09:30 equity $10,876.29 vs yday $10,876.29 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 45 | $118.52 | $2.12 | — | $5,540.77 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5438.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 70 | $77.13 | $2.20 | — | $139.47 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5438.15 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.47 | ▲ close $11,285.72 vs 09:30 $10,876.29 (session +413.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.47 | ▼ 09:30 equity $11,084.27 vs yday $11,285.72 (-201.45) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 45 | $119.80 | $2.18 | $+53.30 | $5,528.29 | ▲ +53.30 after sell → book $11,082.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 70 | $79.34 | $2.26 | $+150.24 | $11,079.83 | ▲ +150.24 after sell → book $11,079.83; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 20 | $267.02 | $2.05 | — | $5,737.38 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5539.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 46 | $118.50 | $2.13 | — | $284.25 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5539.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $284.25 | ▼ close $11,068.85 vs 09:30 $11,084.27 (session -6.80) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $284.25 | ▲ 09:30 equity $11,092.27 vs yday $11,068.85 (+23.42) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 20 | $267.23 | $2.10 | $+0.05 | $5,626.75 | ▲ +0.05 after sell → book $11,090.17; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 9 | $81.65 | $2.02 | — | $4,889.88 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $4,250.13 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $3,579.55 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 3 | $261.47 | $2.00 | — | $2,793.14 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 11 | $70.30 | $2.02 | — | $2,017.82 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $803.82 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,017.82 | ▼ close $10,893.83 vs 09:30 $11,092.27 (session -186.31) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,017.82 | ▼ 09:30 equity $10,871.42 vs yday $10,893.83 (-22.41) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 46 | $115.66 | $2.18 | $-134.95 | $7,336.00 | ▼ -134.95 after sell → book $10,869.24; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 9 | $79.27 | $2.04 | $-25.47 | $8,047.39 | ▼ -25.47 after sell → book $10,867.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $8,681.43 | ▼ -5.71 after sell → book $10,865.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,361.49 | ▲ +9.48 after sell → book $10,863.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 11 | $65.29 | $2.04 | $-59.18 | $10,077.64 | ▼ -59.18 after sell → book $10,861.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $8,129.17 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2015.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $6,125.07 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2015.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,817.05 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2015.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $2,893.27 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $2015.53 | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 48 | $41.74 | $2.13 | — | $887.62 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+2.4; leftover $2015.53 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $887.62 | ▼ close $10,623.80 vs 09:30 $10,871.42 (session -227.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $887.62 | ▲ 09:30 equity $10,637.51 vs yday $10,623.80 (+13.71) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 3 | $257.71 | $2.02 | $-15.30 | $1,658.73 | ▼ -15.30 after sell → book $10,635.49; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 6 | $322.49 | $2.03 | $-15.56 | $3,591.63 | ▼ -15.56 after sell → book $10,633.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $5,481.80 | ▼ -113.94 after sell → book $10,631.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,741.69 | ▼ -48.14 after sell → book $10,629.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 8 | $233.97 | $2.04 | $-54.09 | $8,611.37 | ▼ -54.09 after sell → book $10,627.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 48 | $42.00 | $2.16 | $+8.19 | $10,625.21 | ▲ +8.19 after sell → book $10,625.21; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,625.21 | ▲ close $10,625.21 vs 09:30 $10,637.51 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,625.21 | ▲ 09:30 equity $10,625.21 vs yday $10,625.21 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,625.21 | ▲ close $10,625.21 vs 09:30 $10,625.21 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,625.21 | ▲ 09:30 equity $10,625.21 vs yday $10,625.21 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,625.21 | ▲ close $10,625.21 vs 09:30 $10,625.21 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,625.21 | ▲ 09:30 equity $10,625.21 vs yday $10,625.21 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 7 | $351.74 | $2.01 | — | $8,161.02 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2656.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $5,727.47 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2656.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 55 | $47.60 | $2.15 | — | $3,107.31 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2656.30 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 7 | $354.49 | $2.01 | — | $623.87 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $2656.30 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $623.87 | ▲ close $10,922.36 vs 09:30 $10,625.21 (session +305.33) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $623.87 | ▲ 09:30 equity $10,924.11 vs yday $10,922.36 (+1.75) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 7 | $359.70 | $2.04 | $+51.67 | $3,139.73 | ▲ +51.67 after sell → book $10,922.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 5 | $513.78 | $2.04 | $+133.31 | $5,706.59 | ▲ +133.31 after sell → book $10,920.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 55 | $53.85 | $2.19 | $+339.41 | $8,666.16 | ▲ +339.41 after sell → book $10,917.85; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 7 | $321.67 | $2.04 | $-233.79 | $10,915.81 | ▼ -233.79 after sell → book $10,915.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 13 | $263.36 | $2.03 | — | $7,490.10 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3638.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 15 | $236.82 | $2.04 | — | $3,935.76 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $3638.60 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 26 | $137.35 | $2.07 | — | $362.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $3638.60 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $362.59 | ▲ close $11,238.43 vs 09:30 $10,924.11 (session +328.76) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $362.59 | ▲ 09:30 equity $11,255.47 vs yday $11,238.43 (+17.04) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 13 | $253.72 | $2.07 | $-129.41 | $3,658.89 | ▼ -129.41 after sell → book $11,253.41; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 15 | $267.76 | $2.08 | $+459.99 | $7,673.21 | ▲ +459.99 after sell → book $11,251.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,673.21 | ▼ close $11,222.73 vs 09:30 $11,255.47 (session -28.60) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,673.21 | ▲ 09:30 equity $11,360.53 vs yday $11,222.73 (+137.80) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 26 | $141.82 | $2.11 | $+112.04 | $11,358.42 | ▲ +112.04 after sell → book $11,358.42; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,358.42 | ▲ close $11,358.42 vs 09:30 $11,360.53 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,358.42 | ▲ 09:30 equity $11,358.42 vs yday $11,358.42 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,358.42 | ▲ close $11,358.42 vs 09:30 $11,358.42 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,358.42 | ▲ 09:30 equity $11,358.42 vs yday $11,358.42 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 46 | $242.17 | $2.13 | — | $216.48 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $11358.42 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.48 | ▲ close $11,819.06 vs 09:30 $11,358.42 (session +462.76) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.48 | ▲ 09:30 equity $12,245.94 vs yday $11,819.06 (+426.88) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 46 | $261.51 | $2.23 | $+885.28 | $12,243.70 | ▲ +885.28 after sell → book $12,243.70; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,243.70 | ▲ close $12,243.70 vs 09:30 $12,245.94 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,243.70 | ▲ 09:30 equity $12,243.70 vs yday $12,243.70 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,243.70 | ▲ close $12,243.70 vs 09:30 $12,243.70 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,243.70 | ▲ 09:30 equity $12,243.70 vs yday $12,243.70 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 32 | $189.17 | $2.09 | — | $6,188.18 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $6121.85 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 153 | $39.99 | $2.45 | — | $67.26 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $6121.85 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.26 | ▼ close $11,820.62 vs 09:30 $12,243.70 (session -418.55) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.26 | ▲ 09:30 equity $11,906.67 vs yday $11,820.62 (+86.05) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 32 | $190.35 | $2.14 | $+33.53 | $6,156.31 | ▲ +33.53 after sell → book $11,904.52; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 153 | $37.57 | $2.52 | $-375.23 | $11,902.00 | ▼ -375.23 after sell → book $11,902.00; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 12 | $934.88 | $2.03 | — | $681.42 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer; ret5=-7.0; leftover $11902.00 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $681.42 | ▼ close $11,404.74 vs 09:30 $11,906.67 (session -495.24) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $681.42 | ▲ 09:30 equity $11,669.34 vs yday $11,404.74 (+264.60) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 12 | $915.66 | $2.12 | $-234.79 | $11,667.21 | ▼ -234.79 after sell → book $11,667.21; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,667.21 | ▲ close $11,667.21 vs 09:30 $11,669.34 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,667.21 | ▲ 09:30 equity $11,667.21 vs yday $11,667.21 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,667.21 | ▲ close $11,667.21 vs 09:30 $11,667.21 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,667.21 | ▲ 09:30 equity $11,667.21 vs yday $11,667.21 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,667.21 | ▲ close $11,667.21 vs 09:30 $11,667.21 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,667.21 | ▲ 09:30 equity $11,667.21 vs yday $11,667.21 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 59 | $196.78 | $2.17 | — | $55.02 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $11667.21 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.02 | ▼ close $11,381.25 vs 09:30 $11,667.21 (session -283.79) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.02 | ▲ 09:30 equity $11,398.36 vs yday $11,381.25 (+17.11) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 59 | $192.26 | $2.27 | $-271.11 | $11,396.10 | ▼ -271.11 after sell → book $11,396.10; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,396.10 | ▲ close $11,396.10 vs 09:30 $11,398.36 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,713.27 | ▲ 09:30 equity $8,713.27 vs yday $8,713.27 (+0.00) | 09:30 open · cash $8,713.27 · no holdings · equity $8,713.27 vs prior close $8,713.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 9 | $887.00 | $2.02 | — | $728.25 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $8713.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $728.25 | ▲ close $9,033.14 vs 09:30 $8,713.27 (session +321.88) | 16:00 close · cash $728.25 · equity $9,033.14 vs 09:30 $8,713.27 (+319.87; session marks +321.88) · 1 name(s) marked open→close (per-name table). COST×9 09:30 $887.00 → close $922.76 +321.88 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 803.82 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 803.82 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
