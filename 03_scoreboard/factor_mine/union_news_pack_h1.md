# Factor mine action — `union_news_pack_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **+5.08%** ($10,508) · signal-only (no cash/fees) was +6.91%. Starts YES **21/30**. Fills 117 · skips 29 · realized $+1396.12.

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
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $728.25 | ▲ 09:30 equity $9,052.12 vs yday $9,033.14 (+18.99) | 09:30 open · cash $728.25 (unchanged overnight, no fees) · equity $9,052.12 vs prior close $9,033.14 (+18.99) · 1 name(s) re-marked at the open (per-name table). COST×9 yday $922.76 → 09:30 $924.88 +18.99 | — |
| 2026-09-28 09:30 ET | **SELL** | `COST` | 9 | $924.88 | $2.09 | $+336.76 | $9,050.03 | ▲ +336.76 after sell → book $9,050.03; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,050.03 | ▲ close $9,050.03 vs 09:30 $9,052.12 (session +0.00) | 16:00 close · cash $9,050.03 · no lots left · equity $9,050.03. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,050.03 | ▲ 09:30 equity $9,050.03 vs yday $9,050.03 (+0.00) | 09:30 open · cash $9,050.03 · no holdings · equity $9,050.03 vs prior close $9,050.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,050.03 | ▲ close $9,050.03 vs 09:30 $9,050.03 (session +0.00) | 16:00 close · cash $9,050.03 · no lots left · equity $9,050.03. | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,050.03 | ▲ 09:30 equity $9,050.03 vs yday $9,050.03 (+0.00) | 09:30 open · cash $9,050.03 · no holdings · equity $9,050.03 vs prior close $9,050.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,050.03 | ▲ close $9,050.03 vs 09:30 $9,050.03 (session +0.00) | 16:00 close · cash $9,050.03 · no lots left · equity $9,050.03. | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,050.03 | ▲ 09:30 equity $9,050.03 vs yday $9,050.03 (+0.00) | 09:30 open · cash $9,050.03 · no holdings · equity $9,050.03 vs prior close $9,050.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-01 09:30 ET | **BUY** | `MU` | 8 | $1054.08 | $2.01 | — | $615.38 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-0.6; leftover $9050.03 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $615.38 | ▲ close $9,394.50 vs 09:30 $9,050.03 (session +346.48) | 16:00 close · cash $615.38 · equity $9,394.50 vs 09:30 $9,050.03 (+344.47; session marks +346.48) · 1 name(s) marked open→close (per-name table). MU×8 09:30 $1054.08 → close $1097.39 +346.48 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $615.38 | ▲ 09:30 equity $9,474.98 vs yday $9,394.50 (+80.48) | 09:30 open · cash $615.38 (unchanged overnight, no fees) · equity $9,474.98 vs prior close $9,394.50 (+80.48) · 1 name(s) re-marked at the open (per-name table). MU×8 yday $1097.39 → 09:30 $1107.45 +80.48 | — |
| 2026-10-02 09:30 ET | **SELL** | `MU` | 8 | $1107.45 | $2.09 | $+422.85 | $9,472.89 | ▲ +422.85 after sell → book $9,472.89; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `SNPS` | 9 | $497.86 | $2.02 | — | $4,990.17 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; leftover $4736.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ACN` | 22 | $211.02 | $2.06 | — | $345.68 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer,yday_mover; ret5=+19.7; leftover $4736.44 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $345.68 | ▼ close $9,130.58 vs 09:30 $9,474.98 (session -338.24) | 16:00 close · cash $345.68 · equity $9,130.58 vs 09:30 $9,474.98 (-344.40; session marks -338.24) · 2 name(s) marked open→close (per-name table). SNPS×9 09:30 $497.86 → close $489.90 -71.60; ACN×22 09:30 $211.02 → close $198.90 -266.64 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $345.68 | ▼ 09:30 equity $9,130.53 vs yday $9,130.58 (-0.05) | 09:30 open · cash $345.68 (unchanged overnight, no fees) · equity $9,130.53 vs prior close $9,130.58 (-0.05) · 2 name(s) re-marked at the open (per-name table). ACN×22 yday $198.90 → 09:30 $196.30 -57.20; SNPS×9 yday $489.90 → 09:30 $496.25 +57.15 | — |
| 2026-10-05 09:30 ET | **SELL** | `ACN` | 22 | $196.30 | $2.10 | $-328.00 | $4,662.18 | ▼ -328.00 after sell → book $9,128.43; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SNPS` | 9 | $496.25 | $2.06 | $-18.52 | $9,126.37 | ▼ -18.52 after sell → book $9,126.37; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `COP` | 35 | $127.00 | $2.10 | — | $4,679.27 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=-0.4; leftover $4563.18 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 119 | $38.10 | $2.35 | — | $143.02 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=-1.0; leftover $4563.18 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.02 | ▲ close $9,238.75 vs 09:30 $9,130.53 (session +116.83) | 16:00 close · cash $143.02 · equity $9,238.75 vs 09:30 $9,130.53 (+108.22; session marks +116.83) · 2 name(s) marked open→close (per-name table). COP×35 09:30 $127.00 → close $128.40 +49.00; RRC×119 09:30 $38.10 → close $38.67 +67.83 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.02 | ▼ 09:30 equity $9,236.16 vs yday $9,238.75 (-2.59) | 09:30 open · cash $143.02 (unchanged overnight, no fees) · equity $9,236.16 vs prior close $9,238.75 (-2.59) · 2 name(s) re-marked at the open (per-name table). COP×35 yday $128.40 → 09:30 $128.19 -7.35; RRC×119 yday $38.67 → 09:30 $38.71 +4.76 | — |
| 2026-10-06 09:30 ET | **SELL** | `COP` | 35 | $128.19 | $2.14 | $+37.41 | $4,627.53 | ▲ +37.41 after sell → book $9,234.02; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 09:30 ET | **SELL** | `RRC` | 119 | $38.71 | $2.40 | $+67.84 | $9,231.62 | ▲ +67.84 after sell → book $9,231.62; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,231.62 | ▲ close $9,231.62 vs 09:30 $9,236.16 (session +0.00) | 16:00 close · cash $9,231.62 · no lots left · equity $9,231.62. | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,231.62 | ▲ 09:30 equity $9,231.62 vs yday $9,231.62 (+0.00) | 09:30 open · cash $9,231.62 · no holdings · equity $9,231.62 vs prior close $9,231.62 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,231.62 | ▲ close $9,231.62 vs 09:30 $9,231.62 (session +0.00) | 16:00 close · cash $9,231.62 · no lots left · equity $9,231.62. | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,231.62 | ▲ 09:30 equity $9,231.62 vs yday $9,231.62 (+0.00) | 09:30 open · cash $9,231.62 · no holdings · equity $9,231.62 vs prior close $9,231.62 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,231.62 | ▲ close $9,231.62 vs 09:30 $9,231.62 (session +0.00) | 16:00 close · cash $9,231.62 · no lots left · equity $9,231.62. | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,231.62 | ▲ 09:30 equity $9,231.62 vs yday $9,231.62 (+0.00) | 09:30 open · cash $9,231.62 · no holdings · equity $9,231.62 vs prior close $9,231.62 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-09 09:30 ET | **BUY** | `RRC` | 227 | $40.62 | $2.93 | — | $7.95 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+8.4; leftover $9231.62 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.95 | ▲ close $9,289.98 vs 09:30 $9,231.62 (session +61.29) | 16:00 close · cash $7.95 · equity $9,289.98 vs 09:30 $9,231.62 (+58.36; session marks +61.29) · 1 name(s) marked open→close (per-name table). RRC×227 09:30 $40.62 → close $40.89 +61.29 | — |
| 2026-10-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.95 | ▲ 09:30 equity $9,351.27 vs yday $9,289.98 (+61.29) | 09:30 open · cash $7.95 (unchanged overnight, no fees) · equity $9,351.27 vs prior close $9,289.98 (+61.29) · 1 name(s) re-marked at the open (per-name table). RRC×227 yday $40.89 → 09:30 $41.16 +61.29 | — |
| 2026-10-12 09:30 ET | **SELL** | `RRC` | 227 | $41.16 | $3.04 | $+116.61 | $9,348.23 | ▲ +116.61 after sell → book $9,348.23; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-12 09:30 ET | **BUY** | `AUGO` | 103 | $89.89 | $2.30 | — | $87.26 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer; 🔵; ret5=+11.4; leftover $9348.23 | join🟢 sector🔴 gen🔴 news🟢 digest🔴 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.26 | ▲ close $9,542.66 vs 09:30 $9,351.27 (session +196.73) | 16:00 close · cash $87.26 · equity $9,542.66 vs 09:30 $9,351.27 (+191.39; session marks +196.73) · 1 name(s) marked open→close (per-name table). AUGO×103 09:30 $89.89 → close $91.80 +196.73 | — |
| 2026-10-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.26 | ▲ 09:30 equity $9,555.02 vs yday $9,542.66 (+12.36) | 09:30 open · cash $87.26 (unchanged overnight, no fees) · equity $9,555.02 vs prior close $9,542.66 (+12.36) · 1 name(s) re-marked at the open (per-name table). AUGO×103 yday $91.80 → 09:30 $91.92 +12.36 | — |
| 2026-10-13 09:30 ET | **SELL** | `AUGO` | 103 | $91.92 | $2.39 | $+204.40 | $9,552.63 | ▲ +204.40 after sell → book $9,552.63; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-10-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,552.63 | ▲ close $9,552.63 vs 09:30 $9,555.02 (session +0.00) | 16:00 close · cash $9,552.63 · no lots left · equity $9,552.63. | — |
| 2026-10-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,552.63 | ▲ 09:30 equity $9,552.63 vs yday $9,552.63 (+0.00) | 09:30 open · cash $9,552.63 · no holdings · equity $9,552.63 vs prior close $9,552.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-14 09:30 ET | **BUY** | `AMX` | 220 | $21.62 | $2.84 | — | $4,793.39 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer; 🔵; ret5=-3.0; leftover $4776.31 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-14 09:30 ET | **BUY** | `BAC` | 95 | $49.83 | $2.27 | — | $57.27 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-0.2; leftover $4776.31 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.27 | ▲ close $9,744.12 vs 09:30 $9,552.63 (session +196.60) | 16:00 close · cash $57.27 · equity $9,744.12 vs 09:30 $9,552.63 (+191.49; session marks +196.60) · 2 name(s) marked open→close (per-name table). AMX×220 09:30 $21.62 → close $21.84 +48.40; BAC×95 09:30 $49.83 → close $51.39 +148.20 | — |
| 2026-10-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.27 | ▲ 09:30 equity $9,768.37 vs yday $9,744.12 (+24.25) | 09:30 open · cash $57.27 (unchanged overnight, no fees) · equity $9,768.37 vs prior close $9,744.12 (+24.25) · 2 name(s) re-marked at the open (per-name table). AMX×220 yday $21.84 → 09:30 $21.92 +17.60; BAC×95 yday $51.39 → 09:30 $51.46 +6.65 | — |
| 2026-10-15 09:30 ET | **SELL** | `BAC` | 95 | $51.46 | $2.33 | $+150.25 | $4,943.64 | ▲ +150.25 after sell → book $9,766.04; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-10-15 09:30 ET | **BUY** | `CAH` | 4 | $252.00 | $2.00 | — | $3,933.64 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+6.7; leftover $1235.91 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `COP` | 8 | $143.00 | $2.01 | — | $2,787.62 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+9.6; leftover $1235.91 | join🔴 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `RELX` | 32 | $37.98 | $2.09 | — | $1,570.02 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+7.0; leftover $1235.91 | join🔴 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `BNY` | 8 | $139.81 | $2.01 | — | $449.52 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-3.6; leftover $1235.91 | join🟢 sector🟡 gen🔴 news🟢 digest🟡 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $449.52 | ▲ close $9,898.96 vs 09:30 $9,768.37 (session +141.04) | 16:00 close · cash $449.52 · equity $9,898.96 vs 09:30 $9,768.37 (+130.59; session marks +141.04) · 5 name(s) marked open→close (per-name table). AMX×220 09:30 $21.92 → close $22.43 +112.20; CAH×4 09:30 $252.00 → close $253.71 +6.84; COP×8 09:30 $143.00 → close $141.73 -10.16; RELX×32 09:30 $37.98 → close $38.42 +13.92; BNY×8 09:30 $139.81 → close $142.09 +18.24 | — |
| 2026-10-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $449.52 | ▲ 09:30 equity $9,996.28 vs yday $9,898.96 (+97.32) | 09:30 open · cash $449.52 (unchanged overnight, no fees) · equity $9,996.28 vs prior close $9,898.96 (+97.32) · 5 name(s) re-marked at the open (per-name table). AMX×220 yday $22.43 → 09:30 $22.62 +41.80; BNY×8 yday $142.09 → 09:30 $143.24 +9.20; CAH×4 yday $253.71 → 09:30 $254.81 +4.40; COP×8 yday $141.73 → 09:30 $142.69 +7.68; RELX×32 yday $38.42 → 09:30 $39.49 +34.24 | — |
| 2026-10-16 09:30 ET | **SELL** | `BNY` | 8 | $143.24 | $2.03 | $+23.39 | $1,593.41 | ▲ +23.39 after sell → book $9,994.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `CAH` | 4 | $254.81 | $2.02 | $+7.22 | $2,610.62 | ▲ +7.22 after sell → book $9,992.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `COP` | 8 | $142.69 | $2.03 | $-6.53 | $3,750.11 | ▼ -6.53 after sell → book $9,990.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `RELX` | 32 | $39.49 | $2.11 | $+43.97 | $5,011.68 | ▲ +43.97 after sell → book $9,988.08; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **BUY** | `JBHT` | 21 | $228.00 | $2.05 | — | $221.63 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.0; leftover $5011.68 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $221.63 | ▲ close $10,066.21 vs 09:30 $9,996.28 (session +80.18) | 16:00 close · cash $221.63 · equity $10,066.21 vs 09:30 $9,996.28 (+69.93; session marks +80.18) · 2 name(s) marked open→close (per-name table). AMX×220 09:30 $22.62 → close $22.91 +63.80; JBHT×21 09:30 $228.00 → close $228.78 +16.38 | — |
| 2026-10-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $221.63 | ▼ 09:30 equity $10,019.73 vs yday $10,066.21 (-46.48) | 09:30 open · cash $221.63 (unchanged overnight, no fees) · equity $10,019.73 vs prior close $10,066.21 (-46.48) · 2 name(s) re-marked at the open (per-name table). AMX×220 yday $22.91 → 09:30 $22.84 -15.40; JBHT×21 yday $228.78 → 09:30 $227.30 -31.08 | — |
| 2026-10-19 09:30 ET | **SELL** | `JBHT` | 21 | $227.30 | $2.10 | $-18.85 | $4,992.83 | ▼ -18.85 after sell → book $10,017.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 09:30 ET | **BUY** | `EOG` | 6 | $157.52 | $2.01 | — | $4,045.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+7.3; leftover $998.57 | join🟡 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-19 09:30 ET | **BUY** | `COP` | 6 | $145.73 | $2.01 | — | $3,169.31 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $998.57 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-19 09:30 ET | **BUY** | `RELX` | 25 | $38.96 | $2.06 | — | $2,193.25 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+8.1; leftover $998.57 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-19 09:30 ET | **BUY** | `CHRD` | 6 | $152.81 | $2.01 | — | $1,274.41 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.2; leftover $998.57 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-19 09:30 ET | **BUY** | `CNQ` | 18 | $53.51 | $2.04 | — | $309.19 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+8.4; leftover $998.57 | join🟡 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $309.19 | ▲ close $10,031.42 vs 09:30 $10,019.73 (session +23.92) | 16:00 close · cash $309.19 · equity $10,031.42 vs 09:30 $10,019.73 (+11.69; session marks +23.92) · 6 name(s) marked open→close (per-name table). AMX×220 09:30 $22.84 → close $22.83 -2.20; EOG×6 09:30 $157.52 → close $157.65 +0.78; COP×6 09:30 $145.73 → close $145.24 -2.94; RELX×25 09:30 $38.96 → close $39.25 +7.25; CHRD×6 09:30 $152.81 → close $154.48 +10.05; CNQ×18 09:30 $53.51 → close $54.12 +10.98 | — |
| 2026-10-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $309.19 | ▲ 09:30 equity $10,100.59 vs yday $10,031.42 (+69.17) | 09:30 open · cash $309.19 (unchanged overnight, no fees) · equity $10,100.59 vs prior close $10,031.42 (+69.17) · 6 name(s) re-marked at the open (per-name table). AMX×220 yday $22.83 → 09:30 $22.93 +22.00; CHRD×6 yday $154.48 → 09:30 $156.31 +10.98; CNQ×18 yday $54.12 → 09:30 $55.00 +15.84; COP×6 yday $145.24 → 09:30 $147.15 +11.46; EOG×6 yday $157.65 → 09:30 $159.86 +13.26; RELX×25 yday $39.25 → 09:30 $39.08 -4.37 | — |
| 2026-10-20 09:30 ET | **SELL** | `CHRD` | 6 | $156.31 | $2.03 | $+16.99 | $1,245.02 | ▲ +16.99 after sell → book $10,098.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-20 09:30 ET | **SELL** | `COP` | 6 | $147.15 | $2.03 | $+4.48 | $2,125.89 | ▲ +4.48 after sell → book $10,096.53; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-20 09:30 ET | **SELL** | `EOG` | 6 | $159.86 | $2.03 | $+10.00 | $3,083.03 | ▲ +10.00 after sell → book $10,094.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-20 09:30 ET | **SELL** | `RELX` | 25 | $39.08 | $2.08 | $-1.27 | $4,057.82 | ▼ -1.27 after sell → book $10,092.42; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-10-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,057.82 | ▼ close $10,071.06 vs 09:30 $10,100.59 (session -21.36) | 16:00 close · cash $4,057.82 · equity $10,071.06 vs 09:30 $10,100.59 (-29.53; session marks -21.36) · 2 name(s) marked open→close (per-name table). AMX×220 09:30 $22.93 → close $22.90 -6.60; CNQ×18 09:30 $55.00 → close $54.18 -14.76 | — |
| 2026-10-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,057.82 | ▲ 09:30 equity $10,133.82 vs yday $10,071.06 (+62.76) | 09:30 open · cash $4,057.82 (unchanged overnight, no fees) · equity $10,133.82 vs prior close $10,071.06 (+62.76) · 2 name(s) re-marked at the open (per-name table). AMX×220 yday $22.90 → 09:30 $23.20 +66.00; CNQ×18 yday $54.18 → 09:30 $54.00 -3.24 | — |
| 2026-10-21 09:30 ET | **SELL** | `CNQ` | 18 | $54.00 | $2.06 | $+4.71 | $5,027.76 | ▲ +4.71 after sell → book $10,131.76; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-10-21 09:30 ET | **BUY** | `HLT` | 3 | $360.00 | $2.00 | — | $3,945.76 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+9.0; leftover $1256.94 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `SAP` | 5 | $231.57 | $2.00 | — | $2,785.90 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list overnight,overnight_mega; ret5=+4.2; leftover $1256.94 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `CSX` | 26 | $48.03 | $2.07 | — | $1,535.05 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list overnight,overnight_mega; ret5=+0.7; leftover $1256.94 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `AIP` | 61 | $20.38 | $2.17 | — | $289.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.6; leftover $1256.94 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $289.70 | ▲ close $10,419.03 vs 09:30 $10,133.82 (session +295.52) | 16:00 close · cash $289.70 · equity $10,419.03 vs 09:30 $10,133.82 (+285.21; session marks +295.52) · 5 name(s) marked open→close (per-name table). AMX×220 09:30 $23.20 → close $24.29 +239.80; HLT×3 09:30 $360.00 → close $362.25 +6.75; SAP×5 09:30 $231.57 → close $232.73 +5.80; CSX×26 09:30 $48.03 → close $48.40 +9.62; AIP×61 09:30 $20.38 → close $20.93 +33.55 | — |
| 2026-10-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $289.70 | ▼ 09:30 equity $10,397.98 vs yday $10,419.03 (-21.05) | 09:30 open · cash $289.70 (unchanged overnight, no fees) · equity $10,397.98 vs prior close $10,419.03 (-21.05) · 5 name(s) re-marked at the open (per-name table). AIP×61 yday $20.93 → 09:30 $20.12 -49.41; AMX×220 yday $24.29 → 09:30 $24.14 -33.00; CSX×26 yday $48.40 → 09:30 $50.09 +43.94; HLT×3 yday $362.25 → 09:30 $360.84 -4.23; SAP×5 yday $232.73 → 09:30 $237.06 +21.65 | — |
| 2026-10-22 09:30 ET | **SELL** | `AIP` | 61 | $20.12 | $2.19 | $-20.23 | $1,514.83 | ▼ -20.23 after sell → book $10,395.79; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,514.83 | ▲ close $10,631.01 vs 09:30 $10,397.98 (session +235.22) | 16:00 close · cash $1,514.83 · equity $10,631.01 vs 09:30 $10,397.98 (+233.03; session marks +235.22) · 4 name(s) marked open→close (per-name table). AMX×220 09:30 $24.14 → close $24.98 +184.80; CSX×26 09:30 $50.09 → close $51.67 +41.08; HLT×3 09:30 $360.84 → close $358.07 -8.31; SAP×5 09:30 $237.06 → close $240.59 +17.65 | — |
| 2026-10-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,514.83 | ▼ 09:30 equity $10,566.92 vs yday $10,631.01 (-64.09) | 09:30 open · cash $1,514.83 (unchanged overnight, no fees) · equity $10,566.92 vs prior close $10,631.01 (-64.09) · 4 name(s) re-marked at the open (per-name table). AMX×220 yday $24.98 → 09:30 $24.81 -37.40; CSX×26 yday $51.67 → 09:30 $51.23 -11.44; HLT×3 yday $358.07 → 09:30 $354.57 -10.50; SAP×5 yday $240.59 → 09:30 $239.64 -4.75 | — |
| 2026-10-23 09:30 ET | **SELL** | `CSX` | 26 | $51.23 | $2.09 | $+79.04 | $2,844.72 | ▲ +79.04 after sell → book $10,564.83; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-10-23 09:30 ET | **SELL** | `HLT` | 3 | $354.57 | $2.02 | $-20.31 | $3,906.41 | ▼ -20.31 after sell → book $10,562.81; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-10-23 09:30 ET | **SELL** | `SAP` | 5 | $239.64 | $2.02 | $+36.32 | $5,102.59 | ▲ +36.32 after sell → book $10,560.79; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-10-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,102.59 | ▼ close $10,507.99 vs 09:30 $10,566.92 (session -52.80) | 16:00 close · cash $5,102.59 · equity $10,507.99 vs 09:30 $10,566.92 (-58.93; session marks -52.80) · 1 name(s) marked open→close (per-name table). AMX×220 09:30 $24.81 → close $24.57 -52.80 | — |

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
